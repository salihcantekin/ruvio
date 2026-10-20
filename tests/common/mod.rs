//! Helpers shared by the live integration tests: a real `ruvio` process on a reserved port and a tiny RESP client.
#![allow(dead_code)]

#[path = "../../src/test_ports.rs"]
pub mod test_ports;

use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use test_ports::PortBlock;

pub struct Server {
    child: Option<Child>,
    pub address: String,
    pub data_dir: PathBuf,
    config: PathBuf,
    log: PathBuf,
    env: Vec<(String, String)>,
    /// Held for the server's whole life, restarts included, so no other test takes its ports
    /// while it is down.
    ports: PortBlock,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Server {
    pub fn start() -> Self {
        Self::start_with(&[])
    }

    /// `env` entries are passed as `RUVIO_*` settings (for example `("RUVIO_DURABILITY", "always")`).
    pub fn start_with(env: &[(&str, &str)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        // The template is copied, so a test that writes the config file leaves the original alone.
        let config = dir.path().join("ruvio.toml");

        std::fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/ruvio.example.toml"),
            &config,
        )
        .unwrap();

        let ports = test_ports::reserve();
        let mut server = Self {
            child: None,
            address: ports.address_at(0).to_string(),
            data_dir: dir.path().join("data"),
            config,
            log: dir.path().join("server.log"),
            env: env
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            ports,
            _dir: dir,
        };

        // The block is reserved against every test, but a program outside the tests may still
        // take one of its ports; then this server exits or a stranger answers, and another
        // block is tried.
        for _ in 0..3 {
            server.spawn();
            if server.wait_ready() {
                return server;
            }
            server.stop();
            server.ports = test_ports::reserve();
            server.address = server.ports.address_at(0).to_string();
        }

        panic!(
            "server did not start on any of three port blocks:\n{}",
            server.log_text()
        );
    }

    fn spawn(&mut self) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ruvio"));
        // Appended across restarts, so a failure shows every run of this server.
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .unwrap();

        command
            .arg("--config")
            .arg(&self.config)
            .args(["--address", &self.address, "--data-dir"])
            .arg(&self.data_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log);

        for (key, value) in &self.env {
            command.env(key, value);
        }

        // Not while this process probes ports: see `test_ports::spawn_lock`.
        let spawning = test_ports::spawn_lock();

        self.child = Some(command.spawn().unwrap());
        drop(spawning);
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Waits until this server answers on its address. False when the process exited first,
    /// or when another process answers there.
    fn wait_ready(&mut self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(20);

        loop {
            let child = self.child.as_mut().expect("spawned");

            if child.try_wait().unwrap().is_some() {
                return false;
            }
            // A reset, a timeout or an error reply means whoever answered is not ready or
            // went away mid-probe (a stranger that was stopped, for example); look again.
            if let Ok(ours) = self.answers_as_this_server() {
                return ours;
            }
            assert!(
                Instant::now() < deadline,
                "server did not start:\n{}",
                self.log_text()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn answers_as_this_server(&self) -> std::io::Result<bool> {
        let password = self
            .env
            .iter()
            .find(|(key, _)| key == "RUVIO_REQUIREPASS")
            .map(|(_, password)| password.as_str());

        answers_as(&self.address, &self.data_dir, password)
    }

    /// Kills the process without a clean shutdown (like a crash) and waits until it is gone.
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Restarts and expects the process to refuse to start (to exit on its own); returns its
    /// log. Waits for the exit rather than for an answer: a shard that recovered first may
    /// answer before another one refuses and takes the process down.
    pub fn restart_refused(&mut self) -> String {
        self.stop();
        self.wait_for_data_directory_lock();
        self.spawn();
        let deadline = Instant::now() + Duration::from_secs(20);

        while self
            .child
            .as_mut()
            .expect("spawned")
            .try_wait()
            .unwrap()
            .is_none()
        {
            assert!(
                Instant::now() < deadline,
                "server kept running on {}:\n{}",
                self.address,
                self.log_text()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        self.stop();
        self.log_text()
    }

    /// Sets a `RUVIO_*` setting for the next start (see `restart`).
    pub fn set_env(&mut self, key: &str, value: &str) {
        self.env.retain(|(known, _)| known != key);
        self.env.push((key.to_owned(), value.to_owned()));
    }

    /// Starts the same server again on the same address and data directory.
    pub fn restart(&mut self) {
        self.stop();
        self.wait_for_data_directory_lock();
        self.spawn();
        assert!(
            self.wait_ready(),
            "server did not restart on {}:\n{}",
            self.address,
            self.log_text()
        );
    }

    /// Windows releases the lock a killed process held on the data directory "after a delay
    /// that depends on system resources" (`LockFileEx`), so a server started at once may
    /// still find it taken. Waits until the lock can be taken, and lets it go again.
    ///
    /// Elsewhere the lock is gone with the process `stop` waited for. Probing it there would
    /// even add a race: a parallel test that spawns a server forks a copy of every open
    /// descriptor, the probe's included, which holds the lock until that child's `exec`.
    #[cfg(windows)]
    fn wait_for_data_directory_lock(&self) {
        let Ok(file) = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.data_dir.join("LOCK"))
        else {
            // No persistent data directory, so nothing to wait for.
            return;
        };
        let deadline = Instant::now() + Duration::from_secs(20);

        while file.try_lock().is_err() {
            assert!(
                Instant::now() < deadline,
                "the killed server's data directory lock was not released within 20 s"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(not(windows))]
    fn wait_for_data_directory_lock(&self) {}

    /// The reserved ports after the server's own, for tests that need more servers or peers.
    pub fn ports(&self) -> &PortBlock {
        &self.ports
    }
}

/// Whether the server on `address` uses `data_dir`, read from `CONFIG GET dir`. An error
/// reply (a server not ready yet, or one behind another password) proves nothing either way
/// and is an error, as is a broken connection, since the answering process may be a stranger
/// that is going away.
pub fn answers_as(address: &str, data_dir: &Path, password: Option<&str>) -> std::io::Result<bool> {
    let stream = TcpStream::connect(address)?;

    stream.set_read_timeout(Some(Duration::from_secs(10)))?;

    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let mut call = |args: &[&str]| -> std::io::Result<()> {
        let mut frame = format!("*{}\r\n", args.len());

        for arg in args {
            frame.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        writer.write_all(frame.as_bytes())
    };

    if let Some(password) = password {
        call(&["AUTH", password])?;
        read_line(&mut reader, &mut line)?;
    }

    call(&["CONFIG", "GET", "dir"])?;
    read_line(&mut reader, &mut line)?;

    if line.starts_with('-') {
        return Err(std::io::Error::other(format!(
            "CONFIG GET dir was refused: {}",
            line.trim_end()
        )));
    }

    // `*2`, then `dir` and the path as two bulk strings (four lines).
    let dir = data_dir.to_string_lossy();

    for _ in 0..4 {
        read_line(&mut reader, &mut line)?;
        if line.contains(&*dir) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Reads one line into `line`; a closed connection is an error, not an empty line.
fn read_line(reader: &mut BufReader<TcpStream>, line: &mut String) -> std::io::Result<()> {
    line.clear();
    if reader.read_line(line)? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    Ok(())
}

pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    pub fn connect(address: &str) -> Self {
        let stream = TcpStream::connect(address).unwrap();

        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();

        Self {
            writer: stream.try_clone().unwrap(),
            reader: BufReader::new(stream),
        }
    }

    pub fn call(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.read()
    }

    pub fn send(&mut self, args: &[&str]) {
        let mut frame = format!("*{}\r\n", args.len());

        for arg in args {
            frame.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }

        self.writer.write_all(frame.as_bytes()).unwrap();
    }

    /// Renders a reply as text: integers and simple strings as is, bulk as its payload, nil as
    /// `(nil)`, errors as `ERR:<message>`, arrays and pushes as `[a,b]`.
    pub fn read(&mut self) -> String {
        let mut line = String::new();

        self.reader.read_line(&mut line).unwrap();

        let line = line.trim_end().to_owned();
        let (kind, rest) = line.split_at(1);

        match kind {
            "+" | ":" => rest.to_owned(),
            "-" => format!("ERR:{rest}"),
            "_" => "(nil)".to_owned(),
            "$" => {
                let len: i64 = rest.parse().unwrap();

                if len < 0 {
                    return "(nil)".to_owned();
                }

                let mut payload = vec![0; len as usize + 2];

                self.reader.read_exact(&mut payload).unwrap();
                String::from_utf8_lossy(&payload[..len as usize]).into_owned()
            }
            "*" | ">" => {
                let count: i64 = rest.parse().unwrap();
                let items: Vec<String> = (0..count.max(0)).map(|_| self.read()).collect();

                format!("[{}]", items.join(","))
            }
            other => panic!("unexpected reply type {other}"),
        }
    }
}
