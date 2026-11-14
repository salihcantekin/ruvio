use std::{path::PathBuf, process};

use clap::{Parser, Subcommand};

use ruvio::persistence::{
    BackupSeal, IntegrityReport, PersistenceError, backup_data_dir_with, check_data_dir,
    restore_data_dir_with, verify_backup_signature,
};

#[derive(Debug, Parser)]
#[command(version, about = "Ruvio persistence recovery tooling")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate a data directory without truncating torn WAL tails.
    Check {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Read-only inspection of checkpoint, CLOCK, and WAL continuity.
    Inspect {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Copy CURRENT, CLOCK, checkpoints, and WAL to an empty directory.
    Backup {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        dest: PathBuf,
        /// One word recorded in SIGNATURE. Defaults to the USER environment variable.
        #[arg(long)]
        signer: Option<String>,
        /// File whose bytes key the backup digest. RUVIO_BACKUP_KEY is used when this is absent.
        #[arg(long)]
        sign_key: Option<PathBuf>,
    },
    /// Copy keys from a running Redis or Ruvio into another server with ordinary commands.
    Import {
        /// Source, for example redis://user:@host:6379/0. The password comes from
        /// RUVIO_IMPORT_SOURCE_PASSWORD (a password in the URL shows in the process list).
        #[arg(long)]
        from: String,
        /// Target, for example ruvio://host:6379/0. The password comes from
        /// RUVIO_IMPORT_TARGET_PASSWORD.
        #[arg(long)]
        to: String,
        /// Only keys matching this glob.
        #[arg(long, name = "MATCH")]
        r#match: Option<String>,
        /// Keys asked of SCAN per round.
        #[arg(long, default_value_t = 500)]
        batch: usize,
        /// Overwrite keys that already exist in the target; without it they are skipped.
        #[arg(long, default_value_t = false)]
        replace: bool,
        /// Scan and count only; the target is not written.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Read every copied key back from the target and compare it with the source.
        #[arg(long, default_value_t = false)]
        verify: bool,
    },
    /// Exit 0 when a server answers on the address; the container health check runs it.
    Ping {
        /// Defaults to RUVIO_ADDR with an unspecified host replaced by 127.0.0.1, then 127.0.0.1:6379.
        #[arg(long)]
        address: Option<String>,
        /// Only open the connection; for a server with TLS, which a plaintext PING cannot talk to.
        #[arg(long, default_value_t = false)]
        connect_only: bool,
    },
    /// Stop a running server with SHUTDOWN and wait until it answers again (a supervisor such
    /// as systemd or Docker has to start it). The server saves when it stops unless its
    /// durability is `memory`, which loses every key.
    Restart {
        /// Defaults to RUVIO_ADDR with an unspecified host replaced by 127.0.0.1, then 127.0.0.1:6379.
        #[arg(long)]
        address: Option<String>,
        /// Go ahead after the data-loss warning was read; without it nothing is stopped.
        #[arg(long, default_value_t = false)]
        yes: bool,
        /// Required as well under durability `memory`: every key is lost.
        #[arg(long, default_value_t = false)]
        allow_data_loss: bool,
        /// Seconds to wait for the server to stop and answer again.
        #[arg(long, default_value_t = 120)]
        wait_secs: u64,
    },
    /// Restart every node of one or more replication groups, one at a time, without losing an
    /// acknowledged write: the replicas first, then `COORDINATOR SWITCHOVER`, then the old
    /// primary. Needs `ruvio coordinator` and a supervisor that starts each stopped node again.
    /// The coordinator secret comes from RUVIO_COORDINATOR_SECRET, the nodes' password from
    /// RUVIO_PASSWORD (and RUVIO_USER).
    RollingRestart {
        /// Coordinator addresses (host:port), comma separated or repeated; the leader is found
        /// among them.
        #[arg(long, required = true, value_delimiter = ',')]
        coordinator: Vec<String>,
        /// Group to restart; repeat for several, in that order. Without it every group is
        /// restarted, one after the other.
        #[arg(long)]
        group: Vec<String>,
        /// A node (client host:port) to leave out, for one already restarted by an earlier run.
        #[arg(long)]
        skip: Vec<String>,
        /// Go ahead after the plan was read; without it the plan is printed and nothing is stopped.
        #[arg(long, default_value_t = false)]
        yes: bool,
        /// Seconds to wait for each node to stop and answer again, and for each step after it.
        #[arg(long, default_value_t = 180)]
        wait_secs: u64,
    },
    /// Restore a previously checked backup into a data directory.
    Restore {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long)]
        sign_key: Option<PathBuf>,
    },
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), PersistenceError> {
    match cli.command {
        Command::Ping {
            address,
            connect_only,
        } => ping(address, connect_only)?,
        Command::Restart {
            address,
            yes,
            allow_data_loss,
            wait_secs,
        } => restart(address, yes, allow_data_loss, wait_secs)?,
        Command::RollingRestart {
            coordinator,
            group,
            skip,
            yes,
            wait_secs,
        } => rolling_restart(&coordinator, &group, &skip, yes, wait_secs)?,
        Command::Check { data_dir } => {
            verify_backup_signature(&data_dir, &backup_key(None)?)?;
            print_report(&check_data_dir(data_dir)?, false);
        }

        Command::Inspect { data_dir } => {
            verify_backup_signature(&data_dir, &backup_key(None)?)?;
            print_report(&check_data_dir(data_dir)?, true);
        }

        Command::Backup {
            data_dir,
            dest,
            signer,
            sign_key,
        } => {
            let seal = BackupSeal {
                signer: signer.unwrap_or_else(default_signer),
                key: backup_key(sign_key)?,
            };

            print_report(&backup_data_dir_with(data_dir, &dest, &seal)?, false);
            println!(
                "signature signer={} mac={}",
                seal.signer,
                if seal.key.is_empty() { "no" } else { "yes" }
            );
        }

        Command::Import {
            from,
            to,
            r#match,
            batch,
            replace,
            dry_run,
            verify,
        } => {
            let options = ruvio::import::ImportOptions {
                from,
                to,
                pattern: r#match,
                batch,
                replace,
                dry_run,
                verify,
            };
            let report = ruvio::import::run(&options, &mut |report| {
                eprintln!("scanned={} copied={}", report.scanned, report.copied);
            })
            .map_err(PersistenceError::Failed)?;

            println!(
                "scanned={} copied={} skipped_existing={} skipped_staging={} vanished={} unsupported={}",
                report.scanned,
                report.copied,
                report.skipped_existing,
                report.skipped_staging,
                report.vanished,
                report.unsupported
            );

            for (name, count) in report.by_type {
                println!("type {name}={count}");
            }

            if verify {
                println!(
                    "verified={} mismatched={}",
                    report.verified, report.mismatched
                );
            }

            if dry_run {
                println!("dry run: the target was not written");
            }

            if report.mismatched != 0 {
                return Err(PersistenceError::Failed(format!(
                    "{} keys differ between source and target",
                    report.mismatched
                )));
            }
        }

        Command::Restore {
            source,
            data_dir,
            force,
            sign_key,
        } => {
            print_report(
                &restore_data_dir_with(source, data_dir, force, &backup_key(sign_key)?)?,
                false,
            );
        }
    }

    Ok(())
}

fn default_signer() -> String {
    std::env::var("USER")
        .ok()
        .filter(|name| !name.is_empty() && !name.bytes().any(|byte| byte.is_ascii_whitespace()))
        .unwrap_or_else(|| "ruvio-ctl".into())
}

fn backup_key(path: Option<PathBuf>) -> Result<Vec<u8>, PersistenceError> {
    if let Some(path) = path {
        return std::fs::read(path).map_err(PersistenceError::from);
    }

    Ok(std::env::var("RUVIO_BACKUP_KEY")
        .ok()
        .filter(|value| !value.is_empty())
        .map(String::into_bytes)
        .unwrap_or_default())
}

fn print_report(report: &IntegrityReport, inspect: bool) {
    println!("data_dir={}", report.data_dir.display());
    match report.clock_unix_millis {
        Some(clock) => println!("clock_unix_millis={clock}"),
        None => println!("clock_unix_millis=absent"),
    }

    match &report.checkpoint {
        Some(checkpoint) => {
            println!(
                "checkpoint generation={} created_at_unix_millis={} shards={}",
                checkpoint.generation, checkpoint.created_at_unix_millis, checkpoint.shard_count
            );
        }

        None => println!("checkpoint=absent"),
    }

    for shard in &report.shards {
        println!(
            "shard {} snapshot_sequence={} snapshot_entries={} wal_last_sequence={} records_replayed={} torn_tail_bytes={} recovered_keys={}",
            shard.shard_id,
            shard.snapshot_sequence,
            shard.snapshot_entries,
            shard.wal.last_sequence,
            shard.wal.records_replayed,
            shard.wal.torn_tail_bytes,
            shard.recovered_keys
        );
    }

    if inspect {
        println!("mode=read-only; torn WAL tails are reported and not truncated");
    }
}

/// `NOAUTH` counts as alive: the server answered, it just wants a password.
fn ping(address: Option<String>, connect_only: bool) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind, Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};
    use std::time::Duration;

    let address = address.unwrap_or_else(default_address);
    let timeout = Duration::from_secs(2);
    let target = address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| Error::new(ErrorKind::NotFound, "address did not resolve"))?;
    let mut stream = TcpStream::connect_timeout(&target, timeout)?;

    if connect_only {
        return Ok(());
    }

    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(b"*1\r\n$4\r\nPING\r\n")?;

    let mut reply = [0u8; 16];
    let read = stream.read(&mut reply)?;

    if reply[..read].starts_with(b"+PONG") || reply[..read].starts_with(b"-NOAUTH") {
        Ok(())
    } else {
        Err(Error::other("unexpected reply to PING"))
    }
}

/// RUVIO_ADDR with an unspecified host replaced by 127.0.0.1, then 127.0.0.1:6379.
fn default_address() -> String {
    let configured = std::env::var("RUVIO_ADDR").unwrap_or_default();
    let port = configured.rsplit(':').next().unwrap_or("6379");

    match configured.rsplit_once(':') {
        Some((host, _)) if !matches!(host, "" | "0.0.0.0" | "[::]" | "::") => configured.clone(),
        _ => format!("127.0.0.1:{}", if port.is_empty() { "6379" } else { port }),
    }
}

/// What a stop costs under a durability profile, as the warning `restart` prints.
fn restart_warning(profile: &str) -> &'static str {
    match profile {
        "memory" => "durability memory: nothing is saved, EVERY KEY IS LOST when the server stops",
        "snapshot" => {
            "durability snapshot: a final checkpoint is written when the server stops; a kill or crash would lose writes after the last SAVE"
        }
        "everysec" => {
            "durability everysec: the log is flushed and a final checkpoint written when the server stops; a kill would lose up to one sync window"
        }
        "always" => "durability always: every acknowledged write is on disk",
        _ => "unknown durability profile: check what the server saves before stopping it",
    }
}

/// The value after `durability` in a `CONFIG GET durability` reply.
fn durability_from_reply(reply: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(reply);
    let mut lines = text.split("\r\n");

    lines.find(|line| *line == "durability")?;
    lines.next()?; // the `$n` length line of the value

    lines.next().map(str::to_owned)
}

fn restart(
    address: Option<String>,
    yes: bool,
    allow_data_loss: bool,
    wait_secs: u64,
) -> std::io::Result<()> {
    use std::io::{Error, Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};
    use std::time::{Duration, Instant};

    fn command(parts: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();

        for part in parts {
            out.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
        }

        out
    }

    fn exchange(stream: &mut TcpStream, parts: &[&str]) -> std::io::Result<Vec<u8>> {
        stream.write_all(&command(parts))?;

        let mut reply = vec![0u8; 512];
        let read = stream.read(&mut reply)?;

        reply.truncate(read);

        Ok(reply)
    }

    let address = address.unwrap_or_else(default_address);
    let target = address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| Error::other("address did not resolve"))?;
    let timeout = Duration::from_secs(5);
    let mut stream = TcpStream::connect_timeout(&target, timeout)?;

    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    if let Ok(password) = std::env::var("RUVIO_PASSWORD") {
        let reply = match std::env::var("RUVIO_USER") {
            Ok(user) => exchange(&mut stream, &["AUTH", &user, &password])?,
            Err(_) => exchange(&mut stream, &["AUTH", &password])?,
        };

        if !reply.starts_with(b"+OK") {
            return Err(Error::other("AUTH was refused"));
        }
    }

    let reply = exchange(&mut stream, &["CONFIG", "GET", "durability"])?;
    let profile = durability_from_reply(&reply)
        .ok_or_else(|| Error::other("could not read the durability profile"))?;

    println!("{}", restart_warning(&profile));

    if !yes {
        return Err(Error::other(
            "nothing was stopped; run again with --yes to restart",
        ));
    }

    let nosave = profile == "memory";

    if nosave && !allow_data_loss {
        return Err(Error::other(
            "nothing was stopped; durability is memory, add --allow-data-loss to lose every key",
        ));
    }

    let reply = if nosave {
        exchange(&mut stream, &["SHUTDOWN", "NOSAVE"])?
    } else {
        exchange(&mut stream, &["SHUTDOWN"])?
    };

    if !reply.starts_with(b"+OK") {
        return Err(Error::other(format!(
            "SHUTDOWN was refused: {}",
            String::from_utf8_lossy(&reply).trim_end()
        )));
    }

    drop(stream);

    let started = Instant::now();
    let deadline = started + Duration::from_secs(wait_secs);
    let mut stopped = false;

    // Down first (the connect fails), then up again (a PING is answered).
    while Instant::now() < deadline {
        let up = TcpStream::connect_timeout(&target, Duration::from_millis(500))
            .and_then(|mut probe| {
                probe.set_read_timeout(Some(Duration::from_millis(500)))?;
                exchange(&mut probe, &["PING"])
            })
            .is_ok_and(|reply| reply.starts_with(b"+PONG") || reply.starts_with(b"-NOAUTH"));

        if !up {
            stopped = true;
        } else if stopped {
            println!("restarted in {:.1} s", started.elapsed().as_secs_f64());

            return Ok(());
        }

        std::thread::sleep(Duration::from_millis(200));
    }

    Err(Error::other(if stopped {
        "the server stopped but did not answer again in time; a supervisor has to start it"
    } else {
        "the server was told to stop but still answers; it may be draining clients (RUVIO_SHUTDOWN_GRACE_MS)"
    }))
}

/// One reply of a RESP2 server, as much of it as `rolling-restart` reads.
#[derive(Debug, PartialEq)]
enum Resp {
    Text(String),
    Error(String),
    Int(i64),
    Array(Vec<Resp>),
    Nil,
}

impl Resp {
    fn text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }

    /// A flat key/value array (a Sentinel description) as pairs.
    fn pairs(&self) -> Vec<(String, String)> {
        let Self::Array(items) = self else {
            return Vec::new();
        };

        items
            .chunks(2)
            .filter_map(|pair| Some((pair[0].text()?.to_owned(), pair.get(1)?.text()?.to_owned())))
            .collect()
    }
}

fn read_resp(reader: &mut impl std::io::BufRead) -> std::io::Result<Resp> {
    use std::io::Error;

    let mut line = String::new();

    if reader.read_line(&mut line)? == 0 {
        return Err(Error::other("the connection closed"));
    }
    let line = line.trim_end_matches(['\r', '\n']);
    let (kind, rest) = line.split_at(line.chars().next().map_or(0, char::len_utf8));
    let number = || {
        rest.parse::<i64>()
            .map_err(|_| Error::other("malformed reply"))
    };

    Ok(match kind {
        "+" => Resp::Text(rest.to_owned()),
        "-" => Resp::Error(rest.to_owned()),
        ":" => Resp::Int(number()?),
        "$" => match number()? {
            length if length < 0 => Resp::Nil,
            length => {
                let mut bytes = vec![0u8; length as usize + 2];

                reader.read_exact(&mut bytes)?;
                bytes.truncate(length as usize);
                Resp::Text(String::from_utf8_lossy(&bytes).into_owned())
            }
        },
        "*" => match number()? {
            length if length < 0 => Resp::Nil,
            length => Resp::Array(
                (0..length)
                    .map(|_| read_resp(reader))
                    .collect::<std::io::Result<_>>()?,
            ),
        },
        _ => return Err(Error::other("unexpected reply")),
    })
}

/// A connection that sends commands and reads whole replies, after `AUTH` when asked to.
struct Link {
    reader: std::io::BufReader<std::net::TcpStream>,
}

impl Link {
    fn open(address: &str, auth: &[&str]) -> std::io::Result<Self> {
        use std::net::ToSocketAddrs;
        use std::time::Duration;

        let target = address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| std::io::Error::other(format!("{address} did not resolve")))?;
        let stream = std::net::TcpStream::connect_timeout(&target, Duration::from_secs(3))?;

        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let mut link = Self {
            reader: std::io::BufReader::new(stream),
        };

        if !auth.is_empty() && link.call(auth)? != Resp::Text("OK".into()) {
            return Err(std::io::Error::other(format!(
                "AUTH to {address} was refused"
            )));
        }
        Ok(link)
    }

    fn call(&mut self, parts: &[&str]) -> std::io::Result<Resp> {
        use std::io::Write;

        let mut out = format!("*{}\r\n", parts.len()).into_bytes();

        for part in parts {
            out.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
        }
        self.reader.get_mut().write_all(&out)?;
        read_resp(&mut self.reader)
    }
}

/// Credentials for a data node, from the environment.
fn node_auth() -> Vec<String> {
    match (std::env::var("RUVIO_USER"), std::env::var("RUVIO_PASSWORD")) {
        (Ok(user), Ok(password)) => vec!["AUTH".into(), user, password],
        (Err(_), Ok(password)) => vec!["AUTH".into(), password],
        _ => Vec::new(),
    }
}

/// Sends `parts` to the first coordinator that answers it. A leader-only command skips the
/// coordinators that say they are not the leader.
fn ask_coordinators(
    coordinators: &[String],
    parts: &[&str],
    leader_only: bool,
) -> std::io::Result<Resp> {
    let secret = std::env::var("RUVIO_COORDINATOR_SECRET").ok();
    let auth: Vec<&str> = secret
        .as_deref()
        .map(|secret| vec!["AUTH", secret])
        .unwrap_or_default();
    let mut last = std::io::Error::other("no coordinator answered");

    for address in coordinators {
        let reply = Link::open(address, &auth).and_then(|mut link| link.call(parts));

        match reply {
            Ok(Resp::Error(error))
                if leader_only
                    && (error.starts_with("ERR not the leader")
                        || error.starts_with("ERR no leader")
                        || error.starts_with("ERR the leader is still applying")) =>
            {
                last = std::io::Error::other(format!("{address}: {error}"));
            }
            Ok(reply) => return Ok(reply),
            Err(error) => last = std::io::Error::other(format!("{address}: {error}")),
        }
    }
    Err(last)
}

/// One group as the coordinators describe it: its name, the primary's client address, and the
/// replicas'. A member the coordinators see as down stops the plan.
struct GroupPlan {
    name: String,
    primary: String,
    replicas: Vec<String>,
}

fn address_of(pairs: &[(String, String)]) -> Option<(String, bool)> {
    let value = |key: &str| pairs.iter().find(|(name, _)| name == key).map(|(_, v)| v);
    let down = value("flags")?.contains("down");

    Some((format!("{}:{}", value("ip")?, value("port")?), down))
}

fn plan_groups(coordinators: &[String], wanted: &[String]) -> std::io::Result<Vec<GroupPlan>> {
    use std::io::Error;

    let names: Vec<String> = if wanted.is_empty() {
        match ask_coordinators(coordinators, &["SENTINEL", "masters"], false)? {
            Resp::Array(groups) => groups
                .iter()
                .filter_map(|group| {
                    group
                        .pairs()
                        .into_iter()
                        .find(|(key, _)| key == "name")
                        .map(|(_, name)| name)
                })
                .collect(),
            other => return Err(Error::other(format!("unexpected reply: {other:?}"))),
        }
    } else {
        wanted.to_vec()
    };
    let mut plans = Vec::new();

    for name in names {
        let master = ask_coordinators(coordinators, &["SENTINEL", "master", &name], false)?;
        let Some((primary, down)) = address_of(&master.pairs()) else {
            return Err(Error::other(format!("group {name} has no agreed primary")));
        };
        let Resp::Array(replicas) =
            ask_coordinators(coordinators, &["SENTINEL", "replicas", &name], false)?
        else {
            return Err(Error::other(format!(
                "could not list the replicas of {name}"
            )));
        };
        let mut addresses = Vec::new();

        for replica in &replicas {
            let (address, replica_down) = address_of(&replica.pairs())
                .ok_or_else(|| Error::other(format!("a replica of {name} has no address")))?;

            if replica_down {
                return Err(Error::other(format!(
                    "{address}, a replica of {name}, is down: restarting the others now could leave the group without a copy"
                )));
            }
            addresses.push(address);
        }
        if down {
            return Err(Error::other(format!("the primary of {name} is down")));
        }
        if addresses.is_empty() {
            return Err(Error::other(format!(
                "group {name} has no replica: a rolling restart needs one to hand writes to (use `restart` for a node that may stop)"
            )));
        }
        plans.push(GroupPlan {
            name,
            primary,
            replicas: addresses,
        });
    }
    Ok(plans)
}

/// Waits until the node at `address` answers `ROLE` as a replica connected to its primary.
fn wait_for_replica(address: &str, wait_secs: u64) -> std::io::Result<()> {
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(wait_secs);
    let auth = node_auth();
    let auth: Vec<&str> = auth.iter().map(String::as_str).collect();

    while Instant::now() < deadline {
        let role = Link::open(address, &auth).and_then(|mut link| link.call(&["ROLE"]));

        if let Ok(Resp::Array(role)) = role
            && role.first().and_then(Resp::text) == Some("slave")
            && role.get(3).and_then(Resp::text) == Some("connected")
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err(std::io::Error::other(format!(
        "{address} did not become a connected replica in {wait_secs} s"
    )))
}

fn rolling_restart(
    coordinators: &[String],
    groups: &[String],
    skip: &[String],
    yes: bool,
    wait_secs: u64,
) -> std::io::Result<()> {
    let allow_data_loss = false;
    let plans = plan_groups(coordinators, groups)?;
    let skipped = |address: &String| skip.contains(address);

    for plan in &plans {
        println!("group {}:", plan.name);
        for replica in plan.replicas.iter().filter(|a| !skipped(a)) {
            println!("  restart replica {replica}, wait until it follows the primary again");
        }
        println!(
            "  COORDINATOR SWITCHOVER {} (writes pause briefly)",
            plan.name
        );
        if !skipped(&plan.primary) {
            println!("  restart the old primary {}", plan.primary);
        }
    }
    println!(
        "one node at a time; a supervisor must start each stopped node again; a node under durability memory is refused"
    );
    if !yes {
        return Err(std::io::Error::other(
            "nothing was stopped; run again with --yes to start",
        ));
    }

    for plan in &plans {
        for replica in plan.replicas.iter().filter(|a| !skipped(a)) {
            println!("[{}] restarting replica {replica}", plan.name);
            restart(Some(replica.clone()), true, allow_data_loss, wait_secs)?;
            wait_for_replica(replica, wait_secs)?;
        }

        println!("[{}] switching over", plan.name);
        let reply = ask_coordinators(
            coordinators,
            &["COORDINATOR", "SWITCHOVER", &plan.name],
            true,
        )?;
        let Resp::Text(new_primary) = reply else {
            return Err(std::io::Error::other(format!(
                "the switchover of {} was refused: {reply:?}; nothing more was stopped, and the nodes restarted so far are listed above (rerun with --skip for them)",
                plan.name
            )));
        };

        println!("[{}] the new primary is {new_primary}", plan.name);
        if skipped(&plan.primary) {
            continue;
        }
        wait_for_replica(&plan.primary, wait_secs)?;
        println!(
            "[{}] restarting the old primary {}",
            plan.name, plan.primary
        );
        restart(Some(plan.primary.clone()), true, allow_data_loss, wait_secs)?;
        wait_for_replica(&plan.primary, wait_secs)?;
        println!("[{}] done", plan.name);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_nested_replies_and_key_value_pairs() {
        let reply = b"*4\r\n$4\r\nname\r\n$4\r\nmain\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n";
        let parsed = read_resp(&mut std::io::BufReader::new(&reply[..])).unwrap();

        assert_eq!(
            parsed.pairs(),
            vec![
                ("name".to_owned(), "main".to_owned()),
                ("ip".to_owned(), "127.0.0.1".to_owned())
            ]
        );
        let mixed = b"*3\r\n+slave\r\n:7\r\n$-1\r\n";

        assert_eq!(
            read_resp(&mut std::io::BufReader::new(&mixed[..])).unwrap(),
            Resp::Array(vec![Resp::Text("slave".into()), Resp::Int(7), Resp::Nil])
        );
        assert_eq!(
            read_resp(&mut std::io::BufReader::new(&b"-ERR no\r\n"[..])).unwrap(),
            Resp::Error("ERR no".into())
        );
    }

    #[test]
    fn a_member_flagged_down_is_reported_down() {
        let pair = |key: &str, value: &str| (key.to_owned(), value.to_owned());
        let up = [
            pair("ip", "10.0.0.1"),
            pair("port", "6379"),
            pair("flags", "slave"),
        ];
        let down = [
            pair("ip", "10.0.0.2"),
            pair("port", "6379"),
            pair("flags", "slave,s_down"),
        ];

        assert_eq!(address_of(&up), Some(("10.0.0.1:6379".into(), false)));
        assert_eq!(address_of(&down), Some(("10.0.0.2:6379".into(), true)));
        assert_eq!(address_of(&[]), None);
    }

    #[test]
    fn reads_the_durability_profile_from_a_config_get_reply() {
        let reply = b"*2\r\n$10\r\ndurability\r\n$8\r\neverysec\r\n";

        assert_eq!(durability_from_reply(reply).as_deref(), Some("everysec"));
        assert_eq!(durability_from_reply(b"*0\r\n"), None);
    }

    #[test]
    fn the_memory_warning_says_that_every_key_is_lost() {
        assert!(restart_warning("memory").contains("EVERY KEY IS LOST"));
        assert!(!restart_warning("always").contains("LOST"));
    }
}
