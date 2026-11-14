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

#[cfg(test)]
mod tests {
    use super::*;

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
