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

    let address = address.unwrap_or_else(|| {
        let configured = std::env::var("RUVIO_ADDR").unwrap_or_default();
        let port = configured.rsplit(':').next().unwrap_or("6379");

        match configured.rsplit_once(':') {
            Some((host, _)) if !matches!(host, "" | "0.0.0.0" | "[::]" | "::") => {
                configured.clone()
            }
            _ => format!("127.0.0.1:{}", if port.is_empty() { "6379" } else { port }),
        }
    });
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
