//! `CLIENT LIST`, `CLIENT INFO`, `CLIENT KILL`, `SLOWLOG` and `LATENCY LATEST`, answered from
//! per-shard registries.
//!
//! Each shard knows only its own connections, so with several shards `CLIENT LIST` and
//! `CLIENT KILL` cover the shard that received the command.

use super::*;
use crate::engine::RespVersion;
use crate::protocol::SlowlogOp;
use crate::slowlog::{KEY_PREVIEW_BYTES, SlowEntry};

pub(super) struct ClientEntry {
    pub(super) addr: String,
    pub(super) laddr: String,
    pub(super) created: Instant,
    pub(super) name: Vec<u8>,
    pub(super) db: u8,
    pub(super) resp: u8,
    pub(super) user: Vec<u8>,
}

impl ClientEntry {
    fn line(&self, id: u64, multi: i64) -> Vec<u8> {
        let mut line = format!(
            "id={id} addr={} laddr={} fd=-1 name=",
            self.addr, self.laddr
        )
        .into_bytes();

        line.extend_from_slice(&self.name);
        line.extend_from_slice(
            format!(
                " age={} idle=0 flags=N db={} multi={multi} resp={} user=",
                self.created.elapsed().as_secs(),
                self.db,
                self.resp
            )
            .as_bytes(),
        );
        line.extend_from_slice(&self.user);
        line
    }
}

/// Brings the registry row of the calling connection in line with its session.
pub(super) fn refresh(
    state: &ShardState,
    conn: u64,
    session: &TxSession,
    auth: &crate::acl::ConnectionAuth,
) {
    if let Some(entry) = state.clients.borrow_mut().get_mut(&conn) {
        entry.name.clone_from(&session.client_name);
        entry.db = session.database;
        entry.resp = if session.resp == RespVersion::V3 {
            3
        } else {
            2
        };
        entry.user.clear();
        entry.user.extend_from_slice(&auth.username);
    }
}

pub(super) fn is_identity_change(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::ClientSetName(_)
            | Command::Select(_)
            | Command::Hello { .. }
            | Command::Auth { .. }
    )
}

/// Answers the registry commands. Returns false for any other command.
pub(super) fn answer_client(
    state: &ShardState,
    session: &TxSession,
    auth: &crate::acl::ConnectionAuth,
    conn: u64,
    command: &Command<'_>,
    output: &mut BoundedOutput,
) -> io::Result<bool> {
    match command {
        Command::ClientInfo => {
            refresh(state, conn, session, auth);

            let clients = state.clients.borrow();
            let mut line = clients
                .get(&conn)
                .map(|entry| entry.line(conn, multi_of(session)))
                .unwrap_or_default();

            line.push(b'\n');
            write_response(output, Response::Bulk(Some(std::borrow::Cow::Owned(line))))?;
        }

        Command::ClientList { ids } => {
            refresh(state, conn, session, auth);

            let clients = state.clients.borrow();
            let mut shown: Vec<u64> = clients
                .keys()
                .copied()
                .filter(|id| ids.is_empty() || ids.contains(id))
                .collect();
            let mut text = Vec::new();

            shown.sort_unstable();

            for id in shown {
                let multi = if id == conn { multi_of(session) } else { -1 };

                text.extend_from_slice(&clients[&id].line(id, multi));
                text.push(b'\n');
            }

            write_response(output, Response::Bulk(Some(std::borrow::Cow::Owned(text))))?;
        }

        Command::ClientKill {
            id,
            addr,
            skip_me,
            legacy,
        } => {
            let targets: Vec<u64> = state
                .clients
                .borrow()
                .iter()
                .filter(|(candidate, entry)| {
                    id.is_none_or(|wanted| wanted == **candidate)
                        && addr
                            .as_ref()
                            .is_none_or(|wanted| wanted.as_ref() == entry.addr.as_bytes())
                        && !(*skip_me && **candidate == conn)
                })
                .map(|(candidate, _)| *candidate)
                .collect();

            for target in &targets {
                if let Some(outbound) = state.subscribers.borrow().get(target) {
                    outbound.close();
                }
            }

            if *legacy {
                if targets.is_empty() {
                    output.write_all(b"-ERR No such client\r\n")?;
                } else {
                    output.write_all(b"+OK\r\n")?;
                }
            } else {
                write_response(output, Response::Integer(targets.len() as i64))?;
            }
        }

        Command::Slowlog(op) => answer_slowlog(state, *op, output)?,
        Command::LatencyLatest => write_response(output, Response::Array(Vec::new()))?,
        _ => return Ok(false),
    }

    Ok(true)
}

fn multi_of(session: &TxSession) -> i64 {
    if session.is_multi() {
        session.queued().len() as i64
    } else {
        -1
    }
}

fn answer_slowlog(state: &ShardState, op: SlowlogOp, output: &mut BoundedOutput) -> io::Result<()> {
    let mut log = state.slowlog.borrow_mut();

    match op {
        SlowlogOp::Len => output.write_all(format!(":{}\r\n", log.len()).as_bytes()),
        SlowlogOp::Reset => {
            log.reset();
            output.write_all(b"+OK\r\n")
        }

        SlowlogOp::Get(count) => {
            let entries: Vec<_> = log.newest(count).collect();

            output.write_all(format!("*{}\r\n", entries.len()).as_bytes())?;

            for entry in entries {
                output.write_all(b"*6\r\n")?;
                output.write_all(
                    format!(
                        ":{}\r\n:{}\r\n:{}\r\n",
                        entry.id, entry.unix_secs, entry.micros
                    )
                    .as_bytes(),
                )?;
                output.write_all(format!("*{}\r\n", entry.args.len()).as_bytes())?;

                for arg in &entry.args {
                    output.write_all(format!("${}\r\n", arg.len()).as_bytes())?;
                    output.write_all(arg)?;
                    output.write_all(b"\r\n")?;
                }

                output
                    .write_all(format!("${}\r\n{}\r\n", entry.addr.len(), entry.addr).as_bytes())?;
                output.write_all(format!("${}\r\n", entry.name.len()).as_bytes())?;
                output.write_all(&entry.name)?;
                output.write_all(b"\r\n")?;
            }

            Ok(())
        }
    }
}

/// Commands that can hold the connection waiting: blocking pops and reads, WAIT and
/// subscriptions. Their wait is not slowlog time.
pub(super) fn may_wait(command: &Command<'_>) -> bool {
    crate::protocol::command_may_block(command)
        || matches!(
            command,
            Command::Wait { .. }
                | Command::Subscribe(_)
                | Command::PSubscribe(_)
                | Command::ChangesSubscribe(_)
        )
}

/// What a slowlog entry will say about a command. Only the frame is kept: the arguments
/// are cut out of it when the command turns out slow, so a fast command pays nothing.
pub(super) struct SlowSample<'f> {
    frame: &'f [u8],
    pub(super) blocking: bool,
}

impl<'f> SlowSample<'f> {
    pub(super) fn of(command: &Command<'_>, frame: &'f [u8]) -> Self {
        Self {
            frame,
            blocking: may_wait(command),
        }
    }

    /// A command that never blocks (see `dispatch_plain`).
    pub(super) fn plain(frame: &'f [u8]) -> Self {
        Self {
            frame,
            blocking: false,
        }
    }

    #[cold]
    pub(super) fn record(self, state: &ShardState, micros: u64, session: &TxSession) {
        let args = crate::protocol::slowlog_arguments(self.frame, KEY_PREVIEW_BYTES);

        state.slowlog.borrow_mut().push(
            SlowEntry {
                id: 0,
                unix_secs: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_secs()),
                micros,
                args,
                addr: state
                    .clients
                    .borrow()
                    .get(&session.id)
                    .map(|entry| entry.addr.clone())
                    .unwrap_or_default(),
                name: session.client_name.clone(),
            },
            state.live.with(|live| live.slowlog_max_len),
        );
    }
}
