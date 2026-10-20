use crate::{
    engine::{RespVersion, Response},
    protocol::Command,
    routing::{CrossSlot, command_keys, command_slot},
};

pub const MAX_QUEUED_COMMANDS: usize = 1_024;
/// Most argument bytes one `MULTI` may hold queued: four maximum-size connection buffers.
pub const MAX_QUEUED_BYTES: usize = 4 * crate::protocol::DEFAULT_MAX_CONNECTION_BUFFER_BYTES;
pub const MAX_WATCHED_KEYS: usize = 128;

#[derive(Debug, Default)]
pub struct TxSession {
    pub id: u64,
    pub resp: RespVersion,
    pub in_pubsub: bool,
    pub client_name: Vec<u8>,
    pub tracking: bool,
    pub database: u8,
    multi: bool,
    /// A command was refused while queueing; EXEC discards the whole transaction (as Redis).
    aborted: bool,
    queue: Vec<Command<'static>>,
    /// Queued commands whose arguments were refused: each takes the error as its reply at
    /// its place in the EXEC reply, without running (as Redis fails it at EXEC).
    refused: Vec<(usize, &'static [u8])>,
    queued_bytes: usize,
    watch_slot: Option<u16>,
    /// The shard holding this connection's watches; another one owns the slot after a
    /// slot move, and EXEC then fails as for a changed key.
    watch_shard: usize,
    watched_count: usize,
    /// The last command parked and was then served by another client's write: `Some` with
    /// that write's last WAL record, if it wrote one (see `take_served_after_park`).
    served_after_park: Option<Option<u64>>,
    /// Bit `n` is set once this connection had a command that may write run on shard `n` of the
    /// process (forwarded there in per-shard mode), so its `WAIT` also waits for that shard's
    /// replicas.
    pub forwarded_shards: u64,
    /// `CLIENT DURABILITY`: replies wait until this many replicas hold the writes on disk.
    pub durability: Option<Durability>,
    /// This shard's last record when the previous replies left under `durability`: a later
    /// flush with no record past it has nothing new to wait for.
    pub durable_mark: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Durability {
    pub replicas: u64,
    pub timeout_ms: u64,
}

#[derive(Debug)]
pub enum PrepareResult<'a> {
    Immediate(Command<'a>),
    Reply(Response<'static>),
    Watch(Vec<Vec<u8>>),
    Unwatch,
    Exec,
}

impl TxSession {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            resp: RespVersion::V2,
            in_pubsub: false,
            client_name: Vec::new(),
            tracking: false,
            database: 0,
            multi: false,
            aborted: false,
            queue: Vec::new(),
            refused: Vec::new(),
            queued_bytes: 0,
            watch_slot: None,
            watch_shard: 0,
            watched_count: 0,
            served_after_park: None,
            forwarded_shards: 0,
            durability: None,
            durable_mark: 0,
        }
    }

    pub fn note_served_after_park(&mut self, served_by: Option<u64>) {
        self.served_after_park = Some(served_by);
    }

    /// `Some` when the command just run parked and was served while it waited, with the
    /// last WAL record of the write that served it (`None` inside when it wrote none). The
    /// reply depends on that record, not on the ones other clients wrote meanwhile.
    pub fn take_served_after_park(&mut self) -> Option<Option<u64>> {
        self.served_after_park.take()
    }

    pub fn is_multi(&self) -> bool {
        self.multi
    }

    pub fn watch_slot(&self) -> Option<u16> {
        self.watch_slot
    }

    /// The shard that holds this connection's watches, when it has any.
    pub fn watch_shard(&self) -> Option<usize> {
        self.watch_slot.map(|_| self.watch_shard)
    }

    pub fn queued(&self) -> &[Command<'static>] {
        &self.queue
    }

    pub fn accept_watch(&mut self, slot: u16, count: usize, shard: usize) {
        self.watch_slot = Some(slot);
        self.watch_shard = shard;
        self.watched_count = self.watched_count.saturating_add(count);
    }

    pub fn clear_watches(&mut self) {
        self.watch_slot = None;
        self.watched_count = 0;
    }

    pub fn finish_exec(&mut self) {
        self.multi = false;
        self.aborted = false;
        self.clear_queue();
    }

    /// Empties the queue and lets a large one go, so an idle connection does not keep it.
    fn clear_queue(&mut self) {
        self.queued_bytes = 0;
        self.queue.clear();
        self.refused.clear();

        if self.queue.capacity() > 64 {
            self.queue = Vec::new();
        }
    }

    /// Records that a command inside MULTI was refused before it could be queued.
    pub fn mark_queue_error(&mut self) {
        if self.multi {
            self.aborted = true;
        }
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted
    }

    pub fn take_exec_commands(&mut self) -> Vec<Command<'static>> {
        self.multi = false;
        self.aborted = false;
        self.queued_bytes = 0;
        std::mem::take(&mut self.queue)
    }

    /// Queues a command whose arguments are invalid. It does not fail the transaction:
    /// EXEC runs the others and replies `error` in its place.
    pub fn queue_refusal(&mut self, error: &'static [u8]) -> Response<'static> {
        if self.queue.len() + self.refused.len() >= MAX_QUEUED_COMMANDS {
            self.aborted = true;

            return Response::Error(b"ERR MULTI queue is full");
        }

        self.refused
            .push((self.queue.len() + self.refused.len(), error));
        Response::Simple(b"QUEUED")
    }

    /// Takes the refused commands queued for this EXEC (see [`Self::queue_refusal`]).
    pub fn take_exec_refusals(&mut self) -> Vec<(usize, &'static [u8])> {
        std::mem::take(&mut self.refused)
    }

    pub fn prepare<'a>(&mut self, command: Command<'a>) -> PrepareResult<'a> {
        match command {
            Command::Multi => {
                if self.multi {
                    return PrepareResult::Reply(Response::Error(
                        b"ERR MULTI calls can not be nested",
                    ));
                }

                self.multi = true;
                PrepareResult::Reply(Response::Simple(b"OK"))
            }

            Command::Discard => {
                if !self.multi {
                    return PrepareResult::Reply(Response::Error(b"ERR DISCARD without MULTI"));
                }

                self.multi = false;
                self.aborted = false;
                self.clear_queue();
                PrepareResult::Reply(Response::Simple(b"OK"))
            }

            Command::Exec => {
                if !self.multi {
                    return PrepareResult::Reply(Response::Error(b"ERR EXEC without MULTI"));
                }

                PrepareResult::Exec
            }

            Command::Watch(keys) => {
                if self.multi {
                    return PrepareResult::Reply(Response::Error(
                        b"ERR WATCH inside MULTI is not allowed",
                    ));
                }

                if self.watched_count.saturating_add(keys.len()) > MAX_WATCHED_KEYS {
                    return PrepareResult::Reply(Response::Error(b"ERR too many WATCH keys"));
                }

                PrepareResult::Watch(keys.into_iter().map(|key| key.into_owned()).collect())
            }

            Command::Unwatch => PrepareResult::Unwatch,
            command if self.multi => {
                if !queueable(&command) {
                    self.aborted = true;

                    return PrepareResult::Reply(Response::Error(
                        b"ERR command not allowed inside MULTI",
                    ));
                }

                if self.queue.len() + self.refused.len() >= MAX_QUEUED_COMMANDS {
                    self.aborted = true;

                    return PrepareResult::Reply(Response::Error(b"ERR MULTI queue is full"));
                }

                let (command, bytes) = command.into_owned_measured();

                if self.queued_bytes.saturating_add(bytes) > MAX_QUEUED_BYTES {
                    self.aborted = true;

                    return PrepareResult::Reply(Response::Error(b"ERR MULTI queue is full"));
                }

                self.queued_bytes += bytes;
                self.queue.push(command);
                PrepareResult::Reply(Response::Simple(b"QUEUED"))
            }

            command => PrepareResult::Immediate(command),
        }
    }
}

/// Puts each refused command's error at its place in an EXEC reply. Any other reply (a
/// dirty WATCH, CROSSSLOT, a refused write) stands for the whole transaction.
pub fn splice_refusals(
    response: Response<'static>,
    refused: Vec<(usize, &'static [u8])>,
) -> Response<'static> {
    match response {
        Response::Exec(mut replies) if !refused.is_empty() => {
            for (index, error) in refused {
                replies.insert(index.min(replies.len()), Response::Error(error));
            }

            Response::Exec(replies)
        }

        response => response,
    }
}

fn queueable(command: &Command<'_>) -> bool {
    !matches!(
        command,
        Command::Wait { .. }
            | Command::ListBlock { .. }
            | Command::ListMove { block: Some(_), .. }
            | Command::ListMPop { block: Some(_), .. }
            | Command::FillGet {
                wait_ms: Some(_),
                ..
            }
            | Command::SortedSetBlockPop { .. }
            | Command::StreamRead { block: Some(_), .. }
            | Command::StreamGroupRead { block: Some(_), .. }
            | Command::Save
            | Command::ConfigSet(_)
            | Command::ConfigRewrite
            | Command::ReplicaOf { .. }
            | Command::MoveSlots(_)
            | Command::Topology(_)
            | Command::Migrate(_)
            | Command::Role
            | Command::Bgsave
            | Command::Hello { .. }
            | Command::ClientGetName
            | Command::ClientSetName(_)
            | Command::ClientTracking(_)
            | Command::ClientDurability { .. }
            | Command::Select(_)
            | Command::ShardPublish { .. }
            | Command::ShardSubscribe(_)
            | Command::ShardUnsubscribe(_)
            | Command::Subscribe(_)
            | Command::Unsubscribe(_)
            | Command::PSubscribe(_)
            | Command::PUnsubscribe(_)
            | Command::Auth { .. }
            | Command::Acl(_)
            | Command::ClusterSlots
            | Command::ClusterNodes
            | Command::ClusterInfo
            | Command::ClusterShards
            | Command::ClusterMyId
            | Command::ClusterKeySlot(_)
            | Command::ReadOnly
            | Command::ReadWrite
            | Command::ScriptLoad(_)
            | Command::ScriptExists(_)
            | Command::ScriptFlush
            | Command::ScriptKill
            | Command::FunctionLoad { .. }
            | Command::FunctionList
            | Command::FunctionDelete(_)
    )
}

pub fn transaction_slot(commands: &[Command<'_>]) -> Result<Option<u16>, CrossSlot> {
    let mut slot = None;

    for command in commands {
        let next = command_slot(command)?;

        match (slot, next) {
            (None, next) => slot = next,
            (Some(existing), Some(next)) if existing != next => return Err(CrossSlot),
            _ => {}
        }
    }

    Ok(slot)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn watched_and_command_keys<'a>(
    watches: &'a [Vec<u8>],
    commands: &'a [Command<'a>],
) -> impl Iterator<Item = &'a [u8]> {
    watches
        .iter()
        .map(Vec::as_slice)
        .chain(commands.iter().flat_map(command_keys))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    fn owned(value: &[u8]) -> Cow<'static, [u8]> {
        Cow::Owned(value.to_vec())
    }

    #[test]
    fn queues_until_exec_and_rejects_nested_multi() {
        let mut session = TxSession::new(1);

        assert!(matches!(
            session.prepare(Command::Multi),
            PrepareResult::Reply(Response::Simple(b"OK"))
        ));
        assert!(matches!(
            session.prepare(Command::Multi),
            PrepareResult::Reply(Response::Error(_))
        ));
        assert!(matches!(
            session.prepare(Command::set(owned(b"a"), owned(b"1"))),
            PrepareResult::Reply(Response::Simple(b"QUEUED"))
        ));
        assert!(matches!(
            session.prepare(Command::Exec),
            PrepareResult::Exec
        ));
        assert_eq!(session.queued().len(), 1);
        session.finish_exec();
        assert!(!session.is_multi());
        assert!(session.queued().is_empty());
    }

    #[test]
    fn exec_transfers_queued_payload_without_cloning_and_allows_reuse() {
        let mut session = TxSession::new(1);
        let payload = vec![b'x'; 4096];

        session.accept_watch(42, 1, 0);
        session.prepare(Command::Multi);
        session.prepare(Command::set(owned(b"key"), Cow::Owned(payload)));
        let queued_payload = match &session.queued()[0] {
            Command::Set(_, value, _) => value.as_ref().as_ptr(),
            _ => panic!("expected queued SET"),
        };

        let commands = session.take_exec_commands();
        assert!(!session.is_multi());
        assert!(session.queued().is_empty());
        assert_eq!(session.watch_slot(), Some(42));
        assert!(matches!(
            session.prepare(Command::Exec),
            PrepareResult::Reply(Response::Error(_))
        ));
        match &commands[0] {
            Command::Set(_, value, _) => assert_eq!(value.as_ref().as_ptr(), queued_payload),
            _ => panic!("expected transferred SET"),
        }

        session.prepare(Command::Multi);
        session.prepare(Command::Get(owned(b"key")));
        assert_eq!(session.queued().len(), 1);
        session.finish_exec();
        assert!(session.queued().is_empty());
    }

    #[test]
    fn discard_clears_the_queue_without_unwatch() {
        let mut session = TxSession::new(1);

        session.accept_watch(1, 1, 0);
        session.prepare(Command::Multi);
        session.prepare(Command::set(owned(b"a"), owned(b"1")));
        assert!(matches!(
            session.prepare(Command::Discard),
            PrepareResult::Reply(Response::Simple(b"OK"))
        ));
        assert!(!session.is_multi());
        assert!(session.queued().is_empty());
        assert_eq!(session.watch_slot(), Some(1));
    }

    #[test]
    fn rejects_watch_inside_multi_and_blocking_commands() {
        let mut session = TxSession::new(1);

        session.prepare(Command::Multi);
        assert!(matches!(
            session.prepare(Command::Watch(vec![owned(b"a")])),
            PrepareResult::Reply(Response::Error(_))
        ));
        assert!(matches!(
            session.prepare(Command::ListBlock {
                keys: vec![owned(b"a")],
                timeout_ms: Some(1),
                back: false,
            }),
            PrepareResult::Reply(Response::Error(_))
        ));
        assert!(matches!(
            session.prepare(Command::Wait {
                replicas: 1,
                timeout_ms: 1
            }),
            PrepareResult::Reply(Response::Error(_))
        ));
    }

    #[test]
    fn watched_and_command_keys_union_unique_sources() {
        let watches = [b"w".to_vec()];
        let commands = [Command::Get(owned(b"a"))];
        let keys: Vec<_> = watched_and_command_keys(&watches, &commands).collect();

        assert_eq!(keys, vec![&b"w"[..], &b"a"[..]]);
    }

    #[test]
    fn the_queue_is_bounded_by_argument_bytes_and_released_after_exec() {
        let mut session = TxSession::new(1);

        session.prepare(Command::Multi);
        assert!(matches!(
            session.prepare(Command::set(owned(b"k"), owned(&[b'v'; 1_000]))),
            PrepareResult::Reply(Response::Simple(b"QUEUED"))
        ));
        assert_eq!(session.queued_bytes, 1_001);

        // Close to the limit: a small command fits, a larger one aborts the transaction.
        session.queued_bytes = MAX_QUEUED_BYTES - 10;
        assert!(matches!(
            session.prepare(Command::set(owned(b"k"), owned(b"12345"))),
            PrepareResult::Reply(Response::Simple(b"QUEUED"))
        ));
        assert!(matches!(
            session.prepare(Command::set(owned(b"k"), owned(b"0123456789"))),
            PrepareResult::Reply(Response::Error(b"ERR MULTI queue is full"))
        ));
        assert!(session.is_aborted());

        session.prepare(Command::Discard);
        assert_eq!(session.queued_bytes, 0);

        session.prepare(Command::Multi);

        for _ in 0..500 {
            session.prepare(Command::set(owned(b"k"), owned(b"v")));
        }

        session.finish_exec();
        assert_eq!(session.queued_bytes, 0);
        assert_eq!(session.queue.capacity(), 0, "a large queue is not kept");
    }
}
