use super::*;

#[derive(Default)]
pub(super) struct BufferingSink {
    pub(super) mutations: Vec<OwnedMutation>,
}

impl MutationSink for BufferingSink {
    type Error = std::convert::Infallible;

    fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        // The batch becomes one WAL transaction, which cannot nest: inline a command's own.
        match mutation {
            Mutation::Transaction { mutations } => self.mutations.extend_from_slice(mutations),
            mutation => self.mutations.push(mutation.to_owned()),
        }
        Ok(())
    }

    fn switches_databases(&self) -> bool {
        false
    }
}

pub fn execute_transaction<C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    conn: u64,
    commands: Vec<Command<'static>>,
    database: u8,
) -> Result<Response<'static>, S::Error> {
    execute_transaction_in(shard, sink, waiters, conn, commands, database, true, None)
}

/// What a queued `EVAL`, `EVALSHA` or `FCALL` runs with: the connection's script cache, the
/// running limits, and the ACL rules each script's calls are checked against (none when the
/// user may run every command on every key).
pub struct TransactionScripts<'a> {
    pub cache: &'a mut crate::script::ScriptCache,
    pub limits: crate::config::ScriptLimits,
    pub rules: Option<std::sync::Arc<crate::acl::AclRules>>,
}

/// Runs a queued transaction. `enforce_slots` is off when this shard owns every slot, so
/// keys in different hash slots may share one transaction.
#[allow(clippy::too_many_arguments)]
pub fn execute_transaction_in<C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    conn: u64,
    commands: Vec<Command<'static>>,
    database: u8,
    enforce_slots: bool,
    mut scripts: Option<TransactionScripts<'_>>,
) -> Result<Response<'static>, S::Error> {
    if shard.watches_dirty(conn) {
        shard.unwatch(conn);

        return Ok(Response::NullArray);
    }

    shard.unwatch(conn);

    if enforce_slots && crate::transaction::transaction_slot(&commands).is_err() {
        return Ok(Response::Error(
            b"CROSSSLOT Keys in request do not hash to the same slot",
        ));
    }

    let mut undo = Vec::new();
    let mut collected = BufferingSink::default();
    // The undo log only serves a sink that refuses the batch, and a sink that keeps nothing
    // never does (memory mode): its keys are not copied at all then.
    let rolls_back = sink.persists();
    let mut responses = Vec::with_capacity(commands.len());

    // Blocked clients served by the batch get their data once the batch is recorded.
    let before = sink.written_sequence();

    waiters.hold_deliveries();

    for command in commands {
        if let Command::FlushDb | Command::FlushAll = command {
            responses.push(flush_in_transaction(
                shard,
                &mut collected,
                &mut undo,
                database,
                matches!(command, Command::FlushAll),
            ));
            continue;
        }

        if rolls_back {
            capture_undo(shard, &command, &mut undo, database);
        }

        // A script runs in its place, its writes joining the batch's one record; a script
        // touches only the keys it declares, which the undo log copied above.
        if let Command::Eval { .. } | Command::EvalSha { .. } | Command::FCall { .. } = command {
            let response = match scripts.as_mut() {
                Some(scripts) => {
                    scripts.cache.restrict_next_script(scripts.rules.clone());
                    match crate::script::execute_script_command(
                        shard,
                        &mut collected,
                        waiters,
                        scripts.cache,
                        scripts.limits,
                        command,
                        database,
                    ) {
                        Ok(response) => response,
                        Err(error) => match error {},
                    }
                }
                None => Response::Error(b"ERR script command requires a connection"),
            };

            responses.push(response);
            continue;
        }

        match execute_in_database(shard, command, &mut collected, waiters, database) {
            Ok(response) => responses.push(response.into_owned()),
            Err(error) => match error {},
        }
    }

    if !collected.mutations.is_empty() {
        // The buffer holds only this database's expiry deletes; one in another database (a
        // `MOVE` target) is logged ahead of the batch, so replay finds the target free.
        let recorded = record_removals(shard, sink).and_then(|()| {
            sink.select_database(database);
            sink.record_now(Mutation::Transaction {
                mutations: &collected.mutations,
            })
        });

        if let Err(error) = recorded {
            debug_assert!(rolls_back, "a sink that keeps nothing refused a batch");
            for step in undo.into_iter().rev() {
                step.revert(shard);
            }
            shard.settle_held_removals(false);
            waiters.release_deliveries(false);

            return Err(error);
        }
    }

    shard.settle_held_removals(true);
    waiters.served_by(written_since(before, sink.written_sequence()));
    waiters.release_deliveries(true);
    for step in undo {
        if let Undo::Database(detached) = step {
            shard.release_database(detached);
        }
    }

    Ok(Response::Exec(responses))
}

/// One step of the `MULTI` undo log, reverted newest first when the WAL refuses the batch.
pub(super) enum Undo<I> {
    Entry {
        database: u8,
        key: Box<[u8]>,
        entry: Option<crate::Entry>,
        deadline: Option<crate::time::UnixMillis>,
    },
    Database(crate::store::DetachedDatabase<I>),
    /// A flushed database's JSON index, fill and field TTL state.
    Side(crate::store::DetachedSide),
    Swap(u8, u8),
    /// A `JSON.INDEX CREATE` or `DROP`: the index named as it was before the command.
    JsonIndex {
        name: String,
        before: Option<crate::json::index::Definition>,
        /// The whole index a `DROP` removes, so undoing it needs no rebuild.
        kept: Option<Box<crate::json::index::Index>>,
    },
}

impl<I: StorageIndex> Undo<I> {
    pub(super) fn revert<C: Clock>(self, shard: &mut KvShard<C, I>) {
        match self {
            Undo::Entry {
                database,
                key,
                entry,
                deadline,
            } => {
                let active = shard.active_database();

                shard.enter(database);
                shard.restore_entry(&key, entry);
                // A deadline already past removes the restored key again, as expiry would.
                if let Some(deadline) = deadline {
                    shard.expire_at(&key, deadline);
                }
                shard.enter(active);
            }

            Undo::Database(detached) => shard.restore_database(detached),
            Undo::Side(side) => shard.restore_side_state(side),
            Undo::Swap(first, second) => shard.swap_databases(first, second),
            Undo::JsonIndex { name, before, kept } => {
                let exists = shard
                    .json_index_list()
                    .iter()
                    .any(|(definition, ..)| definition.name == name);

                match before {
                    None if exists => {
                        shard.json_index_drop(&name);
                    }
                    Some(_) if !exists && kept.is_some() => {
                        if let Some(index) = kept {
                            shard.json_index_restore(*index);
                        }
                    }
                    Some(definition) if !exists => {
                        shard.json_index_create(definition);
                    }
                    _ => {}
                }
            }
        }
    }
}

/// `FLUSHDB`/`FLUSHALL` inside `MULTI`: the maps are detached, not freed, until the batch
/// is durable.
pub(super) fn flush_in_transaction<C: Clock, I: StorageIndex>(
    shard: &mut KvShard<C, I>,
    collected: &mut BufferingSink,
    undo: &mut Vec<Undo<I>>,
    database: u8,
    all: bool,
) -> Response<'static> {
    let mut targets = if all {
        shard.database_ids()
    } else {
        vec![database]
    };
    // Database 0 first: its side state takes the whole JSON registry, so the others find
    // nothing to lift and the registry comes back whole, last.
    targets.sort_unstable();

    shard.capture_snapshot_databases(&targets);

    let mutation = if all {
        Mutation::FlushAll
    } else {
        Mutation::FlushDatabase
    };

    let Ok(()) = collected.record(mutation);

    for &target in &targets {
        undo.push(Undo::Side(shard.detach_side_state(target)));
    }
    for target in targets {
        undo.push(Undo::Database(shard.take_database(target)));
    }

    Response::Simple(b"OK")
}

/// Saves what `command` is about to overwrite. A command that only reads copies nothing,
/// and a key a command only reads from (a `*STORE` source) is not copied either: it is
/// copied, still unchanged, when a later command first writes it.
pub(super) fn capture_undo<C: Clock, I: StorageIndex>(
    shard: &mut KvShard<C, I>,
    command: &Command<'_>,
    undo: &mut Vec<Undo<I>>,
    database: u8,
) {
    if !crate::protocol::command_mutates_store(command) {
        return;
    }

    if let Command::SwapDb(first, second) = command {
        if let (Ok(first), Ok(second)) = (u8::try_from(*first), u8::try_from(*second)) {
            undo.push(Undo::Swap(first, second));
        }

        return;
    }

    // An index lives beside the keyspace: its definition key and the index itself are
    // saved apart, since the command names no key.
    if let Command::JsonIndex {
        op: op @ (crate::protocol::JsonIndexKind::Create | crate::protocol::JsonIndexKind::Drop),
        args,
    } = command
    {
        if database == 0
            && let Some(name) = args.first().and_then(|raw| std::str::from_utf8(raw).ok())
        {
            let key = crate::store::json_indexing::definition_key(name);
            let active = shard.active_database();

            shard.enter(0);
            undo.push(Undo::Entry {
                database: 0,
                key: key.as_slice().into(),
                entry: shard.clone_entry(&key),
                deadline: shard.deadline(0, &key),
            });
            shard.enter(active);
            undo.push(Undo::JsonIndex {
                name: name.to_owned(),
                before: shard
                    .json_index_list()
                    .into_iter()
                    .map(|(definition, ..)| definition)
                    .find(|definition| definition.name == name),
                kept: (*op == crate::protocol::JsonIndexKind::Drop)
                    .then(|| shard.json_index_copy(name).map(Box::new))
                    .flatten(),
            });
        }
        return;
    }

    let moved = match command {
        Command::Move { key, db } => u8::try_from(*db).ok().map(|target| (target, key.as_ref())),
        _ => None,
    };

    let keys = written_keys(command)
        .into_iter()
        .map(|key| (database, key))
        .chain(moved);
    let active = shard.active_database();

    for (target, key) in keys {
        let captured = undo
            .iter()
            .rev()
            .map_while(|step| match step {
                Undo::Entry { database, key, .. } => Some((*database, key)),
                _ => None,
            })
            .any(|(existing_database, existing)| {
                existing_database == target && existing.as_ref() == key
            });

        if captured {
            continue;
        }

        shard.enter(target);
        #[cfg(test)]
        UNDO_COPIES.set(UNDO_COPIES.get() + 1);
        undo.push(Undo::Entry {
            database: target,
            key: key.into(),
            entry: shard.clone_entry(key),
            deadline: shard.deadline(target, key),
        });
    }

    shard.enter(active);
}

/// The keys `command` may change: [`command_keys`] without the sources of commands that
/// only write their destination.
pub(crate) fn written_keys<'a>(command: &'a Command<'_>) -> Vec<&'a [u8]> {
    match command {
        Command::PfMerge { destination, .. }
        | Command::BitOp { destination, .. }
        | Command::Copy { destination, .. } => vec![destination.as_ref()],
        Command::SetAlgebra {
            destination: Some(destination),
            ..
        }
        | Command::SortedSetAlgebra {
            destination: Some(destination),
            ..
        } => vec![destination.as_ref()],
        Command::SortedSetAlgebra {
            destination: None, ..
        } => Vec::new(),
        Command::GeoSearch {
            store: Some(store), ..
        } => vec![store.destination.as_ref()],
        _ => command_keys(command),
    }
}

#[cfg(test)]
thread_local! {
    /// Keys copied into an undo log on this thread, for tests that check what is copied.
    pub(crate) static UNDO_COPIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Runs a write that sets a deadline only when it creates the key (`EX`/`PX` on `INCRBY`,
/// `ZADD`, ...). A key that exists keeps its deadline. For a new key the write and the
/// deadline are logged as one record, and undone together if the log refuses it.
pub(super) fn create_with_ttl<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    ttl_ms: u64,
    inner: Command<'a>,
    database: u8,
) -> Result<Response<'a>, S::Error> {
    let Some(key) = crate::routing::command_keys(&inner)
        .first()
        .map(|key| key.to_vec())
    else {
        return execute_in_database(shard, inner, sink, waiters, database);
    };

    shard.expire_due();
    shard.enter(database);

    if shard.contains_key(&key) {
        return execute_in_database(shard, inner, sink, waiters, database);
    }

    let mut undo = Vec::new();
    let mut collected = BufferingSink::default();
    let rolls_back = sink.persists();

    if rolls_back {
        capture_undo(shard, &inner, &mut undo, database);
    }

    let before = sink.written_sequence();

    waiters.hold_deliveries();

    let response = match execute_in_database(&mut *shard, inner, &mut collected, waiters, database)
    {
        Ok(response) => response.into_owned(),
        Err(error) => match error {},
    };

    if shard.contains_key(&key) {
        let now = shard.now_unix_millis();

        if let Some(deadline) = now.checked_add(ttl_ms) {
            shard.expire_at(&key, deadline);
            collected.mutations.push(
                Mutation::ExpireAt {
                    key: &key,
                    unix_millis: deadline.get(),
                }
                .to_owned(),
            );
        }
    }

    if !collected.mutations.is_empty() {
        sink.select_database(database);

        if let Err(error) = sink.record_now(Mutation::Transaction {
            mutations: &collected.mutations,
        }) {
            for step in undo.into_iter().rev() {
                step.revert(shard);
            }
            waiters.release_deliveries(false);

            return Err(error);
        }
    }

    waiters.served_by(written_since(before, sink.written_sequence()));
    waiters.release_deliveries(true);

    Ok(response)
}
