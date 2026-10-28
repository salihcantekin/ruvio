//! Hot-path commands on a warm shard must not touch the allocator.
//!
//! The counter is thread-local, so other test threads do not disturb it.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    time::Duration,
};

use ruvio::{
    DurabilityProfile, KvShard, PersistenceConfig,
    engine::execute_hot_into_with_sink,
    mutation::{ExecuteError, MutationSink, NoopMutationSink},
    persistence::WalSink,
    protocol::{HotCommand, ProtocolLimits},
};

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn bump() {
    // `try_with`: the slot may be gone while the thread is shutting down.
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
}

// SAFETY: every call forwards to `System` unchanged; only a counter is added.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwards the caller's GlobalAlloc contract to System unchanged.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const ROUNDS: usize = 10_000;
const KEY: &[u8] = b"zero-alloc:key";
const COUNTER: &[u8] = b"zero-alloc:counter";
const VALUE: &[u8] = b"0123456789abcdef";

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

fn run<S: MutationSink>(
    shard: &mut KvShard,
    sink: &mut S,
    output: &mut Vec<u8>,
    command: HotCommand<'_>,
) {
    output.clear();

    match execute_hot_into_with_sink(shard, command, output, sink, b"$-1\r\n") {
        Ok(()) => {}
        Err(ExecuteError::Io(error)) => panic!("io: {error}"),
        Err(ExecuteError::Mutation(_)) => panic!("mutation sink failed"),
    }
}

/// Warm every key and buffer, then count allocations over `ROUNDS` repeats of each command.
#[allow(clippy::type_complexity)]
fn assert_zero_alloc<S: MutationSink>(sink: &mut S) {
    let mut shard = KvShard::new();
    let mut output = Vec::with_capacity(4096);

    for _ in 0..16 {
        run(
            &mut shard,
            sink,
            &mut output,
            HotCommand::Set(KEY, VALUE, None),
        );
        run(
            &mut shard,
            sink,
            &mut output,
            HotCommand::Increment(COUNTER, 1),
        );
        run(&mut shard, sink, &mut output, HotCommand::Get(KEY));
        run(&mut shard, sink, &mut output, HotCommand::Delete(KEY));
    }
    run(
        &mut shard,
        sink,
        &mut output,
        HotCommand::Set(KEY, VALUE, None),
    );

    let cases: [(&str, fn() -> HotCommand<'static>); 4] = [
        ("GET", || HotCommand::Get(KEY)),
        ("SET same length", || HotCommand::Set(KEY, VALUE, None)),
        ("INCR", || HotCommand::Increment(COUNTER, 1)),
        ("DEL+SET", || HotCommand::Delete(KEY)),
    ];

    for (name, make) in cases {
        let before = allocations();

        for _ in 0..ROUNDS {
            run(&mut shard, sink, &mut output, make());

            if name == "DEL+SET" {
                run(
                    &mut shard,
                    sink,
                    &mut output,
                    HotCommand::Set(KEY, VALUE, None),
                );
            }
        }

        let used = allocations() - before;

        assert_eq!(used, 0, "{name}: {used} allocations over {ROUNDS} rounds");
    }
}

#[test]
fn memory_durability_hot_path_allocates_nothing() {
    assert_zero_alloc(&mut NoopMutationSink);
}

#[test]
fn everysec_durability_hot_path_allocates_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        profile: DurabilityProfile::Everysec,
        data_dir: directory.path().to_path_buf(),
        // On Linux the WAL asks a helper thread for the next 4 MiB chunk once less than 3 MiB is left
        // ahead of the cursor; that request allocates a few small objects once per chunk, not per
        // command. A 4 MiB segment is one chunk, so the measured rounds see no request and this test
        // keeps checking the per-command path only.
        wal_segment_bytes: 4 * 1024 * 1024,
        // Once a sync interval has passed with dirty bytes, an append starts a background sync
        // thread, which allocates. A slow runner can take longer than the default second over
        // the measured rounds, so the interval is far beyond the test's run time.
        sync_interval: Duration::from_secs(3600),
        ..PersistenceConfig::default()
    };
    let (mut wal, _, _) = WalSink::open(config, 0, ProtocolLimits::default()).unwrap();

    assert_zero_alloc(&mut wal);
}

/// A victim search copies only the chosen key; candidates are borrowed or pooled by position.
#[test]
fn eviction_allocations_do_not_grow_with_sample_size() {
    use ruvio::{EvictionPolicy, MemoryLimits};

    fn per_eviction(samples: usize) -> f64 {
        let mut shard = KvShard::new();
        let mut sink = NoopMutationSink;
        let mut output = Vec::with_capacity(4096);

        for index in 0..4_000u32 {
            let key = format!("evict:{index}");

            run(
                &mut shard,
                &mut sink,
                &mut output,
                HotCommand::Set(key.as_bytes(), VALUE, None),
            );
        }

        let used = shard.memory_usage().charged_bytes();

        shard.apply_memory_limits(MemoryLimits {
            max_shard_bytes: used + 1024,
            reserved_headroom_bytes: 0,
            eviction_policy: EvictionPolicy::AllKeysLru,
            maxmemory_samples: samples,
            ..MemoryLimits::default()
        });

        let before = allocations();
        let before_evicted = shard.memory_stats().evicted_keys;

        for index in 0..2_000u32 {
            let key = format!("fresh:{index}");

            run(
                &mut shard,
                &mut sink,
                &mut output,
                HotCommand::Set(key.as_bytes(), VALUE, None),
            );
        }

        let evictions = shard.memory_stats().evicted_keys - before_evicted;

        assert!(evictions > 1_000, "only {evictions} evictions");
        (allocations() - before) as f64 / evictions as f64
    }

    let few = per_eviction(1);
    let many = per_eviction(16);

    assert!(many <= few + 0.5, "16 samples: {many}, 1 sample: {few}");
}

/// Large multi-value replies stream from the store into the output buffer without
/// building an intermediate collection.
#[test]
fn smembers_and_hgetall_replies_allocate_no_intermediate_vec() {
    use std::borrow::Cow;

    use ruvio::{
        engine::{execute, write_response},
        protocol::Command,
    };

    let mut shard = KvShard::new();
    let mut output = Vec::with_capacity(1 << 20);

    for index in 0..1_000u32 {
        let member = format!("member:{index:05}").into_bytes();

        execute(
            &mut shard,
            Command::SetAdd(Cow::Borrowed(b"set"), vec![Cow::Owned(member.clone())]),
        );
        execute(
            &mut shard,
            Command::HashSet {
                key: Cow::Borrowed(b"hash"),
                pairs: vec![(Cow::Owned(member), Cow::Borrowed(VALUE))],
            },
        );
    }

    for (name, command) in [
        ("SMEMBERS", Command::SetMembers(Cow::Borrowed(b"set"))),
        ("HGETALL", Command::HashGetAll(Cow::Borrowed(b"hash"))),
    ] {
        output.clear();

        let before = allocations();
        let response = execute(&mut shard, command);

        write_response(&mut output, response).unwrap();

        let used = allocations() - before;

        assert!(output.len() > 10_000, "{name}: short reply");
        assert_eq!(
            used, 0,
            "{name}: {used} allocations for a 1000-element reply"
        );
    }
}

/// LRANGE streams from the list without an intermediate collection.
#[test]
fn lrange_replies_allocate_no_intermediate_vec() {
    use std::borrow::Cow;

    use ruvio::{
        engine::{execute, write_response},
        protocol::Command,
    };

    let mut shard = KvShard::new();
    let mut output = Vec::with_capacity(1 << 20);

    for index in 0..1_000u32 {
        let member = format!("member:{index:05}").into_bytes();

        execute(
            &mut shard,
            Command::ListPushBack(Cow::Borrowed(b"list"), vec![Cow::Owned(member)].into()),
        );
    }

    output.clear();

    let before = allocations();
    let response = execute(
        &mut shard,
        Command::ListRange(Cow::Borrowed(b"list"), 0, -1),
    );

    write_response(&mut output, response).unwrap();

    let used = allocations() - before;

    assert!(output.len() > 10_000, "LRANGE: short reply");
    assert_eq!(
        used, 0,
        "LRANGE: {used} allocations for a 1000-element reply"
    );
}

/// Tabled string commands are parsed straight from the frame: the argument spans live in a
/// reused thread-local buffer and every key/value stays borrowed.
#[test]
fn tabled_string_commands_parse_without_allocating() {
    use ruvio::protocol::{Command, parse_command};

    let frames: [&[u8]; 6] = [
        b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
        b"*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n",
        b"*3\r\n$6\r\nINCRBY\r\n$3\r\nkey\r\n$2\r\n10\r\n",
        b"*3\r\n$6\r\nAPPEND\r\n$3\r\nkey\r\n$1\r\nx\r\n",
        b"*2\r\n$6\r\nSTRLEN\r\n$3\r\nkey\r\n",
    ];

    // The first parse sizes the thread-local span buffer.
    for frame in frames {
        parse_command(frame).unwrap().unwrap();
    }

    for frame in frames {
        let before = allocations();
        let (command, consumed) = parse_command(frame).unwrap().unwrap();
        let used = allocations() - before;

        assert_eq!(consumed, frame.len());
        assert!(!matches!(command, Command::Ping(_)));
        assert_eq!(used, 0, "{:?}: {used} allocations", frame);
    }
}

/// HSET, ZADD and LPUSH build the one collection the `Command` keeps and nothing else:
/// no argument vector, and every key, field and member stays borrowed from the frame.
#[test]
fn collection_writes_allocate_only_the_vec_the_command_keeps() {
    use ruvio::protocol::parse_command;

    let frames: [&[u8]; 5] = [
        b"*6\r\n$4\r\nHSET\r\n$1\r\nh\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
        b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n1\r\n$2\r\nm1\r\n$1\r\n2\r\n$2\r\nm2\r\n",
        b"*8\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nNX\r\n$2\r\nCH\r\n$1\r\n1\r\n$2\r\nm1\r\n$1\r\n2\r\n$2\r\nm2\r\n",
        b"*4\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\na\r\n$1\r\nb\r\n",
        b"*4\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n$1\r\nb\r\n",
    ];

    for frame in frames {
        parse_command(frame).unwrap().unwrap();
    }

    for frame in frames {
        let before = allocations();
        let (_, consumed) = parse_command(frame).unwrap().unwrap();
        let used = allocations() - before;

        assert_eq!(consumed, frame.len());
        assert_eq!(used, 1, "{:?}: {used} allocations", frame);
    }
}

/// A single-element LPUSH/RPUSH and a single-pair ZADD keep their one argument inline:
/// parsing them touches no allocator.
#[test]
fn single_element_collection_writes_parse_without_allocating() {
    use ruvio::protocol::parse_command;

    let frames: [&[u8]; 4] = [
        b"*3\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\na\r\n",
        b"*3\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n",
        b"*4\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n1\r\n$2\r\nm1\r\n",
        b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nXX\r\n$2\r\nCH\r\n$1\r\n1\r\n$2\r\nm1\r\n",
    ];

    for frame in frames {
        parse_command(frame).unwrap().unwrap();
    }

    for frame in frames {
        let before = allocations();
        let (_, consumed) = parse_command(frame).unwrap().unwrap();
        let used = allocations() - before;

        assert_eq!(consumed, frame.len());
        assert_eq!(used, 0, "{:?}: {used} allocations", frame);
    }
}

/// On a warm key, a single-element push and a single-pair ZADD that updates an existing
/// member build no temporary collection in the engine.
#[test]
fn single_element_collection_writes_execute_without_temporaries() {
    use std::borrow::Cow;

    use ruvio::{engine::execute, protocol::Command};

    let mut shard = KvShard::new();
    let zadd = |score: f64| Command::SortedSetAdd {
        key: Cow::Borrowed(b"zset"),
        pairs: ruvio::protocol::OneOrMany::One((score, Cow::Borrowed(b"member"))),
        options: Default::default(),
    };
    let push = |back: bool| {
        let value = ruvio::protocol::OneOrMany::One(Cow::Borrowed(VALUE));

        if back {
            Command::ListPushBack(Cow::Borrowed(b"list"), value)
        } else {
            Command::ListPush(Cow::Borrowed(b"list"), value)
        }
    };

    for round in 0..ROUNDS {
        execute(&mut shard, push(round % 2 == 0));
        execute(&mut shard, zadd(round as f64));
    }

    let before = allocations();

    for round in 0..ROUNDS {
        execute(&mut shard, zadd((round % 7) as f64));
    }

    let zadd_used = allocations() - before;
    let before = allocations();

    for round in 0..ROUNDS {
        execute(&mut shard, push(round % 2 == 0));
    }

    let push_used = allocations() - before;

    assert_eq!(
        zadd_used, 0,
        "ZADD: {zadd_used} allocations in {ROUNDS} rounds"
    );
    // Only the list's own nodes grow, about one per a dozen elements; a per-push
    // temporary would cost at least one allocation each.
    assert!(
        push_used < ROUNDS as u64 / 2,
        "LPUSH/RPUSH: {push_used} allocations in {ROUNDS} rounds"
    );
}
