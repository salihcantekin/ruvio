//! A client that follows a failover (two seeds, `follow_primary` on) allocates exactly what a
//! single-seed client allocates per command while the connection is healthy, and keeps no state
//! that grows with the number of commands. Runs against the failover pair that
//! `scripts/run_with_servers.sh` starts (`RUVIO_FAILOVER_ADDRS`, primary first); skipped without it.
//!
//! This binary has its own counting global allocator. Everything lives in one test so no other
//! test thread allocates while a loop is measured.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use ruvio_client::{Client, ClientOptions};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicI64 = AtomicI64::new(0);

fn count(size: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(size as u64, Ordering::Relaxed);
    LIVE.fetch_add(size as i64, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());

        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());

        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);

        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        LIVE.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);

        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const WARMUP: usize = 500;
const ROUNDS: usize = 2_000;

/// Allocations, bytes and net live bytes of `rounds` calls of `operation`, after a warm-up.
fn measure(mut operation: impl FnMut()) -> (u64, u64, i64) {
    for _ in 0..WARMUP {
        operation();
    }

    let before = (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
        LIVE.load(Ordering::Relaxed),
    );

    for _ in 0..ROUNDS {
        operation();
    }

    (
        ALLOCATIONS.load(Ordering::Relaxed) - before.0,
        BYTES.load(Ordering::Relaxed) - before.1,
        LIVE.load(Ordering::Relaxed) - before.2,
    )
}

fn connect(seeds: &[(String, u16)]) -> Client {
    Client::connect_with(ClientOptions {
        seeds: seeds.to_vec(),
        follow_primary: true,
        discover_cluster: false,
        command_timeout: Some(Duration::from_secs(5)),
        ..ClientOptions::default()
    })
    .expect("connect")
}

fn seeds() -> Option<Vec<(String, u16)>> {
    let value = std::env::var("RUVIO_FAILOVER_ADDRS").ok()?;
    let seeds: Vec<_> = value
        .split(',')
        .map(|part| {
            let (host, port) = part.rsplit_once(':').expect("host:port");

            (host.to_owned(), port.parse().expect("port"))
        })
        .collect();

    assert!(seeds.len() >= 2, "RUVIO_FAILOVER_ADDRS needs two addresses");

    Some(seeds)
}

#[test]
fn following_client_allocates_like_a_single_seed_client() {
    let Some(seeds) = seeds() else {
        eprintln!("skipped: RUVIO_FAILOVER_ADDRS is not set");

        return;
    };

    // The first seed is the primary; both clients talk to it.
    let mut single = connect(&seeds[..1]);
    let mut following = connect(&seeds);
    let value = "v".repeat(32);
    let key = "follow:alloc";
    let batch: Vec<Vec<&str>> = (0..16)
        .map(|index| {
            if index % 2 == 0 {
                vec!["SET", key, value.as_str()]
            } else {
                vec!["GET", key]
            }
        })
        .collect();
    let batch: Vec<&[&str]> = batch.iter().map(Vec::as_slice).collect();

    let set_get = |client: &mut Client| {
        client.set(key, &value).unwrap();
        assert!(client.get(key).unwrap().is_some());
    };
    let pipelined = |client: &mut Client| {
        client.execute_many(&batch).unwrap();
    };

    // Alternate the two so any one-off allocator state is shared; the counts are deterministic
    // for a given code path, so no tolerance is needed.
    let single_loop = measure(|| set_get(&mut single));
    let following_loop = measure(|| set_get(&mut following));
    let single_batch = measure(|| pipelined(&mut single));
    let following_batch = measure(|| pipelined(&mut following));

    eprintln!(
        "SET+GET   single {:.2} alloc {:.1} B per pair; following {:.2} alloc {:.1} B per pair",
        single_loop.0 as f64 / ROUNDS as f64,
        single_loop.1 as f64 / ROUNDS as f64,
        following_loop.0 as f64 / ROUNDS as f64,
        following_loop.1 as f64 / ROUNDS as f64,
    );
    eprintln!(
        "pipeline  single {:.2} alloc {:.1} B per batch of 16; following {:.2} alloc {:.1} B",
        single_batch.0 as f64 / ROUNDS as f64,
        single_batch.1 as f64 / ROUNDS as f64,
        following_batch.0 as f64 / ROUNDS as f64,
        following_batch.1 as f64 / ROUNDS as f64,
    );
    eprintln!(
        "live bytes after {ROUNDS} rounds: single {} {}, following {} {}",
        single_loop.2, single_batch.2, following_loop.2, following_batch.2
    );

    assert!(single_loop.0 > 0, "the counting allocator is not in use");
    assert_eq!(
        following_loop.0, single_loop.0,
        "following allocates per SET+GET pair"
    );
    assert_eq!(following_loop.1, single_loop.1, "following allocates bytes");
    assert_eq!(
        following_batch.0, single_batch.0,
        "following allocates per pipeline"
    );
    assert_eq!(
        following_batch.1, single_batch.1,
        "following allocates bytes per pipeline"
    );

    // The follow state does not grow: the bytes still held after thousands of commands equal
    // zero (measured 0 for both clients); 64 bytes of slack covers allocator noise.
    assert!(
        following_loop.2.abs() <= 64 && following_batch.2.abs() <= 64,
        "retained bytes grew while following: {} / {}",
        following_loop.2,
        following_batch.2
    );
}
