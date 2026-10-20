use super::*;

const GENERATION: u32 = 48;

#[test]
fn lease_tokens_only_grow_across_expiry_delete_and_generations() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let grant = |shard: &mut KvShard<TestClock>| {
        let (token, expires_at) = shard.lease_plan(b"lock", 10).unwrap().unwrap();

        shard
            .lease_put(b"lock", token, expires_at, "LEASE")
            .unwrap();
        token
    };
    let first = grant(&mut shard);

    // Expired and granted again in the same millisecond: still larger.
    clock.set(1_010);
    let second = grant(&mut shard);

    assert!(second > first);
    shard.delete(b"lock");
    let third = grant(&mut shard);

    assert!(
        third > second,
        "a deleted lease does not restart its tokens"
    );
    shard.set_fence_generation(3);
    clock.set(2_000);
    let fourth = grant(&mut shard);

    assert_eq!(fourth >> GENERATION, 3);
    assert!(fourth > third);

    // A generation never goes down.
    shard.set_fence_generation(1);
    shard.delete(b"lock");

    assert_eq!(grant(&mut shard) >> GENERATION, 3);
}

#[test]
fn a_replayed_token_of_a_later_generation_is_taken_over_when_that_generation_is_set() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    // A log replayed before the shard read its epoch: the counter ran ahead of the clock.
    let replayed = (5 << GENERATION) | (5_000 << 6);

    shard.lease_put(b"lock", replayed, 2_000, "LEASE").unwrap();
    shard.delete(b"lock");
    shard.set_fence_generation(5);
    let (token, _) = shard.lease_plan(b"lock", 10).unwrap().unwrap();

    assert_eq!(token, replayed + 1);
}

#[test]
fn semaphore_tokens_of_a_recreated_key_are_larger_than_the_old_ones() {
    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let (first, expires_at, _) = shard.semaphore_plan(b"jobs", 2, 10).unwrap().unwrap();

    shard.semaphore_put(b"jobs", first, expires_at).unwrap();
    shard.semaphore_release(b"jobs", first).unwrap();
    let (second, _, fresh) = shard.semaphore_plan(b"jobs", 2, 10).unwrap().unwrap();

    assert!(fresh);
    assert!(second > first);
}
