use super::*;

#[test]
fn string_value_is_not_sized_like_a_sorted_set() {
    assert_eq!(size_of::<Value>(), size_of::<BytesValue>());
    assert_eq!(size_of::<BytesValue>(), size_of::<ByteBody>());
    assert_eq!(size_of::<Entry>(), size_of::<Value>());
    assert_eq!(size_of::<ByteBody>(), 16);
    assert!(size_of::<Value>() < size_of::<SortedSet>());
}

#[test]
fn insert_replaces_and_remove_returns_owned_values() {
    let mut shard = KvShard::new();

    assert_eq!(shard.insert(b"answer", b"41"), None);
    assert_eq!(shard.insert(b"answer", b"42").as_deref(), Some(&b"41"[..]));
    assert_eq!(shard.get(b"answer"), Some(&b"42"[..]));
    assert_eq!(shard.remove(b"answer").as_deref(), Some(&b"42"[..]));
    assert!(shard.is_empty());
}

#[test]
fn bloom_filter_tracks_inserted_values() {
    let mut shard = KvShard::new();

    assert_eq!(shard.bloom_reserve(b"seen", 0.01, 1_000, 2), Ok(()));
    assert_eq!(shard.bloom_add(b"seen", b"alpha"), Ok(BloomAdd::Added));
    assert_eq!(shard.bloom_add(b"seen", b"alpha"), Ok(BloomAdd::Exists));
    assert_eq!(shard.bloom_contains(b"seen", b"alpha"), Ok(true));
    assert_eq!(shard.bloom_contains(b"seen", b"missing"), Ok(false));
}

#[test]
fn reports_wrong_type_without_replacing_data() {
    let mut shard = KvShard::new();

    shard.insert(b"key", b"value");

    assert_eq!(
        shard.list_push_front(b"key", &[b"item".to_vec()]),
        Err(WrongType)
    );
    assert_eq!(shard.get(b"key"), Some(&b"value"[..]));
}

#[test]
fn increments_missing_and_existing_integer_values() {
    let mut shard = KvShard::new();

    assert_eq!(shard.increment(b"counter", 1), Ok(1));
    assert_eq!(shard.increment(b"counter", 1), Ok(2));
    assert_eq!(shard.increment(b"counter", -1), Ok(1));
    assert_eq!(shard.copied_value(b"counter").as_deref(), Some(&b"1"[..]));
}

#[test]
fn rename_moves_a_value_and_its_deadline() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock);

    shard.set_bytes(b"old", b"value");
    assert!(shard.expire_at(b"old", UnixMillis::new(2_000)));
    shard.set_bytes(b"dest", b"replaced");

    assert!(shard.rename(b"old", b"dest"));
    assert_eq!(shard.get(b"old"), None);
    assert_eq!(shard.get(b"dest"), Some(&b"value"[..]));
    assert_eq!(shard.expires_at(b"dest"), Some(UnixMillis::new(2_000)));
}

#[test]
fn increment_preserves_an_existing_deadline() {
    let clock = TestClock::new(10);
    let mut shard = KvShard::with_clock(clock);

    shard.set_bytes(b"counter", b"1");
    assert!(shard.expire_at(b"counter", UnixMillis::new(2_000)));

    assert_eq!(shard.increment(b"counter", 1), Ok(2));
    assert_eq!(shard.copied_value(b"counter").as_deref(), Some(&b"2"[..]));
    assert_eq!(shard.expires_at(b"counter"), Some(UnixMillis::new(2_000)));
}

#[test]
fn failed_increment_does_not_modify_the_value() {
    let mut shard = KvShard::new();

    shard.insert(b"text", b"not-an-integer");
    shard.insert(b"maximum", i64::MAX.to_string().as_bytes());
    shard.insert(b"minimum", i64::MIN.to_string().as_bytes());
    shard.list_push_front(b"list", &[b"item".to_vec()]).unwrap();

    assert_eq!(
        shard.increment(b"text", 1),
        Err(IncrementError::NotIntegerOrOverflow)
    );
    assert_eq!(
        shard.increment(b"maximum", 1),
        Err(IncrementError::NotIntegerOrOverflow)
    );
    assert_eq!(
        shard.increment(b"minimum", -1),
        Err(IncrementError::NotIntegerOrOverflow)
    );
    assert_eq!(shard.increment(b"list", 1), Err(IncrementError::WrongType));
    assert_eq!(shard.get(b"text"), Some(&b"not-an-integer"[..]));
    assert_eq!(shard.get(b"maximum"), Some(i64::MAX.to_string().as_bytes()));
    assert_eq!(shard.get(b"minimum"), Some(i64::MIN.to_string().as_bytes()));
}

#[test]
fn recycles_deleted_byte_buffers_within_a_bounded_pool() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"first", &[7; 4_096]);
    assert!(shard.delete(b"first"));
    assert!(shard.pooled_value_bytes() >= 4_096);

    shard.set_bytes(b"second", &[8; 4_096]);
    assert_eq!(shard.pooled_value_bytes(), 0);
    assert_eq!(shard.get(b"second").unwrap(), &[8; 4_096]);
}

#[test]
fn byte_pool_enforces_class_and_size_limits() {
    let mut shard = KvShard::new();

    for index in 0..(MAX_BUFFERS_PER_CLASS + 8) {
        shard.set_bytes(format!("key:{index}").as_bytes(), &[1; 1_024]);
    }

    for index in 0..(MAX_BUFFERS_PER_CLASS + 8) {
        assert!(shard.delete(format!("key:{index}").as_bytes()));
    }

    assert_eq!(shard.pooled_value_bytes(), MAX_BUFFERS_PER_CLASS * 1_024);

    shard.set_bytes(b"large", &[2; MAX_POOLED_VALUE_BYTES + 1]);
    assert!(shard.delete(b"large"));
    assert_eq!(shard.pooled_value_bytes(), MAX_BUFFERS_PER_CLASS * 1_024);
}

#[test]
fn visiting_the_ends_of_a_missing_key_or_another_type() {
    let mut shard = KvShard::with_clock(TestClock::new(1));
    let mut seen = 0;

    assert_eq!(
        shard.sorted_set_visit_ends(b"none", 3, false, |_, _| seen += 1),
        Ok(())
    );
    shard.set_bytes(b"s", b"v");
    assert_eq!(
        shard.sorted_set_visit_ends(b"s", 3, true, |_, _| seen += 1),
        Err(WrongType)
    );
    assert_eq!(shard.sorted_set_remove_popped(b"none", &[b"m"]), Ok(0));
    assert_eq!(seen, 0);
}

#[test]
fn a_key_larger_than_a_log_page_round_trips() {
    // A key record over one page takes the large-span path, whose offsets are
    // tagged block ids rather than byte addresses.
    let mut shard = KvShard::new();
    let key = vec![b'k'; 5_000];

    assert_eq!(shard.insert(&key, b"value"), None);
    assert_eq!(shard.get(&key), Some(&b"value"[..]));
    assert_eq!(shard.remove(&key).as_deref(), Some(&b"value"[..]));
    assert!(shard.is_empty());
}

#[test]
fn a_key_at_the_header_limit_round_trips() {
    let mut shard = KvShard::new();
    let key = vec![b'k'; usize::from(u16::MAX)];

    assert_eq!(shard.insert(&key, b"value"), None);
    assert_eq!(shard.get(&key), Some(&b"value"[..]));
    assert_eq!(shard.remove(&key).as_deref(), Some(&b"value"[..]));
    assert!(shard.is_empty());
}
