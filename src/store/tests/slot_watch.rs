use super::*;
use crate::routing::hash_slot;

/// A key of `label` whose slot is in `first..=last`, or outside it.
fn key(label: &str, inside: bool) -> Vec<u8> {
    (0..)
        .map(|n| format!("{label}{n}").into_bytes())
        .find(|key| (0..=99).contains(&hash_slot(key)) == inside)
        .unwrap()
}

#[test]
fn a_watched_range_notes_writes_deletes_and_flushes_of_its_keys_only() {
    let mut shard = KvShard::new();
    let (inside, other, outside) = (key("in", true), key("list", true), key("out", false));

    shard.set_bytes(&inside, b"v");
    shard.watch_slots(0, 99);
    assert!(shard.observing);

    shard.set_bytes(&outside, b"v");
    shard.set_bytes(&inside, b"w");
    shard.list_push_back(&other, &[b"a"]).unwrap();
    let mut changes = shard.take_slot_changes();

    changes.keys.sort();
    let mut expected = vec![
        inside.clone().into_boxed_slice(),
        other.clone().into_boxed_slice(),
    ];

    expected.sort();
    assert_eq!(changes.keys, expected);
    assert!(!changes.rewritten);

    shard.delete(&inside);
    assert_eq!(
        shard.take_slot_changes().keys,
        vec![inside.clone().into_boxed_slice()]
    );
    assert!(shard.take_slot_changes().keys.is_empty());

    shard.flush_all();
    assert!(shard.take_slot_changes().rewritten);

    shard.unwatch_slots();
    assert!(!shard.observing);
    shard.set_bytes(&inside, b"v");
    assert!(shard.take_slot_changes().keys.is_empty());
}

#[test]
fn a_watch_and_a_slot_watch_share_the_gate() {
    let mut shard = KvShard::new();

    shard.watch(1, 0, &[b"k"]);
    shard.watch_slots(0, 99);
    shard.unwatch(1);
    assert!(shard.observing, "the slot watch still needs the hook");
    shard.unwatch_slots();
    assert!(!shard.observing);
}

#[test]
fn key_frames_cover_every_database_and_forget_key_is_quiet() {
    let mut shard = KvShard::new();
    let inside = key("in", true);

    shard.set_bytes(&inside, b"zero");
    shard.enter(3);
    shard.set_bytes(&inside, b"three");
    shard.enter(0);

    let mut frames = Vec::new();

    assert_eq!(shard.encode_key_frames(&inside, &mut frames).unwrap(), 2);
    let length = u32::from_le_bytes(frames[..4].try_into().unwrap()) as usize;
    let first = crate::persistence::decode_entry_frame(&frames[..length]).unwrap();
    let second = crate::persistence::decode_entry_frame(&frames[length..]).unwrap();

    assert_eq!((first.database, second.database), (0, 3));
    assert_eq!(first.key, inside);

    shard.watch_slots(0, 99);
    assert!(shard.forget_key(3, &inside));
    assert!(!shard.forget_key(3, &inside));
    assert_eq!(shard.active_database(), 0);
    assert_eq!(
        shard.encode_key_frames(&inside, &mut Vec::new()).unwrap(),
        1
    );
}

#[test]
fn walk_slots_visits_every_key_of_the_range_in_steps() {
    let mut shard = KvShard::new();
    let mut inside = Vec::new();

    for n in 0..2000 {
        let key = format!("k{n}").into_bytes();

        if (0..=999).contains(&hash_slot(&key)) {
            inside.push(key.clone());
        }
        shard.set_bytes(&key, b"v");
    }

    let mut seen = std::collections::BTreeSet::new();
    let mut cursor = 0;

    loop {
        cursor = shard.walk_slots(0, 0, 999, cursor, 16, &mut |key| {
            seen.insert(key.to_vec());
        });
        if cursor == 0 {
            break;
        }
    }

    inside.sort();
    assert_eq!(seen.into_iter().collect::<Vec<_>>(), inside);
}
