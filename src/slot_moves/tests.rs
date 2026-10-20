use super::*;

/// A key whose hash slot is `slot`.
fn key_in_slot(slot: u16) -> Vec<u8> {
    format!("{{{}}}", tag_for_slot(slot)).into_bytes()
}

fn tag_for_slot(slot: u16) -> String {
    (0u32..)
        .map(|n| format!("t{n}"))
        .find(|tag| hash_slot(tag.as_bytes()) == slot)
        .unwrap()
}

fn set(key: &[u8]) -> OwnedMutation {
    OwnedMutation::SetBytes {
        key: key.to_vec(),
        value: b"v".to_vec(),
    }
}

/// Slot 100 (shard 0 of 2 by the formula) moved to shard 1: its copy began at shard 1's
/// sequence 50, and shard 0's first sequence after the hand-over was 70.
fn moved_once() -> SlotMoves {
    let mut moves = SlotMoves::new(2);

    moves.begin(100, 100, 1, 50).unwrap();
    moves.complete(70).unwrap();
    moves
}

#[test]
fn round_trips_and_refuses_damage_or_another_shard_count() {
    let dir = tempfile::tempdir().unwrap();
    let mut moves = moved_once();

    moves.begin(9000, 9100, 0, 80).unwrap();
    moves.save(dir.path()).unwrap();

    assert_eq!(SlotMoves::load(dir.path(), 2).unwrap(), Some(moves.clone()));
    assert_eq!(
        SlotMoves::load(dir.path(), 4).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );

    let path = dir.path().join(FILE_NAME);
    let mut bytes = std::fs::read(&path).unwrap();

    for at in [0, HEADER_LEN, bytes.len() - 1] {
        bytes[at] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            SlotMoves::load(dir.path(), 2).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "byte {at}"
        );
        bytes[at] ^= 1;
    }

    std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
    assert!(SlotMoves::load(dir.path(), 2).is_err());
}

#[test]
fn a_missing_file_means_the_formula_and_a_leftover_temporary_file_is_ignored() {
    let dir = tempfile::tempdir().unwrap();

    assert_eq!(SlotMoves::load(dir.path(), 2).unwrap(), None);

    // A crash between writing the temporary file and renaming it keeps the old table.
    moved_once().save(dir.path()).unwrap();
    std::fs::write(dir.path().join("SLOTS.tmp"), b"half written").unwrap();
    assert_eq!(SlotMoves::load(dir.path(), 2).unwrap(), Some(moved_once()));
}

#[test]
fn only_completed_moves_change_owners() {
    let mut moves = SlotMoves::new(2);
    let table = SlotTable::from_formula(2);

    moves.begin(100, 199, 1, 10).unwrap();
    assert_eq!(moves.owners()[150], 0);

    moves.complete(20).unwrap();
    moves.apply(&table);
    assert_eq!(
        table.ranges()[..3],
        [(0, 99, 0), (100, 199, 1), (200, 8191, 0)]
    );

    assert_eq!(moves.complete(30), Err("no move in progress"));
    assert_eq!(
        moves.begin(150, 250, 0, 5),
        Err("slot range has more than one owner")
    );
    assert_eq!(
        moves.begin(100, 199, 1, 5),
        Err("slot range is already on that shard")
    );
    assert_eq!(moves.begin(100, 199, 2, 5), Err("no such shard"));
    assert_eq!(moves.begin(200, 100, 1, 5), Err("invalid slot range"));
}

#[test]
fn a_shard_no_move_concerns_needs_no_filter() {
    let mut moves = SlotMoves::new(4);

    moves.begin(0, 10, 1, 5).unwrap();
    moves.complete(9).unwrap();

    assert!(moves.replay_filter(2, 0).is_none());
    assert!(moves.replay_filter(0, 0).is_some());
}

#[test]
fn the_source_replays_the_range_then_drops_it() {
    let moved = key_in_slot(100);
    let stays = key_in_slot(101);
    let mut filter = moved_once().replay_filter(0, 0).unwrap();
    let mut drops = Vec::new();

    for sequence in 1..70 {
        assert!(
            filter.keeps_record(sequence, &set(&moved), &mut |s: &[bool]| drops
                .push(s.to_vec()))
        );
    }
    assert!(filter.keeps_record(70, &set(&stays), &mut |s: &[bool]| drops.push(s.to_vec())));
    // Records after the hand-over cannot name the range; a stray one is not applied.
    assert!(!filter.keeps_record(71, &set(&moved), &mut |s: &[bool]| drops.push(s.to_vec())));
    assert!(drops.is_empty());

    let stale = filter.stale_slots().unwrap();

    assert!(stale[100]);
    assert_eq!(stale.iter().filter(|slot| **slot).count(), 1);
}

#[test]
fn the_target_skips_copies_of_a_move_that_did_not_complete() {
    let mut moves = moved_once();

    moves.begin(200, 200, 1, 90).unwrap();

    let copy = key_in_slot(200);
    let mut filter = moves.replay_filter(1, 0).unwrap();
    let mut none = |_: &[bool]| panic!("nothing to drop");

    assert!(!filter.keeps_record(95, &set(&copy), &mut none));
    assert!(!filter.keeps_entry(&copy));
    assert!(filter.stale_slots().is_none());
}

#[test]
fn the_target_keeps_the_range_from_the_first_copy_record() {
    let moved = key_in_slot(100);
    let mut filter = moved_once().replay_filter(1, 0).unwrap();
    let mut none = |_: &[bool]| panic!("nothing to drop");

    assert!(!filter.keeps_record(49, &set(&moved), &mut none));
    assert!(filter.keeps_record(50, &set(&moved), &mut none));
    assert!(filter.keeps_record(80, &set(&moved), &mut none));
    assert!(filter.stale_slots().is_none());
}

#[test]
fn checkpoint_entries_follow_ownership_when_the_checkpoint_was_taken() {
    let moved = key_in_slot(100);

    // Source, checkpoint before the hand-over: the range is loaded, then dropped.
    let mut filter = moved_once().replay_filter(0, 69).unwrap();

    assert!(filter.keeps_entry(&moved));
    assert!(filter.stale_slots().unwrap()[100]);

    // Source, checkpoint after it: keys not cleaned up yet are skipped.
    let mut filter = moved_once().replay_filter(0, 70).unwrap();

    assert!(!filter.keeps_entry(&moved));
    assert!(filter.stale_slots().is_none());

    // Target, checkpoint during the copy of the completed move, and before it.
    assert!(
        moved_once()
            .replay_filter(1, 50)
            .unwrap()
            .keeps_entry(&moved)
    );
    assert!(
        !moved_once()
            .replay_filter(1, 49)
            .unwrap()
            .keeps_entry(&moved)
    );
}

#[test]
fn a_range_that_comes_back_drops_what_the_earlier_stay_left() {
    // Slot 100 goes 0 -> 1 (shard 0 hands over at 70), then 1 -> 0 (copy into shard 0
    // from its sequence 90). Keys deleted while shard 1 owned it must not come back.
    let mut moves = moved_once();

    moves.begin(100, 100, 0, 90).unwrap();
    moves.complete(120).unwrap();

    let moved = key_in_slot(100);
    let mut filter = moves.replay_filter(0, 0).unwrap();
    let mut drops: Vec<Vec<bool>> = Vec::new();

    assert!(filter.keeps_record(10, &set(&moved), &mut |s: &[bool]| drops.push(s.to_vec())));
    assert!(filter.keeps_record(90, &set(&moved), &mut |s: &[bool]| drops.push(s.to_vec())));
    assert_eq!(
        drops.len(),
        1,
        "the earlier stay is dropped before the copy"
    );
    assert!(drops[0][100]);
    assert!(filter.stale_slots().is_none());
}

#[test]
fn cross_slot_records_follow_their_first_key_and_keyless_records_always_apply() {
    let moved = key_in_slot(100);
    let stays = key_in_slot(101);
    let mut filter = moved_once().replay_filter(0, 0).unwrap();
    let mut none = |_: &[bool]| {};
    let rename = OwnedMutation::Rename {
        source: moved.clone(),
        destination: stays.clone(),
    };

    // Before the hand-over shard 0 owned both: the rename replays, so `stays` gets the
    // value, and slot 100's keys are dropped at the end.
    assert!(filter.keeps_record(5, &rename, &mut none));
    assert!(filter.keeps_record(200, &OwnedMutation::FlushAll, &mut none));
    assert!(filter.stale_slots().unwrap()[100]);
}

/// Moves are refused on a restart with another starting table than they were recorded
/// against: an old (formula) `SLOTS` file under a new cluster map, and moves recorded under a
/// map on a restart without it or with other local slots.
#[test]
fn moves_saved_against_another_starting_table_are_refused() {
    let formula: Vec<u16> = (0..HASH_SLOT_COUNT)
        .map(|slot| shard_for_slot(slot as u16, 2) as u16)
        .collect();
    // A map: this node serves 0-8191 over two shards, 8192 and up is another node (entry 3).
    let map: Vec<u16> = (0..HASH_SLOT_COUNT)
        .map(|slot| {
            if slot < 4096 {
                0
            } else if slot < 8192 {
                1
            } else {
                3
            }
        })
        .collect();
    let other_map: Vec<u16> = (0..HASH_SLOT_COUNT)
        .map(|slot| {
            if slot < 2048 {
                0
            } else if slot < 8192 {
                1
            } else {
                4
            }
        })
        .collect();

    // Saved against the formula.
    let dir = tempfile::tempdir().unwrap();

    moved_once().save(dir.path()).unwrap();
    assert!(!dir.path().join(BASE_FILE_NAME).exists());
    SlotMoves::check_base(dir.path(), 2, None).unwrap();
    SlotMoves::check_base(dir.path(), 2, Some(&formula)).unwrap();
    assert!(SlotMoves::check_base(dir.path(), 2, Some(&map)).is_err());

    // Saved against a map: only the same local layout passes (other nodes' entries may differ).
    let dir = tempfile::tempdir().unwrap();
    let mut remapped = map.clone();

    remapped[9000] = 7;
    moved_once()
        .with_base(map.clone().into())
        .save(dir.path())
        .unwrap();
    SlotMoves::check_base(dir.path(), 2, Some(&map)).unwrap();
    SlotMoves::check_base(dir.path(), 2, Some(&remapped)).unwrap();
    assert!(SlotMoves::check_base(dir.path(), 2, None).is_err());
    assert!(SlotMoves::check_base(dir.path(), 2, Some(&other_map)).is_err());

    // Saved again without a map: the base file goes away.
    moved_once().save(dir.path()).unwrap();
    assert!(!dir.path().join(BASE_FILE_NAME).exists());
}
