use super::*;

#[test]
fn ids_are_written_as_decimal_millis_and_sequence() {
    for (millis, sequence) in [
        (0, 0),
        (1, 9),
        (10, 100),
        (1_760_000_000_000, 7),
        (u64::MAX, u64::MAX),
    ] {
        assert_eq!(
            format_id(millis, sequence),
            format!("{millis}-{sequence}").into_bytes()
        );
    }
}

#[test]
fn packed_fields_keep_every_pair_inline_or_on_the_heap() {
    let long = vec![b'x'; 300];
    let cases: [&[(&[u8], &[u8])]; 4] = [
        &[(b"f", b"v")],
        &[(b"", b""), (b"temp", b"23.5")],
        &[(b"payload", &long), (b"k", b"")],
        &[
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
            (b"e", b"5"),
        ],
    ];

    for pairs in cases {
        let fields = StreamFields::from_pairs(pairs.iter().copied());

        assert_eq!(fields.iter().collect::<Vec<_>>(), pairs);
        assert_eq!(fields.len(), pairs.len());
        assert_eq!(
            fields.payload(),
            pairs.iter().map(|(f, v)| f.len() + v.len()).sum::<usize>()
        );
    }

    assert_eq!(
        StreamFields::from_pairs([(&b"f"[..], &b"v"[..])].into_iter()).heap_bytes(),
        0
    );
    assert_eq!(size_of::<StreamEntry>(), 40);
}

#[test]
fn auto_ids_increase_and_trim_keeps_the_tail() {
    let mut stream = StreamValue::new();
    let (millis, sequence) = stream.next_auto_id(10);

    stream
        .append(millis, sequence, &[(b"f".as_slice(), b"a".as_slice())])
        .unwrap();
    let (millis, sequence) = stream.next_auto_id(10);

    stream
        .append(millis, sequence, &[(b"f".as_slice(), b"b".as_slice())])
        .unwrap();
    stream.trim(1);

    assert_eq!(stream.len(), 1);
    assert_eq!(stream.entries().to_vec()[0].sequence, 1);
    assert!(
        stream
            .append(10, 1, &[(b"f".as_slice(), b"c".as_slice())])
            .is_err()
    );
}

#[test]
fn payload_and_id_lookup_stay_cheap_as_the_stream_grows() {
    let mut stream = StreamValue::new();
    let field = b"f".as_slice();
    let value = b"0123456789abcdef".as_slice();
    let started = std::time::Instant::now();

    for sequence in 0..8_000 {
        stream.append(1, sequence, &[(field, value)]).unwrap();
        assert!(stream.contains(1, sequence));
    }

    assert!(stream.contains(1, 7_999));
    assert!(stream.contains(1, 0));
    assert!(!stream.contains(1, 8_000));
    assert_eq!(
        stream
            .range_page(
                StreamBound::Id(1, 10),
                StreamBound::Id(1, 19),
                usize::MAX,
                false
            )
            .len(),
        10
    );
    assert!(started.elapsed() < std::time::Duration::from_millis(400));
}

#[test]
fn mutations_keep_the_payload_counter_exact() {
    let mut stream = StreamValue::new();

    stream
        .append(1, 0, &[(b"field".as_slice(), b"value".as_slice())])
        .unwrap();
    stream
        .append(1, 1, &[(b"field".as_slice(), b"other".as_slice())])
        .unwrap();
    stream.trim(1);
    assert!(!stream.delete(1, 0));
    assert!(stream.delete(1, 1));
    stream
        .append(2, 0, &[(b"n".as_slice(), b"x".as_slice())])
        .unwrap();
    stream.create_group(b"workers", 0, 0).unwrap();
    stream.claim(b"workers", b"ada", 2, 0, 5).unwrap();
    stream
        .reclaim(
            b"workers",
            b"bea",
            ReclaimUpdate {
                millis: 2,
                sequence: 0,
                delivered_at: 6,
                deliveries: 2,
            },
        )
        .unwrap();
    stream.ack(b"workers", 2, 0).unwrap();
    stream.create_group(b"other", 0, 0).unwrap();
    stream.destroy_group(b"other");
    // Like Redis, a consumer outlives its acknowledged and reclaimed entries.
    assert_eq!(
        stream.payload_bytes(),
        b"workers".len() + b"ada".len() + b"bea".len()
    );
    assert_eq!(stream.payload_bytes(), walked_payload(&stream));
    assert_eq!(stream.read_after(StreamCursor::After(2, 0), 10).len(), 0);
    assert_eq!(
        stream
            .range_page(StreamBound::Min, StreamBound::Max, usize::MAX, false)
            .len(),
        1
    );
}

#[test]
fn a_consumer_stays_seen_at_its_last_delivery_after_acknowledging_it() {
    let mut stream = StreamValue::new();

    stream
        .append(1, 0, &[(b"f".as_slice(), b"v".as_slice())])
        .unwrap();
    stream.create_group(b"g", 0, 0).unwrap();
    stream.create_consumer(b"g", b"new").unwrap();
    stream.claim(b"g", b"ada", 1, 0, 5).unwrap();
    stream.ack(b"g", 1, 0).unwrap();

    let rows = stream.consumer_overviews(b"g", 12).unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| (row.name.as_slice(), row.pending, row.idle))
            .collect::<Vec<_>>(),
        [(b"ada".as_slice(), 0, 7), (b"new".as_slice(), 0, 0)]
    );
}

fn walked_payload(stream: &StreamValue) -> usize {
    stream.groups().iter().fold(0usize, |total, group| {
        let mut names: Vec<&[u8]> = group
            .pending
            .iter()
            .map(|delivery| delivery.consumer.as_slice())
            .chain(group.idle.iter().map(Vec::as_slice))
            .collect();

        names.sort_unstable();
        names.dedup();
        total.saturating_add(group.name.len()).saturating_add(
            names
                .iter()
                .fold(0usize, |sum, name| sum.saturating_add(name.len())),
        )
    })
}

#[test]
fn a_pending_row_costs_no_more_than_it_is_charged() {
    // The group's row and the owner's id, as `Group::put` stores them.
    let per_row = |order: &[u64], acked: &[u64]| {
        let mut rows = IdMap::new();
        let mut owned = IdMap::new();

        for &sequence in order {
            rows.insert(
                (1, sequence),
                Delivery {
                    consumer: 0,
                    delivered_at: 0,
                    deliveries: 1,
                },
            );
            owned.insert((1, sequence), ());
        }
        for &sequence in acked {
            rows.remove(&(1, sequence));
            owned.remove(&(1, sequence));
        }

        (rows.heap_bytes() + owned.heap_bytes()) / rows.len()
    };
    let in_order: Vec<u64> = (0..20_000).collect();
    // A fixed odd stride visits every id once in a scattered order.
    let scattered: Vec<u64> = (0..20_000u64).map(|i| i * 7_919 % 20_000).collect();
    let every_other: Vec<u64> = scattered.iter().copied().filter(|i| i % 2 == 0).collect();
    // Just under half of each full run: too many left to merge with a neighbour.
    let most_of_each_run: Vec<u64> = (0..20_000u64).filter(|i| i % 32 < 15).collect();
    let a_third_of_each_run: Vec<u64> = (0..20_000u64).filter(|i| i % 32 < 10).collect();
    // All but one in 64, in a scattered order: runs disappear by merging.
    let all_but_few: Vec<u64> = scattered.iter().copied().filter(|i| i % 64 != 0).collect();

    for bytes in [
        per_row(&in_order, &[]),
        per_row(&scattered, &[]),
        per_row(&in_order, &every_other),
        per_row(&scattered, &every_other),
        per_row(&in_order, &most_of_each_run),
        per_row(&scattered, &most_of_each_run),
        per_row(&in_order, &a_third_of_each_run),
        per_row(&in_order, &all_but_few),
        per_row(&scattered, &all_but_few),
    ] {
        assert!(bytes <= PENDING_ROW_BYTES, "{bytes} bytes per row");
    }
}

mod model {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap as Map;

    type Id = (u64, u64);

    /// The plain form the stream must agree with: entries and pending rows in vectors,
    /// scanned linearly.
    #[derive(Default)]
    struct Model {
        entries: Vec<(Id, Vec<u8>)>,
        groups: Map<Vec<u8>, ModelGroup>,
    }

    #[derive(Default)]
    struct ModelGroup {
        last: Id,
        /// `(id, consumer, delivered_at, deliveries)`, sorted by id.
        pending: Vec<(Id, Vec<u8>, u64, u64)>,
        /// Each consumer with the latest time it was handed an entry.
        consumers: Map<Vec<u8>, Option<u64>>,
    }

    impl ModelGroup {
        fn put(&mut self, id: Id, consumer: &[u8], at: u64, deliveries: u64) {
            let seen = self.consumers.entry(consumer.to_vec()).or_default();

            *seen = Some(seen.map_or(at, |seen| seen.max(at)));
            match self.pending.binary_search_by(|row| row.0.cmp(&id)) {
                Ok(index) => self.pending[index] = (id, consumer.to_vec(), at, deliveries),
                Err(index) => self
                    .pending
                    .insert(index, (id, consumer.to_vec(), at, deliveries)),
            }
        }
    }

    #[derive(Clone, Debug)]
    enum Op {
        Add {
            step: u64,
            value: u8,
        },
        Trim(usize),
        TrimBefore(usize),
        Delete(usize),
        CreateGroup(u8),
        DestroyGroup(u8),
        Read {
            group: u8,
            consumer: u8,
            count: usize,
        },
        Ack {
            group: u8,
            pick: usize,
        },
        Claim {
            group: u8,
            consumer: u8,
            pick: usize,
        },
        Autoclaim {
            group: u8,
            consumer: u8,
            pick: usize,
            min_idle: u64,
            count: usize,
        },
        CreateConsumer {
            group: u8,
            consumer: u8,
        },
        DeleteConsumer {
            group: u8,
            consumer: u8,
        },
        Tick(u64),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            6 => (0u64..3, any::<u8>()).prop_map(|(step, value)| Op::Add { step, value }),
            1 => (0usize..40).prop_map(Op::Trim),
            1 => (0usize..60).prop_map(Op::TrimBefore),
            1 => (0usize..60).prop_map(Op::Delete),
            1 => (0u8..3).prop_map(Op::CreateGroup),
            1 => (0u8..3).prop_map(Op::DestroyGroup),
            4 => (0u8..3, 0u8..4, 1usize..6)
                .prop_map(|(group, consumer, count)| Op::Read { group, consumer, count }),
            3 => (0u8..3, 0usize..60).prop_map(|(group, pick)| Op::Ack { group, pick }),
            2 => (0u8..3, 0u8..4, 0usize..60)
                .prop_map(|(group, consumer, pick)| Op::Claim { group, consumer, pick }),
            2 => (0u8..3, 0u8..4, 0usize..60, 0u64..4, 1usize..5).prop_map(
                |(group, consumer, pick, min_idle, count)| Op::Autoclaim {
                    group,
                    consumer,
                    pick,
                    min_idle,
                    count
                }
            ),
            1 => (0u8..3, 0u8..4)
                .prop_map(|(group, consumer)| Op::CreateConsumer { group, consumer }),
            1 => (0u8..3, 0u8..4)
                .prop_map(|(group, consumer)| Op::DeleteConsumer { group, consumer }),
            2 => (1u64..3).prop_map(Op::Tick),
        ]
    }

    fn group_name(group: u8) -> Vec<u8> {
        format!("g{group}").into_bytes()
    }

    fn consumer_name(consumer: u8) -> Vec<u8> {
        // Names of different lengths keep the payload counter honest.
        "c".repeat(consumer as usize + 1).into_bytes()
    }

    /// An id near the stream: existing entries, a removed one, or one past the end.
    fn pick_id(stream: &StreamValue, model: &Model, pick: usize) -> Id {
        if let Some(group) = model
            .groups
            .values()
            .find(|group| !group.pending.is_empty())
            && pick.is_multiple_of(3)
        {
            return group.pending[pick % group.pending.len()].0;
        }

        let (_, millis, sequence) = stream.high_water();

        match model.entries.get(pick % (model.entries.len() + 1)) {
            Some((id, _)) => *id,
            None => (millis, sequence + (pick as u64 % 2)),
        }
    }

    fn check(stream: &StreamValue, model: &Model, now: u64) {
        let entries: Vec<(Id, Vec<u8>)> = stream
            .entries()
            .to_vec()
            .iter()
            .map(|entry| {
                (
                    (entry.millis, entry.sequence),
                    entry.fields.iter().next().unwrap().1.to_vec(),
                )
            })
            .collect();
        assert_eq!(entries, model.entries);
        assert_eq!(stream.len(), model.entries.len());
        assert_eq!(
            stream
                .range_page(StreamBound::Min, StreamBound::Max, usize::MAX, false)
                .len(),
            model.entries.len()
        );

        // XRANGE / XREVRANGE pages from inside the stream.
        let middle = model
            .entries
            .get(model.entries.len() / 2)
            .map(|entry| entry.0);
        for start in [
            StreamBound::Min,
            middle.map_or(StreamBound::Max, |id| StreamBound::Id(id.0, id.1)),
        ] {
            for (count, reverse) in [(0, false), (3, false), (3, true), (usize::MAX, true)] {
                let got: Vec<Id> = stream
                    .range_page(start, StreamBound::Max, count, reverse)
                    .iter()
                    .map(|entry| (entry.millis, entry.sequence))
                    .collect();
                let mut want: Vec<Id> = model
                    .entries
                    .iter()
                    .map(|entry| entry.0)
                    .filter(|id| bound_ok(start, *id, true))
                    .collect();
                if reverse {
                    want.reverse();
                }
                want.truncate(count);
                assert_eq!(got, want);
            }
        }

        let groups = stream.groups();
        assert_eq!(groups.len(), model.groups.len());

        for (exported, (name, group)) in groups.iter().zip(&model.groups) {
            assert_eq!(&exported.name, name);
            assert_eq!((exported.last_millis, exported.last_sequence), group.last);
            let rows: Vec<_> = exported
                .pending
                .iter()
                .map(|row| {
                    (
                        (row.millis, row.sequence),
                        row.consumer.clone(),
                        row.delivered_at,
                        row.deliveries,
                    )
                })
                .collect();
            assert_eq!(rows, group.pending);

            let summary = stream.pending_summary(name).unwrap();
            assert_eq!(summary.count, group.pending.len());
            assert_eq!(summary.smallest, group.pending.first().map(|row| row.0));
            assert_eq!(summary.greatest, group.pending.last().map(|row| row.0));

            let overviews = stream.consumer_overviews(name, now).unwrap();
            let names: Vec<_> = overviews.iter().map(|row| row.name.clone()).collect();
            assert_eq!(names, group.consumers.keys().cloned().collect::<Vec<_>>());
            for row in &overviews {
                let owned = group.pending.iter().filter(|p| p.1 == row.name).count();
                assert_eq!(row.pending, owned);
                // Seen when last handed an entry, even if it was acknowledged since.
                let seen = group.consumers[&row.name].unwrap_or(now);
                assert_eq!(row.idle, now.saturating_sub(seen));
            }

            // XPENDING ranges, with and without a consumer and an idle floor.
            let ids: Vec<Id> = group.pending.iter().map(|row| row.0).collect();
            let bounds = [
                (StreamBound::Min, StreamBound::Max),
                (
                    ids.first()
                        .map_or(StreamBound::Min, |id| StreamBound::Id(id.0, id.1 + 1)),
                    ids.last()
                        .map_or(StreamBound::Max, |id| StreamBound::Id(id.0, id.1)),
                ),
            ];
            for (start, end) in bounds {
                for consumer in [None, Some(consumer_name(0)), Some(consumer_name(2))] {
                    for (min_idle, count) in [(None, 100), (Some(2), 2)] {
                        let got = stream
                            .pending_rows(
                                name,
                                PendingFilter {
                                    start,
                                    end,
                                    min_idle,
                                    consumer: consumer.as_deref(),
                                    count,
                                    now,
                                },
                            )
                            .unwrap();
                        let want: Vec<_> = group
                            .pending
                            .iter()
                            .filter(|row| {
                                bound_ok(start, row.0, true) && bound_ok(end, row.0, false)
                            })
                            .filter(|row| consumer.as_ref().is_none_or(|c| &row.1 == c))
                            .filter(|row| min_idle.is_none_or(|m| now.saturating_sub(row.2) >= m))
                            .take(count)
                            .map(|row| PendingRow {
                                millis: row.0.0,
                                sequence: row.0.1,
                                consumer: row.1.clone(),
                                idle: now.saturating_sub(row.2),
                                deliveries: row.3,
                            })
                            .collect();
                        assert_eq!(got, want);
                    }
                }
            }
        }

        let expected_payload = model
            .groups
            .iter()
            .map(|(name, group)| name.len() + group.consumers.keys().map(Vec::len).sum::<usize>())
            .sum::<usize>();
        assert_eq!(stream.payload_bytes(), expected_payload);
        let rows: usize = model.groups.values().map(|group| group.pending.len()).sum();
        let packed = stream.collection_bytes() - rows * PENDING_ROW_BYTES;
        let held: usize = model.entries.iter().map(|entry| 1 + entry.1.len()).sum();

        if model.entries.is_empty() {
            assert_eq!(packed, 0);
        } else {
            assert!(packed >= held, "{packed} bytes for {held} of payload");
        }

        let (started, millis, sequence) = stream.high_water();
        let restored = StreamValue::restore(
            started,
            millis,
            sequence,
            stream.entries().to_vec(),
            stream.groups(),
            stream.holds().to_vec(),
        )
        .unwrap();
        // Consumer slots may be numbered differently; everything observable matches.
        assert_eq!(restored.groups(), stream.groups());
        assert_eq!(restored.entries().to_vec(), stream.entries().to_vec());
        assert_eq!(restored.payload_bytes(), stream.payload_bytes());
        assert_eq!(restored.len(), stream.len());
    }

    fn bound_ok(bound: StreamBound, id: Id, start: bool) -> bool {
        match (bound, start) {
            (StreamBound::Min, true) | (StreamBound::Max, false) => true,
            (StreamBound::Min, false) | (StreamBound::Max, true) => false,
            (StreamBound::Id(millis, sequence), true) => id >= (millis, sequence),
            (StreamBound::Id(millis, sequence), false) => id <= (millis, sequence),
        }
    }

    fn apply(stream: &mut StreamValue, model: &mut Model, op: &Op, now: &mut u64) {
        match *op {
            Op::Add { step, value } => {
                *now += step;
                let (millis, sequence) = stream.next_auto_id(*now);
                stream
                    .append(millis, sequence, &[(b"f".as_slice(), [value].as_slice())])
                    .unwrap();
                model.entries.push(((millis, sequence), vec![value]));
            }
            Op::Trim(max_len) => {
                stream.trim(max_len);
                let excess = model.entries.len().saturating_sub(max_len);
                model.entries.drain(..excess);
            }
            Op::TrimBefore(pick) => {
                let id = pick_id(stream, model, pick);
                let removed = stream.trim_before(id.0, id.1);
                let before = model.entries.len();
                model.entries.retain(|entry| entry.0 >= id);
                assert_eq!(removed, before - model.entries.len());
            }
            Op::Delete(pick) => {
                let id = pick_id(stream, model, pick);
                let present = model.entries.iter().any(|entry| entry.0 == id);
                assert_eq!(stream.delete(id.0, id.1), present);
                model.entries.retain(|entry| entry.0 != id);
            }
            Op::CreateGroup(group) => {
                let name = group_name(group);
                let created = stream.create_group(&name, 0, 0).is_ok();
                assert_eq!(created, !model.groups.contains_key(&name));
                model.groups.entry(name).or_default();
            }
            Op::DestroyGroup(group) => {
                let name = group_name(group);
                assert_eq!(
                    stream.destroy_group(&name),
                    model.groups.remove(&name).is_some()
                );
            }
            Op::Read {
                group,
                consumer,
                count,
            } => {
                let name = group_name(group);
                let consumer = consumer_name(consumer);
                let Some(state) = model.groups.get_mut(&name) else {
                    assert!(stream.new_for_group(&name, count, *now).is_err());
                    return;
                };
                let ids: Vec<Id> = stream
                    .new_for_group(&name, count, *now)
                    .unwrap()
                    .iter()
                    .map(|entry| (entry.millis, entry.sequence))
                    .collect();
                let want: Vec<Id> = model
                    .entries
                    .iter()
                    .map(|entry| entry.0)
                    .filter(|id| *id > state.last)
                    .take(count)
                    .collect();
                assert_eq!(ids, want);
                for id in ids {
                    stream.claim(&name, &consumer, id.0, id.1, *now).unwrap();
                    state.last = id;
                    state.put(id, &consumer, *now, 1);
                }
            }
            Op::Ack { group, pick } => {
                let name = group_name(group);
                let id = pick_id(stream, model, pick);
                let Some(state) = model.groups.get_mut(&name) else {
                    assert!(stream.ack(&name, id.0, id.1).is_err());
                    return;
                };
                let before = state.pending.len();
                state.pending.retain(|row| row.0 != id);
                assert_eq!(
                    stream.ack(&name, id.0, id.1).unwrap(),
                    before != state.pending.len()
                );
            }
            Op::Claim {
                group,
                consumer,
                pick,
            } => {
                let name = group_name(group);
                let consumer = consumer_name(consumer);
                let id = pick_id(stream, model, pick);
                let Some(state) = model.groups.get_mut(&name) else {
                    return;
                };
                let row = state.pending.iter().find(|row| row.0 == id).cloned();
                assert_eq!(stream.is_pending(&name, id.0, id.1).unwrap(), row.is_some());
                assert_eq!(
                    stream
                        .pending_delivery(&name, id.0, id.1)
                        .unwrap()
                        .map(|row| (row.consumer, row.delivered_at, row.deliveries)),
                    row.as_ref().map(|row| (row.1.clone(), row.2, row.3))
                );
                let update = ReclaimUpdate {
                    millis: id.0,
                    sequence: id.1,
                    delivered_at: *now,
                    deliveries: row.as_ref().map_or(1, |row| row.3 + 1),
                };
                assert_eq!(
                    stream.reclaim(&name, &consumer, update).unwrap(),
                    row.is_some()
                );
                if row.is_some() {
                    state.put(id, &consumer, *now, update.deliveries);
                }
            }
            Op::Autoclaim {
                group,
                consumer,
                pick,
                min_idle,
                count,
            } => {
                let name = group_name(group);
                let consumer = consumer_name(consumer);
                let start = if pick.is_multiple_of(4) {
                    (0, 0)
                } else {
                    pick_id(stream, model, pick)
                };
                let Some(state) = model.groups.get_mut(&name) else {
                    return;
                };
                let plan = stream
                    .autoclaim_plan(&name, start, min_idle, count, *now)
                    .unwrap();

                let after: Vec<_> = state.pending.iter().filter(|row| row.0 > start).collect();
                let mut steps = Vec::new();
                let mut next = (0, 0);
                let mut claimed = 0;
                for (index, row) in after.iter().enumerate() {
                    if !model.entries.iter().any(|entry| entry.0 == row.0) {
                        steps.push(AutoclaimStep::Drop {
                            millis: row.0.0,
                            sequence: row.0.1,
                        });
                    } else if now.saturating_sub(row.2) >= min_idle {
                        steps.push(AutoclaimStep::Claim {
                            millis: row.0.0,
                            sequence: row.0.1,
                            deliveries: row.3 + 1,
                        });
                        claimed += 1;
                    }
                    // At most ten pending entries looked at per one asked for.
                    if claimed == count || index + 1 >= count * 10 {
                        if index + 1 != after.len() {
                            next = row.0;
                        }
                        break;
                    }
                }
                assert_eq!(plan, AutoclaimPlan { steps, next });

                // Carry the plan out the way the engine does.
                for step in plan.steps {
                    match step {
                        AutoclaimStep::Claim {
                            millis,
                            sequence,
                            deliveries,
                        } => {
                            stream
                                .reclaim(
                                    &name,
                                    &consumer,
                                    ReclaimUpdate {
                                        millis,
                                        sequence,
                                        delivered_at: *now,
                                        deliveries,
                                    },
                                )
                                .unwrap();
                            state.put((millis, sequence), &consumer, *now, deliveries);
                        }
                        AutoclaimStep::Drop { millis, sequence } => {
                            stream.ack(&name, millis, sequence).unwrap();
                            state.pending.retain(|row| row.0 != (millis, sequence));
                        }
                    }
                }
            }
            Op::CreateConsumer { group, consumer } => {
                let name = group_name(group);
                let consumer = consumer_name(consumer);
                match model.groups.get_mut(&name) {
                    Some(state) => {
                        let created = !state.consumers.contains_key(&consumer);

                        state.consumers.entry(consumer.clone()).or_insert(None);
                        assert_eq!(stream.create_consumer(&name, &consumer).unwrap(), created);
                    }
                    None => assert!(stream.create_consumer(&name, &consumer).is_err()),
                }
            }
            Op::DeleteConsumer { group, consumer } => {
                let name = group_name(group);
                let consumer = consumer_name(consumer);
                let Some(state) = model.groups.get_mut(&name) else {
                    return;
                };
                let before = state.pending.len();
                state.pending.retain(|row| row.1 != consumer);
                state.consumers.remove(&consumer);
                assert_eq!(
                    stream.delete_consumer(&name, &consumer).unwrap(),
                    before - state.pending.len()
                );
            }
            Op::Tick(step) => *now += step,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(256)
        ))]

        #[test]
        fn stream_matches_a_linear_model(ops in proptest::collection::vec(op(), 1..120)) {
            let mut stream = StreamValue::new();
            let mut model = Model::default();
            let mut now = 1u64;

            for op in &ops {
                apply(&mut stream, &mut model, op, &mut now);
                check(&stream, &model, now);
            }
        }
    }
}

#[test]
fn auto_id_moves_to_the_next_millisecond_when_the_sequence_is_used_up() {
    let mut stream = StreamValue::new();

    stream
        .append(10, u64::MAX, &[(b"f".as_slice(), b"a".as_slice())])
        .unwrap();

    let (millis, sequence) = stream.next_auto_id(10);

    assert_eq!((millis, sequence), (11, 0));
    assert!(
        stream
            .append(millis, sequence, &[(b"f".as_slice(), b"b".as_slice())])
            .is_ok()
    );
}
