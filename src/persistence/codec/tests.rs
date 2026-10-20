#[test]
fn borrowed_encoding_matches_owned_encoding() {
    use std::borrow::Cow;

    let values = [Cow::Borrowed(&b"a"[..]), Cow::Borrowed(&b"bc"[..])];
    let pairs = [(Cow::Borrowed(&b"f"[..]), Cow::Borrowed(&b"v"[..]))];
    let one_score = [(1.5f64, Cow::Borrowed(&b"m"[..]))];
    let two_scores = [
        (1.5f64, Cow::Borrowed(&b"m"[..])),
        (-2.0, Cow::Borrowed(&b"n"[..])),
    ];
    let owned = [OwnedMutation::FlushAll];
    let borrowed = [
        Mutation::SetBytes {
            key: b"k",
            value: b"v",
        },
        Mutation::SetInteger {
            key: b"k",
            value: -7,
        },
        Mutation::Delete { keys: &values },
        Mutation::ListPushFront {
            key: b"k",
            values: &values,
        },
        Mutation::ListPopFront { key: b"k" },
        Mutation::ListPushBack {
            key: b"k",
            values: &values,
        },
        Mutation::ListPopBack { key: b"k" },
        Mutation::SetAdd {
            key: b"k",
            members: &values,
        },
        Mutation::SetRemove {
            key: b"k",
            members: &values,
        },
        Mutation::HashSet {
            key: b"k",
            pairs: &pairs,
        },
        Mutation::HashDelete {
            key: b"k",
            fields: &values,
        },
        Mutation::SortedSetAdd {
            key: b"k",
            pairs: &one_score,
        },
        Mutation::SortedSetAdd {
            key: b"k",
            pairs: &two_scores,
        },
        Mutation::SortedSetRemove {
            key: b"k",
            members: &values,
        },
        Mutation::ExpireAt {
            key: b"k",
            unix_millis: 99,
        },
        Mutation::Persist { key: b"k" },
        Mutation::SetVersioned {
            key: b"k",
            value: b"v",
            version: 3,
        },
        Mutation::Rename {
            source: b"a",
            destination: b"b",
        },
        Mutation::Transaction { mutations: &owned },
        Mutation::FlushDatabase,
    ];

    for mutation in borrowed {
        let mut fast = Vec::new();

        encode_borrowed_record_into(&mut fast, 2, 9, 1, &mutation).unwrap();
        assert_eq!(
            fast,
            encode_record(2, 9, 1, &mutation.to_owned()).unwrap(),
            "{mutation:?}"
        );
    }
}

use super::*;

#[test]
fn round_trips_every_wal_mutation_type() {
    let mutations = vec![
        OwnedMutation::SetBytes {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        },
        OwnedMutation::SlotCopy {
            key: b"k".to_vec(),
            entry: Some(b"frame".to_vec()),
        },
        OwnedMutation::SlotCopy {
            key: b"k".to_vec(),
            entry: None,
        },
        OwnedMutation::SlotsMoved {
            first: 100,
            last: 16_383,
            to: 3,
            position: u64::MAX,
        },
        OwnedMutation::SlotsIncoming {
            first: 7,
            last: 9,
            step: 2,
        },
        OwnedMutation::SlotsIncoming {
            first: 7,
            last: 9,
            step: crate::slot_moves::IncomingStep::Completed as u8,
        },
        OwnedMutation::SlotsGiven {
            first: 0,
            last: 16_383,
            address: b"10.0.0.2:7001".to_vec(),
            position: 42,
        },
        OwnedMutation::SetInteger {
            key: b"i".to_vec(),
            value: -42,
        },
        OwnedMutation::Delete {
            keys: vec![b"a".to_vec(), b"b".to_vec()],
        },
        OwnedMutation::ListPushFront {
            key: b"l".to_vec(),
            values: vec![b"a".to_vec()],
        },
        OwnedMutation::ListPopFront { key: b"l".to_vec() },
        OwnedMutation::SetAdd {
            key: b"s".to_vec(),
            members: vec![b"a".to_vec()],
        },
        OwnedMutation::SetRemove {
            key: b"s".to_vec(),
            members: vec![b"a".to_vec()],
        },
        OwnedMutation::SortedSetAdd {
            key: b"z".to_vec(),
            pairs: vec![(1.5, b"m".to_vec())],
        },
        OwnedMutation::SortedSetAdd {
            key: b"z".to_vec(),
            pairs: vec![(1.0, b"a".to_vec()), (2.0, b"b".to_vec())],
        },
        OwnedMutation::SortedSetRemove {
            key: b"z".to_vec(),
            members: vec![b"a".to_vec()],
        },
        OwnedMutation::Transaction {
            mutations: vec![
                OwnedMutation::SetBytes {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                OwnedMutation::SetInteger {
                    key: b"i".to_vec(),
                    value: 2,
                },
            ],
        },
        OwnedMutation::BloomReserve {
            key: b"b".to_vec(),
            error_rate: 0.01,
            capacity: 100,
            expansion: 2,
        },
        OwnedMutation::BloomReserve {
            key: b"b".to_vec(),
            error_rate: 0.01,
            capacity: 100,
            expansion: 0,
        },
        OwnedMutation::BloomAdd {
            key: b"b".to_vec(),
            value: b"m".to_vec(),
        },
        OwnedMutation::BloomAddMany {
            key: b"b".to_vec(),
            values: vec![b"m".to_vec(), b"n".to_vec()],
        },
        OwnedMutation::StreamAppend {
            key: b"stream".to_vec(),
            millis: 10,
            sequence: 0,
            fields: vec![(b"field".to_vec(), b"value".to_vec())],
        },
        OwnedMutation::StreamDelete {
            key: b"stream".to_vec(),
            millis: 10,
            sequence: 0,
        },
        OwnedMutation::StreamTrim {
            key: b"stream".to_vec(),
            max_len: 1,
        },
        OwnedMutation::StreamTrimMinId {
            key: b"stream".to_vec(),
            millis: 2,
            sequence: 0,
        },
        OwnedMutation::StreamSetId {
            key: b"stream".to_vec(),
            millis: 10,
            sequence: 1,
        },
        OwnedMutation::StreamGroupCreate {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            millis: 0,
            sequence: 0,
            mkstream: true,
        },
        OwnedMutation::StreamClaim {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            consumer: b"c1".to_vec(),
            millis: 10,
            sequence: 0,
            delivered_at: 50,
        },
        OwnedMutation::StreamReclaim {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            consumer: b"c2".to_vec(),
            millis: 10,
            sequence: 0,
            delivered_at: 80,
            deliveries: 2,
        },
        OwnedMutation::StreamForceClaim {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            consumer: b"c3".to_vec(),
            millis: 11,
            sequence: 0,
            delivered_at: 90,
            deliveries: 1,
        },
        OwnedMutation::StreamAck {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            millis: 10,
            sequence: 0,
        },
        OwnedMutation::StreamGroupDestroy {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
        },
        OwnedMutation::StreamGroupSetId {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            millis: 20,
            sequence: 1,
        },
        OwnedMutation::StreamGroupDelConsumer {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            consumer: b"c1".to_vec(),
        },
        OwnedMutation::StreamGroupCreateConsumer {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            consumer: b"c2".to_vec(),
        },
        OwnedMutation::StreamHold {
            key: b"stream".to_vec(),
            millis: 30,
            sequence: 1,
            ready_at: 9_000,
        },
        OwnedMutation::LeasePut {
            key: b"lock".to_vec(),
            token: 2,
            expires_at: 5_000,
        },
        OwnedMutation::SetVersioned {
            key: b"doc".to_vec(),
            value: b"12".to_vec(),
            version: 2,
        },
        OwnedMutation::LimitPut {
            key: b"login".to_vec(),
            used: 2,
            expires_at: Some(5_000),
        },
        OwnedMutation::LimitPut {
            key: b"login".to_vec(),
            used: 3,
            expires_at: None,
        },
        OwnedMutation::OncePut {
            key: b"pay".to_vec(),
            expires_at: 9_000,
        },
        OwnedMutation::IdempotencyPut {
            key: b"request".to_vec(),
            value: b"RVIDEM01\x01\x01\x01fo\x00\xff".to_vec(),
            expires_at: 9_000,
        },
        OwnedMutation::StreamGroupAdvance {
            key: b"stream".to_vec(),
            group: b"workers".to_vec(),
            millis: 30,
            sequence: 0,
        },
        OwnedMutation::ExpireAt {
            key: b"k".to_vec(),
            unix_millis: 42,
        },
        OwnedMutation::Persist { key: b"k".to_vec() },
        OwnedMutation::Rename {
            source: b"old".to_vec(),
            destination: b"new".to_vec(),
        },
        OwnedMutation::ListPushBack {
            key: b"l".to_vec(),
            values: vec![b"z".to_vec()],
        },
        OwnedMutation::ListPopBack { key: b"l".to_vec() },
        OwnedMutation::ListTrim {
            key: b"l".to_vec(),
            start: 0,
            stop: 1,
        },
        OwnedMutation::SetRange {
            key: b"bits".to_vec(),
            offset: 7,
            bytes: b"xy".to_vec(),
        },
        OwnedMutation::ListSet {
            key: b"l".to_vec(),
            index: -1,
            value: b"v".to_vec(),
        },
        OwnedMutation::ListInsert {
            key: b"l".to_vec(),
            before: true,
            pivot: b"p".to_vec(),
            value: b"v".to_vec(),
        },
        OwnedMutation::ListRemove {
            key: b"l".to_vec(),
            count: -2,
            value: b"v".to_vec(),
        },
        OwnedMutation::SetStore {
            key: b"s".to_vec(),
            members: vec![b"a".to_vec(), b"c".to_vec()],
        },
        OwnedMutation::SetMove {
            source: b"from".to_vec(),
            destination: b"to".to_vec(),
            member: b"a".to_vec(),
        },
        OwnedMutation::HashSet {
            key: b"h".to_vec(),
            pairs: vec![(b"f".to_vec(), b"v".to_vec())],
        },
        OwnedMutation::HashDelete {
            key: b"h".to_vec(),
            fields: vec![b"f".to_vec()],
        },
        OwnedMutation::HashFieldExpire {
            key: b"h".to_vec(),
            fields: vec![b"f".to_vec(), b"g".to_vec()],
            unix_millis: 9_000,
        },
        OwnedMutation::HashFieldPersist {
            key: b"h".to_vec(),
            fields: vec![b"f".to_vec()],
        },
        OwnedMutation::SemaphoreAcquire {
            key: b"jobs".to_vec(),
            token: 1 << 20,
            expires_at: 9_000,
        },
        OwnedMutation::SemaphoreRelease {
            key: b"jobs".to_vec(),
            token: 1 << 20,
        },
    ];

    for (index, mutation) in mutations.into_iter().enumerate() {
        let encoded = encode_record(3, index as u64 + 1, 0, &mutation).unwrap();
        let decoded = decode_record(&encoded, 3, ProtocolLimits::default()).unwrap();

        assert_eq!(decoded.sequence, index as u64 + 1);
        assert_eq!(decoded.mutation, mutation);
    }
}

#[test]
fn json_put_round_trips_with_a_distinct_opcode_and_the_set_bytes_payload() {
    let key = b"document".to_vec();
    let value = br#"{"nested":[true,null,42]}"#.to_vec();
    let json = crate::mutation::Mutation::JsonPut {
        key: &key,
        value: &value,
    }
    .to_owned();
    assert_eq!(json.write_keys(), vec![key.clone()]);
    let bytes = OwnedMutation::SetBytes { key, value };
    let encoded = encode_record(2, 7, 3, &json).unwrap();
    let encoded_bytes = encode_record(2, 7, 3, &bytes).unwrap();

    assert_eq!(u16::from_le_bytes(encoded[6..8].try_into().unwrap()), 54);
    assert_eq!(
        &encoded[RECORD_HEADER_LEN..encoded.len() - 4],
        &encoded_bytes[RECORD_HEADER_LEN..encoded_bytes.len() - 4]
    );
    let decoded = decode_record(&encoded, 2, ProtocolLimits::default()).unwrap();
    assert_eq!(decoded.database, 3);
    assert_eq!(decoded.mutation, json);

    let transaction = OwnedMutation::Transaction {
        mutations: vec![json, bytes],
    };
    let encoded = encode_record(2, 8, 3, &transaction).unwrap();
    assert_eq!(
        decode_record(&encoded, 2, ProtocolLimits::default())
            .unwrap()
            .mutation,
        transaction
    );
}

#[test]
fn a_batch_of_one_write_is_encoded_as_that_write() {
    let write = OwnedMutation::SetBytes {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
    };
    let single = [write.clone()];
    let mut borrowed = Vec::new();

    encode_borrowed_record_into(
        &mut borrowed,
        2,
        9,
        1,
        &crate::mutation::Mutation::Transaction { mutations: &single },
    )
    .unwrap();
    assert_eq!(borrowed, encode_record(2, 9, 1, &write).unwrap());
    assert_eq!(
        encode_record(
            2,
            9,
            1,
            &OwnedMutation::Transaction {
                mutations: single.to_vec()
            }
        )
        .unwrap(),
        borrowed
    );

    let pair = [write.clone(), write];
    let transaction = OwnedMutation::Transaction {
        mutations: pair.to_vec(),
    };

    encode_borrowed_record_into(
        &mut borrowed,
        2,
        9,
        1,
        &crate::mutation::Mutation::Transaction { mutations: &pair },
    )
    .unwrap();
    assert_eq!(borrowed, encode_record(2, 9, 1, &transaction).unwrap());
    assert_eq!(
        decode_record(&borrowed, 2, ProtocolLimits::default())
            .unwrap()
            .mutation,
        transaction
    );
}

#[test]
fn json_path_deltas_round_trip_with_distinct_opcodes_and_write_keys() {
    let key = b"document";
    let path = b"$.nested[0]";
    let value = br#"{"answer":42}"#;
    let mutations = [
        (
            55,
            crate::mutation::Mutation::JsonSetPath { key, path, value }.to_owned(),
        ),
        (
            56,
            crate::mutation::Mutation::JsonMergePath { key, path, value }.to_owned(),
        ),
        (
            57,
            crate::mutation::Mutation::JsonDelPath { key, path }.to_owned(),
        ),
    ];

    for (opcode, mutation) in &mutations {
        assert_eq!(mutation.write_keys(), vec![key.to_vec()]);
        let encoded = encode_record(2, 7, 3, mutation).unwrap();
        assert_eq!(
            u16::from_le_bytes(encoded[6..8].try_into().unwrap()),
            *opcode
        );
        let decoded = decode_record(&encoded, 2, ProtocolLimits::default()).unwrap();
        assert_eq!(decoded.database, 3);
        assert_eq!(&decoded.mutation, mutation);
    }

    let transaction = OwnedMutation::Transaction {
        mutations: mutations
            .iter()
            .map(|(_, mutation)| mutation.clone())
            .collect(),
    };
    assert_eq!(transaction.write_keys(), vec![key.to_vec(); 3]);
    let encoded = encode_record(2, 8, 3, &transaction).unwrap();
    assert_eq!(
        decode_record(&encoded, 2, ProtocolLimits::default())
            .unwrap()
            .mutation,
        transaction
    );
}

#[test]
fn json_path_delta_decoder_rejects_oversize_and_malformed_payloads() {
    let limits = ProtocolLimits {
        max_key_bytes: 3,
        max_value_bytes: 4,
        ..ProtocolLimits::default()
    };
    for opcode in [55, 56, 57] {
        let mut payload = Vec::new();
        put_bytes(&mut payload, b"long").unwrap();
        put_bytes(&mut payload, b"$").unwrap();
        if opcode != 57 {
            put_bytes(&mut payload, b"null").unwrap();
        }
        assert!(matches!(
            decode_payload(opcode, &payload, limits),
            Err(PersistenceError::Corruption(_))
        ));

        payload.clear();
        put_bytes(&mut payload, b"key").unwrap();
        put_bytes(&mut payload, b"$.long").unwrap();
        if opcode != 57 {
            put_bytes(&mut payload, b"null").unwrap();
        }
        assert!(matches!(
            decode_payload(opcode, &payload, limits),
            Err(PersistenceError::Corruption(_))
        ));

        payload.clear();
        put_bytes(&mut payload, b"key").unwrap();
        put_bytes(&mut payload, b"$").unwrap();
        if opcode != 57 {
            put_bytes(&mut payload, b"12345").unwrap();
            assert!(matches!(
                decode_payload(opcode, &payload, limits),
                Err(PersistenceError::Corruption(_))
            ));
            payload.clear();
            put_bytes(&mut payload, b"key").unwrap();
            put_bytes(&mut payload, b"$").unwrap();
            put_bytes(&mut payload, b"null").unwrap();
        }
        assert!(decode_payload(opcode, &payload[..payload.len() - 1], limits).is_err());
        assert!(decode_payload(opcode, &payload, limits).is_ok());
        payload.push(0);
        assert!(decode_payload(opcode, &payload, limits).is_err());
    }
}

#[test]
fn records_carry_their_database_in_the_flags() {
    let set = OwnedMutation::SetBytes {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };

    assert_eq!(
        encode_record(0, 1, 0, &set).unwrap(),
        load_hex_fixture("wal-set-bytes-record.hex"),
        "database 0 records keep the pre-database bytes"
    );

    let mutations = [
        set,
        OwnedMutation::FlushDatabase,
        OwnedMutation::FlushAll,
        OwnedMutation::SwapDatabases {
            first: 0,
            second: 255,
        },
        OwnedMutation::MoveKey {
            key: b"k".to_vec(),
            destination: 9,
        },
        OwnedMutation::Transaction {
            mutations: vec![
                OwnedMutation::FlushDatabase,
                OwnedMutation::SwapDatabases {
                    first: 1,
                    second: 2,
                },
            ],
        },
    ];

    for (index, mutation) in mutations.into_iter().enumerate() {
        let sequence = index as u64 + 1;
        let encoded = encode_record(0, sequence, 7, &mutation).unwrap();
        let decoded = decode_record(&encoded, 0, ProtocolLimits::default()).unwrap();

        assert_eq!(decoded.database, 7);
        assert_eq!(decoded.mutation, mutation);
    }
}

#[test]
fn rejects_unknown_record_flags() {
    let mut record = encode_record(0, 1, 3, &OwnedMutation::FlushDatabase).unwrap();

    record[9] = 1;
    let end = record.len() - 4;
    let checksum = crc32c::crc32c(&record[4..end]);

    record[end..].copy_from_slice(&checksum.to_le_bytes());
    assert!(decode_record(&record, 0, ProtocolLimits::default()).is_err());
}

#[test]
fn record_header_check_agrees_with_the_decoder_on_header_fields() {
    let mutation = OwnedMutation::Persist { key: b"k".to_vec() };
    let record = encode_record(2, 7, 1, &mutation).unwrap();

    assert!(record_header_plausible(&record, 2, 7));
    assert!(!record_header_plausible(&record, 3, 7));
    assert!(!record_header_plausible(&record, 2, 8));
    assert!(!record_header_plausible(&record[..record.len() - 1], 2, 7));
    for (at, flip) in [(4, 1u8), (11, 1), (24, 1)] {
        let mut changed = record.clone();

        changed[at] ^= flip;
        assert!(!record_header_plausible(&changed, 2, 7), "byte {at}");
    }
    // The payload and checksum are left to the decoder.
    let mut payload = record.clone();
    payload[28] ^= 1;
    assert!(record_header_plausible(&payload, 2, 7));
    assert!(decode_record(&payload, 2, ProtocolLimits::default()).is_err());
}

#[test]
fn detects_header_and_record_corruption() {
    let mut header = encode_segment_header(1, 2, 3);

    header[20] ^= 1;
    assert!(matches!(
        decode_segment_header(&header),
        Err(PersistenceError::Corruption(_))
    ));

    let mutation = OwnedMutation::Persist { key: b"k".to_vec() };

    let mut record = encode_record(1, 1, 0, &mutation).unwrap();

    record[28] ^= 1;
    assert!(matches!(
        decode_record(&record, 1, ProtocolLimits::default()),
        Err(PersistenceError::Corruption(_))
    ));
}

#[test]
fn version_one_set_bytes_fixture_remains_stable() {
    assert_v1_record_fixture(
        "wal-set-bytes-record.hex",
        0,
        1,
        &OwnedMutation::SetBytes {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        },
    );
}

#[test]
fn value_with_deadline_record_round_trips_from_the_borrowed_encoder() {
    let borrowed = Mutation::SetBytesExpiring {
        key: b"lock",
        value: b"token",
        expires_at: 1_700_000_030_000,
    };
    let mut bytes = Vec::new();

    encode_borrowed_record_into(&mut bytes, 0, 9, 2, &borrowed).unwrap();
    assert_eq!(bytes, encode_record(0, 9, 2, &borrowed.to_owned()).unwrap());

    let record = decode_record(&bytes, 0, ProtocolLimits::default()).unwrap();

    assert_eq!(record.sequence, 9);
    assert_eq!(record.database, 2);
    assert_eq!(record.mutation, borrowed.to_owned());
}

#[test]
fn version_one_wal_record_fixtures_cover_every_mutation() {
    for (name, shard, sequence, mutation) in v1_wal_records() {
        assert_v1_record_fixture(name, shard, sequence, &mutation);
        cover_owned_mutation(&mutation);
    }

    assert_eq!(v1_wal_records().len(), 23);
}

#[test]
fn every_record_type_the_decoder_knows_has_a_format_minor() {
    let limits = ProtocolLimits::default();

    for record_type in 0..=u16::from(u8::MAX) {
        let known = !matches!(
            decode_payload(record_type, &[], limits),
            Err(PersistenceError::Unsupported(_))
        );

        assert_eq!(
            known,
            (1..=LAST_RECORD_TYPE).contains(&record_type),
            "record type {record_type}: a new record type needs a RECORD_MINORS row, \
                 LAST_RECORD_TYPE and a FORMAT_MINOR bump"
        );
    }

    assert!(
        RECORD_MINORS
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0 && pair[1].1 == pair[0].1 + 1)
    );
    assert!(RECORD_MINORS.last().unwrap().0 <= LAST_RECORD_TYPE);
    assert_eq!(RECORD_MINORS.last().unwrap().1, FORMAT_MINOR);

    let future = encode_segment_header_at(1, FORMAT_MINOR + 1, 0, 0, 1, 1, 0);
    assert!(matches!(
        decode_segment_header(&future),
        Err(PersistenceError::Unsupported(_))
    ));
    let current = encode_segment_header_at(1, FORMAT_MINOR, 0, 0, 1, 1, 0);
    assert!(decode_segment_header(&current).is_ok());
}

#[test]
fn version_one_wal_header_fixtures_decode_and_reject_future_major() {
    let header = load_hex_fixture("wal-header.hex");

    assert_eq!(
        encode_segment_header_at(1, 0, 0, 0, 1, 1_700_000_000_000, 0),
        header
    );
    let decoded = decode_segment_header(&header).unwrap();

    assert_eq!(decoded.shard_id, 0);
    assert_eq!(decoded.segment_id, 0);
    assert_eq!(decoded.first_sequence, 1);

    let unsupported = load_hex_fixture("wal-unsupported-major.hex");

    assert_eq!(
        encode_segment_header_at(2, 0, 0, 0, 1, 1_700_000_000_000, 0),
        unsupported
    );
    assert!(matches!(
        decode_segment_header(&unsupported),
        Err(PersistenceError::Unsupported(_))
    ));
}

#[test]
fn version_one_wal_fixtures_apply_without_reencoding() {
    let mut shard = crate::KvShard::new();

    crate::persistence::apply_mutation(
        &mut shard,
        0,
        &decode_record(
            &load_hex_fixture("wal-set-bytes-record.hex"),
            0,
            ProtocolLimits::default(),
        )
        .unwrap()
        .mutation,
    )
    .unwrap();
    assert_eq!(shard.get_bytes(b"k").unwrap(), Some(b"v".as_slice()));

    crate::persistence::apply_mutation(
        &mut shard,
        0,
        &decode_record(
            &load_hex_fixture("wal-transaction.hex"),
            0,
            ProtocolLimits::default(),
        )
        .unwrap()
        .mutation,
    )
    .unwrap();
    assert_eq!(shard.copied_value(b"a").as_deref(), Some(b"1".as_slice()));
    assert_eq!(shard.copied_value(b"i").as_deref(), Some(b"2".as_slice()));
}

#[test]
fn writes_v1_wal_fixtures_when_requested() {
    if std::env::var_os("RUVIO_WRITE_FIXTURES").is_none() {
        return;
    }

    write_hex_fixture(
        "wal-header.hex",
        &encode_segment_header_at(1, 0, 0, 0, 1, 1_700_000_000_000, 0),
    );
    write_hex_fixture(
        "wal-unsupported-major.hex",
        &encode_segment_header_at(2, 0, 0, 0, 1, 1_700_000_000_000, 0),
    );

    for (name, shard, sequence, mutation) in v1_wal_records() {
        write_hex_fixture(name, &encode_record(shard, sequence, 0, &mutation).unwrap());
    }
}

fn v1_wal_records() -> Vec<(&'static str, u32, u64, OwnedMutation)> {
    vec![
        (
            "wal-set-bytes-record.hex",
            0,
            1,
            OwnedMutation::SetBytes {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        ),
        (
            "wal-set-integer.hex",
            0,
            1,
            OwnedMutation::SetInteger {
                key: b"i".to_vec(),
                value: -42,
            },
        ),
        (
            "wal-delete.hex",
            0,
            1,
            OwnedMutation::Delete {
                keys: vec![b"gone".to_vec()],
            },
        ),
        (
            "wal-list-push-front.hex",
            0,
            1,
            OwnedMutation::ListPushFront {
                key: b"l".to_vec(),
                values: vec![b"a".to_vec()],
            },
        ),
        (
            "wal-list-pop-front.hex",
            0,
            1,
            OwnedMutation::ListPopFront { key: b"l".to_vec() },
        ),
        (
            "wal-list-push-back.hex",
            0,
            1,
            OwnedMutation::ListPushBack {
                key: b"l".to_vec(),
                values: vec![b"z".to_vec()],
            },
        ),
        (
            "wal-list-pop-back.hex",
            0,
            1,
            OwnedMutation::ListPopBack { key: b"l".to_vec() },
        ),
        (
            "wal-list-trim.hex",
            0,
            1,
            OwnedMutation::ListTrim {
                key: b"l".to_vec(),
                start: 0,
                stop: 1,
            },
        ),
        (
            "wal-set-add.hex",
            0,
            1,
            OwnedMutation::SetAdd {
                key: b"s".to_vec(),
                members: vec![b"a".to_vec()],
            },
        ),
        (
            "wal-set-remove.hex",
            0,
            1,
            OwnedMutation::SetRemove {
                key: b"s".to_vec(),
                members: vec![b"a".to_vec()],
            },
        ),
        (
            "wal-set-store.hex",
            0,
            1,
            OwnedMutation::SetStore {
                key: b"s".to_vec(),
                members: vec![b"a".to_vec(), b"c".to_vec()],
            },
        ),
        (
            "wal-set-move.hex",
            0,
            1,
            OwnedMutation::SetMove {
                source: b"from".to_vec(),
                destination: b"to".to_vec(),
                member: b"a".to_vec(),
            },
        ),
        (
            "wal-hash-set.hex",
            0,
            1,
            OwnedMutation::HashSet {
                key: b"h".to_vec(),
                pairs: vec![(b"f".to_vec(), b"v".to_vec())],
            },
        ),
        (
            "wal-hash-delete.hex",
            0,
            1,
            OwnedMutation::HashDelete {
                key: b"h".to_vec(),
                fields: vec![b"f".to_vec()],
            },
        ),
        (
            "wal-zadd-single.hex",
            0,
            1,
            OwnedMutation::SortedSetAdd {
                key: b"z".to_vec(),
                pairs: vec![(1.5, b"m".to_vec())],
            },
        ),
        (
            "wal-zadd-multi.hex",
            0,
            1,
            OwnedMutation::SortedSetAdd {
                key: b"z".to_vec(),
                pairs: vec![(1.0, b"a".to_vec()), (2.0, b"b".to_vec())],
            },
        ),
        (
            "wal-zrem.hex",
            0,
            1,
            OwnedMutation::SortedSetRemove {
                key: b"z".to_vec(),
                members: vec![b"a".to_vec()],
            },
        ),
        (
            "wal-bloom-reserve.hex",
            0,
            1,
            OwnedMutation::BloomReserve {
                key: b"b".to_vec(),
                error_rate: 0.01,
                capacity: 100,
                expansion: 2,
            },
        ),
        (
            "wal-bloom-add.hex",
            0,
            1,
            OwnedMutation::BloomAdd {
                key: b"b".to_vec(),
                value: b"m".to_vec(),
            },
        ),
        (
            "wal-expire-at.hex",
            0,
            1,
            OwnedMutation::ExpireAt {
                key: b"k".to_vec(),
                unix_millis: 42,
            },
        ),
        (
            "wal-persist.hex",
            0,
            1,
            OwnedMutation::Persist { key: b"k".to_vec() },
        ),
        (
            "wal-rename.hex",
            0,
            1,
            OwnedMutation::Rename {
                source: b"old".to_vec(),
                destination: b"new".to_vec(),
            },
        ),
        (
            "wal-transaction.hex",
            0,
            1,
            OwnedMutation::Transaction {
                mutations: vec![
                    OwnedMutation::SetBytes {
                        key: b"a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    OwnedMutation::SetInteger {
                        key: b"i".to_vec(),
                        value: 2,
                    },
                ],
            },
        ),
    ]
}

fn cover_owned_mutation(mutation: &OwnedMutation) {
    match mutation {
        OwnedMutation::SetBytes { .. }
        | OwnedMutation::SetBytesExpiring { .. }
        | OwnedMutation::JsonPut { .. }
        | OwnedMutation::JsonSetPath { .. }
        | OwnedMutation::JsonMergePath { .. }
        | OwnedMutation::JsonDelPath { .. }
        | OwnedMutation::SetInteger { .. }
        | OwnedMutation::Delete { .. }
        | OwnedMutation::ListPushFront { .. }
        | OwnedMutation::ListPopFront { .. }
        | OwnedMutation::ListPushBack { .. }
        | OwnedMutation::ListPopBack { .. }
        | OwnedMutation::ListTrim { .. }
        | OwnedMutation::SetAdd { .. }
        | OwnedMutation::SetRemove { .. }
        | OwnedMutation::SetStore { .. }
        | OwnedMutation::SetMove { .. }
        | OwnedMutation::HashSet { .. }
        | OwnedMutation::HashDelete { .. }
        | OwnedMutation::HashFieldExpire { .. }
        | OwnedMutation::HashFieldPersist { .. }
        | OwnedMutation::SemaphoreAcquire { .. }
        | OwnedMutation::SemaphoreRelease { .. }
        | OwnedMutation::SortedSetAdd { .. }
        | OwnedMutation::SortedSetRemove { .. }
        | OwnedMutation::BloomReserve { .. }
        | OwnedMutation::BloomAdd { .. }
        | OwnedMutation::BloomAddMany { .. }
        | OwnedMutation::StreamAppend { .. }
        | OwnedMutation::StreamDelete { .. }
        | OwnedMutation::StreamTrim { .. }
        | OwnedMutation::StreamTrimMinId { .. }
        | OwnedMutation::StreamSetId { .. }
        | OwnedMutation::StreamGroupCreate { .. }
        | OwnedMutation::StreamClaim { .. }
        | OwnedMutation::StreamReclaim { .. }
        | OwnedMutation::StreamForceClaim { .. }
        | OwnedMutation::StreamAck { .. }
        | OwnedMutation::StreamGroupDestroy { .. }
        | OwnedMutation::StreamGroupSetId { .. }
        | OwnedMutation::StreamGroupDelConsumer { .. }
        | OwnedMutation::StreamGroupCreateConsumer { .. }
        | OwnedMutation::StreamHold { .. }
        | OwnedMutation::LeasePut { .. }
        | OwnedMutation::SetVersioned { .. }
        | OwnedMutation::LimitPut { .. }
        | OwnedMutation::OncePut { .. }
        | OwnedMutation::IdempotencyPut { .. }
        | OwnedMutation::StreamGroupAdvance { .. }
        | OwnedMutation::ExpireAt { .. }
        | OwnedMutation::Persist { .. }
        | OwnedMutation::Rename { .. }
        | OwnedMutation::Transaction { .. } => {}
        OwnedMutation::FlushDatabase
        | OwnedMutation::FlushAll
        | OwnedMutation::SwapDatabases { .. }
        | OwnedMutation::MoveKey { .. }
        | OwnedMutation::SlotCopy { .. }
        | OwnedMutation::SlotsMoved { .. }
        | OwnedMutation::SlotsIncoming { .. }
        | OwnedMutation::SlotsGiven { .. }
        | OwnedMutation::SetRange { .. }
        | OwnedMutation::ListSet { .. }
        | OwnedMutation::ListInsert { .. }
        | OwnedMutation::ListRemove { .. } => {
            panic!("these records are newer than the v1 fixtures")
        }
    }
}

fn assert_v1_record_fixture(name: &str, shard: u32, sequence: u64, mutation: &OwnedMutation) {
    let expected = load_hex_fixture(name);

    assert_eq!(
        encode_record(shard, sequence, 0, mutation).unwrap(),
        expected,
        "{name} encoding drifted"
    );
    let decoded = decode_record(&expected, shard, ProtocolLimits::default()).unwrap();

    assert_eq!(decoded.sequence, sequence);
    assert_eq!(decoded.mutation, *mutation);
}

fn fixture_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/persistence/v1")
}

fn load_hex_fixture(name: &str) -> Vec<u8> {
    let text = std::fs::read_to_string(fixture_dir().join(name))
        .unwrap_or_else(|error| panic!("missing v1 fixture {name}: {error}"));

    parse_hex(&text)
}

fn write_hex_fixture(name: &str, bytes: &[u8]) {
    let directory = fixture_dir();

    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join(name), to_hex(bytes)).unwrap();
}

fn parse_hex(text: &str) -> Vec<u8> {
    let compact: String = text.chars().filter(|ch| !ch.is_whitespace()).collect();

    compact
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2 + 1);

    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }

    text.push('\n');
    text
}

#[test]
fn header_fields_sit_at_their_documented_offsets() {
    // Absolute offsets, not the codec's own ranges: two same-width fields swapped in the
    // codec must fail here. The fixtures cannot catch that, they use shard 0 and database 0.
    let bytes = encode_segment_header_at(1, 2, 0x0304_0506, 0x0708, 0x090a, 0x0b0c, 0x0d0e);

    assert_eq!(&bytes[0..8], WAL_MAGIC);
    assert_eq!(bytes[8..10], 1u16.to_le_bytes());
    assert_eq!(bytes[10..12], 2u16.to_le_bytes());
    assert_eq!(bytes[12..16], 52u32.to_le_bytes());
    assert_eq!(bytes[16..20], 0x0304_0506u32.to_le_bytes());
    assert_eq!(bytes[20..28], 0x0708u64.to_le_bytes());
    assert_eq!(bytes[28..36], 0x090au64.to_le_bytes());
    assert_eq!(bytes[36..44], 0x0b0cu64.to_le_bytes());
    assert_eq!(bytes[44..48], 0x0d0eu32.to_le_bytes());
    assert_eq!(bytes[48..52], crc32c::crc32c(&bytes[..48]).to_le_bytes());

    let unflagged = encode_segment_header_at(1, 2, 0x0304_0506, 0x0708, 0x090a, 0x0b0c, 0);
    let header = decode_segment_header(&unflagged).unwrap();

    assert_eq!(header.minor, 2);
    assert_eq!(header.shard_id, 0x0304_0506);
    assert_eq!(header.segment_id, 0x0708);
    assert_eq!(header.first_sequence, 0x090a);

    let record = encode_record(7, 0x1122_3344, 3, &OwnedMutation::FlushAll).unwrap();

    assert_eq!(record[0..4], (record.len() as u32).to_le_bytes());
    assert_eq!(record[4..6], 1u16.to_le_bytes());
    assert_eq!(record[8..12], 3u32.to_le_bytes());
    assert_eq!(record[12..16], 7u32.to_le_bytes());
    assert_eq!(record[16..24], 0x1122_3344u64.to_le_bytes());
    assert_eq!(record[24..28], ((record.len() - 32) as u32).to_le_bytes());

    let decoded = decode_record(&record, 7, ProtocolLimits::default()).unwrap();

    assert_eq!(decoded.sequence, 0x1122_3344);
    assert_eq!(decoded.database, 3);
    assert_eq!(super::record_sequence(&record), Some(0x1122_3344));
    assert_eq!(super::record_sequence(&record[..23]), None);
    assert!(super::record_header_plausible(&record, 7, 0x1122_3344));
    assert!(!super::record_header_plausible(&record, 7, 0x1122_3345));
    assert!(!super::record_header_plausible(&record, 6, 0));
}

#[test]
fn a_slot_copy_record_replays_the_value_and_its_deadline_or_removes_the_key() {
    use crate::store::{SnapshotEntry, SnapshotValue};

    let entry = SnapshotEntry {
        database: 0,
        key: b"moved".to_vec(),
        expires_at: Some(crate::UnixMillis::new(u64::MAX / 2)),
        value: SnapshotValue::List(vec![b"a".to_vec(), b"b".to_vec()]),
    };
    let mut frame = Vec::new();

    crate::persistence::encode_entry(&mut frame, &entry).unwrap();
    let copy = OwnedMutation::SlotCopy {
        key: b"moved".to_vec(),
        entry: Some(frame),
    };
    let record = encode_record(0, 1, 2, &copy).unwrap();
    let decoded = decode_record(&record, 0, ProtocolLimits::default()).unwrap();

    assert_eq!(decoded.mutation, copy);
    assert_eq!(decoded.database, 2);

    let mut shard = crate::KvShard::new();

    shard.enter(2);
    shard.set_bytes(b"moved", b"old");
    shard.enter(0);
    crate::persistence::apply_mutation(&mut shard, 2, &decoded.mutation).unwrap();
    shard.enter(2);
    assert_eq!(shard.list_len(b"moved").unwrap(), 2);
    assert!(shard.deadline(2, b"moved").is_some());

    crate::persistence::apply_mutation(
        &mut shard,
        2,
        &OwnedMutation::SlotCopy {
            key: b"moved".to_vec(),
            entry: None,
        },
    )
    .unwrap();
    assert!(!shard.contains_key(b"moved"));

    // A damaged frame is corruption, not a silent skip.
    let damaged = OwnedMutation::SlotCopy {
        key: b"moved".to_vec(),
        entry: Some(b"not a frame".to_vec()),
    };
    assert!(crate::persistence::apply_mutation(&mut shard, 2, &damaged).is_err());
}

#[test]
fn a_slot_copy_larger_than_any_command_still_decodes() {
    let copy = OwnedMutation::SlotCopy {
        key: b"big".to_vec(),
        entry: Some(vec![7; 4096]),
    };
    let record = encode_record(0, 1, 0, &copy).unwrap();
    let small = ProtocolLimits {
        max_command_bytes: 1024,
        ..ProtocolLimits::default()
    };

    assert_eq!(decode_record(&record, 0, small).unwrap().mutation, copy);
}
