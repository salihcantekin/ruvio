//! Durable change stream: turns WAL records into client events.
//!
//! A subscriber names a cursor `<generation>:<shard>:<sequence>` and receives every record after
//! it, so a client that stores the last cursor it processed resumes after a disconnect or restart
//! without loss or repeats. Records are atomic: all events of one record share a cursor and a
//! write. The generation is the replication epoch the shard's log was written under: a promoted
//! replica continues its old primary's sequence numbers only up to the fork, so a cursor past it
//! names records that no longer exist (see [`ChangeLineage::resume`]).

use crate::{
    mutation::OwnedMutation,
    protocol::{ProtocolError, glob_match},
};

/// Event categories a subscriber can filter on (`TYPES`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeKind {
    Put,
    Json,
    Del,
    Expire,
    Rename,
    List,
    Set,
    Hash,
    Zset,
    Stream,
    Flush,
    Other,
    /// A slot range left this shard: key `first-last`, value the cursor where the new
    /// owner's log continues it.
    Moved,
}

const KINDS: [(&str, ChangeKind); 13] = [
    ("put", ChangeKind::Put),
    ("json", ChangeKind::Json),
    ("del", ChangeKind::Del),
    ("expire", ChangeKind::Expire),
    ("rename", ChangeKind::Rename),
    ("list", ChangeKind::List),
    ("set", ChangeKind::Set),
    ("hash", ChangeKind::Hash),
    ("zset", ChangeKind::Zset),
    ("stream", ChangeKind::Stream),
    ("flush", ChangeKind::Flush),
    ("other", ChangeKind::Other),
    ("moved", ChangeKind::Moved),
];

impl ChangeKind {
    pub fn name(self) -> &'static str {
        KINDS
            .iter()
            .find(|(_, kind)| *kind == self)
            .map_or("other", |(name, _)| name)
    }

    fn bit(self) -> u16 {
        1 << KINDS
            .iter()
            .position(|(_, kind)| *kind == self)
            .unwrap_or(11)
    }
}

/// `CHANGES SUBSCRIBE` arguments.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangeSubscription {
    /// `None` starts at the current durable position (`NOW`).
    pub from: Option<ChangeCursor>,
    pub pattern: Option<Vec<u8>>,
    /// Bit set of [`ChangeKind`]; `0` accepts every kind.
    pub kinds: u16,
    pub values: bool,
    /// `COMMITTED`: only records at or below the shard's committed sequence.
    pub committed: bool,
}

impl ChangeSubscription {
    pub fn accepts(&self, kind: ChangeKind, key: Option<&[u8]>) -> bool {
        // A move names slots, not keys, and ends what this shard reports about them: every
        // subscriber needs it, whatever it filters.
        if kind == ChangeKind::Moved {
            return true;
        }
        if self.kinds != 0 && self.kinds & kind.bit() == 0 {
            return false;
        }

        match (&self.pattern, key) {
            (Some(pattern), Some(key)) => glob_match(pattern, key),
            (Some(_), None) => false,
            (None, _) => true,
        }
    }

    pub fn parse(arguments: &[&[u8]]) -> Result<Self, ProtocolError> {
        let mut subscription = Self::default();
        let mut index = 0;

        while index < arguments.len() {
            let word = arguments[index];

            if word.eq_ignore_ascii_case(b"FROM") {
                let value = arguments.get(index + 1).ok_or(ProtocolError::Invalid(
                    "CHANGES SUBSCRIBE FROM needs a cursor",
                ))?;

                subscription.from = if value.eq_ignore_ascii_case(b"NOW") {
                    None
                } else {
                    Some(ChangeCursor::parse(value)?)
                };
                index += 2;
            } else if word.eq_ignore_ascii_case(b"MATCH") {
                let value = arguments.get(index + 1).ok_or(ProtocolError::Invalid(
                    "CHANGES SUBSCRIBE MATCH needs a pattern",
                ))?;

                subscription.pattern = Some(value.to_vec());
                index += 2;
            } else if word.eq_ignore_ascii_case(b"TYPES") {
                let value = arguments.get(index + 1).ok_or(ProtocolError::Invalid(
                    "CHANGES SUBSCRIBE TYPES needs a list",
                ))?;

                for name in value.split(|byte| *byte == b',') {
                    let kind = KINDS
                        .iter()
                        .find(|(known, _)| known.as_bytes().eq_ignore_ascii_case(name))
                        .map(|(_, kind)| *kind)
                        .ok_or(ProtocolError::Invalid("unknown change type"))?;

                    subscription.kinds |= kind.bit();
                }
                index += 2;
            } else if word.eq_ignore_ascii_case(b"VALUES") {
                subscription.values = true;
                index += 1;
            } else if word.eq_ignore_ascii_case(b"COMMITTED") {
                subscription.committed = true;
                index += 1;
            } else {
                return Err(ProtocolError::Invalid("unknown CHANGES SUBSCRIBE option"));
            }
        }

        Ok(subscription)
    }
}

/// Position in one shard's WAL: the last sequence the client has processed, in the log
/// written under `generation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangeCursor {
    /// `None` for the older `<shard>:<sequence>` form, read as the shard's current generation.
    pub generation: Option<u64>,
    pub shard: u32,
    pub sequence: u64,
}

impl ChangeCursor {
    pub fn parse(text: &[u8]) -> Result<Self, ProtocolError> {
        const INVALID: ProtocolError = ProtocolError::Invalid(
            "invalid change cursor, expected <generation>:<shard>:<sequence>",
        );
        let text = std::str::from_utf8(text).map_err(|_| INVALID)?;
        let (head, sequence) = text.rsplit_once(':').ok_or(INVALID)?;
        let (generation, shard) = match head.split_once(':') {
            Some((generation, shard)) => (Some(generation.parse().map_err(|_| INVALID)?), shard),
            None => (None, head),
        };

        Ok(Self {
            generation,
            shard: shard.parse().map_err(|_| INVALID)?,
            sequence: sequence.parse().map_err(|_| INVALID)?,
        })
    }

    pub fn text(self) -> String {
        match self.generation {
            Some(generation) => format!("{generation}:{}:{}", self.shard, self.sequence),
            None => format!("{}:{}", self.shard, self.sequence),
        }
    }
}

/// The history a shard's log continues: its generation and, for a promoted replica, the
/// generation it forked from and the last sequence the two logs share.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangeLineage {
    pub generation: u64,
    pub fork: Option<ChangeFork>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangeFork {
    pub generation: u64,
    pub sequence: u64,
}

/// Where a stream starts, in this shard's generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeStart {
    /// The records after the cursor are this log's: stream them.
    Resume(ChangeCursor),
    /// The client saw records past the fork that this log never had: tell it once, then
    /// stream from the fork.
    Rewound(ChangeCursor),
}

impl ChangeLineage {
    /// Where a client holding `cursor` continues on this shard, whose log is durable through
    /// `durable`. Only the generation this one forked from is known: a cursor from an older
    /// one, or from a newer primary, is refused.
    pub fn resume(
        &self,
        cursor: ChangeCursor,
        shard: u32,
        durable: u64,
    ) -> Result<ChangeStart, &'static [u8]> {
        if cursor.shard != shard {
            return Err(b"ERR change cursor belongs to a different shard");
        }

        let here = |sequence| ChangeCursor {
            generation: Some(self.generation),
            shard,
            sequence,
        };
        let generation = cursor.generation.unwrap_or(self.generation);

        if generation == self.generation {
            if cursor.sequence > durable {
                return Err(b"ERR change cursor is ahead of this server");
            }

            return Ok(ChangeStart::Resume(here(cursor.sequence)));
        }
        if generation > self.generation {
            return Err(b"ERR change cursor generation is newer than this server");
        }

        match self.fork {
            Some(fork) if fork.generation == generation && cursor.sequence <= fork.sequence => {
                Ok(ChangeStart::Resume(here(cursor.sequence)))
            }
            Some(fork) if fork.generation == generation => {
                Ok(ChangeStart::Rewound(here(fork.sequence)))
            }
            _ => Err(b"ERR change cursor generation is unknown to this server"),
        }
    }
}

/// One client-visible change.
pub struct ChangeEvent<'a> {
    pub kind: ChangeKind,
    pub key: Option<&'a [u8]>,
    pub value: Option<Vec<u8>>,
}

/// Calls `emit` once per key a record touched, in record order. Transactions expand in place.
/// `generation_of` names the generation of a shard of this server a range moved to.
pub fn describe(
    mutation: &OwnedMutation,
    generation_of: &dyn Fn(u32) -> u64,
    emit: &mut dyn FnMut(ChangeEvent<'_>),
) {
    use OwnedMutation as M;

    let mut one = |kind: ChangeKind, key: &[u8], value: Option<Vec<u8>>| {
        emit(ChangeEvent {
            kind,
            key: Some(key),
            value,
        });
    };

    match mutation {
        M::SetBytes { key, value }
        | M::SetVersioned { key, value, .. }
        | M::IdempotencyPut { key, value, .. } => one(ChangeKind::Put, key, Some(value.clone())),
        M::SetBytesExpiring { key, value, .. } => {
            // Same events as the separate value and deadline records it replaces.
            one(ChangeKind::Put, key, Some(value.clone()));
            one(ChangeKind::Expire, key, None);
        }
        M::SetInteger { key, value } => {
            one(ChangeKind::Put, key, Some(value.to_string().into_bytes()));
        }
        M::JsonPut { key, value }
        | M::JsonSetPath { key, value, .. }
        | M::JsonMergePath { key, value, .. } => one(ChangeKind::Json, key, Some(value.clone())),
        M::JsonDelPath { key, .. } => one(ChangeKind::Json, key, None),
        M::Delete { keys } => {
            for key in keys {
                one(ChangeKind::Del, key, None);
            }
        }
        M::ExpireAt { key, .. } | M::Persist { key } => one(ChangeKind::Expire, key, None),
        M::Rename {
            source,
            destination,
        } => {
            one(ChangeKind::Rename, source, None);
            one(ChangeKind::Rename, destination, None);
        }
        M::MoveKey { key, .. } => one(ChangeKind::Rename, key, None),
        M::ListPushFront { key, .. }
        | M::ListPopFront { key }
        | M::ListPushBack { key, .. }
        | M::ListPopBack { key }
        | M::ListTrim { key, .. }
        | M::ListSet { key, .. }
        | M::ListInsert { key, .. }
        | M::ListRemove { key, .. } => one(ChangeKind::List, key, None),
        // A partial overwrite: the record carries no whole value to publish.
        M::SetRange { key, .. } => one(ChangeKind::Put, key, None),
        M::SetAdd { key, .. } | M::SetRemove { key, .. } | M::SetStore { key, .. } => {
            one(ChangeKind::Set, key, None);
        }
        M::SetMove {
            source,
            destination,
            ..
        } => {
            one(ChangeKind::Set, source, None);
            one(ChangeKind::Set, destination, None);
        }
        M::HashSet { key, .. }
        | M::HashDelete { key, .. }
        | M::HashFieldExpire { key, .. }
        | M::HashFieldPersist { key, .. } => one(ChangeKind::Hash, key, None),
        M::SortedSetAdd { key, .. } | M::SortedSetRemove { key, .. } => {
            one(ChangeKind::Zset, key, None);
        }
        M::StreamAppend { key, .. }
        | M::StreamDelete { key, .. }
        | M::StreamTrim { key, .. }
        | M::StreamTrimMinId { key, .. }
        | M::StreamSetId { key, .. }
        | M::StreamGroupCreate { key, .. }
        | M::StreamClaim { key, .. }
        | M::StreamReclaim { key, .. }
        | M::StreamForceClaim { key, .. }
        | M::StreamAck { key, .. }
        | M::StreamGroupDestroy { key, .. }
        | M::StreamGroupSetId { key, .. }
        | M::StreamGroupDelConsumer { key, .. }
        | M::StreamGroupCreateConsumer { key, .. }
        | M::StreamHold { key, .. }
        | M::StreamGroupAdvance { key, .. } => one(ChangeKind::Stream, key, None),
        M::FlushDatabase | M::FlushAll | M::SwapDatabases { .. } => emit(ChangeEvent {
            kind: ChangeKind::Flush,
            key: None,
            value: None,
        }),
        M::Transaction { mutations } => {
            for inner in mutations {
                describe(inner, generation_of, emit);
            }
        }
        // The copy a slot move wrote here is not a client's write.
        M::SlotCopy { .. } => {}
        M::SlotsMoved {
            first,
            last,
            to,
            position,
        } => {
            let range = format!("{first}-{last}");
            let to = u32::from(*to);
            let cursor = ChangeCursor {
                generation: Some(generation_of(to)),
                shard: to,
                sequence: *position,
            }
            .text();

            emit(ChangeEvent {
                kind: ChangeKind::Moved,
                key: Some(range.as_bytes()),
                value: Some(cursor.into_bytes()),
            });
        }
        // A step of a range arriving from another node: the copies were not reported either.
        M::SlotsIncoming { .. } => {}
        M::SlotsGiven {
            first,
            last,
            address,
            position,
        } => {
            let range = format!("{first}-{last}");
            // `host:port:sequence`: the other node's generation is not known here, so the
            // client resumes there with the `<shard>:<sequence>` form, its current generation.
            let mut cursor = address.to_vec();

            cursor.extend_from_slice(format!(":{position}").as_bytes());
            emit(ChangeEvent {
                kind: ChangeKind::Moved,
                key: Some(range.as_bytes()),
                value: Some(cursor),
            });
        }
        // Ruvio's own structures. Listed by name, not caught by a wildcard, so a new record
        // kind fails to compile here until it is given an event.
        M::SemaphoreAcquire { .. }
        | M::SemaphoreRelease { .. }
        | M::BloomReserve { .. }
        | M::BloomAdd { .. }
        | M::BloomAddMany { .. }
        | M::LeasePut { .. }
        | M::LimitPut { .. }
        | M::OncePut { .. } => {
            for key in mutation.write_keys() {
                one(ChangeKind::Other, &key, None);
            }
        }
    }
}

/// Appends one event as a RESP3 push (`>`) or, for RESP2 clients, a plain array.
pub fn encode_event(
    out: &mut Vec<u8>,
    resp3: bool,
    cursor: ChangeCursor,
    database: u8,
    kind: ChangeKind,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) {
    out.extend_from_slice(if resp3 { b">6\r\n" } else { b"*6\r\n" });
    bulk(out, b"change");
    bulk(out, cursor.text().as_bytes());
    out.extend_from_slice(format!(":{database}\r\n").as_bytes());
    bulk(out, kind.name().as_bytes());
    nullable(out, resp3, key);
    nullable(out, resp3, value);
}

/// A cursor heartbeat, so a filter that drops every record still advances the client.
pub fn encode_cursor(out: &mut Vec<u8>, resp3: bool, cursor: ChangeCursor) {
    out.extend_from_slice(if resp3 { b">2\r\n" } else { b"*2\r\n" });
    bulk(out, b"cursor");
    bulk(out, cursor.text().as_bytes());
}

/// Tells the client that the records after its cursor were taken back by a failover: undo or
/// reload what it applied past `cursor`, the fork, where the stream continues.
pub fn encode_rewound(out: &mut Vec<u8>, resp3: bool, cursor: ChangeCursor) {
    out.extend_from_slice(if resp3 { b">2\r\n" } else { b"*2\r\n" });
    bulk(out, b"rewound");
    bulk(out, cursor.text().as_bytes());
}

/// Tells the client its cursor is no longer retained: reload fully, then `FROM NOW`.
pub fn encode_resync(out: &mut Vec<u8>, resp3: bool, reason: &str) {
    out.extend_from_slice(if resp3 { b">2\r\n" } else { b"*2\r\n" });
    bulk(out, b"resync");
    bulk(out, reason.as_bytes());
}

fn bulk(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
}

fn nullable(out: &mut Vec<u8>, resp3: bool, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => bulk(out, bytes),
        None => out.extend_from_slice(if resp3 { b"_\r\n" } else { b"$-1\r\n" }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Seen = (ChangeKind, Option<Vec<u8>>, Option<Vec<u8>>);

    fn collect(mutation: &OwnedMutation) -> Vec<Seen> {
        let mut events = Vec::new();

        describe(mutation, &|shard| u64::from(shard) + 10, &mut |event| {
            events.push((event.kind, event.key.map(<[u8]>::to_vec), event.value));
        });
        events
    }

    fn at(generation: Option<u64>, shard: u32, sequence: u64) -> ChangeCursor {
        ChangeCursor {
            generation,
            shard,
            sequence,
        }
    }

    #[test]
    fn cursors_round_trip_and_reject_garbage() {
        let cursor = ChangeCursor::parse(b"2:3:41").unwrap();

        assert_eq!(cursor, at(Some(2), 3, 41));
        assert_eq!(cursor.text(), "2:3:41");
        // The older form names no generation, and is written back as it came.
        let old = ChangeCursor::parse(b"3:41").unwrap();

        assert_eq!(old, at(None, 3, 41));
        assert_eq!(old.text(), "3:41");
        for garbage in [
            &b"41"[..],
            b"a:1",
            b"1:a:1",
            b"x:1:1",
            b"1:2:3:4",
            b"-1:0:1",
            b":0:1",
            b"0:1:",
        ] {
            assert!(ChangeCursor::parse(garbage).is_err(), "{garbage:?}");
        }
    }

    #[test]
    fn a_cursor_resumes_rewinds_past_the_fork_or_is_refused() {
        let promoted = ChangeLineage {
            generation: 3,
            fork: Some(ChangeFork {
                generation: 2,
                sequence: 100,
            }),
        };
        let resume = |cursor| promoted.resume(cursor, 0, 150);

        // This log's own cursors, in either form.
        assert_eq!(
            resume(at(Some(3), 0, 120)),
            Ok(ChangeStart::Resume(at(Some(3), 0, 120)))
        );
        assert_eq!(
            resume(at(None, 0, 120)),
            Ok(ChangeStart::Resume(at(Some(3), 0, 120)))
        );
        // The old primary's, up to the fork, are this log's too.
        assert_eq!(
            resume(at(Some(2), 0, 100)),
            Ok(ChangeStart::Resume(at(Some(3), 0, 100)))
        );
        assert_eq!(
            resume(at(Some(2), 0, 0)),
            Ok(ChangeStart::Resume(at(Some(3), 0, 0)))
        );
        // Past the fork: the client saw records this log never had, even where this log
        // has reached the same sequence since.
        assert_eq!(
            resume(at(Some(2), 0, 101)),
            Ok(ChangeStart::Rewound(at(Some(3), 0, 100)))
        );
        assert_eq!(
            resume(at(Some(2), 0, 500)),
            Ok(ChangeStart::Rewound(at(Some(3), 0, 100)))
        );
        assert_eq!(
            resume(at(Some(4), 0, 1)),
            Err(&b"ERR change cursor generation is newer than this server"[..])
        );
        assert_eq!(
            resume(at(Some(1), 0, 1)),
            Err(&b"ERR change cursor generation is unknown to this server"[..])
        );
        assert_eq!(
            resume(at(Some(3), 0, 151)),
            Err(&b"ERR change cursor is ahead of this server"[..])
        );
        assert_eq!(
            resume(at(Some(3), 1, 1)),
            Err(&b"ERR change cursor belongs to a different shard"[..])
        );

        // A log that never forked knows only its own generation.
        let root = ChangeLineage {
            generation: 0,
            fork: None,
        };

        assert_eq!(
            root.resume(at(None, 0, 7), 0, 7),
            Ok(ChangeStart::Resume(at(Some(0), 0, 7)))
        );
        assert_eq!(
            root.resume(at(Some(1), 0, 7), 0, 7),
            Err(&b"ERR change cursor generation is newer than this server"[..])
        );
    }

    #[test]
    fn subscription_options_parse_and_filter() {
        let parsed = ChangeSubscription::parse(&[
            b"FROM", b"0:7", b"match", b"user:*", b"TYPES", b"put,del", b"values",
        ])
        .unwrap();

        assert_eq!(parsed.from, Some(at(None, 0, 7)));
        assert!(parsed.values);
        assert!(!parsed.committed);
        assert!(parsed.accepts(ChangeKind::Put, Some(b"user:1")));
        assert!(!parsed.accepts(ChangeKind::Put, Some(b"order:1")));
        assert!(!parsed.accepts(ChangeKind::List, Some(b"user:1")));
        assert!(ChangeSubscription::parse(&[b"TYPES", b"nope"]).is_err());
        assert!(ChangeSubscription::parse(&[b"FROM"]).is_err());
        assert_eq!(
            ChangeSubscription::parse(&[b"FROM", b"NOW"]).unwrap().from,
            None
        );

        let committed = ChangeSubscription::parse(&[b"from", b"1:0:7", b"committed"]).unwrap();

        assert_eq!(committed.from, Some(at(Some(1), 0, 7)));
        assert!(committed.committed);
        assert!(ChangeSubscription::parse(&[b"FROM", b"1:x:7"]).is_err());
    }

    #[test]
    fn a_move_inside_the_server_names_the_new_owner_generation() {
        let events = collect(&OwnedMutation::SlotsMoved {
            first: 0,
            last: 99,
            to: 1,
            position: 42,
        });

        assert_eq!(
            events,
            vec![(
                ChangeKind::Moved,
                Some(b"0-99".to_vec()),
                Some(b"11:1:42".to_vec())
            )]
        );
    }

    #[test]
    fn records_expand_to_one_event_per_key_in_order() {
        let events = collect(&OwnedMutation::Transaction {
            mutations: vec![
                OwnedMutation::SetBytes {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                OwnedMutation::Delete {
                    keys: vec![b"b".to_vec(), b"c".to_vec()],
                },
            ],
        });

        assert_eq!(
            events,
            vec![
                (ChangeKind::Put, Some(b"a".to_vec()), Some(b"1".to_vec())),
                (ChangeKind::Del, Some(b"b".to_vec()), None),
                (ChangeKind::Del, Some(b"c".to_vec()), None),
            ]
        );
    }

    #[test]
    fn events_encode_as_push_or_array() {
        let cursor = at(Some(1), 0, 9);
        let mut resp3 = Vec::new();
        let mut resp2 = Vec::new();

        encode_event(
            &mut resp3,
            true,
            cursor,
            0,
            ChangeKind::Del,
            Some(b"k"),
            None,
        );
        encode_event(
            &mut resp2,
            false,
            cursor,
            0,
            ChangeKind::Del,
            Some(b"k"),
            None,
        );

        assert_eq!(
            resp3,
            b">6\r\n$6\r\nchange\r\n$5\r\n1:0:9\r\n:0\r\n$3\r\ndel\r\n$1\r\nk\r\n_\r\n"
        );
        assert_eq!(
            resp2,
            b"*6\r\n$6\r\nchange\r\n$5\r\n1:0:9\r\n:0\r\n$3\r\ndel\r\n$1\r\nk\r\n$-1\r\n"
        );

        let (mut resp3, mut resp2) = (Vec::new(), Vec::new());

        encode_rewound(&mut resp3, true, at(Some(2), 0, 100));
        encode_rewound(&mut resp2, false, at(Some(2), 0, 100));
        assert_eq!(resp3, b">2\r\n$7\r\nrewound\r\n$7\r\n2:0:100\r\n");
        assert_eq!(resp2, b"*2\r\n$7\r\nrewound\r\n$7\r\n2:0:100\r\n");
    }
}
