//! Durable change stream: turns WAL records into client events.
//!
//! A subscriber names a cursor `<shard>:<sequence>` and receives every record after it, so a
//! client that stores the last cursor it processed resumes after a disconnect or restart without
//! loss or repeats. Records are atomic: all events of one record share a cursor and a write.

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
            } else {
                return Err(ProtocolError::Invalid("unknown CHANGES SUBSCRIBE option"));
            }
        }

        Ok(subscription)
    }
}

/// Position in one shard's WAL: the last sequence the client has processed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangeCursor {
    pub shard: u32,
    pub sequence: u64,
}

impl ChangeCursor {
    pub fn parse(text: &[u8]) -> Result<Self, ProtocolError> {
        const INVALID: ProtocolError =
            ProtocolError::Invalid("invalid change cursor, expected <shard>:<sequence>");
        let text = std::str::from_utf8(text).map_err(|_| INVALID)?;
        let (shard, sequence) = text.split_once(':').ok_or(INVALID)?;

        Ok(Self {
            shard: shard.parse().map_err(|_| INVALID)?,
            sequence: sequence.parse().map_err(|_| INVALID)?,
        })
    }

    pub fn text(self) -> String {
        format!("{}:{}", self.shard, self.sequence)
    }
}

/// One client-visible change.
pub struct ChangeEvent<'a> {
    pub kind: ChangeKind,
    pub key: Option<&'a [u8]>,
    pub value: Option<Vec<u8>>,
}

/// Calls `emit` once per key a record touched, in record order. Transactions expand in place.
pub fn describe(mutation: &OwnedMutation, emit: &mut dyn FnMut(ChangeEvent<'_>)) {
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
                describe(inner, emit);
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
            let cursor = format!("{to}:{position}");

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

        describe(mutation, &mut |event| {
            events.push((event.kind, event.key.map(<[u8]>::to_vec), event.value));
        });
        events
    }

    #[test]
    fn cursors_round_trip_and_reject_garbage() {
        let cursor = ChangeCursor::parse(b"3:41").unwrap();

        assert_eq!(
            cursor,
            ChangeCursor {
                shard: 3,
                sequence: 41
            }
        );
        assert_eq!(cursor.text(), "3:41");
        assert!(ChangeCursor::parse(b"41").is_err());
        assert!(ChangeCursor::parse(b"a:1").is_err());
    }

    #[test]
    fn subscription_options_parse_and_filter() {
        let parsed = ChangeSubscription::parse(&[
            b"FROM", b"0:7", b"match", b"user:*", b"TYPES", b"put,del", b"values",
        ])
        .unwrap();

        assert_eq!(
            parsed.from,
            Some(ChangeCursor {
                shard: 0,
                sequence: 7
            })
        );
        assert!(parsed.values);
        assert!(parsed.accepts(ChangeKind::Put, Some(b"user:1")));
        assert!(!parsed.accepts(ChangeKind::Put, Some(b"order:1")));
        assert!(!parsed.accepts(ChangeKind::List, Some(b"user:1")));
        assert!(ChangeSubscription::parse(&[b"TYPES", b"nope"]).is_err());
        assert!(ChangeSubscription::parse(&[b"FROM"]).is_err());
        assert_eq!(
            ChangeSubscription::parse(&[b"FROM", b"NOW"]).unwrap().from,
            None
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
        let cursor = ChangeCursor {
            shard: 0,
            sequence: 9,
        };
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
            b">6\r\n$6\r\nchange\r\n$3\r\n0:9\r\n:0\r\n$3\r\ndel\r\n$1\r\nk\r\n_\r\n"
        );
        assert_eq!(
            resp2,
            b"*6\r\n$6\r\nchange\r\n$3\r\n0:9\r\n:0\r\n$3\r\ndel\r\n$1\r\nk\r\n$-1\r\n"
        );
    }
}
