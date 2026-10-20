//! Redis keyspace commands beyond the basics: `KEYS`, `RANDOMKEY`, `TOUCH`,
//! `RENAMENX`, `EXPIRETIME`, `PEXPIRETIME` and `COPY`.

use super::*;
use crate::mutation::OwnedMutation;

/// `KEYS` walks the whole database in one call, so it refuses databases past this size.
/// Use `SCAN` there.
pub(super) const KEYS_LIMIT: usize = 100_000;

pub(super) fn keys_matching<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    pattern: &[u8],
) -> Response<'a> {
    if shard.database_len() > KEYS_LIMIT {
        return Response::Error(b"ERR KEYS is limited to 100000 keys per database, use SCAN");
    }

    let mut found = Vec::new();
    let mut cursor = 0;

    loop {
        let (next, keys) = shard.scan(cursor, 256, Some(pattern), None);

        found.extend(keys.into_iter().map(|key| Some(Cow::Borrowed(key))));

        if next == 0 {
            return Response::Array(found);
        }

        cursor = next;
    }
}

/// Scan batches `RANDOMKEY` looks at before it answers nil, like the tries Redis bounds its
/// search with. A keyspace of only expired, unreaped keys would otherwise be walked whole.
const RANDOMKEY_TRIES: usize = 100;

pub(super) fn random_key<'a, C: Clock, I: StorageIndex>(shard: &'a KvShard<C, I>) -> Response<'a> {
    let mut cursor = super::collections_ext::draw(usize::MAX >> 1) as u64;
    let mut wrapped = false;

    for _ in 0..RANDOMKEY_TRIES {
        let (next, keys) = shard.scan(cursor, 16, None, None);

        if !keys.is_empty() {
            let key = keys[super::collections_ext::draw(keys.len())];

            return Response::Bulk(Some(Cow::Borrowed(key)));
        }

        if next == 0 {
            if wrapped {
                break;
            }

            wrapped = true;
        }

        cursor = next;
    }

    Response::Bulk(None)
}

pub(super) fn touch<C: Clock, I: StorageIndex>(
    shard: &mut KvShard<C, I>,
    keys: &[crate::protocol::Bytes<'_>],
) -> Response<'static> {
    let mut found = 0;

    for key in keys {
        // The access time a recency policy evicts by was refreshed before the command ran
        // (`touch_read_keys`).
        if shard.contains_key(key) {
            found += 1;
        }
    }

    Response::Integer(found)
}

pub(super) fn expire_time<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
    millis: bool,
) -> Response<'static> {
    let Some(deadline) = shard.live_deadline(key) else {
        return Response::Integer(-2);
    };

    Response::Integer(match deadline {
        None => -1,
        Some(at) if millis => i64::try_from(at.get()).unwrap_or(i64::MAX),
        Some(at) => i64::try_from(at.get() / 1_000).unwrap_or(i64::MAX),
    })
}

pub(super) fn rename_nx<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    source: crate::protocol::Bytes<'a>,
    destination: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    if !shard.contains_key(&source) {
        return Ok(Response::Error(b"ERR no such key"));
    }

    if shard.contains_key(&destination) {
        return Ok(Response::Integer(0));
    }

    Ok(
        match keyspace::rename_key(shard, sink, source, destination)? {
            Response::Simple(_) => Response::Integer(1),
            other => other,
        },
    )
}

const HASH_FIELD_TTL: &[u8] = b"ERR COPY of a hash with field TTLs is not supported";

/// What `COPY` carries over, read out of the source before anything is written.
enum Copied {
    Bytes(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    Sorted(Vec<(f64, Vec<u8>)>),
    Json(Vec<u8>),
}

const BAD_DATA_FORMAT: &[u8] = b"ERR Bad data format";

impl Copied {
    /// True when the value is one this server could have produced: no empty collection, no
    /// NaN score, and the rebuilding mutation replays on an empty shard into a key.
    fn restorable(&self, key: &[u8]) -> bool {
        let shaped = match self {
            Self::Bytes(_) => true,
            // Stored documents are canonical compact JSON, and nothing re-checks them later.
            Self::Json(document) => {
                crate::json::set_root(document).is_ok_and(|canonical| canonical == *document)
            }
            Self::List(items) | Self::Set(items) => !items.is_empty(),
            Self::Hash(pairs) => !pairs.is_empty(),
            Self::Sorted(pairs) => {
                !pairs.is_empty() && pairs.iter().all(|(score, _)| !score.is_nan())
            }
        };

        if !shaped {
            return false;
        }

        let mut scratch = crate::KvShard::new();

        crate::persistence::apply_mutation(&mut scratch, 0, &self.operation(key)).is_ok()
            && scratch.contains_key(key)
    }

    fn payload_bytes(&self) -> usize {
        match self {
            Self::Bytes(value) | Self::Json(value) => value.len(),
            Self::List(items) | Self::Set(items) => items.iter().map(Vec::len).sum(),
            Self::Hash(pairs) => pairs
                .iter()
                .map(|(field, value)| field.len() + value.len())
                .sum(),
            Self::Sorted(pairs) => pairs.iter().map(|(_, member)| member.len() + 8).sum(),
        }
    }

    fn operation(&self, key: &[u8]) -> OwnedMutation {
        let key = key.to_vec();

        match self {
            Self::Bytes(value) => OwnedMutation::SetBytes {
                key,
                value: value.clone(),
            },
            Self::Json(value) => OwnedMutation::JsonPut {
                key,
                value: value.clone(),
            },
            Self::List(values) => OwnedMutation::ListPushBack {
                key,
                values: values.clone(),
            },
            Self::Set(members) => OwnedMutation::SetStore {
                key,
                members: members.clone(),
            },
            Self::Hash(pairs) => OwnedMutation::HashSet {
                key,
                pairs: pairs.clone(),
            },
            Self::Sorted(pairs) => OwnedMutation::SortedSetAdd {
                key,
                pairs: pairs.clone(),
            },
        }
    }
}

// A new value kind must decide here whether COPY and DUMP carry it: no wildcard arm.
#[deny(clippy::wildcard_enum_match_arm)]
fn read_source<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
) -> Result<Copied, &'static [u8]> {
    const UNSUPPORTED: &[u8] = b"ERR COPY is not supported for this type";

    match shard.value_kind(key) {
        Some(ValueKind::Bytes) => shard
            .get_bytes(key)
            .ok()
            .flatten()
            .map(|value| Copied::Bytes(value.to_vec()))
            .ok_or(UNSUPPORTED),
        Some(ValueKind::List) => shard
            .list_range(key, 0, -1)
            .map(|items| Copied::List(items.into_iter().map(<[u8]>::to_vec).collect()))
            .map_err(|_| UNSUPPORTED),
        Some(ValueKind::Set) => shard
            .set_members(key)
            .map(|members| Copied::Set(members.into_iter().map(Cow::into_owned).collect()))
            .map_err(|_| UNSUPPORTED),
        Some(ValueKind::Hash) => {
            if shard.hash_has_field_ttl(key) {
                return Err(HASH_FIELD_TTL);
            }

            shard
                .hash_get_all(key)
                .map(|pairs| {
                    Copied::Hash(
                        pairs
                            .into_iter()
                            .map(|(field, value)| (field.to_vec(), value.to_vec()))
                            .collect(),
                    )
                })
                .map_err(|_| UNSUPPORTED)
        }
        Some(ValueKind::SortedSet) => shard
            .sorted_set_range_entries(key, 0, -1, false)
            .map(|entries| {
                Copied::Sorted(
                    entries
                        .into_iter()
                        .map(|(member, score)| (score, member.to_vec()))
                        .collect(),
                )
            })
            .map_err(|_| UNSUPPORTED),
        Some(ValueKind::Json) => {
            if let Some(tree) = shard.json_tree(key) {
                return Ok(Copied::Json(tree.to_vec()));
            }

            shard
                .json_view(key)
                .ok()
                .flatten()
                .map(|value| Copied::Json(value.to_vec()))
                .ok_or(UNSUPPORTED)
        }
        None
        | Some(
            ValueKind::BloomFilter
            | ValueKind::Stream
            | ValueKind::Lease
            | ValueKind::Versioned
            | ValueKind::Limit
            | ValueKind::Once
            | ValueKind::Semaphore,
        ) => Err(UNSUPPORTED),
    }
}

pub(super) fn copy_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    source: crate::protocol::Bytes<'a>,
    destination: crate::protocol::Bytes<'a>,
    replace: bool,
) -> Result<Response<'a>, S::Error> {
    if source == destination {
        return Ok(Response::Error(
            b"ERR source and destination objects are the same",
        ));
    }

    let Some(deadline) = shard.live_deadline(&source) else {
        return Ok(Response::Integer(0));
    };

    if !replace && shard.contains_key(&destination) {
        return Ok(Response::Integer(0));
    }

    // A string is copied like `SET`: it replaces any destination type and overwrites a
    // destination string in place, with no delete and reinsert. Short values go through
    // the stack: the source must be released before the destination is written.
    let mut inline = [0u8; 256];
    let mut spilled = Vec::new();
    let length = match shard.value_view(&source) {
        Ok(Some(crate::store::ByteView::Borrowed(bytes))) if bytes.len() <= inline.len() => {
            inline[..bytes.len()].copy_from_slice(bytes);
            Some(bytes.len())
        }
        Ok(Some(crate::store::ByteView::Borrowed(bytes))) => {
            spilled = bytes.to_vec();
            Some(spilled.len())
        }
        Ok(Some(crate::store::ByteView::Integer(number))) => {
            let mut digits = [0u8; 20];
            let length = crate::store::format_integer(number, &mut digits);

            inline[..length].copy_from_slice(&digits[..length]);
            Some(length)
        }
        _ => None,
    };

    if let Some(length) = length {
        let value: &[u8] = if spilled.is_empty() {
            &inline[..length]
        } else {
            &spilled
        };
        let extra = value.len() + destination.len() + ADMISSION_OVERHEAD_BYTES;

        if let Some(rejected) = before_mutation(shard, sink, [destination.as_ref()], extra)? {
            return Ok(rejected);
        }

        match deadline {
            None => sink.record(Mutation::SetBytes {
                key: &destination,
                value,
            })?,
            Some(at) => sink.record(Mutation::Transaction {
                mutations: &[
                    OwnedMutation::SetBytes {
                        key: destination.to_vec(),
                        value: value.to_vec(),
                    },
                    OwnedMutation::ExpireAt {
                        key: destination.to_vec(),
                        unix_millis: at.get(),
                    },
                ],
            })?,
        }

        shard.set_bytes(&destination, value);

        if let Some(at) = deadline {
            shard.expire_at(&destination, at);
        }

        return Ok(Response::Integer(1));
    }

    let copied = match read_source(shard, &source) {
        Ok(copied) => copied,
        Err(message) => return Ok(Response::Error(message)),
    };

    if let Some(rejected) = write_copied(shard, sink, &destination, &copied, deadline)? {
        return Ok(rejected);
    }

    Ok(Response::Integer(1))
}

/// Replaces `destination` with `copied` and the deadline, as one atomic WAL transaction.
/// Returns a rejection response when the memory budget refuses the write.
fn write_copied<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    destination: &[u8],
    copied: &Copied,
    deadline: Option<crate::UnixMillis>,
) -> Result<Option<Response<'static>>, S::Error> {
    let extra = copied.payload_bytes() + destination.len() + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [destination], extra)? {
        return Ok(Some(rejected));
    }

    // Checked on this shard before anything is recorded: the log must not hold a value
    // memory then refuses. A sorted set is the one value that can still not fit.
    if let Copied::Sorted(pairs) = copied
        && !shard.sorted_set_make_room(
            destination,
            true,
            pairs.iter().map(|(_, member)| member.as_slice()),
        )
    {
        return Ok(Some(sorted_set_error(SortedSetError::Full)));
    }

    let mut operations = vec![
        OwnedMutation::Delete {
            keys: vec![destination.to_vec()],
        },
        copied.operation(destination),
    ];

    if let Some(at) = deadline {
        operations.push(OwnedMutation::ExpireAt {
            key: destination.to_vec(),
            unix_millis: at.get(),
        });
    }

    sink.record(Mutation::Transaction {
        mutations: &operations,
    })?;
    shard.delete(destination);

    // The value was checked (or read from a live key), so these do not fail; if one ever
    // did, say so instead of answering OK for a key that is not there.
    let applied = match copied {
        Copied::Bytes(value) => {
            shard.set_bytes(destination, value);
            true
        }
        Copied::Json(value) => shard.put_json(destination, value.as_slice()).is_ok(),
        Copied::List(values) => shard.list_push_back(destination, values).is_ok(),
        Copied::Set(members) => {
            shard.set_store(destination, members);
            true
        }
        Copied::Hash(pairs) => shard.hash_set(destination, pairs).is_ok(),
        Copied::Sorted(pairs) => shard.sorted_set_add_pairs(destination, pairs).is_ok(),
    };

    if !applied {
        // The log holds the value; delete it there too, so a restart agrees with memory.
        let keys = [Cow::Borrowed(destination)];

        sink.record(Mutation::Delete { keys: &keys })?;
        shard.delete(destination);
        return Ok(Some(Response::Error(b"ERR value could not be applied")));
    }

    if let Some(at) = deadline {
        shard.expire_at(destination, at);
    }

    Ok(None)
}

impl Copied {
    fn from_mutation(mutation: OwnedMutation) -> Option<Self> {
        Some(match mutation {
            OwnedMutation::SetBytes { value, .. } => Self::Bytes(value),
            OwnedMutation::JsonPut { value, .. } => Self::Json(value),
            OwnedMutation::ListPushBack { values, .. } => Self::List(values),
            OwnedMutation::SetStore { members, .. } => Self::Set(members),
            OwnedMutation::HashSet { pairs, .. } => Self::Hash(pairs),
            OwnedMutation::SortedSetAdd { pairs, .. } => Self::Sorted(pairs),
            _ => return None,
        })
    }
}

pub(super) fn dump_key<'a, C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
) -> Response<'a> {
    if !shard.contains_key(key) {
        return Response::Bulk(None);
    }

    let copied = match read_source(shard, key) {
        Ok(copied) => copied,
        Err(HASH_FIELD_TTL) => {
            return Response::Error(b"ERR DUMP of a hash with field TTLs is not supported");
        }
        Err(_) => return Response::Error(b"ERR DUMP is not supported for this type"),
    };

    match crate::dump::encode(&copied.operation(b"")) {
        Some(payload) => Response::Bulk(Some(Cow::Owned(payload))),
        None => Response::Error(b"ERR DUMP payload could not be built"),
    }
}

pub(super) fn restore_key<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    ttl: i64,
    payload: crate::protocol::Bytes<'a>,
    replace: bool,
    absolute: bool,
) -> Result<Response<'a>, S::Error> {
    if ttl < 0 {
        return Ok(Response::Error(b"ERR Invalid TTL value, must be >= 0"));
    }

    let Some(copied) = crate::dump::decode(&payload)
        .ok()
        .and_then(|mutation| crate::dump::rekey(mutation, &key))
        .and_then(Copied::from_mutation)
    else {
        return Ok(Response::Error(crate::dump::BAD_PAYLOAD));
    };
    // The payload's checksum is not a signature: a crafted one must still not reach the
    // WAL with a value that replay (or a snapshot load) refuses.
    if !copied.restorable(&key) {
        return Ok(Response::Error(BAD_DATA_FORMAT));
    }

    let exists = shard.contains_key(&key);

    if exists && !replace {
        return Ok(Response::Error(b"BUSYKEY Target key name already exists."));
    }

    let now = shard.now_unix_millis();
    let deadline = match ttl {
        0 => None,
        ttl if absolute => Some(crate::UnixMillis::new(ttl as u64)),
        ttl => match now.checked_add(ttl as u64) {
            Some(deadline) => Some(deadline),
            None => {
                return Ok(Response::Error(
                    b"ERR invalid expire time in 'restore' command",
                ));
            }
        },
    };

    if deadline.is_some_and(|deadline| deadline <= now) {
        if exists {
            let keys = [key];

            before_mutation_allow_oom(shard, sink, [keys[0].as_ref()], 0)?;
            sink.record(Mutation::Delete { keys: &keys })?;
            shard.delete(&keys[0]);
        }

        return Ok(Response::Simple(b"OK"));
    }

    Ok(match write_copied(shard, sink, &key, &copied, deadline)? {
        Some(rejected) => rejected,
        None => Response::Simple(b"OK"),
    })
}

pub(super) fn memory_usage<'a, C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    key: &[u8],
) -> Response<'a> {
    match shard.key_memory_usage(key) {
        Some(bytes) => Response::Integer((bytes + ADMISSION_OVERHEAD_BYTES) as i64),
        None => Response::Bulk(None),
    }
}

pub(super) fn object<'a, C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    kind: crate::protocol::ObjectKind,
    key: &[u8],
) -> Response<'a> {
    use crate::protocol::ObjectKind;

    // Strings are the common case: one lookup answers existence, type and encoding.
    if matches!(kind, ObjectKind::Encoding) {
        match shard.value_view(key) {
            Ok(None) => return Response::Bulk(None),
            Ok(Some(view)) => {
                return Response::Bulk(Some(Cow::Borrowed(string_encoding(view))));
            }
            Err(_) => {}
        }
    }

    if !shard.contains_key(key) {
        return Response::Bulk(None);
    }

    match kind {
        ObjectKind::Encoding => Response::Bulk(Some(Cow::Borrowed(encoding_name(shard, key)))),
        ObjectKind::IdleTime => match shard.idle_seconds(key) {
            Some(seconds) => Response::Integer(seconds as i64),
            None if matches!(shard.eviction_policy(), crate::EvictionPolicy::AllKeysLfu) => {
                Response::Error(
                    b"ERR An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
                )
            }
            None => Response::Error(
                b"ERR idle time is not tracked under this maxmemory-policy, use an LRU policy",
            ),
        },
        ObjectKind::Freq => match shard.access_frequency(key) {
            Some(counter) => Response::Integer(i64::from(counter)),
            None => Response::Error(
                b"ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
            ),
        },
    }
}

/// The name Redis would report for this value with its default thresholds. It describes the
/// logical shape (small or large, integer or text), not Ruvio's physical layout.
/// `int` for a canonical `i64` text, `embstr` up to 44 bytes, else `raw`.
fn string_encoding(view: crate::store::ByteView<'_>) -> &'static [u8] {
    let crate::store::ByteView::Borrowed(value) = view else {
        return b"int";
    };

    if value.len() <= 20
        && std::str::from_utf8(value)
            .ok()
            .and_then(|text| text.parse::<i64>().ok())
            .is_some_and(|number| {
                let mut digits = [0u8; 20];
                let len = crate::store::format_integer(number, &mut digits);

                &digits[..len] == value
            })
    {
        b"int"
    } else if value.len() <= 44 {
        b"embstr"
    } else {
        b"raw"
    }
}

fn encoding_name<C: Clock, I: StorageIndex>(shard: &KvShard<C, I>, key: &[u8]) -> &'static [u8] {
    const LISTPACK_ENTRIES: usize = 128;
    const LISTPACK_VALUE: usize = 64;
    const INTSET_ENTRIES: usize = 512;

    match shard.value_kind(key) {
        Some(ValueKind::Bytes) => match shard.value_view(key) {
            Ok(Some(view)) => string_encoding(view),
            _ => b"raw",
        },
        // Redis 7.0 names: lists are always quicklists, and a set that is not all integers is a
        // hashtable. Redis 7.2 reports listpack for small ones.
        Some(ValueKind::List) => b"quicklist",
        Some(ValueKind::Set) => {
            let all_integers = shard.set_card(key).is_ok_and(|len| len <= INTSET_ENTRIES)
                && shard.set_members(key).is_ok_and(|members| {
                    members.iter().all(|member| {
                        std::str::from_utf8(member).ok().is_some_and(|text| {
                            text.parse::<i64>().is_ok_and(|n| n.to_string() == text)
                        })
                    })
                });

            if all_integers {
                b"intset"
            } else {
                b"hashtable"
            }
        }
        Some(ValueKind::Hash) => {
            let small = shard.hash_len(key).is_ok_and(|len| len <= LISTPACK_ENTRIES)
                && shard.hash_get_all(key).is_ok_and(|pairs| {
                    pairs
                        .iter()
                        .all(|(field, value)| field.len().max(value.len()) <= LISTPACK_VALUE)
                });

            if small { b"listpack" } else { b"hashtable" }
        }
        Some(ValueKind::SortedSet) => {
            let small = shard
                .sorted_set_card(key)
                .is_ok_and(|len| len <= LISTPACK_ENTRIES)
                && shard
                    .sorted_set_range_entries(key, 0, -1, false)
                    .is_ok_and(|entries| {
                        entries
                            .iter()
                            .all(|(member, _)| member.len() <= LISTPACK_VALUE)
                    });

            if small { b"listpack" } else { b"skiplist" }
        }
        Some(ValueKind::Stream) => b"stream",
        _ => b"raw",
    }
}
