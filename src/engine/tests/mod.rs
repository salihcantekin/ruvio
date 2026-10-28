use super::*;
use crate::protocol::{
    ClaimOptions, FieldExpireCondition, FieldExpireTime, FieldTtlReply, LexBound, ScoreBound,
    SortedRemoveRange, SortedSetAddOptions, parse_command,
};

/// Streamed replies borrow the store; compare them by value.
fn execute_owned(response: Response<'_>) -> Response<'static> {
    response.into_owned()
}

/// Set and hash replies carry no order guarantee; sort them before comparing.
fn unordered(response: Response<'_>) -> Response<'static> {
    match response.into_owned() {
        Response::Set(mut members) => {
            members.sort();
            Response::Set(members)
        }
        Response::Map(mut pairs) => {
            pairs.sort_by(|left, right| left.0.cmp(&right.0));
            Response::Map(pairs)
        }
        Response::Array(mut values) => {
            values.sort();
            Response::Array(values)
        }
        other => other,
    }
}

/// A command parsed from its RESP parts, as a client would send it.
fn command_of(parts: &[&[u8]]) -> Command<'static> {
    let mut frame = format!("*{}\r\n", parts.len()).into_bytes();

    for part in parts {
        frame.extend(format!("${}\r\n", part.len()).bytes());
        frame.extend_from_slice(part);
        frame.extend_from_slice(b"\r\n");
    }

    let (command, _) = parse_command(&frame).unwrap().expect("complete frame");

    command.into_owned()
}

fn owned(value: &[u8]) -> Cow<'static, [u8]> {
    Cow::Owned(value.to_vec())
}

fn field_expire(
    key: &[u8],
    at: u64,
    condition: FieldExpireCondition,
    fields: &[&[u8]],
) -> Command<'static> {
    Command::HashExpire {
        key: owned(key),
        time: FieldExpireTime::AtMillis(at),
        condition,
        fields: fields.iter().map(|field| owned(field)).collect(),
        name: "hpexpireat",
    }
}

fn field_ttl(key: &[u8], fields: &[&[u8]]) -> Command<'static> {
    Command::HashTtl {
        key: owned(key),
        reply: FieldTtlReply::AtMillis,
        fields: fields.iter().map(|field| owned(field)).collect(),
    }
}

fn integers(values: &[i64]) -> Response<'static> {
    Response::Exec(
        values
            .iter()
            .map(|value| Response::Integer(*value))
            .collect(),
    )
}

#[derive(Debug, PartialEq, Eq)]
enum Reply {
    Simple(&'static [u8]),
    Bulk(Vec<u8>),
    Nil,
    Integer(i64),
    Array(Vec<Option<Vec<u8>>>),
    Error(&'static [u8]),
}

fn reply(shard: &mut KvShard, frame: &[u8]) -> Reply {
    let (command, consumed) = parse_command(frame).unwrap().expect("complete frame");

    assert_eq!(consumed, frame.len());
    match execute(shard, command.into_owned()) {
        Response::Simple(value) => Reply::Simple(value),
        Response::Bulk(None) => Reply::Nil,
        Response::Bulk(Some(value)) => Reply::Bulk(value.into_owned()),
        Response::Integer(value) => Reply::Integer(value),
        Response::Double(value) => Reply::Bulk(format_score(value)),
        Response::Bool(true) => Reply::Integer(1),
        Response::Bool(false) => Reply::Integer(0),
        Response::Array(values) => Reply::Array(
            values
                .into_iter()
                .map(|value| value.map(|bytes| bytes.into_owned()))
                .collect(),
        ),
        Response::Set(members) => Reply::Array(
            members
                .into_iter()
                .map(|member| Some(member.into_owned()))
                .collect(),
        ),
        Response::Map(pairs) => {
            let mut values = Vec::with_capacity(pairs.len().saturating_mul(2));

            for (key, value) in pairs {
                values.push(Some(key.into_owned()));
                match value {
                    Response::Bulk(Some(value)) => values.push(Some(value.into_owned())),
                    Response::Bulk(None) => values.push(None),
                    other => panic!("unexpected map value {other:?}"),
                }
            }

            Reply::Array(values)
        }

        Response::Error(value) => Reply::Error(value),
        other => panic!("unexpected {other:?}"),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RecordedMutation {
    SetBytes(Vec<u8>, Vec<u8>),
    SetInteger(Vec<u8>, i64),
    Delete(Vec<Vec<u8>>),
    Other,
}

#[derive(Default)]
struct RecordingSink {
    mutations: Vec<RecordedMutation>,
}

impl MutationSink for RecordingSink {
    type Error = std::convert::Infallible;

    fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        let mutation = match mutation {
            Mutation::SetBytes { key, value } => {
                RecordedMutation::SetBytes(key.to_vec(), value.to_vec())
            }

            Mutation::SetInteger { key, value } => {
                RecordedMutation::SetInteger(key.to_vec(), value)
            }

            Mutation::Delete { keys } => {
                RecordedMutation::Delete(keys.iter().map(|key| key.to_vec()).collect())
            }

            _ => RecordedMutation::Other,
        };

        self.mutations.push(mutation);
        Ok(())
    }
}

struct FailingSink;

impl MutationSink for FailingSink {
    type Error = &'static str;

    fn record(&mut self, _mutation: Mutation<'_>) -> Result<(), Self::Error> {
        Err("persistence unavailable")
    }
}

/// Test sink that keeps the database of every record, like the WAL header does.
#[derive(Default)]
struct DatabaseLog {
    database: u8,
    records: Vec<(u8, OwnedMutation)>,
}

impl MutationSink for DatabaseLog {
    type Error = std::convert::Infallible;

    fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        self.records.push((self.database, mutation.to_owned()));
        Ok(())
    }

    fn select_database(&mut self, database: u8) {
        self.database = database;
    }
}

fn in_db<'a>(
    shard: &'a mut KvShard,
    sink: &mut DatabaseLog,
    db: u8,
    command: Command<'a>,
) -> Response<'a> {
    let Ok(response) = execute_in_database(shard, command, sink, &mut (), db);

    response
}

fn replayed(sink: &DatabaseLog) -> KvShard {
    let mut shard = KvShard::new();

    for (database, mutation) in &sink.records {
        crate::persistence::apply_mutation(&mut shard, *database, mutation).unwrap();
    }

    shard
}

fn db_get(shard: &mut KvShard, db: u8, key: &[u8]) -> Option<Vec<u8>> {
    match in_db(
        shard,
        &mut DatabaseLog::default(),
        db,
        Command::Get(owned(key)),
    ) {
        Response::Bulk(value) => value.map(Cow::into_owned),
        other => panic!("unexpected GET reply {other:?}"),
    }
}

fn pending_row(id: &[u8], consumer: &[u8], idle: i64, deliveries: i64) -> Response<'static> {
    Response::Exec(vec![
        Response::Bulk(Some(Cow::Owned(id.to_vec()))),
        Response::Bulk(Some(Cow::Owned(consumer.to_vec()))),
        Response::Integer(idle),
        Response::Integer(deliveries),
    ])
}

mod admin;
mod bloom;
mod collections;
mod complexity;
mod conditional;
mod create_ttl;
mod databases;
mod due;
mod durable_ttl;
mod evict;
mod expire;
mod geo;
mod hash;
mod hot;
mod json;
mod json_ops_more;
mod lease;
mod limit_modes;
mod misc;
mod replay_failures;
mod response;
mod scan_churn;
mod shared_steps;
mod stream;
mod strings;
mod transaction;
mod transfer;
mod zset;
