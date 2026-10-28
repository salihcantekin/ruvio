use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RespVersion {
    #[default]
    V2,
    V3,
}

impl RespVersion {
    pub fn null_bulk(self) -> &'static [u8] {
        match self {
            Self::V2 => b"$-1\r\n",
            Self::V3 => b"_\r\n",
        }
    }

    pub fn null_aggregate(self) -> &'static [u8] {
        match self {
            Self::V2 => b"*-1\r\n",
            Self::V3 => b"_\r\n",
        }
    }
}

/// Entries a sorted-set pop took out. The store no longer holds them once the pop is
/// applied, so they are copied into one buffer, each as
/// `member length u32 LE ‖ score f64 LE ‖ member`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct PoppedEntries {
    bytes: Vec<u8>,
    len: usize,
}

impl PoppedEntries {
    const HEADER: usize = 4 + 8;

    pub(crate) fn push(&mut self, member: &[u8], score: f64) {
        // Members are bounded by `max_value_bytes`, far below 4 GiB.
        let len = u32::try_from(member.len()).expect("sorted-set member under 4 GiB");

        self.bytes.reserve(Self::HEADER + member.len());
        self.bytes.extend_from_slice(&len.to_le_bytes());
        self.bytes.extend_from_slice(&score.to_le_bytes());
        self.bytes.extend_from_slice(member);
        self.len += 1;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `(member, score)` in pop order.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], f64)> + '_ {
        let mut rest = self.bytes.as_slice();

        std::iter::from_fn(move || {
            let (header, tail) = rest.split_first_chunk::<{ Self::HEADER }>()?;
            let (len, score) = header.split_at(4);
            let len = u32::from_le_bytes(len.try_into().expect("4 bytes")) as usize;
            let score = f64::from_le_bytes(score.try_into().expect("8 bytes"));
            let (member, tail) = tail.split_at(len);

            rest = tail;
            Some((member, score))
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Response<'a> {
    Simple(&'static [u8]),
    Bulk(Option<Cow<'a, [u8]>>),
    JsonTree(&'a crate::json::TreeDocument),
    Integer(i64),
    Array(Vec<Option<Cow<'a, [u8]>>>),
    Map(Vec<(Cow<'a, [u8]>, Response<'a>)>),
    Set(Vec<Cow<'a, [u8]>>),
    /// A set written member by member straight from the store (RESP3 set, RESP2 array).
    Members(crate::store::MemberStream<'a>),
    /// `LRANGE` written item by item straight from the list.
    ListSlice(crate::store::ListStream<'a>),
    /// Sorted-set range entries written directly; scores follow each member when asked.
    Scored(Vec<(&'a [u8], f64)>, bool),
    /// What a sorted-set pop took, each member followed by its score, after the key when
    /// one is given (`BZPOPMIN`). Written as `Scored(_, true)` is.
    Popped(Option<crate::protocol::Bytes<'a>>, PoppedEntries),
    /// What `ZPOPBYSCORE` took: members, each followed by its score when the flag is set.
    PoppedByScore(PoppedEntries, bool),
    /// A hash written straight from the store: pairs as a map, or only fields or values.
    HashEntries(crate::store::HashStream<'a>, crate::store::HashStreamKind),
    Double(f64),
    Bool(bool),
    Scan {
        cursor: u64,
        keys: Vec<Cow<'a, [u8]>>,
    },
    NullArray,
    BlockList {
        keys: Vec<crate::protocol::Bytes<'a>>,
        timeout_ms: Option<u64>,
        back: bool,
        /// `Some(max)` when a sorted-set pop parks instead of a list pop.
        sorted: Option<bool>,
    },
    BlockStream {
        keys: Vec<crate::protocol::Bytes<'a>>,
        cursors: Vec<(u64, u64)>,
        timeout_ms: Option<u64>,
    },
    BlockGroup {
        key: crate::protocol::Bytes<'a>,
        group: crate::protocol::Bytes<'a>,
        consumer: crate::protocol::Bytes<'a>,
        cursor: (u64, u64),
        timeout_ms: Option<u64>,
        noack: bool,
    },
    /// Nothing to move yet: the connection re-runs the command until it can, or times out.
    BlockRetry {
        timeout_ms: Option<u64>,
    },
    Exec(Vec<Response<'a>>),
    /// An array of one integer (`None`: nil) without the `Vec` of `Exec`.
    OneInteger(Option<i64>),
    Error(&'static [u8]),
    OwnedError(Vec<u8>),
    OwnedSimple(Vec<u8>),
}

impl Response<'_> {
    pub(crate) fn into_owned(self) -> Response<'static> {
        match self {
            Self::Simple(value) => Response::Simple(value),
            Self::Bulk(value) => Response::Bulk(value.map(|value| Cow::Owned(value.into_owned()))),
            Self::JsonTree(tree) => Response::Bulk(Some(Cow::Owned(tree.to_vec()))),
            Self::Integer(value) => Response::Integer(value),
            Self::OneInteger(value) => Response::OneInteger(value),
            Self::Array(values) => Response::Array(
                values
                    .into_iter()
                    .map(|value| value.map(|value| Cow::Owned(value.into_owned())))
                    .collect(),
            ),
            Self::Map(pairs) => Response::Map(
                pairs
                    .into_iter()
                    .map(|(key, value)| (Cow::Owned(key.into_owned()), value.into_owned()))
                    .collect(),
            ),
            Self::Set(members) => Response::Set(
                members
                    .into_iter()
                    .map(|member| Cow::Owned(member.into_owned()))
                    .collect(),
            ),
            Self::Members(stream) => {
                let mut members = Vec::with_capacity(stream.len());
                let _ = stream.visit::<()>(|member| {
                    members.push(Cow::Owned(member.to_vec()));
                    Ok(())
                });

                Response::Set(members)
            }
            Self::ListSlice(stream) => {
                let mut items = Vec::with_capacity(stream.len());
                let _ = stream.visit::<()>(|item| {
                    items.push(Some(Cow::Owned(item.to_vec())));
                    Ok(())
                });

                Response::Array(items)
            }
            Self::Scored(entries, with_scores) => {
                let mut values = Vec::with_capacity(entries.len() * (1 + usize::from(with_scores)));

                for (member, score) in entries {
                    values.push(Some(Cow::Owned(member.to_vec())));

                    if with_scores {
                        values.push(Some(Cow::Owned(format_score(score))));
                    }
                }

                Response::Array(values)
            }
            Self::Popped(key, entries) => {
                Response::Popped(key.map(|key| Cow::Owned(key.into_owned())), entries)
            }
            Self::PoppedByScore(entries, with_scores) => {
                Response::PoppedByScore(entries, with_scores)
            }
            Self::HashEntries(stream, kind) => {
                use crate::store::HashStreamKind;

                let mut pairs = Vec::with_capacity(stream.len());
                let _ = stream.visit::<()>(|field, value| {
                    pairs.push((field.to_vec(), value.to_vec()));
                    Ok(())
                });

                match kind {
                    HashStreamKind::Pairs => Response::Map(
                        pairs
                            .into_iter()
                            .map(|(field, value)| {
                                (Cow::Owned(field), Response::Bulk(Some(Cow::Owned(value))))
                            })
                            .collect(),
                    ),
                    HashStreamKind::Fields => Response::Array(
                        pairs
                            .into_iter()
                            .map(|(field, _)| Some(Cow::Owned(field)))
                            .collect(),
                    ),
                    HashStreamKind::Values => Response::Array(
                        pairs
                            .into_iter()
                            .map(|(_, value)| Some(Cow::Owned(value)))
                            .collect(),
                    ),
                }
            }
            Self::Double(value) => Response::Double(value),
            Self::Bool(value) => Response::Bool(value),
            Self::Scan { cursor, keys } => Response::Scan {
                cursor,
                keys: keys
                    .into_iter()
                    .map(|key| Cow::Owned(key.into_owned()))
                    .collect(),
            },
            Self::NullArray => Response::NullArray,
            Self::BlockList { .. }
            | Self::BlockStream { .. }
            | Self::BlockGroup { .. }
            | Self::BlockRetry { .. } => Response::Error(b"ERR command not allowed inside MULTI"),

            Self::Exec(responses) => {
                Response::Exec(responses.into_iter().map(Response::into_owned).collect())
            }

            Self::Error(message) => Response::Error(message),
            Self::OwnedError(message) => Response::OwnedError(message),
            Self::OwnedSimple(message) => Response::OwnedSimple(message),
        }
    }
}

pub fn write_response(writer: &mut impl Write, response: Response<'_>) -> io::Result<()> {
    write_response_in(writer, response, RespVersion::V2)
}

pub fn write_response_in(
    writer: &mut impl Write,
    response: Response<'_>,
    version: RespVersion,
) -> io::Result<()> {
    match response {
        Response::Simple(value) => {
            writer.write_all(b"+")?;
            writer.write_all(value)?;
            writer.write_all(b"\r\n")?;
        }

        Response::Bulk(Some(value)) => write_bulk(writer, value.as_ref())?,
        Response::Bulk(None) => writer.write_all(version.null_bulk())?,
        Response::JsonTree(tree) => {
            write_header(writer, b'$', tree.len())?;
            tree.write_to(&mut *writer)?;
            writer.write_all(b"\r\n")?;
        }

        Response::Integer(value) => write_integer(writer, b':', value)?,
        Response::Array(values) => {
            write_header(writer, b'*', values.len())?;

            for value in values {
                match value {
                    Some(value) => write_bulk(writer, value.as_ref())?,
                    None => writer.write_all(version.null_bulk())?,
                }
            }
        }

        Response::Map(pairs) => {
            match version {
                RespVersion::V3 => write_header(writer, b'%', pairs.len())?,
                RespVersion::V2 => write_header(writer, b'*', pairs.len().saturating_mul(2))?,
            }

            for (key, value) in pairs {
                write_bulk(writer, key.as_ref())?;
                write_response_in(writer, value, version)?;
            }
        }

        Response::Set(members) => {
            match version {
                RespVersion::V3 => write_header(writer, b'~', members.len())?,
                RespVersion::V2 => write_header(writer, b'*', members.len())?,
            }

            for member in members {
                write_bulk(writer, member.as_ref())?;
            }
        }

        Response::Members(stream) => {
            match version {
                RespVersion::V3 => write_header(writer, b'~', stream.len())?,
                RespVersion::V2 => write_header(writer, b'*', stream.len())?,
            }

            stream.visit(|member| write_bulk(&mut *writer, member))?;
        }

        Response::ListSlice(stream) => {
            write_header(writer, b'*', stream.len())?;
            stream.visit(|item| write_bulk(&mut *writer, item))?;
        }

        Response::Scored(entries, with_scores) => {
            write_header(writer, b'*', entries.len() * (1 + usize::from(with_scores)))?;

            for (member, score) in entries {
                write_bulk(writer, member)?;

                if with_scores {
                    write_score_bulk(writer, score)?;
                }
            }
        }

        Response::Popped(key, entries) => {
            write_header(
                writer,
                b'*',
                usize::from(key.is_some()) + entries.len().saturating_mul(2),
            )?;

            if let Some(key) = key {
                write_bulk(writer, key.as_ref())?;
            }

            for (member, score) in entries.iter() {
                write_bulk(writer, member)?;
                write_score_bulk(writer, score)?;
            }
        }

        Response::PoppedByScore(entries, with_scores) => {
            write_header(writer, b'*', entries.len() * (1 + usize::from(with_scores)))?;

            for (member, score) in entries.iter() {
                write_bulk(writer, member)?;

                if with_scores {
                    write_score_bulk(writer, score)?;
                }
            }
        }

        Response::HashEntries(stream, kind) => {
            use crate::store::HashStreamKind;

            match (kind, version) {
                (HashStreamKind::Pairs, RespVersion::V3) => {
                    write_header(writer, b'%', stream.len())?
                }
                (HashStreamKind::Pairs, RespVersion::V2) => {
                    write_header(writer, b'*', stream.len().saturating_mul(2))?
                }
                _ => write_header(writer, b'*', stream.len())?,
            }

            stream.visit(|field, value| match kind {
                HashStreamKind::Pairs => {
                    write_bulk(&mut *writer, field)?;
                    write_bulk(&mut *writer, value)
                }
                HashStreamKind::Fields => write_bulk(&mut *writer, field),
                HashStreamKind::Values => write_bulk(&mut *writer, value),
            })?;
        }

        Response::Double(value) => {
            let mut stack = ScoreText::default();
            let heap;
            let text: &[u8] = match stack.format(value) {
                Some(text) => text,
                None => {
                    heap = format_score(value);
                    &heap
                }
            };

            match version {
                RespVersion::V3 => {
                    writer.write_all(b",")?;
                    writer.write_all(text)?;
                    writer.write_all(b"\r\n")?;
                }

                RespVersion::V2 => write_bulk(writer, text)?,
            }
        }

        Response::Bool(value) => match (version, value) {
            (RespVersion::V3, true) => writer.write_all(b"#t\r\n")?,
            (RespVersion::V3, false) => writer.write_all(b"#f\r\n")?,
            (RespVersion::V2, true) => writer.write_all(b":1\r\n")?,
            (RespVersion::V2, false) => writer.write_all(b":0\r\n")?,
        },
        Response::Scan { cursor, keys } => {
            writer.write_all(b"*2\r\n")?;
            let cursor = cursor.to_string();

            write_bulk(writer, cursor.as_bytes())?;
            write_header(writer, b'*', keys.len())?;

            for key in keys {
                write_bulk(writer, key.as_ref())?;
            }
        }

        Response::OneInteger(value) => {
            write_header(writer, b'*', 1)?;
            write_response_in(
                writer,
                value.map_or(Response::Bulk(None), Response::Integer),
                version,
            )?;
        }
        Response::NullArray => writer.write_all(version.null_aggregate())?,
        Response::Exec(responses) => {
            write_header(writer, b'*', responses.len())?;

            for response in responses {
                write_response_in(writer, response, version)?;
            }
        }

        Response::BlockList { .. }
        | Response::BlockStream { .. }
        | Response::BlockGroup { .. }
        | Response::BlockRetry { .. } => {
            return Err(io::Error::other(
                "blocking list command requires runtime waiter support",
            ));
        }

        Response::Error(message) => {
            writer.write_all(b"-")?;
            writer.write_all(message)?;
            writer.write_all(b"\r\n")?;
        }

        Response::OwnedError(message) => {
            writer.write_all(b"-")?;
            writer.write_all(&message)?;
            writer.write_all(b"\r\n")?;
        }

        Response::OwnedSimple(message) => {
            writer.write_all(b"+")?;
            writer.write_all(&message)?;
            writer.write_all(b"\r\n")?;
        }
    }

    Ok(())
}

pub fn hello_response(
    version: RespVersion,
    connection_id: u64,
    cluster: bool,
) -> Response<'static> {
    let proto = match version {
        RespVersion::V2 => 2,
        RespVersion::V3 => 3,
    };

    let mode: &'static [u8] = if cluster { b"cluster" } else { b"standalone" };

    Response::Map(vec![
        (
            Cow::Borrowed(b"server"),
            Response::Bulk(Some(Cow::Borrowed(b"ruvio"))),
        ),
        (
            Cow::Borrowed(b"version"),
            Response::Bulk(Some(Cow::Owned(
                env!("CARGO_PKG_VERSION").as_bytes().to_vec(),
            ))),
        ),
        (Cow::Borrowed(b"proto"), Response::Integer(proto)),
        (
            Cow::Borrowed(b"id"),
            Response::Integer(i64::try_from(connection_id).unwrap_or(i64::MAX)),
        ),
        (
            Cow::Borrowed(b"mode"),
            Response::Bulk(Some(Cow::Borrowed(mode))),
        ),
        (
            Cow::Borrowed(b"role"),
            Response::Bulk(Some(Cow::Borrowed(b"master"))),
        ),
        (Cow::Borrowed(b"modules"), Response::Array(Vec::new())),
    ])
}

/// Values up to this size are framed with a small stack header, no staging copy of the value.
pub(super) const SMALL_BULK: usize = 128;

pub(super) fn write_bulk(writer: &mut impl Write, value: &[u8]) -> io::Result<()> {
    if value.len() <= SMALL_BULK {
        let mut header = [0u8; 6];
        let mut position = 4;
        let mut len = value.len();

        loop {
            position -= 1;
            header[position] = b'0' + (len % 10) as u8;
            len /= 10;

            if len == 0 {
                break;
            }
        }

        // `$` goes right before the digits; the CRLF follows them.
        position -= 1;
        header[position] = b'$';
        header[4] = b'\r';
        header[5] = b'\n';
        writer.write_all(&header[position..])?;
        writer.write_all(value)?;

        return writer.write_all(b"\r\n");
    }

    writer.write_all(b"$")?;
    write_usize(writer, value.len())?;
    writer.write_all(b"\r\n")?;
    writer.write_all(value)?;
    writer.write_all(b"\r\n")
}

pub(super) fn write_header(writer: &mut impl Write, prefix: u8, len: usize) -> io::Result<()> {
    writer.write_all(&[prefix])?;
    write_usize(writer, len)?;
    writer.write_all(b"\r\n")
}

pub(super) fn write_integer(writer: &mut impl Write, prefix: u8, value: i64) -> io::Result<()> {
    if value < 0 {
        writer.write_all(&[prefix, b'-'])?;
    } else {
        writer.write_all(&[prefix])?;
    }
    write_u64(writer, value.unsigned_abs())?;
    writer.write_all(b"\r\n")
}

pub(super) fn write_usize(writer: &mut impl Write, value: usize) -> io::Result<()> {
    write_u64(writer, value as u64)
}

pub(super) fn write_u64(writer: &mut impl Write, value: u64) -> io::Result<()> {
    let mut buffer = [0u8; 20];

    writer.write_all(crate::output::decimal(value, &mut buffer))
}

pub(super) fn write_i64(writer: &mut impl Write, value: i64) -> io::Result<()> {
    if value < 0 {
        writer.write_all(b"-")?;
    }

    let mut value = value.unsigned_abs();
    let mut buffer = [0u8; 20];
    let mut position = buffer.len();

    loop {
        position -= 1;
        buffer[position] = b'0' + (value % 10) as u8;
        value /= 10;

        if value == 0 {
            return writer.write_all(&buffer[position..]);
        }
    }
}

pub(super) fn owned_byte_set(values: Vec<Vec<u8>>) -> Response<'static> {
    Response::Set(values.into_iter().map(Cow::Owned).collect())
}

pub(super) fn apply_range_window(
    mut entries: Vec<(&[u8], f64)>,
    reverse: bool,
    limit: Option<crate::protocol::RangeLimit>,
) -> Vec<(&[u8], f64)> {
    if reverse {
        entries.reverse();
    }

    let Some(limit) = limit else {
        return entries;
    };

    let skipped = entries.into_iter().skip(limit.offset);

    match limit.count {
        Some(count) => skipped.take(count).collect(),
        None => skipped.collect(),
    }
}

pub(super) fn sorted_set_range_response(
    entries: Vec<(&[u8], f64)>,
    with_scores: bool,
) -> Response<'_> {
    Response::Scored(entries, with_scores)
}

/// Writes a score as a bulk string without allocating. A formatted `f64` is at most
/// 330 bytes, so the stack buffer always fits.
fn write_score_bulk(writer: &mut impl Write, score: f64) -> io::Result<()> {
    let mut buffer = [0u8; 352];
    let mut cursor = io::Cursor::new(&mut buffer[..]);

    let written = if score == f64::INFINITY {
        cursor.write_all(b"inf")
    } else if score == f64::NEG_INFINITY {
        cursor.write_all(b"-inf")
    } else if score.fract() == 0.0 && score.abs() <= i64::MAX as f64 {
        write!(cursor, "{}", score as i64)
    } else {
        write!(cursor, "{score}")
    };

    match written {
        Ok(()) => {
            let length = cursor.position() as usize;

            write_bulk(writer, &buffer[..length])
        }
        Err(_) => write_bulk(writer, &format_score(score)),
    }
}

/// A score printed into a stack buffer: the same text as `format_score` without the `Vec`.
struct ScoreText {
    bytes: [u8; 48],
    len: usize,
}

impl std::fmt::Write for ScoreText {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let end = self.len + text.len();

        self.bytes
            .get_mut(self.len..end)
            .ok_or(std::fmt::Error)?
            .copy_from_slice(text.as_bytes());
        self.len = end;

        Ok(())
    }
}

impl Default for ScoreText {
    fn default() -> Self {
        Self {
            bytes: [0; 48],
            len: 0,
        }
    }
}

impl ScoreText {
    /// `None` when the text does not fit (huge magnitudes): use `format_score` then.
    fn format(&mut self, score: f64) -> Option<&[u8]> {
        use std::fmt::Write;

        let written = if score == f64::INFINITY {
            self.write_str("inf")
        } else if score == f64::NEG_INFINITY {
            self.write_str("-inf")
        } else if score.fract() == 0.0 && score.abs() <= i64::MAX as f64 {
            write!(self, "{}", score as i64)
        } else {
            write!(self, "{score}")
        };

        written.ok()?;

        Some(&self.bytes[..self.len])
    }
}

pub(crate) fn format_score(score: f64) -> Vec<u8> {
    if score == f64::INFINITY {
        return b"inf".to_vec();
    }

    if score == f64::NEG_INFINITY {
        return b"-inf".to_vec();
    }

    if score.fract() == 0.0 && score.abs() <= i64::MAX as f64 {
        return (score as i64).to_string().into_bytes();
    }

    score.to_string().into_bytes()
}

pub(super) fn wrong_type() -> Response<'static> {
    Response::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
}

pub(super) fn increment_error(error: IncrementError) -> Response<'static> {
    match error {
        IncrementError::WrongType => wrong_type(),
        IncrementError::NotIntegerOrOverflow => {
            Response::Error(b"ERR value is not an integer or out of range")
        }
    }
}

pub(super) fn sorted_set_error(error: SortedSetError) -> Response<'static> {
    match error {
        SortedSetError::WrongType => wrong_type(),
        SortedSetError::InvalidScore => Response::Error(b"ERR score is not a valid float"),
        SortedSetError::InvalidIncrement => Response::Error(b"ERR increment would produce NaN"),
        SortedSetError::Full => Response::Error(b"ERR sorted set cannot hold more members"),
    }
}

pub(super) fn bloom_error(error: BloomError) -> Response<'static> {
    match error {
        BloomError::WrongType => wrong_type(),
        BloomError::KeyExists => Response::Error(b"ERR item exists"),
        BloomError::InvalidConfiguration => Response::Error(b"ERR invalid Bloom configuration"),
    }
}

#[cfg(test)]
mod bulk_tests {
    use super::*;

    #[test]
    fn write_bulk_frames_every_length_like_the_wire_format() {
        for length in [0, 1, 9, 10, 99, 100, 127, 128, 129, 999, 1000, 100_000] {
            let value = vec![b'x'; length];
            let mut out = Vec::new();

            write_bulk(&mut out, &value).unwrap();

            let mut expected = format!("${length}\r\n").into_bytes();

            expected.extend_from_slice(&value);
            expected.extend_from_slice(b"\r\n");
            assert_eq!(out, expected, "{length}");
        }
    }

    #[test]
    fn write_u64_matches_to_string() {
        for value in [0, 7, 10, 99, 100, 12_345, u64::MAX] {
            let mut out = Vec::new();

            write_u64(&mut out, value).unwrap();
            assert_eq!(out, value.to_string().as_bytes());
        }
    }
}

#[cfg(test)]
mod score_text_tests {
    use super::*;

    #[test]
    #[allow(clippy::approx_constant)]
    fn stack_score_text_matches_format_score() {
        for score in [
            0.0,
            -0.0,
            1.0,
            -2.5,
            0.1,
            1e15,
            1e21,
            1e300,
            -1e300,
            1e-7,
            3.14159,
            9.2e18,
            1.5e19,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
            123_456_789.123_456_79,
        ] {
            let mut stack = ScoreText::default();
            let expected = format_score(score);

            match stack.format(score) {
                Some(text) => assert_eq!(text, expected.as_slice(), "{score}"),
                None => assert!(expected.len() > 48, "{score}"),
            }
        }
    }
}
