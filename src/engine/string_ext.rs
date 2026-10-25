//! Redis string commands that compose from the same primitives as `SET`/`APPEND`.

use super::bitmap::{
    MAX_BITMAP_BYTES, current_bytes, patch_bytes, resolve_range, store_bytes, with_bytes,
};
use super::*;

const TOO_BIG: &[u8] = b"ERR string exceeds maximum allowed size";

pub(super) fn set_nx<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    value: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    if shard.contains_key(&key) {
        return Ok(Response::Integer(0));
    }

    Ok(
        match set_key(shard, sink, key, value, SetOptions::default())? {
            Response::Simple(_) => Response::Integer(1),
            other => other,
        },
    )
}

pub(super) fn get_del<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    let bytes = match shard.copy_string(&key) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Ok(Response::Bulk(None)),
        Err(_) => return Ok(wrong_type()),
    };

    before_mutation_allow_oom(shard, sink, [key.as_ref()], 0)?;

    sink.record(Mutation::Delete {
        keys: std::slice::from_ref(&key),
    })?;
    shard.delete(&key);

    Ok(Response::Bulk(Some(Cow::Owned(bytes))))
}

pub(super) fn mset_nx<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    pairs: Vec<(crate::protocol::Bytes<'a>, crate::protocol::Bytes<'a>)>,
) -> Result<Response<'a>, S::Error> {
    if pairs.iter().any(|(key, _)| shard.contains_key(key)) {
        return Ok(Response::Integer(0));
    }

    let extra = pairs
        .iter()
        .map(|(key, value)| key.len() + value.len() + ADMISSION_OVERHEAD_BYTES)
        .sum();

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        pairs.iter().map(|(key, _)| key.as_ref()),
        extra,
    )? {
        return Ok(rejected);
    }

    for (key, value) in &pairs {
        sink.record(Mutation::SetBytes { key, value })?;
    }

    for (key, value) in &pairs {
        shard.set_bytes(key, value);
    }

    Ok(Response::Integer(1))
}

/// Plain decimal text with at most 16 significant digits and no trailing zeros.
///
/// Redis computes in `long double`; a double carries 15-17 digits, so rounding to 16 keeps sums like
/// `0.1 + 0.2` printing `0.3` as Redis does while keeping more of the value than 15 would. The
/// result is stored as this text, so digits beyond the 16th are lost and later increments start from
/// the rounded value.
pub(super) fn float_text(value: f64) -> String {
    if value == 0.0 {
        return "0".into();
    }

    let scientific = format!("{value:.15e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let negative = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let point = exponent + 1;
    let mut text = String::new();

    if point <= 0 {
        text.push_str("0.");
        text.push_str(&"0".repeat((-point) as usize));
        text.push_str(&digits);
    } else if point as usize >= digits.len() {
        text.push_str(&digits);
        text.push_str(&"0".repeat(point as usize - digits.len()));
    } else {
        text.push_str(&digits[..point as usize]);
        text.push('.');
        text.push_str(&digits[point as usize..]);
    }

    if text.contains('.') {
        text.truncate(text.trim_end_matches('0').trim_end_matches('.').len());
    }

    if negative { format!("-{text}") } else { text }
}

pub(super) fn increment_float<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    delta: f64,
) -> Result<Response<'a>, S::Error> {
    let Ok(current) = current_bytes(shard, &key) else {
        return Ok(wrong_type());
    };
    let base = match current {
        None => 0.0,
        Some(bytes) => match std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| text.parse::<f64>().ok())
            .filter(|value| value.is_finite())
        {
            Some(value) => value,
            None => return Ok(Response::Error(b"ERR value is not a valid float")),
        },
    };
    let sum = base + delta;

    if !sum.is_finite() {
        return Ok(Response::Error(
            b"ERR increment would produce NaN or Infinity",
        ));
    }

    let text = float_text(sum).into_bytes();

    if let Some(rejected) = before_mutation(
        shard,
        sink,
        [key.as_ref()],
        key.len() + text.len() + ADMISSION_OVERHEAD_BYTES,
    )? {
        return Ok(rejected);
    }

    store_bytes(shard, sink, &key, &text)?;
    Ok(Response::Bulk(Some(Cow::Owned(text))))
}

pub(super) fn get_range<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    key: &[u8],
    start: i64,
    end: i64,
) -> Response<'a> {
    match shard.value_view(key) {
        Ok(Some(ByteView::Borrowed(bytes))) => Response::Bulk(Some(Cow::Borrowed(
            resolve_range(start, end, bytes.len() as i64)
                .map_or(&[][..], |(from, to)| &bytes[from..=to]),
        ))),
        Ok(Some(ByteView::Integer(value))) => {
            let bytes = crate::store::integer_bytes(value);
            let slice = resolve_range(start, end, bytes.len() as i64)
                .map(|(from, to)| bytes[from..=to].to_vec())
                .unwrap_or_default();

            Response::Bulk(Some(Cow::Owned(slice)))
        }
        Ok(None) => Response::Bulk(Some(Cow::Borrowed(&[]))),
        Err(_) => wrong_type(),
    }
}

pub(super) fn set_range<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    offset: u64,
    value: crate::protocol::Bytes<'a>,
) -> Result<Response<'a>, S::Error> {
    // Overwriting inside a plain string (the usual case): one lookup finds the bytes, and
    // the write goes to that address.
    if shard.addresses_survive_admission()
        && !value.is_empty()
        && let Some((at, length)) = shard.log_string_loc(&key)
        && offset <= length as u64
        && offset + value.len() as u64 <= length as u64
    {
        if let Some(rejected) =
            before_mutation(shard, sink, [key.as_ref()], ADMISSION_OVERHEAD_BYTES)?
        {
            return Ok(rejected);
        }

        sink.record(Mutation::SetRange {
            key: &key,
            offset,
            bytes: &value,
        })?;
        shard.write_log_range(&key, at, offset as usize, &value);

        return Ok(Response::Integer(length as i64));
    }

    let Ok(existing) = with_bytes(shard, &key, <[u8]>::len) else {
        return Ok(wrong_type());
    };

    if value.is_empty() {
        return Ok(Response::Integer(existing.unwrap_or(0) as i64));
    }

    let end = offset.saturating_add(value.len() as u64);

    if end > MAX_BITMAP_BYTES as u64 {
        return Ok(Response::Error(TOO_BIG));
    }

    let created = existing.is_none();
    let length = existing.unwrap_or(0);
    let grow = (end as usize).saturating_sub(length);
    let extra = grow + if created { key.len() } else { 0 } + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    let length = patch_bytes(shard, sink, &key, offset as usize, &value)?;
    Ok(Response::Integer(length as i64))
}


pub(super) fn transfer<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    source: crate::protocol::Bytes<'a>,
    destination: crate::protocol::Bytes<'a>,
    amount: i64,
    max: Option<i64>,
) -> Result<Response<'a>, S::Error> {
    let (from, to) = match shard.preview_transfer(&source, &destination, amount, max) {
        Ok(Some(balances)) => balances,
        Ok(None) => return Ok(Response::Bulk(None)),
        Err(error) => return Ok(increment_error(error)),
    };

    let extra = if shard.contains_key(&destination) {
        0
    } else {
        destination.len() + ADMISSION_OVERHEAD_BYTES
    };

    if let Some(rejected) =
        before_mutation(shard, sink, [source.as_ref(), destination.as_ref()], extra)?
    {
        return Ok(rejected);
    }

    // Both balances go out as one record: a torn WAL tail or a replica never sees one
    // side without the other.
    sink.record(Mutation::Transaction {
        mutations: &[
            Mutation::SetInteger {
                key: &source,
                value: from,
            }
            .to_owned(),
            Mutation::SetInteger {
                key: &destination,
                value: to,
            }
            .to_owned(),
        ],
    })?;

    shard
        .set_integer(&source, from)
        .expect("transfer type was validated before recording");
    shard
        .set_integer(&destination, to)
        .expect("transfer type was validated before recording");

    Ok(Response::Exec(vec![
        Response::Integer(from),
        Response::Integer(to),
    ]))
}

#[cfg(test)]
mod tests {
    use super::float_text;

    #[test]
    fn floats_print_like_redis() {
        for (value, text) in [
            (10.6, "10.6"),
            (0.1 + 0.2, "0.3"),
            (3.0, "3"),
            (-2.5, "-2.5"),
            (1e20, "100000000000000000000"),
            (3.0e-5, "0.00003"),
            (5.0e3, "5000"),
            (0.0, "0"),
            (1234567.891234568, "1234567.891234568"),
            (12345678901234.51, "12345678901234.51"),
        ] {
            assert_eq!(float_text(value), text, "{value}");
        }
    }

    #[test]
    fn small_increments_survive_sixteen_digits() {
        let mut stored = 12345678901234.5_f64;

        for _ in 0..3 {
            stored = float_text(stored + 0.01).parse().unwrap();
        }

        assert_eq!(float_text(stored), "12345678901234.53");
    }
}
