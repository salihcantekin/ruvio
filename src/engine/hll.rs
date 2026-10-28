//! HyperLogLog: Redis-compatible hashing, both Redis encodings and the improved
//! (Ertl) estimator. The value is a plain string, a `HYLL` header plus registers, so it
//! persists through the ordinary string WAL records.
//!
//! A new HyperLogLog is sparse, as in Redis: runs of zero registers and short runs of equal
//! values, a few hundred bytes for small cardinalities. It turns dense (12,288 register
//! bytes) once it would grow past 3,000 bytes or a register needs a value above 32. Sparse
//! updates follow Redis' `hllSparseSet` step for step, so the stored bytes match Redis.

use super::*;

const HEADER: usize = 16;
const PRECISION: u32 = 14;
const REGISTERS: usize = 1 << PRECISION;
const REGISTER_MAX: u8 = 63;
const BODY: usize = (REGISTERS * 6).div_ceil(8);
const DENSE_SIZE: usize = HEADER + BODY;
const Q: u32 = 64 - PRECISION;
const NOT_HLL: &[u8] = b"WRONGTYPE Key is not a valid HyperLogLog string value.";
const CORRUPT: &[u8] = b"INVALIDOBJ Corrupted HLL object detected";

/// Header byte 4: the register encoding.
const ENCODING: usize = 4;
const DENSE: u8 = 0;
const SPARSE: u8 = 1;
const SPARSE_MAX_BYTES: usize = 3000;
const SPARSE_VAL_MAX_VALUE: u8 = 32;
const SPARSE_VAL_MAX_LEN: usize = 4;
const SPARSE_ZERO_MAX_LEN: usize = 64;

fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut hash = seed ^ (key.len() as u64).wrapping_mul(M);
    let (chunks, tail) = key.as_chunks::<8>();

    for chunk in chunks {
        let mut k = u64::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        hash ^= k;
        hash = hash.wrapping_mul(M);
    }

    if !tail.is_empty() {
        for (index, &byte) in tail.iter().enumerate().rev() {
            hash ^= u64::from(byte) << (8 * index);
        }
        hash = hash.wrapping_mul(M);
    }

    hash ^= hash >> R;
    hash = hash.wrapping_mul(M);
    hash ^ (hash >> R)
}

/// Register index and run length (`1..=51`) for one element, as Redis `hllPatLen`.
fn pattern(element: &[u8]) -> (usize, u8) {
    let hash = murmur64a(element, 0xadc83b19);
    let index = (hash & (REGISTERS as u64 - 1)) as usize;
    let rest = (hash >> PRECISION) | (1u64 << Q);
    (index, rest.trailing_zeros() as u8 + 1)
}

fn get_register(registers: &[u8], index: usize) -> u8 {
    let byte = index * 6 / 8;
    let shift = (index * 6) & 7;
    let low = u32::from(registers[byte]);
    let high = u32::from(registers.get(byte + 1).copied().unwrap_or(0));

    (((low >> shift) | (high << (8 - shift))) & u32::from(REGISTER_MAX)) as u8
}

fn set_register(registers: &mut [u8], index: usize, value: u8) {
    set_register_at(registers, 0, index, value);
}

/// `set_register` on a copy of the register bytes that starts at byte `base`.
fn set_register_at(registers: &mut [u8], base: usize, index: usize, value: u8) {
    let byte = index * 6 / 8 - base;
    let shift = (index * 6) & 7;
    let value = u32::from(value);
    let max = u32::from(REGISTER_MAX);

    registers[byte] = ((u32::from(registers[byte]) & !(max << shift)) | (value << shift)) as u8;

    if let Some(next) = registers.get_mut(byte + 1) {
        *next = ((u32::from(*next) & !(max >> (8 - shift))) | (value >> (8 - shift))) as u8;
    }
}

/// A new, empty HyperLogLog: sparse, one XZERO run over every register.
fn new_value() -> Vec<u8> {
    let mut value = vec![0u8; HEADER];
    value[..4].copy_from_slice(b"HYLL");
    value[ENCODING] = SPARSE;
    // Every new value is written by PFADD or PFMERGE, which mark the cached count stale.
    value[CACHE_FLAG] = 0x80;
    push_zeros(&mut value, REGISTERS);
    value
}

/// Whether `value` has a HyperLogLog header, as Redis `isHLLObjectOrReply` checks it.
/// Sparse runs are checked when they are read.
fn valid(value: &[u8]) -> bool {
    value.len() >= HEADER
        && &value[..4] == b"HYLL"
        && match value[ENCODING] {
            DENSE => value.len() == DENSE_SIZE,
            SPARSE => true,
            _ => false,
        }
}

fn is_dense(value: &[u8]) -> bool {
    value[ENCODING] == DENSE
}

/// The sparse opcode at `at`: register value (0 for a zero run), run length, opcode size.
fn opcode(bytes: &[u8], at: usize) -> Option<(u8, usize, usize)> {
    let byte = *bytes.get(at)?;

    Some(if byte & 0x80 != 0 {
        (((byte >> 2) & 0x1f) + 1, usize::from(byte & 3) + 1, 1)
    } else if byte & 0x40 != 0 {
        let low = *bytes.get(at + 1)?;
        (
            0,
            ((usize::from(byte & 0x3f) << 8) | usize::from(low)) + 1,
            2,
        )
    } else {
        (0, usize::from(byte) + 1, 1)
    })
}

fn val_opcode(value: u8, length: usize) -> u8 {
    0x80 | ((value - 1) << 2) | (length - 1) as u8
}

/// ZERO (up to 64) or XZERO opcode for `length` zero registers, as Redis picks them.
fn zero_opcode(out: &mut [u8], length: usize) -> usize {
    if length > SPARSE_ZERO_MAX_LEN {
        let length = length - 1;
        out[0] = 0x40 | (length >> 8) as u8;
        out[1] = length as u8;
        2
    } else {
        out[0] = (length - 1) as u8;
        1
    }
}

fn push_zeros(value: &mut Vec<u8>, mut length: usize) {
    while length > 0 {
        let run = length.min(REGISTERS);
        let mut opcode = [0u8; 2];
        let size = zero_opcode(&mut opcode, run);
        value.extend_from_slice(&opcode[..size]);
        length -= run;
    }
}

/// Calls `run(first, length, value)` for each run of the sparse value. `None` when the
/// runs do not cover exactly every register.
fn sparse_runs(value: &[u8], mut run: impl FnMut(usize, usize, u8)) -> Option<()> {
    let mut at = HEADER;
    let mut first = 0;

    while at < value.len() {
        let (register, length, size) = opcode(value, at)?;

        if first + length > REGISTERS {
            return None;
        }

        run(first, length, register);
        first += length;
        at += size;
    }

    (first == REGISTERS).then_some(())
}

fn sparse_to_dense(value: &[u8]) -> Option<Vec<u8>> {
    let mut dense = vec![0u8; DENSE_SIZE];
    dense[..HEADER].copy_from_slice(&value[..HEADER]);
    dense[ENCODING] = DENSE;
    let body = &mut dense[HEADER..];

    sparse_runs(value, |first, length, register| {
        if register > 0 {
            for index in first..first + length {
                set_register(body, index, register);
            }
        }
    })?;

    Some(dense)
}

enum SparseSet {
    Unchanged,
    Updated,
    /// The value has to turn dense first; it was not changed.
    Promote,
}

/// The opcode that holds one register of a sparse value.
#[derive(Clone, Copy)]
struct Located {
    at: usize,
    first: usize,
    previous: Option<usize>,
    register: u8,
    span: usize,
    size: usize,
}

/// Finds the opcode holding register `index`. `None` for corrupt runs.
fn locate(value: &[u8], index: usize) -> Option<Located> {
    let mut at = HEADER;
    let mut first = 0;
    let mut previous = None;

    loop {
        let (register, span, size) = opcode(value, at)?;

        if index < first + span {
            return Some(Located {
                at,
                first,
                previous,
                register,
                span,
                size,
            });
        }

        previous = Some(at);
        at += size;
        first += span;
    }
}

/// Raises register `index` to `count` in a sparse value, Redis `hllSparseSet` without the
/// promotion itself. `None` for corrupt runs.
fn sparse_set(value: &mut Vec<u8>, index: usize, count: u8) -> Option<SparseSet> {
    if count > SPARSE_VAL_MAX_VALUE {
        return Some(SparseSet::Promote);
    }

    let located = locate(value, index)?;

    sparse_set_at(value, located, index, count)
}

/// `sparse_set` for a register already found with `locate` on this same value.
fn sparse_set_at(
    value: &mut Vec<u8>,
    located: Located,
    index: usize,
    count: u8,
) -> Option<SparseSet> {
    let Located {
        at,
        first,
        previous,
        register: old,
        span,
        size,
    } = located;

    if old >= count {
        return Some(SparseSet::Unchanged);
    }

    if span == 1 && size == 1 {
        // A VAL or ZERO run of one register is rewritten in place.
        value[at] = val_opcode(count, 1);
    } else {
        // Split the run into up to three: before, the new value, after.
        let last = first + span - 1;
        let mut sequence = [0u8; 5];
        let mut length = 0;

        if index != first {
            length += if old == 0 {
                zero_opcode(&mut sequence[length..], index - first)
            } else {
                sequence[length] = val_opcode(old, index - first);
                1
            };
        }

        sequence[length] = val_opcode(count, 1);
        length += 1;

        if index != last {
            length += if old == 0 {
                zero_opcode(&mut sequence[length..], last - index)
            } else {
                sequence[length] = val_opcode(old, last - index);
                1
            };
        }

        if length > size && value.len() + length - size > SPARSE_MAX_BYTES {
            return Some(SparseSet::Promote);
        }

        value.splice(at..at + size, sequence[..length].iter().copied());
    }

    // Merge adjacent VAL runs of the same value around the change, as Redis does.
    let mut at = previous.unwrap_or(HEADER);
    let mut scan = 5;

    while at < value.len() && scan > 0 {
        scan -= 1;
        let byte = value[at];

        if byte & 0x80 == 0 {
            at += if byte & 0x40 != 0 { 2 } else { 1 };
            continue;
        }

        if let Some(&next) = value.get(at + 1)
            && next & 0x80 != 0
        {
            let (left, left_length, _) = opcode(value, at)?;
            let (right, right_length, _) = opcode(value, at + 1)?;

            if left == right && left_length + right_length <= SPARSE_VAL_MAX_LEN {
                value[at + 1] = val_opcode(left, left_length + right_length);
                value.remove(at);
                continue;
            }
        }

        at += 1;
    }

    Some(SparseSet::Updated)
}

/// Raises register `index` to `count` in either encoding, turning a sparse value dense when
/// it has to. Whether the register changed; `None` for corrupt sparse runs.
fn raise(value: &mut Vec<u8>, index: usize, count: u8) -> Option<bool> {
    if !is_dense(value) {
        let outcome = sparse_set(value, index, count)?;

        return settle(value, index, count, outcome);
    }

    raise_dense(value, index, count)
}

/// `raise` on a sparse value whose register was already found with `locate`.
fn raise_located(value: &mut Vec<u8>, located: Located, index: usize, count: u8) -> Option<bool> {
    let outcome = sparse_set_at(value, located, index, count)?;

    settle(value, index, count, outcome)
}

fn settle(value: &mut Vec<u8>, index: usize, count: u8, outcome: SparseSet) -> Option<bool> {
    match outcome {
        SparseSet::Unchanged => Some(false),
        SparseSet::Updated => Some(true),
        SparseSet::Promote => {
            *value = sparse_to_dense(value)?;
            raise_dense(value, index, count)
        }
    }
}

fn raise_dense(value: &mut [u8], index: usize, count: u8) -> Option<bool> {
    let body = &mut value[HEADER..];

    if get_register(body, index) < count {
        set_register(body, index, count);
        Some(true)
    } else {
        Some(false)
    }
}

/// How many registers hold each value. `None` for corrupt sparse runs.
fn histogram(value: &[u8]) -> Option<[u32; 64]> {
    let mut histogram = [0u32; 64];

    if is_dense(value) {
        let body = &value[HEADER..];

        for index in 0..REGISTERS {
            histogram[get_register(body, index) as usize] += 1;
        }
    } else {
        sparse_runs(value, |_, length, register| {
            histogram[register as usize] += length as u32;
        })?;
    }

    Some(histogram)
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }

    let mut y = 1.0;
    let mut z = x;

    loop {
        x *= x;
        let previous = z;
        z += x * y;
        y += y;

        if previous == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }

    let mut y = 1.0;
    let mut z = 1.0 - x;

    loop {
        x = x.sqrt();
        let previous = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;

        if previous == z {
            return z / 3.0;
        }
    }
}

fn estimate(histogram: &[u32; 64]) -> u64 {
    let m = REGISTERS as f64;
    let mut z = m * tau((m - f64::from(histogram[Q as usize + 1])) / m);

    for count in histogram[1..=Q as usize].iter().rev() {
        z += f64::from(*count);
        z *= 0.5;
    }

    z += m * sigma(f64::from(histogram[0]) / m);
    (0.721_347_520_444_481_7 * m * m / z).round() as u64
}

/// The stored HyperLogLog under `key`, borrowed. `Ok(None)` for a missing key, `Err` for
/// a wrong type or an invalid HyperLogLog.
fn load<'s, C: Clock, I: StorageIndex>(
    shard: &'s KvShard<C, I>,
    key: &[u8],
) -> Result<Option<&'s [u8]>, Response<'static>> {
    match shard.value_view(key) {
        Ok(Some(ByteView::Borrowed(bytes))) if valid(bytes) => Ok(Some(bytes)),
        Ok(Some(_)) => Err(Response::Error(NOT_HLL)),
        Ok(None) => Ok(None),
        Err(_) => Err(wrong_type()),
    }
}

/// Header byte whose top bit marks the cached cardinality (bytes 8..16) stale, as in Redis.
const CACHE_FLAG: usize = 15;

fn cached_count(value: &[u8]) -> Option<u64> {
    (value[CACHE_FLAG] & 0x80 == 0)
        .then(|| u64::from_le_bytes(value[8..16].try_into().expect("8 bytes")))
}

pub(super) fn pf_add<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    elements: Vec<crate::protocol::Bytes<'a>>,
) -> Result<Response<'a>, S::Error> {
    let existing = match load(shard, &key) {
        Ok(existing) => existing,
        Err(response) => return Ok(response),
    };

    let value = match existing {
        Some(value) if is_dense(value) => value,
        _ => return pf_add_sparse(shard, sink, key, &elements),
    };

    // Only the register bytes that change are copied, patched and logged.
    let body = &value[HEADER..];
    let mut updates: Vec<(usize, u8)> = Vec::new();

    for element in &elements {
        let (index, length) = pattern(element);

        if get_register(body, index) < length {
            match updates.iter_mut().find(|(at, _)| *at == index) {
                Some(update) => update.1 = update.1.max(length),
                None => updates.push((index, length)),
            }
        }
    }

    if updates.is_empty() {
        return Ok(Response::Integer(0));
    }

    let first = updates
        .iter()
        .map(|&(index, _)| index * 6 / 8)
        .min()
        .expect("one update");
    let last = updates
        .iter()
        .map(|&(index, _)| (index * 6 / 8 + 2).min(BODY))
        .max()
        .expect("one update");
    let mut window = body[first..last].to_vec();

    for &(index, length) in &updates {
        set_register_at(&mut window, first, index, length);
    }

    let stale = value[CACHE_FLAG] & 0x80 != 0;
    let flag = value[CACHE_FLAG] | 0x80;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], ADMISSION_OVERHEAD_BYTES)?
    {
        return Ok(rejected);
    }

    // The stale mark goes first: a crash between the two records leaves a correct count.
    if !stale {
        patch_bytes(shard, sink, &key, CACHE_FLAG, &[flag])?;
    }

    patch_bytes(shard, sink, &key, HEADER + first, &window)?;
    Ok(Response::Integer(1))
}

/// PFADD to a missing key or a sparse value: the whole value, at most 3,000 bytes or the
/// dense value it turned into, is rewritten.
fn pf_add_sparse<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    elements: &[crate::protocol::Bytes<'a>],
) -> Result<Response<'a>, S::Error> {
    // Re-adding what the sketch already holds is the common case: no copy for it. The scan
    // that finds the first element that changes also finds its opcode, so it is not searched
    // for again when the value is updated.
    let mut start = 0;
    let mut located = None;

    if let Ok(Some(ByteView::Borrowed(bytes))) = shard.value_view(&key)
        && !is_dense(bytes)
    {
        let mut unchanged = true;

        for (at, element) in elements.iter().enumerate() {
            let (index, length) = pattern(element);

            if length > SPARSE_VAL_MAX_VALUE {
                start = at;
                unchanged = false;
                break;
            }

            match locate(bytes, index) {
                Some(found) if found.register >= length => {}
                Some(found) => {
                    start = at;
                    located = Some(found);
                    unchanged = false;
                    break;
                }
                None => {
                    unchanged = false;
                    break;
                }
            }
        }

        if unchanged {
            return Ok(Response::Integer(0));
        }
    }

    let (mut value, mut updated, before) = match shard.value_view(&key) {
        Ok(Some(ByteView::Borrowed(bytes))) => (bytes.to_vec(), false, bytes.len()),
        _ => (new_value(), true, 0),
    };

    for (at, element) in elements.iter().enumerate().skip(start) {
        let (index, length) = pattern(element);
        let changed = match located.take().filter(|_| at == start) {
            Some(found) => raise_located(&mut value, found, index, length),
            None => raise(&mut value, index, length),
        };

        match changed {
            Some(changed) => updated |= changed,
            None => return Ok(Response::Error(CORRUPT)),
        }
    }

    if !updated {
        return Ok(Response::Integer(0));
    }

    value[CACHE_FLAG] |= 0x80;
    let extra = value.len().saturating_sub(before)
        + if before == 0 { key.len() } else { 0 }
        + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [key.as_ref()], extra)? {
        return Ok(rejected);
    }

    store(shard, sink, &key, &value)?;
    Ok(Response::Integer(1))
}

fn store<C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    key: &[u8],
    value: &[u8],
) -> Result<(), S::Error> {
    let deadline = shard.expires_at(key);

    // One record: a log cut between the value and its deadline would drop the TTL.
    match deadline {
        Some(expires_at) => sink.record(Mutation::SetBytesExpiring {
            key,
            value,
            expires_at: expires_at.get(),
        })?,
        None => sink.record(Mutation::SetBytes { key, value })?,
    }

    shard.set_bytes(key, value);

    if let Some(expires_at) = deadline {
        shard.expire_at(key, expires_at);
    }

    Ok(())
}

/// Register-wise maximum of every existing source, one byte per register, and whether
/// any source is dense.
fn union<C: Clock, I: StorageIndex>(
    shard: &KvShard<C, I>,
    keys: &[&[u8]],
) -> Result<(Vec<u8>, bool), Response<'static>> {
    let mut max = vec![0u8; REGISTERS];
    let mut any_dense = false;

    for key in keys {
        let Some(source) = load(shard, key)? else {
            continue;
        };

        if is_dense(source) {
            any_dense = true;
            let body = &source[HEADER..];

            for (index, register) in max.iter_mut().enumerate() {
                *register = (*register).max(get_register(body, index));
            }
        } else {
            sparse_runs(source, |first, length, value| {
                for register in &mut max[first..first + length] {
                    *register = (*register).max(value);
                }
            })
            .ok_or(Response::Error(CORRUPT))?;
        }
    }

    Ok((max, any_dense))
}

pub(super) fn pf_count<'a, C: Clock, I: StorageIndex>(
    shard: &'a mut KvShard<C, I>,
    keys: &[crate::protocol::Bytes<'a>],
) -> Response<'a> {
    if let [key] = keys {
        let key = key.as_ref();
        let count = match load(shard, key) {
            Ok(Some(bytes)) => match cached_count(bytes) {
                Some(count) => return Response::Integer(count as i64),
                None => match histogram(bytes) {
                    Some(histogram) => estimate(&histogram),
                    None => return Response::Error(CORRUPT),
                },
            },
            Ok(None) => return Response::Integer(0),
            Err(response) => return response,
        };

        // Cache it in the header, as Redis does. The registers did not change, so this is
        // not a write: it is not logged, replicated or announced (a replica caches its own).
        shard.patch_in_place_quietly(key, 8, &count.to_le_bytes());
        return Response::Integer(count as i64);
    }

    let keys: Vec<&[u8]> = keys.iter().map(AsRef::as_ref).collect();

    match union(shard, &keys) {
        Ok((max, _)) => {
            let mut histogram = [0u32; 64];

            for &register in &max {
                histogram[register as usize] += 1;
            }

            Response::Integer(estimate(&histogram) as i64)
        }
        Err(response) => response,
    }
}

pub(super) fn pf_merge<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    destination: crate::protocol::Bytes<'a>,
    sources: Vec<crate::protocol::Bytes<'a>>,
) -> Result<Response<'a>, S::Error> {
    // The destination takes part in the union, as in Redis.
    let mut keys: Vec<&[u8]> = vec![destination.as_ref()];
    keys.extend(sources.iter().map(AsRef::as_ref));

    let (max, any_dense) = match union(shard, &keys) {
        Ok(union) => union,
        Err(response) => return Ok(response),
    };

    // As Redis: the destination (or a new sparse value) turns dense if any input is dense,
    // then takes every non-zero maximum in register order.
    let (mut value, before) = match shard.value_view(&destination) {
        Ok(Some(ByteView::Borrowed(bytes))) => (bytes.to_vec(), bytes.len()),
        _ => (new_value(), 0),
    };

    if any_dense && !is_dense(&value) {
        match sparse_to_dense(&value) {
            Some(dense) => value = dense,
            None => return Ok(Response::Error(CORRUPT)),
        }
    }

    for (index, &register) in max.iter().enumerate() {
        if register != 0 && raise(&mut value, index, register).is_none() {
            return Ok(Response::Error(CORRUPT));
        }
    }

    value[CACHE_FLAG] |= 0x80;
    let extra = value.len().saturating_sub(before) + destination.len() + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, keys.iter().copied(), extra)? {
        return Ok(rejected);
    }

    store(shard, sink, &destination, &value)?;
    Ok(Response::Simple(b"OK"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_round_trip_at_every_index() {
        let mut body = vec![0u8; BODY];

        for index in 0..REGISTERS {
            set_register(&mut body, index, (index % 63 + 1) as u8);
        }
        for index in 0..REGISTERS {
            assert_eq!(
                get_register(&body, index),
                (index % 63 + 1) as u8,
                "{index}"
            );
        }
    }

    #[test]
    fn estimate_is_within_a_few_percent() {
        let mut body = vec![0u8; BODY];

        for number in 0..100_000u32 {
            let (index, length) = pattern(number.to_string().as_bytes());

            if get_register(&body, index) < length {
                set_register(&mut body, index, length);
            }
        }

        let mut value = vec![0u8; HEADER];
        value.extend_from_slice(&body);
        let counted = estimate(&histogram(&value).expect("dense")) as f64;
        assert!((counted - 100_000.0).abs() / 100_000.0 < 0.03, "{counted}");
    }

    #[test]
    fn sparse_values_match_dense_registers_until_promotion() {
        let mut value = new_value();
        let mut model = vec![0u8; REGISTERS];
        let mut promoted = false;

        for number in 0..3_000u32 {
            let (index, length) = pattern(format!("e{number}").as_bytes());
            let expected = model[index] < length;
            model[index] = model[index].max(length);

            assert_eq!(raise(&mut value, index, length), Some(expected), "{number}");
            assert!(value.len() <= SPARSE_MAX_BYTES || is_dense(&value));

            if is_dense(&value) {
                promoted = true;
            } else {
                // Adjacent VAL runs of one value are merged, so no run can be extended.
                let mut registers = vec![0u8; REGISTERS];
                sparse_runs(&value, |first, length, register| {
                    registers[first..first + length].fill(register)
                })
                .expect("valid runs");
                assert_eq!(registers, model, "{number}");
            }
        }

        assert!(promoted, "3,000 elements outgrow the sparse encoding");
        let body = &value[HEADER..];
        assert!((0..REGISTERS).all(|index| get_register(body, index) == model[index]));
    }

    #[test]
    fn a_new_value_is_eighteen_bytes_and_counts_zero() {
        let value = new_value();
        assert_eq!(value.len(), 18);
        assert_eq!(&value[HEADER..], &[0x7f, 0xff]);
        assert_eq!(estimate(&histogram(&value).expect("valid")), 0);
    }

    #[test]
    fn values_above_thirty_two_promote() {
        let mut value = new_value();
        assert_eq!(raise(&mut value, 7, 33), Some(true));
        assert!(is_dense(&value));
        assert_eq!(get_register(&value[HEADER..], 7), 33);
    }

    #[test]
    fn corrupt_sparse_runs_are_rejected() {
        let mut value = new_value();
        value.truncate(HEADER + 1);
        assert!(histogram(&value).is_none());
        assert!(raise(&mut value, 20_000 % REGISTERS, 3).is_none());
    }

    #[test]
    fn raising_a_located_register_matches_raising_blind() {
        let mut value = new_value();

        for number in 0..2_000u32 {
            let (index, length) = pattern(format!("l{number}").as_bytes());
            let mut blind = value.clone();
            let expected = raise(&mut blind, index, length);
            let mut located = value.clone();
            let actual = if is_dense(&located) || length > SPARSE_VAL_MAX_VALUE {
                raise(&mut located, index, length)
            } else {
                let found = locate(&located, index).expect("valid runs");

                raise_located(&mut located, found, index, length)
            };

            assert_eq!(actual, expected, "{number}");
            assert_eq!(located, blind, "{number}");
            value = blind;
        }
    }
}
