//! Encoding and decoding snapshot entries and their values.

use super::*;

pub(crate) fn encode_entry(
    output: &mut Vec<u8>,
    entry: &SnapshotEntry,
) -> Result<(), PersistenceError> {
    encode_entry_with(
        output,
        entry.database,
        &entry.key,
        entry.expires_at,
        0,
        |payload| encode_value_into(payload, &entry.value),
    )
}

/// Appends one entry frame whose payload `write_payload` writes straight into `output`,
/// returning the value type: no copy of the value is made on the way. `payload_len` is
/// reserved up front when known (0 otherwise). On error `output` is left as it was.
pub(crate) fn encode_entry_with(
    output: &mut Vec<u8>,
    database: u8,
    key: &[u8],
    expires_at: Option<UnixMillis>,
    payload_len: usize,
    write_payload: impl FnOnce(&mut Vec<u8>) -> Result<u8, PersistenceError>,
) -> Result<(), PersistenceError> {
    let start = output.len();
    let written = (|| {
        // Length, key, expiry, type, database and payload length, the payload, then CRC.
        let frame = payload_len.saturating_add(key.len()).saturating_add(28);

        // A frame past the length field's range fails anyway; refuse it before reserving.
        if u32::try_from(frame).is_err() {
            return Err(PersistenceError::Failed(
                "snapshot entry exceeds 4 GiB".into(),
            ));
        }

        // A value larger than the buffer gets exactly its size, not a doubled buffer.
        if frame > output.capacity() {
            output.reserve_exact(frame);
        } else {
            output.reserve(frame);
        }
        put_u32(output, 0);
        put_bytes(output, key)?;
        match expires_at {
            Some(deadline) => {
                output.push(1);
                put_u64(output, deadline.get());
            }

            None => {
                output.push(0);
                put_u64(output, 0);
            }
        }

        let type_at = output.len();

        output.push(0);
        put_u16(output, u16::from(database));
        put_u32(output, 0);

        let payload_start = output.len();

        output[type_at] = write_payload(output)?;
        let payload = u32::try_from(output.len() - payload_start)
            .map_err(|_| PersistenceError::Failed("snapshot value exceeds 4 GiB".into()))?;
        let frame_length = u32::try_from(output.len() - start + 4)
            .map_err(|_| PersistenceError::Failed("snapshot entry exceeds 4 GiB".into()))?;

        output[payload_start - 4..payload_start].copy_from_slice(&payload.to_le_bytes());
        output[start..start + 4].copy_from_slice(&frame_length.to_le_bytes());
        let checksum = crc32c::crc32c(&output[start + 4..]);

        put_u32(output, checksum);
        Ok(())
    })();

    if written.is_err() {
        output.truncate(start);
    }

    written
}

/// One entry frame as [`encode_entry`] wrote it, carried by a slot move's copy record. The
/// source already accepted the value, so the only bound is the frame itself.
pub(crate) fn decode_entry_frame(bytes: &[u8]) -> Result<SnapshotEntry, PersistenceError> {
    let limits = ProtocolLimits {
        max_arguments: bytes.len(),
        max_key_bytes: bytes.len(),
        max_value_bytes: bytes.len(),
        max_command_bytes: bytes.len(),
        ..ProtocolLimits::default()
    };
    let (entry, used) = decode_entry(bytes, limits)?;

    if used != bytes.len() {
        return Err(PersistenceError::Corruption(
            "slot copy entry has trailing bytes".into(),
        ));
    }

    Ok(entry)
}

pub(super) fn decode_entry(
    bytes: &[u8],
    limits: ProtocolLimits,
) -> Result<(SnapshotEntry, usize), PersistenceError> {
    if bytes.len() < 4 {
        return Err(PersistenceError::Corruption(
            "truncated snapshot entry".into(),
        ));
    }

    let length = u32_at(bytes, 0)? as usize;

    if length < 24 || length > bytes.len() || length > limits.max_command_bytes {
        return Err(PersistenceError::Corruption(
            "invalid snapshot entry length".into(),
        ));
    }

    let checksum_offset = length - 4;

    if crc32c::crc32c(&bytes[4..checksum_offset]) != u32_at(bytes, checksum_offset)? {
        return Err(PersistenceError::Corruption(
            "snapshot entry checksum mismatch".into(),
        ));
    }

    let mut decoder = Decoder::new(&bytes[4..checksum_offset]);
    let key = decoder.bytes(limits.max_key_bytes)?;
    let expiration_flag = decoder.u8()?;
    let expiration_value = decoder.u64()?;
    let expires_at = match expiration_flag {
        0 if expiration_value == 0 => None,
        1 => Some(UnixMillis::new(expiration_value)),
        _ => {
            return Err(PersistenceError::Corruption(
                "invalid snapshot expiration flag".into(),
            ));
        }
    };

    let value_type = decoder.u8()?;
    let database = u8::try_from(decoder.u16()?)
        .map_err(|_| PersistenceError::Unsupported("snapshot entry database".into()))?;

    let payload_length = decoder.u32()? as usize;
    let payload = decoder.take(payload_length)?;

    if !decoder.is_empty() {
        return Err(PersistenceError::Corruption(
            "trailing snapshot entry bytes".into(),
        ));
    }

    let value = decode_value(value_type, payload, limits)?;

    Ok((
        SnapshotEntry {
            database,
            key,
            expires_at,
            value,
        },
        length,
    ))
}

#[cfg(test)]
pub(super) fn encode_value(value: &SnapshotValue) -> Result<(u8, Vec<u8>), PersistenceError> {
    let mut payload = Vec::new();
    let kind = encode_value_into(&mut payload, value)?;

    Ok((kind, payload))
}

/// Appends the payload of `value` to `payload` and returns its value type.
fn encode_value_into(payload: &mut Vec<u8>, value: &SnapshotValue) -> Result<u8, PersistenceError> {
    let kind = match value {
        SnapshotValue::Bytes(value) => {
            payload.extend_from_slice(value);
            1
        }

        SnapshotValue::Json(value) => {
            payload.extend_from_slice(value);
            15
        }

        SnapshotValue::List(values) => {
            put_byte_list(payload, values)?;
            2
        }

        SnapshotValue::Set(values) => {
            put_byte_list(payload, values)?;
            3
        }

        SnapshotValue::Hash(pairs) => {
            put_u32(
                payload,
                u32::try_from(pairs.len())
                    .map_err(|_| PersistenceError::Failed("too many hash fields".into()))?,
            );

            for (field, value) in pairs {
                put_bytes(payload, field)?;
                put_bytes(payload, value)?;
            }

            6
        }

        SnapshotValue::HashTtl(fields) => {
            put_u32(
                payload,
                u32::try_from(fields.len())
                    .map_err(|_| PersistenceError::Failed("too many hash fields".into()))?,
            );

            for (field, value, at) in fields {
                put_bytes(payload, field)?;
                put_bytes(payload, value)?;
                put_u64(payload, *at);
            }

            13
        }

        SnapshotValue::SortedSet(values) => {
            put_u32(
                payload,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::Failed("too many sorted-set members".into()))?,
            );

            for (score, member) in values {
                put_u64(payload, score.to_bits());
                put_bytes(payload, member)?;
            }

            4
        }

        SnapshotValue::BloomFilter { words, hash_count } => {
            payload.push(*hash_count);
            payload.extend_from_slice(&[0; 3]);
            put_u32(
                payload,
                u32::try_from(words.len())
                    .map_err(|_| PersistenceError::Failed("Bloom snapshot too large".into()))?,
            );

            for word in words {
                put_u64(payload, *word);
            }

            5
        }

        SnapshotValue::BloomLayers {
            layers,
            error_rate,
            expansion,
        } => {
            put_u64(payload, error_rate.to_bits());
            put_u32(payload, *expansion);
            put_u32(
                payload,
                u32::try_from(layers.len())
                    .map_err(|_| PersistenceError::Failed("too many Bloom layers".into()))?,
            );

            for layer in layers {
                payload.push(layer.hash_count);
                put_u64(payload, layer.capacity);
                put_u64(payload, layer.count);
                put_u32(
                    payload,
                    u32::try_from(layer.words.len())
                        .map_err(|_| PersistenceError::Failed("Bloom snapshot too large".into()))?,
                );

                for word in layer.words.iter() {
                    put_u64(payload, *word);
                }
            }

            16
        }

        SnapshotValue::Lease { token, expires_at } => {
            put_u64(payload, *token);
            put_u64(payload, *expires_at);
            8
        }

        SnapshotValue::Limit { used } => {
            put_u64(payload, *used);
            12
        }

        SnapshotValue::Once => 11,

        SnapshotValue::Semaphore {
            next_token,
            holders,
        } => {
            put_u64(payload, *next_token);
            put_u32(
                payload,
                u32::try_from(holders.len())
                    .map_err(|_| PersistenceError::Failed("too many semaphore holders".into()))?,
            );

            for (token, expires_at) in holders {
                put_u64(payload, *token);
                put_u64(payload, *expires_at);
            }

            14
        }

        SnapshotValue::Versioned { version, bytes } => {
            put_u64(payload, *version);
            payload.extend_from_slice(bytes);
            9
        }

        SnapshotValue::Stream {
            started,
            last_millis,
            last_sequence,
            entries,
            groups,
            delayed,
        } => {
            payload.push(u8::from(*started));
            put_u64(payload, *last_millis);
            put_u64(payload, *last_sequence);
            put_u32(
                payload,
                u32::try_from(entries.len())
                    .map_err(|_| PersistenceError::Failed("too many stream entries".into()))?,
            );

            for entry in entries {
                put_u64(payload, entry.millis);
                put_u64(payload, entry.sequence);
                put_u32(
                    payload,
                    u32::try_from(entry.fields.len())
                        .map_err(|_| PersistenceError::Failed("too many stream fields".into()))?,
                );

                for (field, value) in &entry.fields {
                    put_bytes(payload, field)?;
                    put_bytes(payload, value)?;
                }
            }

            put_u32(
                payload,
                u32::try_from(groups.len())
                    .map_err(|_| PersistenceError::Failed("too many stream groups".into()))?,
            );

            for group in groups {
                put_bytes(payload, &group.name)?;
                put_u64(payload, group.last_millis);
                put_u64(payload, group.last_sequence);
                put_u32(
                    payload,
                    u32::try_from(group.pending.len()).map_err(|_| {
                        PersistenceError::Failed("too many stream pending entries".into())
                    })?,
                );

                for delivery in &group.pending {
                    put_u64(payload, delivery.millis);
                    put_u64(payload, delivery.sequence);
                    put_bytes(payload, &delivery.consumer)?;
                    put_u64(payload, delivery.delivered_at);
                    put_u64(payload, delivery.deliveries);
                }
            }

            let idle_groups: Vec<_> = groups
                .iter()
                .filter(|group| !group.idle.is_empty())
                .collect();

            if !idle_groups.is_empty() || !delayed.is_empty() {
                put_u32(
                    payload,
                    u32::try_from(idle_groups.len()).map_err(|_| {
                        PersistenceError::Failed("too many idle consumer groups".into())
                    })?,
                );

                for group in idle_groups {
                    put_bytes(payload, &group.name)?;
                    put_u32(
                        payload,
                        u32::try_from(group.idle.len()).map_err(|_| {
                            PersistenceError::Failed("too many idle consumers".into())
                        })?,
                    );

                    for name in &group.idle {
                        put_bytes(payload, name)?;
                    }
                }
            }

            if !delayed.is_empty() {
                put_u32(
                    payload,
                    u32::try_from(delayed.len()).map_err(|_| {
                        PersistenceError::Failed("too many delayed stream entries".into())
                    })?,
                );

                for (millis, sequence, ready_at) in delayed {
                    put_u64(payload, *millis);
                    put_u64(payload, *sequence);
                    put_u64(payload, *ready_at);
                }
            }

            7
        }
    };

    Ok(kind)
}

pub(super) fn decode_value(
    kind: u8,
    payload: &[u8],
    limits: ProtocolLimits,
) -> Result<SnapshotValue, PersistenceError> {
    if kind == 1 || kind == 15 {
        if payload.len() > limits.max_value_bytes {
            return Err(PersistenceError::Corruption(
                "snapshot bytes value exceeds configured limit".into(),
            ));
        }

        return Ok(if kind == 1 {
            SnapshotValue::Bytes(payload.to_vec())
        } else {
            SnapshotValue::Json(payload.to_vec())
        });
    }

    let mut decoder = Decoder::new(payload);
    let value = match kind {
        2 => SnapshotValue::List(decoder.byte_list(limits.max_command_bytes)?),
        3 => SnapshotValue::Set(decoder.byte_list(limits.max_command_bytes)?),
        6 => {
            let count = decoder.u32()? as usize;

            if count > limits.max_command_bytes / 4 {
                return Err(PersistenceError::Corruption(
                    "snapshot hash field count is excessive".into(),
                ));
            }

            let mut pairs = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                pairs.push((
                    decoder.bytes(limits.max_value_bytes)?,
                    decoder.bytes(limits.max_value_bytes)?,
                ));
            }

            SnapshotValue::Hash(pairs)
        }

        13 => {
            let count = decoder.u32()? as usize;

            if count > limits.max_command_bytes / 4 {
                return Err(PersistenceError::Corruption(
                    "snapshot hash field count is excessive".into(),
                ));
            }

            let mut fields = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                fields.push((
                    decoder.bytes(limits.max_value_bytes)?,
                    decoder.bytes(limits.max_value_bytes)?,
                    decoder.u64()?,
                ));
            }

            SnapshotValue::HashTtl(fields)
        }

        4 => {
            let count = decoder.u32()? as usize;
            let mut values = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                values.push((
                    f64::from_bits(decoder.u64()?),
                    decoder.bytes(limits.max_value_bytes)?,
                ));
            }

            SnapshotValue::SortedSet(values)
        }

        5 => {
            let hash_count = decoder.u8()?;

            if decoder.take(3)? != [0; 3] {
                return Err(PersistenceError::Unsupported("Bloom snapshot flags".into()));
            }

            let count = decoder.u32()? as usize;

            if count == 0 || count > limits.max_command_bytes / 8 {
                return Err(PersistenceError::Corruption(
                    "invalid Bloom snapshot word count".into(),
                ));
            }

            let mut words = Vec::with_capacity(count);

            for _ in 0..count {
                words.push(decoder.u64()?);
            }

            SnapshotValue::BloomFilter { words, hash_count }
        }

        16 => {
            let error_rate = f64::from_bits(decoder.u64()?);
            let expansion = decoder.u32()?;
            let layer_count = decoder.u32()? as usize;

            if layer_count == 0 || layer_count > 64 {
                return Err(PersistenceError::Corruption(
                    "invalid Bloom snapshot layer count".into(),
                ));
            }

            let mut layers = Vec::with_capacity(layer_count);

            for _ in 0..layer_count {
                let hash_count = decoder.u8()?;
                let capacity = decoder.u64()?;
                let count = decoder.u64()?;
                let word_count = decoder.u32()? as usize;

                if word_count == 0 || word_count > limits.max_command_bytes / 8 {
                    return Err(PersistenceError::Corruption(
                        "invalid Bloom snapshot word count".into(),
                    ));
                }

                let mut words = Vec::with_capacity(word_count);

                for _ in 0..word_count {
                    words.push(decoder.u64()?);
                }

                layers.push(crate::store::BloomLayer {
                    words: words.into_boxed_slice(),
                    hash_count,
                    capacity,
                    count,
                });
            }

            SnapshotValue::BloomLayers {
                layers,
                error_rate,
                expansion,
            }
        }

        7 => {
            let started = match decoder.u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(PersistenceError::Corruption(
                        "invalid stream snapshot flag".into(),
                    ));
                }
            };

            let last_millis = decoder.u64()?;
            let last_sequence = decoder.u64()?;
            let count = decoder.u32()? as usize;

            if count > limits.max_command_bytes / 16 {
                return Err(PersistenceError::Corruption(
                    "snapshot stream entry count is excessive".into(),
                ));
            }

            let mut entries = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                let millis = decoder.u64()?;
                let sequence = decoder.u64()?;
                let field_count = decoder.u32()? as usize;

                if field_count == 0 || field_count > limits.max_arguments {
                    return Err(PersistenceError::Corruption(
                        "invalid stream field count in snapshot".into(),
                    ));
                }

                let mut fields = Vec::with_capacity(field_count);

                for _ in 0..field_count {
                    fields.push((
                        decoder.bytes(limits.max_value_bytes)?,
                        decoder.bytes(limits.max_value_bytes)?,
                    ));
                }

                entries.push(crate::stream::StreamEntry {
                    millis,
                    sequence,
                    fields: crate::stream::StreamFields::from_pairs(
                        fields
                            .iter()
                            .map(|(field, value)| (field.as_slice(), value.as_slice())),
                    ),
                });
            }

            let (groups, delayed) = if decoder.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                let count = decoder.u32()? as usize;

                if count > limits.max_arguments {
                    return Err(PersistenceError::Corruption(
                        "snapshot stream group count is excessive".into(),
                    ));
                }

                let mut groups = Vec::with_capacity(count);

                for _ in 0..count {
                    let name = decoder.bytes(limits.max_value_bytes)?;
                    let last_millis = decoder.u64()?;
                    let last_sequence = decoder.u64()?;
                    let pending_count = decoder.u32()? as usize;

                    if pending_count > limits.max_arguments {
                        return Err(PersistenceError::Corruption(
                            "snapshot stream pending count is excessive".into(),
                        ));
                    }

                    let mut pending = Vec::with_capacity(pending_count);

                    for _ in 0..pending_count {
                        pending.push(crate::stream::PendingDelivery {
                            millis: decoder.u64()?,
                            sequence: decoder.u64()?,
                            consumer: decoder.bytes(limits.max_value_bytes)?,
                            delivered_at: decoder.u64()?,
                            deliveries: decoder.u64()?,
                        });
                    }

                    groups.push(crate::stream::ConsumerGroup {
                        name,
                        last_millis,
                        last_sequence,
                        pending,
                        idle: Vec::new(),
                    });
                }

                let mut delayed = Vec::new();

                if !decoder.is_empty() {
                    let idle_groups = decoder.u32()? as usize;

                    for _ in 0..idle_groups {
                        let name = decoder.bytes(limits.max_value_bytes)?;
                        let count = decoder.u32()? as usize;
                        let group = groups
                            .iter_mut()
                            .find(|group| group.name == name)
                            .ok_or_else(|| {
                                PersistenceError::Corruption(
                                    "snapshot idle consumers name a missing group".into(),
                                )
                            })?;

                        for _ in 0..count {
                            group.idle.push(decoder.bytes(limits.max_value_bytes)?);
                        }
                    }

                    if !decoder.is_empty() {
                        let count = decoder.u32()? as usize;

                        if count > limits.max_arguments {
                            return Err(PersistenceError::Corruption(
                                "snapshot delayed stream count is excessive".into(),
                            ));
                        }

                        for _ in 0..count {
                            delayed.push((decoder.u64()?, decoder.u64()?, decoder.u64()?));
                        }
                    }
                }

                (groups, delayed)
            };

            SnapshotValue::Stream {
                started,
                last_millis,
                last_sequence,
                entries,
                groups,
                delayed,
            }
        }

        8 => SnapshotValue::Lease {
            token: decoder.u64()?,
            expires_at: decoder.u64()?,
        },
        // Older snapshots also stored the window end here. The entry deadline already holds it.
        10 => {
            let used = decoder.u64()?;
            let _ = decoder.u64()?;

            SnapshotValue::Limit { used }
        }
        12 => SnapshotValue::Limit {
            used: decoder.u64()?,
        },
        11 => SnapshotValue::Once,
        14 => {
            let next_token = decoder.u64()?;
            let count = decoder.u32()? as usize;

            if count > limits.max_command_bytes / 16 {
                return Err(PersistenceError::Corruption(
                    "snapshot semaphore holder count is excessive".into(),
                ));
            }

            let mut holders = Vec::with_capacity(count.min(1024));

            for _ in 0..count {
                holders.push((decoder.u64()?, decoder.u64()?));
            }

            SnapshotValue::Semaphore {
                next_token,
                holders,
            }
        }
        9 => {
            let version = decoder.u64()?;
            let bytes = decoder.take_rest();

            SnapshotValue::Versioned { version, bytes }
        }
        _ => {
            return Err(PersistenceError::Unsupported(format!(
                "snapshot value type {kind}"
            )));
        }
    };

    if !decoder.is_empty() {
        return Err(PersistenceError::Corruption(
            "trailing snapshot value bytes".into(),
        ));
    }

    Ok(value)
}
