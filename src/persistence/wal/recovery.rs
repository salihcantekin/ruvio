//! Startup replay of WAL segments and torn-tail detection.

use super::*;

pub(super) fn recover_segments(
    paths: &[PathBuf],
    shard_id: u32,
    limits: ProtocolLimits,
    maximum_segment_bytes: u64,
    after_sequence: u64,
    read_only: bool,
) -> Result<(RecoveredMutations, RecoveryReport, Option<Segment>), PersistenceError> {
    let start = first_usable_segment_index(paths, after_sequence)?;
    let paths = paths.get(start..).unwrap_or(&[]);
    let mut mutations = Vec::new();
    let mut report = RecoveryReport {
        last_sequence: after_sequence,
        ..RecoveryReport::default()
    };

    let mut expected_sequence = 0u64;
    let mut saw_segment = false;
    let mut last_segment = None;

    for (index, path) in paths.iter().enumerate() {
        let is_last = index + 1 == paths.len();
        let mut file = if read_only {
            File::open(path)?
        } else {
            OpenOptions::new().read(true).write(is_last).open(path)?
        };

        let physical_length = file.metadata()?.len();

        if physical_length < WAL_HEADER_LEN as u64 {
            drop(file);

            if !is_last {
                return Err(PersistenceError::Corruption(
                    "truncated WAL segment header".into(),
                ));
            }

            report.torn_tail_bytes += physical_length;

            if !read_only {
                fs::remove_file(path)?;
            }

            break;
        }

        // A segment written before `wal_segment_bytes` was lowered is longer than the setting
        // (or than the default, for the offline tools): still a valid segment. The writer starts
        // a new one before appending to it again.
        if physical_length > maximum_segment_bytes {
            tracing::warn!(
                segment = %path.display(),
                bytes = physical_length,
                wal_segment_bytes = maximum_segment_bytes,
                "WAL segment is longer than wal_segment_bytes; reading it anyway"
            );
        }

        let mut header_bytes = [0; WAL_HEADER_LEN];

        file.read_exact(&mut header_bytes).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                PersistenceError::Corruption("truncated WAL segment header".into())
            } else {
                error.into()
            }
        })?;

        // A power cut after `create_segment` wrote the header but before its sync completed can
        // leave the file at its length with zeros for content. Nothing in it was acknowledged
        // (records are appended only after the header is synced), so like a short header on the
        // final segment it is a torn tail. A non-zero byte after it, or a later segment, means
        // the header had been synced, so either stays corruption.
        if is_last
            && header_bytes.iter().all(|byte| *byte == 0)
            && rest_is_zero(&mut file, WAL_HEADER_LEN as u64)?
        {
            drop(file);
            report.torn_tail_bytes += physical_length;

            if !read_only {
                fs::remove_file(path)?;
            }

            break;
        }

        let header = decode_segment_header(&header_bytes)?;

        if header.shard_id != shard_id {
            return Err(PersistenceError::Corruption(
                "WAL segment shard mismatch".into(),
            ));
        }

        let file_first_sequence = segment_first_sequence(path)?;

        if header.first_sequence != file_first_sequence {
            return Err(PersistenceError::Corruption(
                "WAL segment sequence discontinuity".into(),
            ));
        }

        if !saw_segment {
            if after_sequence == 0 && header.first_sequence != 1 {
                return Err(PersistenceError::Corruption(
                    "WAL segment sequence discontinuity".into(),
                ));
            }

            if after_sequence > 0 && header.first_sequence > after_sequence.saturating_add(1) {
                return Err(PersistenceError::Corruption(
                    "WAL segment sequence discontinuity".into(),
                ));
            }

            expected_sequence = header.first_sequence;
            saw_segment = true;
        }

        if header.first_sequence != expected_sequence {
            return Err(PersistenceError::Corruption(
                "WAL segment sequence discontinuity".into(),
            ));
        }

        let mut valid_length = WAL_HEADER_LEN as u64;

        loop {
            let record_start = valid_length;
            let mut length_bytes = [0; 4];

            match read_partial(&mut file, &mut length_bytes)? {
                0 => break,
                4 => {}
                _count if is_last => {
                    report.torn_tail_bytes += physical_length - record_start;

                    if !read_only {
                        file.set_len(record_start)?;
                        file.seek(SeekFrom::End(0))?;
                    }

                    break;
                }

                _ => {
                    return Err(PersistenceError::Corruption(
                        "torn record length in non-final WAL segment".into(),
                    ));
                }
            }

            let record_length = u32::from_le_bytes(length_bytes) as usize;

            if record_length == 0 && rest_is_zero(&mut file, record_start)? {
                // Unwritten preallocated tail, not a torn record.
                if is_last && !read_only {
                    file.set_len(record_start)?;
                    file.seek(SeekFrom::End(0))?;
                }

                break;
            }

            // A zeroed length with data after it in the last segment: pages lost behind a
            // failed sync or a torn write leave such a tail. It is cut only when no intact
            // record continuing the log follows; otherwise records that were synced would be
            // dropped silently, so startup stops and says where.
            if record_length == 0 && is_last {
                if let Some((offset, sequence)) = intact_record_after(
                    &mut file,
                    record_start,
                    physical_length,
                    shard_id,
                    limits,
                    expected_sequence,
                )? {
                    return Err(PersistenceError::Corruption(format!(
                        "WAL segment {} has a zeroed record length at offset {record_start}, \
                         but an intact record (sequence {sequence}) follows at offset \
                         {offset}; refusing to drop it. To start without the records from \
                         offset {record_start} on, move the segment aside or truncate it \
                         there (truncate -s {record_start} <segment>)",
                        path.display()
                    )));
                }
                tracing::warn!(
                    segment = %path.display(),
                    offset = record_start,
                    bytes = physical_length - record_start,
                    "WAL ends at a zeroed record length; dropping the bytes after it"
                );
                report.torn_tail_bytes += physical_length - record_start;

                if !read_only {
                    file.set_len(record_start)?;
                    file.seek(SeekFrom::End(0))?;
                }

                break;
            }

            // Not the configured command limit: records written under a larger one (or by a
            // primary with one) must still replay. A record never outgrows its segment file.
            if record_length < RECORD_MIN_LEN {
                return Err(PersistenceError::Corruption(
                    "invalid WAL record length".into(),
                ));
            }

            let mut frame = Vec::new();
            // Longer than its segment: torn or corrupt, and never worth the allocation.
            let body_read = if record_length as u64 <= physical_length {
                frame.resize(record_length, 0);
                frame[..4].copy_from_slice(&length_bytes);
                read_partial(&mut file, &mut frame[4..])?
            } else {
                0
            };

            if body_read != record_length - 4 {
                if !is_last {
                    return Err(PersistenceError::Corruption(
                        "torn record in non-final WAL segment".into(),
                    ));
                }

                report.torn_tail_bytes += physical_length - record_start;

                if !read_only {
                    file.set_len(record_start)?;
                    file.seek(SeekFrom::End(0))?;
                }

                break;
            }

            match decode_record(&frame, shard_id, limits) {
                Ok(record) => {
                    if record.sequence != expected_sequence {
                        return Err(PersistenceError::Corruption(
                            "WAL record sequence discontinuity".into(),
                        ));
                    }

                    expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                        PersistenceError::Corruption("WAL sequence overflow".into())
                    })?;

                    report.last_sequence = record.sequence;

                    if record.sequence > after_sequence {
                        report.records_replayed += 1;
                        mutations.push((record.database, record.mutation));
                    }

                    valid_length += record_length as u64;
                }

                // Preallocated zeros after the record count as end of file, so a bad last
                // record is still the final physical record: a torn tail, not mid-log
                // corruption.
                Err(PersistenceError::Corruption(_))
                    if is_last
                        && (record_start + record_length as u64 == physical_length
                            || rest_is_zero(&mut file, record_start + record_length as u64)
                                .unwrap_or(false)) =>
                {
                    report.torn_tail_bytes += physical_length - record_start;

                    if !read_only {
                        file.set_len(record_start)?;
                        file.seek(SeekFrom::End(0))?;
                    }

                    break;
                }

                Err(error) => return Err(error),
            }
        }

        last_segment = Some(Segment {
            path: path.clone(),
            segment_id: header.segment_id,
            first_sequence: header.first_sequence,
            next_sequence: expected_sequence,
            minor: header.minor,
        });
    }

    Ok((mutations, report, last_segment))
}

/// The first record after a damaged one at `damaged` (offset and sequence) whose checksum
/// holds and whose sequence continues the log (`>= expected`). Reads the rest of the segment
/// once; only startup with a damaged last segment comes here.
fn intact_record_after(
    file: &mut File,
    damaged: u64,
    end: u64,
    shard_id: u32,
    limits: ProtocolLimits,
    expected: u64,
) -> Result<Option<(u64, u64)>, PersistenceError> {
    let mut rest = Vec::new();

    file.seek(SeekFrom::Start(damaged))?;
    file.take(end.saturating_sub(damaged))
        .read_to_end(&mut rest)?;

    for start in 1..rest.len().saturating_sub(RECORD_MIN_LEN - 1) {
        let length = u32::from_le_bytes(rest[start..start + 4].try_into().unwrap()) as usize;

        if length < RECORD_MIN_LEN || length > rest.len() - start {
            continue;
        }

        // Most offsets fail on the header; only the rest pay for a checksum.
        let frame = &rest[start..start + length];
        if !record_header_plausible(frame, shard_id, expected) {
            continue;
        }

        if let Ok(record) = decode_record(frame, shard_id, limits)
            && record.sequence >= expected
        {
            return Ok(Some((damaged + start as u64, record.sequence)));
        }
    }

    Ok(None)
}

/// True when every byte of `file` from `from` to its end is zero (an unwritten preallocated tail).
fn rest_is_zero(file: &mut File, from: u64) -> io::Result<bool> {
    file.seek(SeekFrom::Start(from))?;
    let mut buffer = [0u8; 8192];

    loop {
        let count = file.read(&mut buffer)?;

        if count == 0 {
            return Ok(true);
        }

        if buffer[..count].iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
    }
}

pub(super) fn read_partial(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut read = 0;

    while read < buffer.len() {
        match reader.read(&mut buffer[read..])? {
            0 => break,
            count => read += count,
        }
    }

    Ok(read)
}
