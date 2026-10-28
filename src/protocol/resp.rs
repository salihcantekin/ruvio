use super::*;

pub fn read_command<R: BufRead>(reader: &mut R) -> Result<Option<Command<'static>>, ProtocolError> {
    read_command_with_limits(reader, ProtocolLimits::default())
}

pub fn read_command_with_limits<R: BufRead>(
    reader: &mut R,
    limits: ProtocolLimits,
) -> Result<Option<Command<'static>>, ProtocolError> {
    let Some(prefix) = read_byte(reader)? else {
        return Ok(None);
    };

    if prefix != b'*' {
        return Err(ProtocolError::Invalid("expected an array"));
    }

    let count_line = read_line(reader, 20)?;
    let argument_count = parse_usize(&count_line)?;

    if argument_count == 0 {
        return Err(ProtocolError::Invalid("command array cannot be empty"));
    }

    // Checked before the vector is reserved, so a hostile count header cannot force an allocation.
    if argument_count > limits.max_arguments {
        return Err(ProtocolError::LimitExceeded("too many arguments"));
    }

    let mut command_bytes = checked_frame_add(1, count_line.len() + 2, limits)?;
    let mut arguments = Vec::with_capacity(argument_count.min(1024));

    for _ in 0..argument_count {
        if read_byte(reader)? != Some(b'$') {
            return Err(ProtocolError::Invalid("expected a bulk string"));
        }

        let length_line = read_line(reader, 20)?;
        let length = parse_usize(&length_line)?;

        if length > limits.max_bulk_bytes() {
            return Err(ProtocolError::LimitExceeded("bulk string is too large"));
        }

        command_bytes = checked_frame_add(command_bytes, 1 + length_line.len() + 2, limits)?;
        command_bytes = checked_frame_add(command_bytes, length + 2, limits)?;

        let mut argument = vec![0; length];

        reader.read_exact(&mut argument)?;
        expect_crlf(reader)?;
        arguments.push(Cow::Owned(argument));
    }

    let command = match parse_arguments(arguments) {
        Ok(command) => command,
        Err(ProtocolError::Invalid(message)) => {
            return Err(ProtocolError::Rejected {
                message,
                consumed: 0,
            });
        }

        Err(error) => return Err(error),
    };

    validate_command_payloads(&command, limits)?;
    Ok(Some(command))
}

/// Parse one command from a network buffer.
///
/// `Ok(None)` means the frame is incomplete and more bytes are required.
pub fn parse_command(bytes: &[u8]) -> Result<Option<(Command<'_>, usize)>, ProtocolError> {
    parse_command_with_limits(bytes, ProtocolLimits::default())
}

pub fn parse_command_with_limits(
    bytes: &[u8],
    limits: ProtocolLimits,
) -> Result<Option<(Command<'_>, usize)>, ProtocolError> {
    if bytes.first().is_some_and(|&byte| byte != b'*') {
        return super::inline::parse_inline_command(bytes, limits);
    }

    let mut spans = FrameSpans::new();
    let Some(consumed) = parse_frame_spans(bytes, limits, &mut spans)? else {
        return Ok(None);
    };
    let spans = spans.as_slice();
    let (start, end) = spans[0];
    let name = &bytes[start as usize..end as usize];
    let command = match super::table::lookup(name) {
        Some(spec) => (spec.parse)(&super::table::Args::from_frame(bytes, &spans[1..])),
        None => Err(ProtocolError::Invalid("unknown command")),
    };

    let command = match command {
        Ok(command) => command,
        Err(ProtocolError::Invalid(message)) => {
            return Err(ProtocolError::Rejected { message, consumed });
        }

        Err(error) => return Err(error),
    };

    // Every limit `validate_command_payloads` checks is on argument bytes (alone or summed
    // with a small constant), all of which fit in the frame: a frame no larger than the
    // smaller limit cannot fail any of them, so the per-command walk is skipped.
    if consumed > limits.max_key_bytes.min(limits.max_value_bytes) {
        match validate_command_payloads(&command, limits) {
            // The whole frame is in hand: refuse the command and keep the connection.
            Err(ProtocolError::LimitExceeded(message)) => {
                return Err(ProtocolError::Rejected { message, consumed });
            }

            other => other?,
        }
    }

    Ok(Some((command, consumed)))
}

/// Locate every bulk argument of the RESP command at the start of `bytes`, pushing
/// `(start, end)` offsets (name first) into `spans`. Returns the frame length, or `None`
/// when the frame is incomplete.
pub(super) fn parse_frame_spans(
    bytes: &[u8],
    limits: ProtocolLimits,
    spans: &mut FrameSpans,
) -> Result<Option<usize>, ProtocolError> {
    spans.clear();

    if bytes.is_empty() {
        return Ok(None);
    }

    if bytes[0] != b'*' {
        return Err(ProtocolError::Invalid("expected an array"));
    }

    let mut position = 1;
    let Some(argument_count) = read_length_line(bytes, &mut position)? else {
        return Ok(None);
    };

    if argument_count == 0 {
        return Err(ProtocolError::Invalid("command array cannot be empty"));
    }

    if argument_count > limits.max_arguments {
        // A frame the size limit could hold (each argument is at least `$0\r\n\r\n`)
        // is skipped and refused, so the client reads the error instead of a reset.
        return Err(
            match argument_count
                .checked_mul(MIN_BULK_FRAME)
                .and_then(|least| least.checked_add(position))
            {
                Some(least) if least <= limits.max_command_bytes => ProtocolError::Oversized {
                    message: TOO_MANY_ARGUMENTS,
                    parsed: position,
                    skip: 0,
                    args_left: argument_count,
                },
                _ => ProtocolError::LimitExceeded("too many arguments"),
            },
        );
    }

    // Each argument takes at least `MIN_BULK_FRAME` bytes, so the buffer bounds how many
    // can be read now even when the header claims more.
    spans.reserve(argument_count.min((bytes.len() - position) / MIN_BULK_FRAME));

    for index in 0..argument_count {
        if position == bytes.len() {
            return Ok(None);
        }

        if bytes[position] != b'$' {
            return Err(ProtocolError::Invalid("expected a bulk string"));
        }

        position += 1;

        let Some(length) = read_length_line(bytes, &mut position)? else {
            return Ok(None);
        };

        if length > limits.max_bulk_bytes() {
            // A length the frame limit allows is skipped and refused; a larger one is
            // not worth reading past.
            return Err(
                match position
                    .checked_add(length)
                    .and_then(|end| end.checked_add(2))
                {
                    Some(end) if end <= limits.max_command_bytes => ProtocolError::Oversized {
                        message: OVERSIZED_ARGUMENT,
                        parsed: position,
                        skip: length + 2,
                        args_left: argument_count - index - 1,
                    },
                    _ => ProtocolError::LimitExceeded("bulk string is too large"),
                },
            );
        }

        let payload_end = position
            .checked_add(length)
            .ok_or(ProtocolError::LimitExceeded("length overflow"))?;
        let frame_end = payload_end
            .checked_add(2)
            .ok_or(ProtocolError::LimitExceeded("length overflow"))?;

        if frame_end > limits.max_command_bytes {
            return Err(ProtocolError::LimitExceeded("command is too large"));
        }

        if frame_end > bytes.len() {
            return Ok(None);
        }

        if bytes[payload_end..frame_end] != *b"\r\n" {
            return Err(ProtocolError::Invalid("bulk string must end with CRLF"));
        }

        spans.push((position as u32, payload_end as u32));
        position = frame_end;
    }

    Ok(Some(position))
}

const TOO_MANY_ARGUMENTS: &str = "too many arguments (max_arguments); command not executed";
/// Bytes of the smallest bulk argument, `$0\r\n\r\n`.
const MIN_BULK_FRAME: usize = 6;

const OVERSIZED_ARGUMENT: &str =
    "argument exceeds max_key_bytes/max_value_bytes; command not executed";

/// The `$<length>\r\n` header of the bulk argument at the start of `bytes`: its byte
/// length and the payload length that follows (CRLF not included). `None` while the
/// header is incomplete.
pub fn bulk_header(bytes: &[u8]) -> Result<Option<(usize, usize)>, ProtocolError> {
    match bytes.first() {
        None => return Ok(None),
        Some(b'$') => {}
        Some(_) => return Err(ProtocolError::Invalid("expected a bulk string")),
    }

    let mut position = 1;

    Ok(read_length_line(bytes, &mut position)?.map(|length| (position, length)))
}

const INLINE_SPANS: usize = 32;

/// Argument spans of one frame: on the stack for up to `INLINE_SPANS` arguments (nearly
/// every command, and an `MSET` of up to 15 pairs), on the heap beyond.
pub(super) struct FrameSpans {
    inline: [(u32, u32); INLINE_SPANS],
    heap: Vec<(u32, u32)>,
    len: usize,
}

impl FrameSpans {
    pub(super) fn new() -> Self {
        Self {
            inline: [(0, 0); INLINE_SPANS],
            heap: Vec::new(),
            len: 0,
        }
    }

    fn clear(&mut self) {
        self.heap.clear();
        self.len = 0;
    }

    /// Room for `count` arguments in one allocation when they do not fit inline, so a
    /// long frame never regrows the list.
    fn reserve(&mut self, count: usize) {
        if count > INLINE_SPANS {
            self.heap.reserve_exact(count);
        }
    }

    #[inline]
    fn push(&mut self, span: (u32, u32)) {
        if self.len < INLINE_SPANS {
            self.inline[self.len] = span;
        } else {
            if self.len == INLINE_SPANS {
                self.heap.extend_from_slice(&self.inline);
            }

            self.heap.push(span);
        }

        self.len += 1;
    }

    pub(super) fn as_slice(&self) -> &[(u32, u32)] {
        if self.len <= INLINE_SPANS {
            &self.inline[..self.len]
        } else {
            &self.heap
        }
    }
}

/// Reads an unsigned decimal terminated by CRLF at `position` (a RESP count or bulk length)
/// in one pass, and moves past the CRLF. `None` when the line is incomplete.
#[inline]
pub(super) fn read_length_line(
    bytes: &[u8],
    position: &mut usize,
) -> Result<Option<usize>, ProtocolError> {
    if let Some((value, next)) = short_length_line(bytes, *position) {
        *position = next;
        return Ok(Some(value));
    }

    read_length_line_general(bytes, position)
}

/// The common case of `read_length_line`: one to three digits and CRLF, read from a fixed
/// five-byte window with no overflow checks (999 fits any `usize`). `None` sends every
/// other line, including a complete one too close to the end of `bytes`, to the general
/// reader, which also reports the errors.
#[inline(always)]
fn short_length_line(bytes: &[u8], at: usize) -> Option<(usize, usize)> {
    const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");

    let window: &[u8; 5] = bytes.get(at..at.wrapping_add(5))?.try_into().ok()?;
    let crlf_at = |index: usize| u16::from_ne_bytes([window[index], window[index + 1]]) == CRLF;
    let digit = |index: usize| {
        let value = window[index].wrapping_sub(b'0');
        (value <= 9).then_some(usize::from(value))
    };

    let mut value = digit(0)?;

    if crlf_at(1) {
        return Some((value, at + 3));
    }

    value = value * 10 + digit(1)?;

    if crlf_at(2) {
        return Some((value, at + 4));
    }

    value = value * 10 + digit(2)?;

    crlf_at(3).then_some((value, at + 5))
}

#[inline(never)]
fn read_length_line_general(
    bytes: &[u8],
    position: &mut usize,
) -> Result<Option<usize>, ProtocolError> {
    const MAX_DIGITS: usize = 20;

    let start = *position;
    let mut at = start;
    let mut value = 0usize;

    loop {
        let Some(&byte) = bytes.get(at) else {
            return Ok(None);
        };

        match byte {
            b'0'..=b'9' => {
                if at - start == MAX_DIGITS {
                    return Err(ProtocolError::LimitExceeded("line is too long"));
                }

                value = value
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(usize::from(byte - b'0')))
                    .ok_or(ProtocolError::LimitExceeded("length overflow"))?;
                at += 1;
            }

            b'\r' if at == start => return Err(ProtocolError::Invalid("empty length")),
            b'\r' => {
                return match bytes.get(at + 1) {
                    None => Ok(None),
                    Some(b'\n') => {
                        *position = at + 2;
                        Ok(Some(value))
                    }
                    Some(_) => Err(ProtocolError::Invalid("length is not an unsigned integer")),
                };
            }

            _ => return Err(ProtocolError::Invalid("length is not an unsigned integer")),
        }
    }
}

/// Lower bound, in bytes, for the total size of the incomplete RESP command at the
/// start of `bytes`. Used to skip re-parsing a large frame until enough has arrived.
/// Returns `bytes.len() + 1` when nothing better is known (or the frame is malformed;
/// the normal parser reports that).
pub fn incomplete_frame_need(bytes: &[u8], limits: ProtocolLimits) -> usize {
    let unknown = bytes.len().saturating_add(1);

    if bytes.first() != Some(&b'*') {
        return unknown;
    }

    let mut position = 1;
    let Ok(Some(count_bytes)) = read_line_slice(bytes, &mut position, 20) else {
        return unknown;
    };
    let Ok(count) = parse_usize(count_bytes) else {
        return unknown;
    };

    if count == 0 || count > limits.max_arguments {
        return unknown;
    }

    for _ in 0..count {
        if bytes.get(position) != Some(&b'$') {
            return unknown;
        }

        position += 1;

        let Ok(Some(length_bytes)) = read_line_slice(bytes, &mut position, 20) else {
            return unknown;
        };
        let Ok(length) = parse_usize(length_bytes) else {
            return unknown;
        };
        let Some(frame_end) = position
            .checked_add(length)
            .and_then(|end| end.checked_add(2))
        else {
            return unknown;
        };

        if frame_end > bytes.len() {
            return frame_end.min(limits.max_command_bytes).max(unknown);
        }

        position = frame_end;
    }

    unknown
}

pub(super) fn read_line_slice<'a>(
    bytes: &'a [u8],
    position: &mut usize,
    maximum: usize,
) -> Result<Option<&'a [u8]>, ProtocolError> {
    let remaining = &bytes[*position..];
    // Only the first `maximum + 2` bytes can hold an acceptable terminator, so the scan
    // stops there; a CRLF further on is "too long" either way.
    let window = &remaining[..remaining.len().min(maximum.saturating_add(2))];
    let mut from = 0;
    let found = loop {
        match window[from..].iter().position(|&byte| byte == b'\r') {
            Some(offset) if window.get(from + offset + 1) == Some(&b'\n') => {
                break Some(from + offset);
            }
            Some(offset) => from += offset + 1,
            None => break None,
        }
    };
    let Some(relative_end) = found else {
        if remaining.len() > window.len() {
            return Err(ProtocolError::LimitExceeded("line is too long"));
        }

        return Ok(None);
    };

    let line = &remaining[..relative_end];

    *position += relative_end + 2;
    Ok(Some(line))
}

pub(super) fn read_byte<R: BufRead>(reader: &mut R) -> Result<Option<u8>, ProtocolError> {
    let buffer = reader.fill_buf()?;
    let byte = buffer.first().copied();

    if byte.is_some() {
        reader.consume(1);
    }

    Ok(byte)
}

pub(super) fn read_line<R: BufRead>(
    reader: &mut R,
    maximum: usize,
) -> Result<Vec<u8>, ProtocolError> {
    let mut line = Vec::new();

    loop {
        let buffer = reader.fill_buf()?;

        if buffer.is_empty() {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }

        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(buffer.len(), |index| index + 1);
        let content_length = newline.unwrap_or(buffer.len());

        if line.len() + content_length > maximum + 1 {
            return Err(ProtocolError::LimitExceeded("line is too long"));
        }

        line.extend_from_slice(&buffer[..content_length]);
        reader.consume(consumed);

        if newline.is_some() {
            if line.pop() != Some(b'\r') {
                return Err(ProtocolError::Invalid("line must end with CRLF"));
            }

            return Ok(line);
        }
    }
}

pub(super) fn expect_crlf<R: BufRead>(reader: &mut R) -> Result<(), ProtocolError> {
    let mut terminator = [0; 2];

    reader.read_exact(&mut terminator)?;

    if terminator != *b"\r\n" {
        return Err(ProtocolError::Invalid("bulk string must end with CRLF"));
    }

    Ok(())
}

const REDACTED: &[u8] = b"(redacted)";

/// Replaces passwords in `AUTH`, `HELLO ... AUTH`, `ACL SETUSER` and secret `CONFIG SET`
/// values, so `SLOWLOG GET` never hands them to another client.
fn redact_secrets(arguments: &mut [&[u8]]) {
    let Some(name) = arguments.first() else {
        return;
    };

    if name.eq_ignore_ascii_case(b"AUTH") {
        arguments[1..].fill(REDACTED);
    } else if name.eq_ignore_ascii_case(b"HELLO") {
        if let Some(at) = arguments
            .iter()
            .skip(1)
            .position(|word| word.eq_ignore_ascii_case(b"AUTH"))
        {
            let start = at + 2;
            let end = (start + 2).min(arguments.len());

            arguments[start.min(end)..end].fill(REDACTED);
        }
    } else if name.eq_ignore_ascii_case(b"ACL")
        && arguments
            .get(1)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(b"SETUSER"))
    {
        for rule in arguments.iter_mut().skip(3) {
            if matches!(rule.first(), Some(b'>' | b'<' | b'#' | b'!')) {
                *rule = REDACTED;
            }
        }
    } else if name.eq_ignore_ascii_case(b"CONFIG")
        && arguments
            .get(1)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(b"SET"))
    {
        let mut index = 2;

        while index + 1 < arguments.len() {
            let parameter = arguments[index].to_ascii_lowercase();

            if parameter.windows(4).any(|part| part == b"pass")
                || parameter.windows(6).any(|part| part == b"secret")
                || parameter.windows(5).any(|part| part == b"token")
            {
                arguments[index + 1] = REDACTED;
            }

            index += 2;
        }
    } else if name.eq_ignore_ascii_case(b"RUVIO")
        && arguments
            .get(1)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(b"MIGRATE"))
        && arguments
            .get(2)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(b"EXPORT"))
        && arguments
            .get(8)
            .is_some_and(|word| word.eq_ignore_ascii_case(b"AUTH"))
    {
        // `RUVIO MIGRATE EXPORT id first last host port AUTH [user] password`.
        arguments[9..].fill(REDACTED);
    }
}

/// The arguments `SLOWLOG GET` shows for a command frame, as Redis keeps them: at most 32
/// (the last one then says how many more there were), each cut to `max_bytes` with a note of
/// how many bytes were dropped. Inline frames are split on whitespace.
pub fn slowlog_arguments(frame: &[u8], max_bytes: usize) -> Vec<Vec<u8>> {
    const MAX_ARGS: usize = 32;

    let mut arguments: Vec<&[u8]> = if frame.first() == Some(&b'*') {
        let mut spans = FrameSpans::new();
        let limits = ProtocolLimits {
            max_arguments: usize::MAX,
            max_key_bytes: usize::MAX,
            max_value_bytes: usize::MAX,
            max_command_bytes: usize::MAX,
            max_pipeline_commands: usize::MAX,
            max_connection_buffer_bytes: usize::MAX,
            databases: 0,
        };

        match parse_frame_spans(frame, limits, &mut spans) {
            Ok(Some(_)) => spans
                .as_slice()
                .iter()
                .map(|&(start, end)| &frame[start as usize..end as usize])
                .collect(),
            _ => Vec::new(),
        }
    } else {
        frame
            .split(|byte| byte.is_ascii_whitespace())
            .filter(|word| !word.is_empty())
            .collect()
    };

    redact_secrets(&mut arguments);
    let shown = if arguments.len() > MAX_ARGS {
        MAX_ARGS - 1
    } else {
        arguments.len()
    };
    let mut out: Vec<Vec<u8>> = arguments[..shown]
        .iter()
        .map(|argument| {
            if argument.len() > max_bytes {
                let mut cut = argument[..max_bytes].to_vec();
                cut.extend_from_slice(
                    format!("... ({} more bytes)", argument.len() - max_bytes).as_bytes(),
                );
                cut
            } else {
                argument.to_vec()
            }
        })
        .collect();

    if arguments.len() > MAX_ARGS {
        out.push(format!("... ({} more arguments)", arguments.len() - shown).into_bytes());
    }

    out
}
