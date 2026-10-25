use std::io::BufRead;

use crate::error::Error;
use crate::value::RespValue;

const MAX_LINE_BYTES: usize = 1024 * 1024;
const MAX_BULK_BYTES: usize = 64 * 1024 * 1024;
const MAX_ARRAY_LENGTH: usize = 1024 * 1024;

pub fn append_strings(payload: &mut Vec<u8>, arguments: &[&str]) -> Result<(), Error> {
    if arguments.is_empty() {
        return Err(Error::Protocol(
            "a command needs at least one argument".to_owned(),
        ));
    }

    payload.push(b'*');
    push_decimal(payload, arguments.len());
    payload.extend_from_slice(b"\r\n");

    for argument in arguments {
        append_bulk(payload, argument.as_bytes());
    }

    Ok(())
}

/// Binary-safe form of [`append_strings`]: each argument is raw bytes.
pub fn append_bytes(payload: &mut Vec<u8>, arguments: &[&[u8]]) -> Result<(), Error> {
    if arguments.is_empty() {
        return Err(Error::Protocol(
            "a command needs at least one argument".to_owned(),
        ));
    }

    payload.push(b'*');
    push_decimal(payload, arguments.len());
    payload.extend_from_slice(b"\r\n");

    for argument in arguments {
        append_bulk(payload, argument);
    }

    Ok(())
}

fn append_bulk(payload: &mut Vec<u8>, argument: &[u8]) {
    payload.push(b'$');
    push_decimal(payload, argument.len());
    payload.extend_from_slice(b"\r\n");
    payload.extend_from_slice(argument);
    payload.extend_from_slice(b"\r\n");
}

fn push_decimal(payload: &mut Vec<u8>, value: usize) {
    let mut digits = [0_u8; 20];
    let mut count = 0;
    let mut remaining = value;

    loop {
        digits[count] = b'0' + (remaining % 10) as u8;
        count += 1;
        remaining /= 10;

        if remaining == 0 {
            break;
        }
    }

    while count > 0 {
        count -= 1;
        payload.push(digits[count]);
    }
}

pub fn read_value(reader: &mut impl BufRead) -> Result<RespValue, Error> {
    let mut prefix = [0_u8; 1];

    reader.read_exact(&mut prefix)?;

    match prefix[0] {
        b'+' => Ok(RespValue::Simple(read_text(reader)?)),
        b'-' => Ok(RespValue::Error(read_text(reader)?)),
        b':' => Ok(RespValue::Integer(read_integer(reader, "integer")?)),
        b'$' => read_bulk(reader),
        b'*' => read_array(reader),
        other => Err(Error::Protocol(format!(
            "unexpected RESP prefix 0x{other:02x}"
        ))),
    }
}

fn read_bulk(reader: &mut impl BufRead) -> Result<RespValue, Error> {
    let length = read_integer(reader, "bulk length")?;

    if length == -1 {
        return Ok(RespValue::Null);
    }

    if length < 0 || length as usize > MAX_BULK_BYTES {
        return Err(Error::Protocol("bulk length is out of range".to_owned()));
    }

    // Payload and its CRLF are read together so the trailer needs no second call.
    let length = length as usize;
    let mut payload = vec![0_u8; length + 2];

    reader.read_exact(&mut payload)?;

    if payload[length..] != *b"\r\n" {
        return Err(Error::Protocol(
            "bulk payload was not terminated".to_owned(),
        ));
    }

    payload.truncate(length);

    Ok(RespValue::Bulk(payload))
}

fn read_array(reader: &mut impl BufRead) -> Result<RespValue, Error> {
    let length = read_integer(reader, "array length")?;

    if length == -1 {
        return Ok(RespValue::Null);
    }

    if length < 0 || length as usize > MAX_ARRAY_LENGTH {
        return Err(Error::Protocol("array length is out of range".to_owned()));
    }

    let mut items = Vec::with_capacity((length as usize).min(1024));

    for _ in 0..length {
        items.push(read_value(reader)?);
    }

    Ok(RespValue::Array(items))
}

// Only simple and error lines are decoded as UTF-8; bulk values stay Vec<u8>, so binary data is never forced through UTF-8.
fn read_text(reader: &mut impl BufRead) -> Result<String, Error> {
    with_line(reader, |line| {
        std::str::from_utf8(line)
            .map(str::to_owned)
            .map_err(|_| Error::Protocol("RESP line is not UTF-8".to_owned()))
    })
}

fn read_integer(reader: &mut impl BufRead, label: &str) -> Result<i64, Error> {
    with_line(reader, |line| parse_integer(line, label))
}

/// Hands one line (without its CRLF) to `parse`. A line that sits whole in the reader's buffer,
/// which every reply header does, is parsed in place; only a line split across reads is copied.
fn with_line<T>(
    reader: &mut impl BufRead,
    parse: impl FnOnce(&[u8]) -> Result<T, Error>,
) -> Result<T, Error> {
    let buffered = reader.fill_buf()?;

    if let Some(end) = buffered.iter().position(|&byte| byte == b'\n') {
        let result = trim_line(&buffered[..=end]).and_then(parse);

        reader.consume(end + 1);

        return result;
    }

    let mut line = Vec::new();
    let read = reader.read_until(b'\n', &mut line)?;

    if read == 0 || line.last() != Some(&b'\n') {
        return Err(Error::Protocol("connection closed".to_owned()));
    }

    parse(trim_line(&line)?)
}

/// The line body of `line`, which ends in `\n`: strips the CRLF and enforces the length cap.
fn trim_line(line: &[u8]) -> Result<&[u8], Error> {
    let Some(body) = line
        .strip_suffix(b"\n")
        .and_then(|rest| rest.strip_suffix(b"\r"))
    else {
        return Err(Error::Protocol("expected CR before LF".to_owned()));
    };

    if body.len() > MAX_LINE_BYTES {
        return Err(Error::Protocol("RESP line is too long".to_owned()));
    }

    Ok(body)
}

fn parse_integer(bytes: &[u8], label: &str) -> Result<i64, Error> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| Error::Protocol(format!("bad RESP {label}")))?;

    text.parse()
        .map_err(|_| Error::Protocol(format!("bad RESP {label}")))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{append_bytes, append_strings, read_value};

    use crate::value::RespValue;

    #[test]
    fn encode_writes_a_resp2_array_of_bulk_strings() {
        let mut payload = Vec::new();

        append_strings(&mut payload, &["GET", "key"]).expect("encode");

        assert_eq!(payload, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }

    #[test]
    fn binary_encode_matches_text_encode_and_keeps_raw_bytes() {
        let mut text = Vec::new();
        let mut binary = Vec::new();

        append_strings(&mut text, &["SET", "k", "v"]).unwrap();
        append_bytes(&mut binary, &[b"SET", b"k", b"v"]).unwrap();
        assert_eq!(text, binary);

        binary.clear();
        append_bytes(&mut binary, &[b"SET", b"k", &[0, 0xff, b'\r', b'\n']]).unwrap();
        assert_eq!(
            binary,
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\n\x00\xff\r\n\r\n"
        );
    }

    #[test]
    fn reader_decodes_the_reply_kinds_ruvio_sends() {
        let payload = b"+OK\r\n-ERR nope\r\n:7\r\n$4\r\nping\r\n$-1\r\n*2\r\n$1\r\na\r\n:2\r\n";
        let mut reader = Cursor::new(&payload[..]);

        assert_eq!(
            read_value(&mut reader).unwrap(),
            RespValue::Simple("OK".to_owned())
        );
        assert_eq!(
            read_value(&mut reader).unwrap(),
            RespValue::Error("ERR nope".to_owned())
        );
        assert_eq!(read_value(&mut reader).unwrap(), RespValue::Integer(7));
        assert_eq!(
            read_value(&mut reader).unwrap(),
            RespValue::Bulk(b"ping".to_vec())
        );
        assert_eq!(read_value(&mut reader).unwrap(), RespValue::Null);

        assert_eq!(
            read_value(&mut reader).unwrap(),
            RespValue::Array(vec![RespValue::Bulk(b"a".to_vec()), RespValue::Integer(2)])
        );
    }

    #[test]
    fn reader_keeps_crlf_inside_a_bulk_string() {
        let mut framed = b"$4\r\n".to_vec();

        framed.extend_from_slice(b"a\r\nb");
        framed.extend_from_slice(b"\r\n");

        let mut reader = Cursor::new(framed);

        assert_eq!(
            read_value(&mut reader).unwrap(),
            RespValue::Bulk(b"a\r\nb".to_vec())
        );
    }

    use std::io::{BufRead, BufReader, Read};

    use crate::error::Error;

    fn decode(bytes: &[u8]) -> Result<RespValue, Error> {
        read_value(&mut Cursor::new(bytes.to_vec()))
    }

    fn protocol_message(result: Result<RespValue, Error>) -> String {
        match result {
            Err(Error::Protocol(message)) => message,
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn encoding_with_no_arguments_is_rejected() {
        let mut payload = Vec::new();
        assert!(matches!(
            append_strings(&mut payload, &[]),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            append_bytes(&mut payload, &[]),
            Err(Error::Protocol(_))
        ));
        assert!(payload.is_empty());
    }

    #[test]
    fn encoding_handles_multi_digit_lengths_and_empty_arguments() {
        let big = "x".repeat(1234);
        let mut payload = Vec::new();
        append_strings(&mut payload, &["SET", "", &big]).unwrap();
        let text = String::from_utf8(payload).unwrap();
        assert!(text.starts_with("*3\r\n$3\r\nSET\r\n$0\r\n\r\n$1234\r\n"));
        assert!(text.ends_with("\r\n"));
    }

    #[test]
    fn unknown_prefix_is_a_protocol_error() {
        assert!(protocol_message(decode(b"?what\r\n")).contains("0x3f"));
    }

    #[test]
    fn empty_input_is_an_io_error() {
        assert!(matches!(decode(b""), Err(Error::Io(_))));
    }

    #[test]
    fn null_array_and_empty_array_and_empty_bulk() {
        assert_eq!(decode(b"*-1\r\n").unwrap(), RespValue::Null);
        assert_eq!(decode(b"*0\r\n").unwrap(), RespValue::Array(vec![]));
        assert_eq!(decode(b"$0\r\n\r\n").unwrap(), RespValue::Bulk(vec![]));
    }

    #[test]
    fn negative_or_oversized_lengths_are_rejected() {
        assert!(protocol_message(decode(b"$-2\r\n")).contains("bulk length"));
        assert!(protocol_message(decode(b"$67108865\r\n")).contains("bulk length"));
        assert!(protocol_message(decode(b"*-2\r\n")).contains("array length"));
        assert!(protocol_message(decode(b"*1048577\r\n")).contains("array length"));
    }

    #[test]
    fn unparseable_integers_are_rejected() {
        assert!(protocol_message(decode(b":abc\r\n")).contains("integer"));
        assert!(protocol_message(decode(b":\r\n")).contains("integer"));
        assert!(protocol_message(decode(b"$x\r\n")).contains("bulk length"));
        assert!(protocol_message(decode(b"*x\r\n")).contains("array length"));
        assert!(protocol_message(decode(b":99999999999999999999\r\n")).contains("integer"));
        assert!(protocol_message(decode(b":\xff\r\n")).contains("integer"));
        assert_eq!(decode(b":-12\r\n").unwrap(), RespValue::Integer(-12));
    }

    #[test]
    fn bulk_without_crlf_trailer_is_rejected() {
        assert!(protocol_message(decode(b"$3\r\nabcXY")).contains("not terminated"));
    }

    #[test]
    fn truncated_bulk_and_array_are_io_errors() {
        assert!(matches!(decode(b"$5\r\nab"), Err(Error::Io(_))));
        assert!(matches!(decode(b"*2\r\n:1\r\n"), Err(Error::Io(_))));
    }

    #[test]
    fn line_without_carriage_return_or_newline_is_rejected() {
        assert!(protocol_message(decode(b"+OK\n")).contains("CR before LF"));
        assert!(protocol_message(decode(b"+OK")).contains("connection closed"));
        assert!(protocol_message(decode(b"+")).contains("connection closed"));
    }

    #[test]
    fn non_utf8_simple_and_error_lines_are_rejected() {
        assert!(protocol_message(decode(b"+\xff\xfe\r\n")).contains("not UTF-8"));
        assert!(protocol_message(decode(b"-\xff\r\n")).contains("not UTF-8"));
    }

    #[test]
    fn overlong_line_is_rejected() {
        let mut framed = b"+".to_vec();
        framed.extend(std::iter::repeat_n(b'a', 1024 * 1024 + 1));
        framed.extend_from_slice(b"\r\n");
        assert!(protocol_message(decode(&framed)).contains("too long"));
        let mut ok = b"+".to_vec();
        ok.extend(std::iter::repeat_n(b'a', 1024 * 1024));
        ok.extend_from_slice(b"\r\n");
        assert!(matches!(decode(&ok), Ok(RespValue::Simple(text)) if text.len() == 1024 * 1024));
    }

    /// A reader that hands out one byte per read, forcing the slow split-line path.
    struct Trickle(Cursor<Vec<u8>>);

    impl Read for Trickle {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let limit = buffer.len().min(1);
            self.0.read(&mut buffer[..limit])
        }
    }

    #[test]
    fn replies_split_across_reads_decode_the_same() {
        let bytes = b"*3\r\n+hello world\r\n:42\r\n$5\r\nabcde\r\n".to_vec();
        let mut reader = BufReader::with_capacity(1, Trickle(Cursor::new(bytes)));
        assert_eq!(
            read_value(&mut reader).unwrap(),
            RespValue::Array(vec![
                RespValue::Simple("hello world".into()),
                RespValue::Integer(42),
                RespValue::Bulk(b"abcde".to_vec()),
            ])
        );
        assert!(reader.fill_buf().unwrap().is_empty());
    }

    #[test]
    fn split_line_that_ends_without_newline_is_a_closed_connection() {
        let mut reader = BufReader::with_capacity(1, Trickle(Cursor::new(b":12".to_vec())));
        assert!(protocol_message(read_value(&mut reader)).contains("connection closed"));
    }

    #[test]
    fn nested_arrays_decode() {
        assert_eq!(
            decode(b"*2\r\n*1\r\n:1\r\n*0\r\n").unwrap(),
            RespValue::Array(vec![
                RespValue::Array(vec![RespValue::Integer(1)]),
                RespValue::Array(vec![]),
            ])
        );
    }
}
