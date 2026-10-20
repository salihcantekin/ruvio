//! The small RESP2 subset the coordinator speaks: commands to data nodes, and the Sentinel
//! API clients call.

use std::{io, time::Duration};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    time::timeout,
};

use super::tls::Stream;

/// A RESP2 reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
}

impl Value {
    pub fn text(&self) -> Option<String> {
        match self {
            Self::Simple(text) => Some(text.clone()),
            Self::Bulk(Some(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
            Self::Integer(value) => Some(value.to_string()),
            _ => None,
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Simple(text) => out.extend_from_slice(format!("+{text}\r\n").as_bytes()),
            Self::Error(text) => out.extend_from_slice(format!("-{text}\r\n").as_bytes()),
            Self::Integer(value) => out.extend_from_slice(format!(":{value}\r\n").as_bytes()),
            Self::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
            Self::Bulk(Some(bytes)) => {
                out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            Self::Array(None) => out.extend_from_slice(b"*-1\r\n"),
            Self::Array(Some(items)) => {
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                for item in items {
                    item.encode(out);
                }
            }
        }
    }

    pub fn bulk(text: impl AsRef<[u8]>) -> Self {
        Self::Bulk(Some(text.as_ref().to_vec()))
    }

    pub fn array(items: Vec<Value>) -> Self {
        Self::Array(Some(items))
    }
}

/// Encodes a command as a RESP array of bulk strings.
pub fn command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();

    Value::array(args.iter().map(Value::bulk).collect()).encode(&mut out);
    out
}

const MAX_BULK: usize = 64 * 1024 * 1024;
const MAX_ITEMS: usize = 1024 * 1024;

/// Reads one reply. `None` at a clean end of stream.
pub async fn read_value<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Option<Value>> {
    let mut line = String::new();

    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let line = line.trim_end_matches(['\r', '\n']);
    let (kind, rest) = line.split_at(line.len().min(1));
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad RESP line {line:?}"),
        )
    };

    Ok(Some(match kind {
        "+" => Value::Simple(rest.to_owned()),
        "-" => Value::Error(rest.to_owned()),
        ":" => Value::Integer(rest.parse().map_err(|_| invalid())?),
        "$" => {
            let length: i64 = rest.parse().map_err(|_| invalid())?;

            if length < 0 {
                Value::Bulk(None)
            } else {
                let length = usize::try_from(length).map_err(|_| invalid())?;

                if length > MAX_BULK {
                    return Err(invalid());
                }
                let mut bytes = vec![0; length + 2];

                reader.read_exact(&mut bytes).await?;
                bytes.truncate(length);
                Value::Bulk(Some(bytes))
            }
        }
        "*" => {
            let count: i64 = rest.parse().map_err(|_| invalid())?;

            if count < 0 {
                Value::Array(None)
            } else {
                let count = usize::try_from(count).map_err(|_| invalid())?;

                if count > MAX_ITEMS {
                    return Err(invalid());
                }
                let mut items = Vec::with_capacity(count.min(64));

                for _ in 0..count {
                    let item = Box::pin(read_value(reader))
                        .await?
                        .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;

                    items.push(item);
                }
                Value::Array(Some(items))
            }
        }
        _ => return Err(invalid()),
    }))
}

/// Reads one client command (an array of bulk strings) as its arguments.
pub async fn read_command<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<Vec<Vec<u8>>>> {
    match read_value(reader).await? {
        None => Ok(None),
        Some(Value::Array(Some(items))) => items
            .into_iter()
            .map(|item| match item {
                Value::Bulk(Some(bytes)) => Ok(bytes),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a command is an array of bulk strings",
                )),
            })
            .collect::<io::Result<_>>()
            .map(Some),
        Some(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a command is an array of bulk strings",
        )),
    }
}

/// A connection to one data node, opened per call, over TLS with `node_tls`, and logged in
/// when `auth` is set.
pub struct NodeClient {
    reader: BufReader<Stream>,
}

impl NodeClient {
    pub async fn connect(
        address: &super::config::Endpoint,
        auth: Option<&str>,
        wait: Duration,
    ) -> io::Result<Self> {
        let stream = timeout(wait, super::tls::get().dial_node(address))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
        let mut client = Self {
            reader: BufReader::new(stream),
        };

        if let Some(auth) = auth {
            let mut args: Vec<&[u8]> = vec![b"AUTH"];

            args.extend(auth.split_whitespace().map(str::as_bytes));
            if let Value::Error(error) = client.call(&args, wait).await? {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, error));
            }
        }

        Ok(client)
    }

    pub async fn call(&mut self, args: &[&[u8]], wait: Duration) -> io::Result<Value> {
        timeout(wait, async {
            self.reader.get_mut().write_all(&command(args)).await?;
            read_value(&mut self.reader)
                .await?
                .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))
        })
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn values_round_trip() {
        let value = Value::array(vec![
            Value::Simple("OK".into()),
            Value::Error("ERR x".into()),
            Value::Integer(-3),
            Value::bulk("a\r\nb"),
            Value::Bulk(None),
            Value::Array(None),
            Value::array(vec![Value::bulk("nested")]),
        ]);
        let mut bytes = Vec::new();

        value.encode(&mut bytes);
        let mut reader = BufReader::new(bytes.as_slice());

        assert_eq!(read_value(&mut reader).await.unwrap(), Some(value));
        assert_eq!(read_value(&mut reader).await.unwrap(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commands_are_arrays_of_bulk_strings() {
        let bytes = command(&[b"SENTINEL", b"masters"]);
        let mut reader = BufReader::new(bytes.as_slice());

        assert_eq!(
            read_command(&mut reader).await.unwrap(),
            Some(vec![b"SENTINEL".to_vec(), b"masters".to_vec()])
        );
        let mut bad = BufReader::new(&b":1\r\n"[..]);

        assert!(read_command(&mut bad).await.is_err());
    }
}
