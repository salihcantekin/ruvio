use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};

use std::sync::{Arc, Mutex};

use std::net::{TcpStream, ToSocketAddrs};

use std::time::Duration;

use crate::blocking::{Wait, blocking_wait, pipeline_wait};
use crate::client::{ClientOptions, NoticeHook, ReadPolicy};
use crate::codec::read_value;
use crate::command_keys::{COMMAND_KEY_RULES, KeyRule};

use crate::error::Error;
use crate::value::RespValue;

pub const SLOT_COUNT: u16 = 16_384;

const MAX_REDIRECTS: usize = 3;

/// Sleeps before each retry of a command answered `TRYAGAIN`, doubling from 5 ms (635 ms in
/// all): a slot range moving between nodes is frozen for a few hundred milliseconds.
const TRYAGAIN_BACKOFF: [Duration; 7] = [
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(20),
    Duration::from_millis(40),
    Duration::from_millis(80),
    Duration::from_millis(160),
    Duration::from_millis(320),
];

/// What the server answers for a command whose keys span slots; the client answers it the same
/// way without sending the command.
const CROSSSLOT: &str = "CROSSSLOT Keys in request do not hash to the same slot";

const SPLIT: &[&str] = &["DEL", "EXISTS", "MGET", "MSET", "TOUCH", "UNLINK"];

const TRANSACTION: &[&str] = &["DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH"];

/// Returns the hash slot (0 to 16383) of a key: CRC16-XMODEM of the key modulo 16384, the
/// same rule as the Ruvio server and Redis Cluster.
///
/// If the key contains a non-empty `{tag}`, only the text between the first `{` and the
/// next `}` is hashed, so `{user:1}:profile` and `{user:1}:cart` land in the same slot. Keys
/// that must be used together in a multi-key command, transaction or script should share a
/// tag.
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # use std::time::Duration;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// assert_eq!(hash_slot("{user:1}:profile"), hash_slot("{user:1}:cart"));
/// assert!(hash_slot("any-key") < 16384);
/// # Ok(())
/// # }
/// ```
pub fn hash_slot(key: &str) -> u16 {
    hash_slot_bytes(key.as_bytes())
}

/// [`hash_slot`] for a binary (non-UTF-8) key.
pub fn hash_slot_bytes(key: &[u8]) -> u16 {
    crc16(hash_tag(key)) % SLOT_COUNT
}

fn hash_tag(key: &[u8]) -> &[u8] {
    let Some(open) = find_byte(key, b'{') else {
        return key;
    };

    let tagged = &key[open + 1..];
    let Some(close) = find_byte(tagged, b'}') else {
        return key;
    };

    if close == 0 { key } else { &tagged[..close] }
}

// Built at compile time (const fn), so each byte costs one table lookup and nothing runs at startup.
const CRC_TABLE: [u16; 256] = crc_table();

const fn crc_table() -> [u16; 256] {
    let mut table = [0_u16; 256];
    let mut value = 0;

    while value < 256 {
        let mut crc = (value as u16) << 8;
        let mut bit = 0;

        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };

            bit += 1;
        }

        table[value] = crc;
        value += 1;
    }

    table
}

fn crc16(bytes: &[u8]) -> u16 {
    let mut crc = 0_u16;

    for byte in bytes {
        let index = usize::from((crc >> 8) ^ u16::from(*byte));

        crc = (crc << 8) ^ CRC_TABLE[index];
    }

    crc
}

#[cfg(target_arch = "x86_64")]
fn find_byte(haystack: &[u8], needle: u8) -> Option<usize> {
    find_byte_sse2(haystack, needle)
}

#[cfg(target_arch = "aarch64")]
fn find_byte(haystack: &[u8], needle: u8) -> Option<usize> {
    find_byte_neon(haystack, needle)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn find_byte(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().position(|byte| *byte == needle)
}

#[cfg(target_arch = "x86_64")]
fn find_byte_sse2(haystack: &[u8], needle: u8) -> Option<usize> {
    use std::arch::x86_64::{
        __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
    };

    let width = 16;
    let mut offset = 0;
    let pattern = unsafe { _mm_set1_epi8(needle as i8) };

    while offset + width <= haystack.len() {
        let block = unsafe { _mm_loadu_si128(haystack[offset..].as_ptr() as *const __m128i) };

        let mask = unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(block, pattern)) };

        if mask != 0 {
            return Some(offset + mask.trailing_zeros() as usize);
        }

        offset += width;
    }

    haystack[offset..]
        .iter()
        .position(|byte| *byte == needle)
        .map(|index| offset + index)
}

#[cfg(target_arch = "aarch64")]
fn find_byte_neon(haystack: &[u8], needle: u8) -> Option<usize> {
    use std::arch::aarch64::{vceqq_u8, vdupq_n_u8, vld1q_u8, vst1q_u8};

    let width = 16;
    let mut offset = 0;
    let pattern = unsafe { vdupq_n_u8(needle) };

    while offset + width <= haystack.len() {
        let block = unsafe { vld1q_u8(haystack[offset..].as_ptr()) };

        let equal = unsafe { vceqq_u8(block, pattern) };

        let mut lanes = [0_u8; 16];

        unsafe { vst1q_u8(lanes.as_mut_ptr(), equal) };

        if let Some(lane) = lanes.iter().position(|lane| *lane != 0) {
            return Some(offset + lane);
        }

        offset += width;
    }

    haystack[offset..]
        .iter()
        .position(|byte| *byte == needle)
        .map(|index| offset + index)
}

fn command_name(arguments: &[&str]) -> String {
    arguments
        .first()
        .map(|name| name.to_ascii_uppercase())
        .unwrap_or_default()
}

/// Longest command name upper-cased on the stack; every name in the key table fits.
const INLINE_NAME: usize = 32;

/// A command name in upper case, without allocating for any known command.
struct CommandName {
    inline: [u8; INLINE_NAME],
    len: usize,
    spilled: Option<String>,
}

impl CommandName {
    fn of(arguments: &[&str]) -> Self {
        Self::from_bytes(arguments.first().map_or(&[][..], |name| name.as_bytes()))
    }

    fn from_bytes(raw: &[u8]) -> Self {
        let mut inline = [0_u8; INLINE_NAME];

        if raw.len() > INLINE_NAME {
            return Self {
                inline,
                len: 0,
                spilled: Some(String::from_utf8_lossy(raw).to_ascii_uppercase()),
            };
        }

        for (slot, byte) in inline.iter_mut().zip(raw) {
            *slot = byte.to_ascii_uppercase();
        }

        Self {
            inline,
            len: raw.len(),
            spilled: None,
        }
    }

    fn as_str(&self) -> &str {
        match &self.spilled {
            Some(name) => name,
            // Upper-casing only changes ASCII bytes, so valid UTF-8 stays valid; a binary name
            // that is not UTF-8 matches no command, as its lossy form would not.
            None => std::str::from_utf8(&self.inline[..self.len]).unwrap_or("\u{fffd}"),
        }
    }
}

/// The key of a command whose only key is its first argument (`GET`, `SET`, `HSET`, ... and
/// commands the table does not know), found without building the key list. `None` means the
/// command has another rule, and [`command_keys`] answers.
fn single_key<'a>(name: &str, arguments: &[&'a str]) -> Option<&'a str> {
    let key = *arguments.get(1)?;

    match name {
        "SPUBLISH" => Some(key),
        "SSUBSCRIBE" | "SUNSUBSCRIBE" => None,
        _ => match key_rule(name) {
            Some(KeyRule::Range {
                first: 1,
                last: 1,
                step: 1,
            })
            | None => Some(key),
            Some(_) => None,
        },
    }
}

fn listed(list: &[&str], name: &str) -> bool {
    list.contains(&name)
}

/// The keys of a command, in argument order. Empty for keyless commands.
pub(crate) fn command_keys<'a>(name: &str, arguments: &[&'a str]) -> Vec<&'a str> {
    if arguments.len() < 2 {
        return Vec::new();
    }

    match name {
        // Sharded Pub/Sub has no keys in the command table, but routes by channel.
        "SPUBLISH" => vec![arguments[1]],
        "SSUBSCRIBE" | "SUNSUBSCRIBE" => arguments[1..].to_vec(),
        _ => match key_rule(name) {
            Some(rule) => rule_keys(rule, arguments),
            // A command the table does not know is routed by its first argument.
            None => vec![arguments[1]],
        },
    }
}

/// The command table's key rule for an upper-case command name.
fn key_rule(name: &str) -> Option<KeyRule> {
    COMMAND_KEY_RULES
        .binary_search_by(|(entry, ..)| (*entry).cmp(name))
        .ok()
        .map(|index| COMMAND_KEY_RULES[index].1)
}

fn keyless(name: &str) -> bool {
    key_rule(name) == Some(KeyRule::None)
}

fn rule_keys<'a>(rule: KeyRule, arguments: &[&'a str]) -> Vec<&'a str> {
    let len = arguments.len();

    match rule {
        KeyRule::None => Vec::new(),
        KeyRule::Range { first, last, step } => {
            // A negative last counts from the end: -1 is the last argument.
            let last = if last < 0 {
                len as isize + isize::from(last)
            } else {
                isize::from(last)
            };
            let end = (last + 1).clamp(0, len as isize) as usize;

            arguments
                .get(first as usize..end)
                .unwrap_or(&[])
                .iter()
                .step_by(step as usize)
                .copied()
                .collect()
        }
        // The count promises more keys than there are: take what is there. A count that is not
        // a number means no counted keys.
        KeyRule::NumKeys { count_at, before } => {
            let count_at = usize::from(count_at);
            let count = arguments
                .get(count_at)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let mut keys = Vec::new();

            if before > 0 {
                keys.extend(arguments.get(usize::from(before)).copied());
            }

            keys.extend(arguments.iter().skip(count_at + 1).take(count).copied());

            keys
        }
        KeyRule::KeywordHalf { keyword } => {
            let Some(index) = arguments[1..]
                .iter()
                .position(|argument| argument.eq_ignore_ascii_case(keyword))
            else {
                return Vec::new();
            };
            let rest = &arguments[index + 2..];

            rest[..rest.len() / 2].to_vec()
        }
        KeyRule::KeyAndStore { first, keywords } => {
            let first = usize::from(first);
            let mut keys: Vec<&str> = arguments.get(first).copied().into_iter().collect();
            let mut index = first + 1;

            while index + 1 < len {
                if keywords
                    .iter()
                    .any(|keyword| arguments[index].eq_ignore_ascii_case(keyword))
                {
                    keys.push(arguments[index + 1]);
                    index += 1;
                }

                index += 1;
            }

            keys
        }
    }
}

pub(crate) struct Connection {
    stream: BufReader<TcpStream>,
    scratch: Vec<u8>,
    host: String,
    port: u16,
    broken: bool,
    /// The socket's read timeout between commands (`ClientOptions::command_timeout` or
    /// `set_read_timeout`); a blocking command widens it for its own reply.
    timeout: Option<Duration>,
}

impl Connection {
    pub(crate) fn open(host: &str, port: u16, options: &ClientOptions) -> Result<Self, Error> {
        let mut last_error = None;

        for address in (host, port).to_socket_addrs()? {
            match TcpStream::connect_timeout(&address, options.connect_timeout) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;

                    if options.command_timeout.is_some() {
                        stream.set_read_timeout(options.command_timeout)?;
                    }

                    let mut connection = Self {
                        stream: BufReader::new(stream),
                        scratch: Vec::with_capacity(256),
                        host: host.to_owned(),
                        port,
                        broken: false,
                        timeout: options.command_timeout,
                    };

                    if let Some(password) = options.password.clone() {
                        let reply = if let Some(username) = options.username.clone() {
                            connection.send(&["AUTH", &username, &password])?
                        } else {
                            connection.send(&["AUTH", &password])?
                        };

                        throw_error(reply)?;
                    }

                    if let Some(durability) = options.durability {
                        let replicas = durability.replicas.to_string();
                        let reply = match durability.timeout_ms {
                            Some(timeout) => connection.send(&[
                                "CLIENT",
                                "DURABILITY",
                                "REPLICAS",
                                &replicas,
                                "TIMEOUT",
                                &timeout.to_string(),
                            ])?,
                            None => {
                                connection.send(&["CLIENT", "DURABILITY", "REPLICAS", &replicas])?
                            }
                        };

                        throw_error(reply)?;
                    }

                    return Ok(connection);
                }

                Err(error) => last_error = Some(error),
            }
        }

        Err(Error::Io(last_error.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "no addresses for host")
        })))
    }

    pub(crate) fn send(&mut self, arguments: &[&str]) -> Result<RespValue, Error> {
        if self.timeout.is_some() {
            let wait = blocking_wait(arguments);

            if wait != Wait::None {
                return self.waiting(wait, |connection| {
                    connection.write_commands(&[arguments])?;
                    connection.read_reply()
                });
            }
        }

        self.write_commands(&[arguments])?;

        self.read_reply()
    }

    /// `send`, preceded by `ASKING` in the same write when `asking` is set. A failed `ASKING`
    /// reply is returned instead of the command's.
    pub(crate) fn send_with(
        &mut self,
        arguments: &[&str],
        asking: bool,
    ) -> Result<RespValue, Error> {
        if !asking {
            return self.send(arguments);
        }

        self.asked(
            self.timeout.is_some().then(|| blocking_wait(arguments)),
            |connection| connection.write_commands(&[&["ASKING"], arguments]),
        )
    }

    pub(crate) fn send_owned_with(
        &mut self,
        arguments: &[String],
        asking: bool,
    ) -> Result<RespValue, Error> {
        let borrowed: Vec<&str> = arguments.iter().map(String::as_str).collect();

        self.send_with(&borrowed, asking)
    }

    /// Binary-safe `send_with`.
    pub(crate) fn send_bytes_with(
        &mut self,
        arguments: &[&[u8]],
        asking: bool,
    ) -> Result<RespValue, Error> {
        if !asking {
            return self.send_bytes(arguments);
        }

        self.asked(
            self.timeout.is_some().then(|| blocking_wait(arguments)),
            |connection| connection.write_bytes_commands(&[&[b"ASKING"], arguments]),
        )
    }

    /// Writes `ASKING` and a command with `write`, reads both replies and returns the command's,
    /// or the `ASKING` error. Only the `ASK` path runs this.
    #[cold]
    fn asked(
        &mut self,
        wait: Option<Wait>,
        write: impl FnOnce(&mut Self) -> Result<(), Error>,
    ) -> Result<RespValue, Error> {
        let wait = wait.unwrap_or(Wait::None);

        self.widen(wait)?;

        let result = write(self).and_then(|()| {
            let asking = self.read_reply()?;
            let reply = self.read_reply()?;

            Ok(if matches!(asking, RespValue::Error(_)) {
                asking
            } else {
                reply
            })
        });

        self.narrow();

        result
    }

    fn write_bytes_commands(&mut self, commands: &[&[&[u8]]]) -> Result<(), Error> {
        if self.broken {
            return Err(closed_socket());
        }

        let mut scratch = std::mem::take(&mut self.scratch);

        scratch.clear();

        let encoded = commands
            .iter()
            .try_for_each(|command| crate::codec::append_bytes(&mut scratch, command));
        let write = encoded.and_then(|()| {
            let stream = self.stream.get_mut();

            stream
                .write_all(&scratch)
                .and_then(|()| stream.flush())
                .map_err(|error| {
                    self.broken = true;

                    Error::Io(error)
                })
        });

        self.scratch = scratch;

        write
    }

    /// Runs `run` with the read timeout widened by a blocking command's own wait, then puts
    /// the command timeout back.
    #[cold]
    fn waiting<T>(
        &mut self,
        wait: Wait,
        run: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.widen(wait)?;

        let result = run(self);

        self.narrow();

        result
    }

    /// Sets the read timeout a pipeline holding `wait` needs; pair it with `narrow`.
    pub(crate) fn widen(&mut self, wait: Wait) -> Result<(), Error> {
        if self.timeout.is_some() && wait != Wait::None {
            self.stream
                .get_ref()
                .set_read_timeout(wait.deadline(self.timeout))?;
        }

        Ok(())
    }

    /// Restores the command timeout after `widen`.
    pub(crate) fn narrow(&mut self) {
        if self.timeout.is_some()
            && self
                .stream
                .get_ref()
                .set_read_timeout(self.timeout)
                .is_err()
        {
            self.broken = true;
        }
    }

    /// Binary-safe single command.
    pub(crate) fn send_bytes(&mut self, arguments: &[&[u8]]) -> Result<RespValue, Error> {
        if self.timeout.is_some() {
            let wait = blocking_wait(arguments);

            if wait != Wait::None {
                return self.waiting(wait, |connection| connection.send_bytes_now(arguments));
            }
        }

        self.send_bytes_now(arguments)
    }

    fn send_bytes_now(&mut self, arguments: &[&[u8]]) -> Result<RespValue, Error> {
        if self.broken {
            return Err(closed_socket());
        }

        let mut scratch = std::mem::take(&mut self.scratch);

        scratch.clear();

        let encoded = crate::codec::append_bytes(&mut scratch, arguments);
        let write = encoded.and_then(|()| {
            let stream = self.stream.get_mut();

            stream
                .write_all(&scratch)
                .and_then(|()| stream.flush())
                .map_err(|error| {
                    self.broken = true;

                    Error::Io(error)
                })
        });

        self.scratch = scratch;
        write?;

        self.read_reply()
    }

    pub(crate) fn send_owned(&mut self, arguments: &[String]) -> Result<RespValue, Error> {
        let borrowed: Vec<&str> = arguments.iter().map(String::as_str).collect();

        self.send(&borrowed)
    }

    pub(crate) fn send_replies(
        &mut self,
        arguments: &[&str],
        replies: usize,
    ) -> Result<Vec<RespValue>, Error> {
        self.write_commands(&[arguments])?;

        let mut values = Vec::with_capacity(replies);

        for _ in 0..replies {
            values.push(self.read_reply()?);
        }

        Ok(values)
    }

    pub(crate) fn send_many(&mut self, commands: &[&[&str]]) -> Result<Vec<RespValue>, Error> {
        if self.timeout.is_some() {
            let wait = pipeline_wait(commands);

            if wait != Wait::None {
                return self.waiting(wait, |connection| connection.send_many_now(commands));
            }
        }

        self.send_many_now(commands)
    }

    fn send_many_now(&mut self, commands: &[&[&str]]) -> Result<Vec<RespValue>, Error> {
        self.write_commands(commands)?;

        let mut values = Vec::with_capacity(commands.len());

        for _ in 0..commands.len() {
            values.push(self.read_reply()?);
        }

        Ok(values)
    }

    pub(crate) fn send_many_owned(
        &mut self,
        commands: &[Vec<String>],
    ) -> Result<Vec<RespValue>, Error> {
        let borrowed: Vec<Vec<&str>> = commands
            .iter()
            .map(|command| command.iter().map(String::as_str).collect())
            .collect();
        let refs: Vec<&[&str]> = borrowed.iter().map(Vec::as_slice).collect();

        self.send_many(&refs)
    }

    pub(crate) fn is_broken(&self) -> bool {
        self.broken
    }

    fn write_commands(&mut self, commands: &[&[&str]]) -> Result<(), Error> {
        self.write_selected(commands, |_| true)
    }

    /// Encodes the commands `pick` accepts, in order, and writes them in one go. A pipeline
    /// spread over nodes calls this once per node with the same command list.
    pub(crate) fn write_selected(
        &mut self,
        commands: &[&[&str]],
        mut pick: impl FnMut(usize) -> bool,
    ) -> Result<(), Error> {
        if self.broken {
            return Err(closed_socket());
        }

        let mut scratch = std::mem::take(&mut self.scratch);

        scratch.clear();

        for (index, command) in commands.iter().enumerate() {
            if !pick(index) {
                continue;
            }

            if let Err(error) = crate::codec::append_strings(&mut scratch, command) {
                self.scratch = scratch;

                return Err(error);
            }
        }

        let write = self.stream.get_mut().write_all(&scratch);

        self.scratch = scratch;

        if let Err(error) = write {
            self.broken = true;

            return Err(Error::Io(error));
        }

        if let Err(error) = self.stream.get_mut().flush() {
            self.broken = true;

            return Err(Error::Io(error));
        }

        Ok(())
    }

    /// Reads the reply to a command that was just written. A read timeout here leaves that reply
    /// on its way, so the socket is out of step and must be opened again; only subscriber reads
    /// (`read`) may time out and carry on.
    fn read_reply(&mut self) -> Result<RespValue, Error> {
        let reply = self.read();

        if matches!(reply, Err(Error::Io(_))) {
            self.broken = true;
        }

        reply
    }

    pub(crate) fn read(&mut self) -> Result<RespValue, Error> {
        if self.broken {
            return Err(closed_socket());
        }

        // A timeout while nothing has arrived is a harmless idle wait. Once the first bytes of a reply
        // are in, a timeout leaves a half-read frame behind and the stream can no longer be trusted.
        match self.stream.fill_buf() {
            Err(error) if idle_wait(&error) => return Err(Error::Io(error)),
            Err(error) => {
                self.broken = true;

                return Err(Error::Io(error));
            }
            Ok(_) => {}
        }

        match read_value(&mut self.stream) {
            Err(Error::Io(error)) => {
                self.broken = true;

                Err(Error::Io(error))
            }

            other => other,
        }
    }

    pub(crate) fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        self.stream.get_ref().set_read_timeout(timeout)?;
        self.timeout = timeout;

        Ok(())
    }
}

fn idle_wait(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

pub(crate) fn throw_error(reply: RespValue) -> Result<RespValue, Error> {
    match reply {
        RespValue::Error(message) => Err(Error::Server(message)),
        value => Ok(value),
    }
}

pub(crate) enum Discovery {
    Standalone(Connection),
    Cluster(Box<ClusterRouter>),
}

#[derive(Clone)]
struct Topology {
    owners: Vec<Option<usize>>,
    nodes: Vec<(String, u16)>,
    /// Per node of `nodes`: the replicas `CLUSTER SLOTS` lists after it, by address.
    replicas: Vec<Vec<(String, u16)>>,
}

impl Topology {
    fn parse(reply: &RespValue) -> Option<Self> {
        let RespValue::Array(ranges) = reply else {
            return None;
        };

        if ranges.is_empty() {
            return None;
        }

        let mut owners = vec![None; SLOT_COUNT as usize];
        let mut nodes = Vec::new();
        let mut replicas: Vec<Vec<(String, u16)>> = Vec::new();
        let mut ordered = ranges.clone();

        ordered.sort_by_key(|range| match range {
            RespValue::Array(fields) => fields.first().and_then(RespValue::as_integer).unwrap_or(0),
            _ => 0,
        });

        let mut next = 0;
        for range in &ordered {
            let RespValue::Array(fields) = range else {
                return None;
            };

            if fields.len() < 3 {
                return None;
            }

            let RespValue::Array(primary) = &fields[2] else {
                return None;
            };

            if primary.len() < 2 {
                return None;
            }

            let host = primary[0].as_string().ok().flatten()?;
            let port = u16::try_from(primary[1].as_integer()?).ok()?;
            let node = match nodes
                .iter()
                .position(|(existing, existing_port)| existing == &host && *existing_port == port)
            {
                Some(index) => index,
                None => {
                    nodes.push((host, port));
                    replicas.push(Vec::new());
                    nodes.len() - 1
                }
            };

            // Replicas follow the primary; one that cannot be read is skipped, not fatal.
            for replica in &fields[3..] {
                let RespValue::Array(replica) = replica else {
                    continue;
                };
                let (Some(Ok(Some(host))), Some(port)) = (
                    replica.first().map(RespValue::as_string),
                    replica
                        .get(1)
                        .and_then(RespValue::as_integer)
                        .and_then(|port| u16::try_from(port).ok()),
                ) else {
                    continue;
                };

                if !replicas[node].contains(&(host.clone(), port)) {
                    replicas[node].push((host, port));
                }
            }

            let first = usize::try_from(fields[0].as_integer()?).ok()?;
            let last = usize::try_from(fields[1].as_integer()?).ok()?;
            if first != next || last < first || last >= SLOT_COUNT as usize {
                return None;
            }
            owners[first..=last].fill(Some(node));
            next = last + 1;
        }

        (next == SLOT_COUNT as usize).then_some(Self {
            owners,
            nodes,
            replicas,
        })
    }

    fn node_index(&self, host: &str, port: u16) -> Option<usize> {
        self.nodes
            .iter()
            .position(|(known_host, known_port)| known_host == host && *known_port == port)
    }

    fn owner(&self, slot: u16) -> Option<&(String, u16)> {
        self.owners
            .get(usize::from(slot))
            .and_then(|node| node.and_then(|index| self.nodes.get(index)))
    }
}

/// A node the client knows. Its index in [`ClusterRouter::endpoints`] never changes, so the slot
/// routes and the pinned or subscribed node can hold the index instead of a `host:port` string.
struct Endpoint {
    host: String,
    port: u16,
    connection: Option<Connection>,
    /// A replica chosen for reads: `READONLY` is sent each time its socket is opened.
    replica: bool,
}

pub(crate) struct ClusterRouter {
    options: ClientOptions,
    /// The configured seed addresses, asked for the slot map when no known node answers.
    seeds: Vec<(String, u16)>,
    seed: usize,
    endpoints: Vec<Endpoint>,
    topology: Topology,
    /// The endpoint of each topology node, rebuilt with the topology.
    routes: Vec<usize>,
    /// Per topology node, the replica endpoint its reads go to; `None` when it has none or
    /// the chosen one could not be used. Empty with [`ReadPolicy::Primary`].
    read_routes: Vec<Option<usize>>,
    /// Pipeline scratch kept between calls: each command's endpoint, and the endpoints in use.
    targets: Vec<usize>,
    used: Vec<usize>,
    queued: Vec<Vec<String>>,
    reconnect_delay: Option<Arc<dyn Fn(u32) -> Option<Duration> + Send + Sync>>,
    on_lost: NoticeHook,
    on_restored: NoticeHook,
    pinned: Option<usize>,
    multi_pending: bool,
    in_multi: bool,
    subscriber: Option<usize>,
}

impl ClusterRouter {
    pub(crate) fn discover(
        mut seed: Connection,
        options: ClientOptions,
        seeds: Vec<(String, u16)>,
    ) -> Result<Discovery, Error> {
        let reply = seed.send(&["CLUSTER", "SLOTS"])?;
        let Some(topology) = Topology::parse(&reply) else {
            if matches!(reply, RespValue::Array(_)) {
                return Err(Error::Protocol("invalid CLUSTER SLOTS topology".to_owned()));
            }
            return Ok(Discovery::Standalone(seed));
        };

        Ok(Discovery::Cluster(Box::new(Self::new(
            seed, topology, options, seeds,
        ))))
    }

    fn new(
        seed: Connection,
        topology: Topology,
        options: ClientOptions,
        seeds: Vec<(String, u16)>,
    ) -> Self {
        let mut router = Self {
            options,
            seeds,
            seed: 0,
            endpoints: vec![Endpoint {
                host: seed.host.clone(),
                port: seed.port,
                connection: Some(seed),
                replica: false,
            }],
            topology: Topology {
                owners: Vec::new(),
                nodes: Vec::new(),
                replicas: Vec::new(),
            },
            routes: Vec::new(),
            read_routes: Vec::new(),
            targets: Vec::new(),
            used: Vec::new(),
            queued: Vec::new(),
            reconnect_delay: None,
            on_lost: Arc::new(Mutex::new(None)),
            on_restored: Arc::new(Mutex::new(None)),
            pinned: None,
            multi_pending: false,
            in_multi: false,
            subscriber: None,
        };

        router.install(topology);

        router
    }

    /// Makes `topology` current and points each of its nodes at an endpoint.
    fn install(&mut self, topology: Topology) {
        self.routes.clear();
        self.read_routes.clear();

        for (host, port) in &topology.nodes {
            let endpoint = self.endpoint_index(host, *port);

            self.routes.push(endpoint);
        }

        // Each shard's reads go to one of its replicas, picked at random per client so many
        // clients spread over the replicas, and kept until the next reload.
        if self.options.read_policy != ReadPolicy::Primary {
            let pick = random_index();

            for replicas in &topology.replicas {
                let read = (!replicas.is_empty()).then(|| {
                    let (host, port) = &replicas[pick % replicas.len()];
                    let endpoint = self.endpoint_index(host, *port);

                    self.endpoints[endpoint].replica = true;
                    endpoint
                });

                self.read_routes.push(read);
            }
        }

        self.topology = topology;
    }

    /// The endpoint for a node, added (without a connection) the first time it is named.
    fn endpoint_index(&mut self, host: &str, port: u16) -> usize {
        if let Some(index) = self
            .endpoints
            .iter()
            .position(|endpoint| endpoint.host == host && endpoint.port == port)
        {
            return index;
        }

        self.endpoints.push(Endpoint {
            host: host.to_owned(),
            port,
            connection: None,
            replica: false,
        });

        self.endpoints.len() - 1
    }

    pub(crate) fn node_count(&self) -> usize {
        self.topology.nodes.len()
    }

    pub(crate) fn set_reconnect_delay(
        &mut self,
        delay: Arc<dyn Fn(u32) -> Option<Duration> + Send + Sync>,
    ) {
        self.reconnect_delay = Some(delay);
    }

    pub(crate) fn share_hooks(&mut self, on_lost: NoticeHook, on_restored: NoticeHook) {
        self.on_lost = on_lost;
        self.on_restored = on_restored;
    }

    pub(crate) fn reconnect_broken(&mut self) -> Result<(), Error> {
        for node in 0..self.routes.len() {
            self.open(self.routes[node], true)?;
        }

        Ok(())
    }

    pub(crate) fn subscription_endpoint(&self, arguments: &[&str]) -> (String, u16) {
        let name = CommandName::of(arguments);
        let slot = if matches!(name.as_str(), "SSUBSCRIBE" | "SUNSUBSCRIBE") && arguments.len() > 1
        {
            hash_slot(arguments[1])
        } else {
            0
        };

        if let Some((host, port)) = self.topology.owner(slot).cloned() {
            return (host, port);
        }

        let seed = &self.endpoints[self.seed];

        (seed.host.clone(), seed.port)
    }

    pub(crate) fn execute(&mut self, arguments: &[&str]) -> Result<RespValue, Error> {
        let name = CommandName::of(arguments);
        let name = name.as_str();

        if self.pinned.is_some() || self.multi_pending {
            return self.transaction(name, arguments);
        }

        match name {
            "MULTI" => {
                self.multi_pending = true;

                return Ok(RespValue::Simple("OK".to_owned()));
            }

            "WATCH" => return self.watch(arguments),
            "DBSIZE" | "FLUSHDB" | "FLUSHALL" => return self.every_node(name, arguments),
            "SCRIPT" | "FUNCTION" if spreads_to_every_node(arguments) => {
                return self.every_node(name, arguments);
            }

            "SCAN" => return self.scan(arguments),
            "PUBLISH" => {
                let target = self.for_slot(0)?;

                return self.send_routed(target, arguments);
            }

            _ => {}
        }

        let target = match single_key(name, arguments) {
            Some(key) => self.slot_target(name, hash_slot(key))?,
            None => {
                let keys = command_keys(name, arguments);

                match slots_of(&keys) {
                    Slots::None => self.seed_target()?,
                    Slots::One(slot) => self.slot_target(name, slot)?,
                    Slots::Many if listed(SPLIT, name) => {
                        return self.split(name, arguments, &keys);
                    }
                    Slots::Many => return Ok(cross_slot()),
                }
            }
        };

        let reply = self.send_routed(target, arguments)?;

        if self.options.read_policy != ReadPolicy::Primary
            && self.endpoints[target].replica
            && loading(&reply)
        {
            return self.read_again_from_primary(target, reply, |connection, asking| {
                connection.send_with(arguments, asking)
            });
        }

        Ok(reply)
    }

    /// The endpoint for a command on `slot`: a replica for a read under a replica read policy,
    /// the slot's primary otherwise.
    fn slot_target(&mut self, name: &str, slot: u16) -> Result<usize, Error> {
        if self.options.read_policy != ReadPolicy::Primary && reads(name) {
            return self.read_target(slot);
        }

        self.for_slot(slot)
    }

    /// The replica `slot`'s reads go to, opened; the primary when it has none or it cannot be
    /// opened under [`ReadPolicy::PreferReplica`], an error under [`ReadPolicy::Replica`].
    fn read_target(&mut self, slot: u16) -> Result<usize, Error> {
        let node = self
            .topology
            .owners
            .get(usize::from(slot))
            .copied()
            .flatten();
        let replica = node.and_then(|node| self.read_routes.get(node).copied().flatten());

        if let (Some(node), Some(endpoint)) = (node, replica) {
            match self.open(endpoint, false) {
                Ok(()) => return Ok(endpoint),
                Err(error) => {
                    self.read_routes[node] = None;

                    if self.options.read_policy == ReadPolicy::Replica {
                        return Err(error);
                    }
                }
            }
        } else if self.options.read_policy == ReadPolicy::Replica {
            return Err(Error::Protocol(format!(
                "no replica is known for slot {slot} (read policy Replica)"
            )));
        }

        self.for_slot(slot)
    }

    /// A replica answered `LOADING`: it is still copying its primary. It is skipped until the
    /// next reload, and the read goes to the primary (or the reply is returned under
    /// [`ReadPolicy::Replica`]).
    fn read_again_from_primary(
        &mut self,
        replica: usize,
        reply: RespValue,
        mut send: impl FnMut(&mut Connection, bool) -> Result<RespValue, Error>,
    ) -> Result<RespValue, Error> {
        for read in &mut self.read_routes {
            if *read == Some(replica) {
                *read = None;
            }
        }

        if self.options.read_policy == ReadPolicy::Replica {
            return Ok(reply);
        }

        let primary = self
            .routes
            .iter()
            .zip(&self.topology.replicas)
            .find(|(_, replicas)| {
                let endpoint = &self.endpoints[replica];

                replicas.contains(&(endpoint.host.clone(), endpoint.port))
            })
            .map(|(primary, _)| *primary);
        let Some(primary) = primary else {
            return Ok(reply);
        };

        self.open(primary, false)?;
        let reply = send(self.connection(primary)?, false)?;

        self.follow_moved(primary, reply, send)
    }

    /// Binary-safe command. Routed by its second argument (the key) unless the command is
    /// keyless; multi-key splitting and transactions are not applied on this path.
    pub(crate) fn execute_binary(&mut self, arguments: &[&[u8]]) -> Result<RespValue, Error> {
        let name = CommandName::from_bytes(arguments.first().copied().unwrap_or_default());
        let current = match arguments.get(1) {
            Some(key) if !keyless(name.as_str()) => {
                self.slot_target(name.as_str(), hash_slot_bytes(key))?
            }
            _ => self.seed_target()?,
        };

        let reply = self.connection(current)?.send_bytes(arguments)?;
        let reply = self.follow_moved(current, reply, |connection, asking| {
            connection.send_bytes_with(arguments, asking)
        })?;

        if self.options.read_policy != ReadPolicy::Primary
            && self.endpoints[current].replica
            && loading(&reply)
        {
            return self.read_again_from_primary(current, reply, |connection, asking| {
                connection.send_bytes_with(arguments, asking)
            });
        }

        Ok(reply)
    }

    pub(crate) fn execute_many(&mut self, commands: &[&[&str]]) -> Result<Vec<RespValue>, Error> {
        if self.pinned.is_some()
            || self.multi_pending
            || commands
                .iter()
                .any(|command| listed(TRANSACTION, CommandName::of(command).as_str()))
        {
            let target = self.first_keyed_target(commands)?;
            let owned: Vec<Vec<String>> = commands
                .iter()
                .map(|command| command.iter().map(|item| (*item).to_owned()).collect())
                .collect();

            return self.connection(target)?.send_many_owned(&owned);
        }

        let mut targets = std::mem::take(&mut self.targets);
        let mut used = std::mem::take(&mut self.used);
        let routed = self.route_pipeline(commands, &mut targets, &mut used);
        let result = match routed {
            Ok(true) => self.send_routed_pipeline(commands, &targets, &used),
            Ok(false) => self.execute_many_split(commands),
            Err(error) => Err(error),
        };

        self.targets = targets;
        self.used = used;

        result
    }

    /// Stores each command's endpoint in `targets` and the distinct endpoints in `used`. `false`
    /// when a command's keys span slots, which the split path handles.
    fn route_pipeline(
        &mut self,
        commands: &[&[&str]],
        targets: &mut Vec<usize>,
        used: &mut Vec<usize>,
    ) -> Result<bool, Error> {
        targets.clear();
        used.clear();

        // A pipeline goes to replicas only when every command in it is a read, so it never
        // reads its own writes from a replica that has not received them.
        let replica_reads = self.options.read_policy != ReadPolicy::Primary
            && commands
                .iter()
                .all(|command| reads(CommandName::of(command).as_str()));

        for command in commands {
            let name = CommandName::of(command);
            let name = name.as_str();
            let target = match single_key(name, command) {
                Some(key) if replica_reads => self.read_target(hash_slot(key))?,
                Some(key) => self.for_slot(hash_slot(key))?,
                None => match slots_of(&command_keys(name, command)) {
                    Slots::None => self.seed_target()?,
                    Slots::One(slot) if replica_reads => self.read_target(slot)?,
                    Slots::One(slot) => self.for_slot(slot)?,
                    Slots::Many => return Ok(false),
                },
            };

            targets.push(target);

            if !used.contains(&target) {
                used.push(target);
            }
        }

        Ok(true)
    }

    /// Sends a routed pipeline: one node takes the plain pipeline path; several nodes are all
    /// written before any is read, so the pipeline costs one round trip, not one per node.
    fn send_routed_pipeline(
        &mut self,
        commands: &[&[&str]],
        targets: &[usize],
        used: &[usize],
    ) -> Result<Vec<RespValue>, Error> {
        let mut replies = if let [only] = used {
            self.connection(*only)?.send_many(commands)?
        } else {
            self.send_spread(commands, targets, used)?
        };

        for (index, reply) in replies.iter_mut().enumerate() {
            if matches!(reply, RespValue::Error(_)) {
                let first = std::mem::replace(reply, RespValue::Null);

                *reply = self.follow_moved(targets[index], first, |connection, asking| {
                    connection.send_with(commands[index], asking)
                })?;
            }
        }

        Ok(replies)
    }

    fn send_spread(
        &mut self,
        commands: &[&[&str]],
        targets: &[usize],
        used: &[usize],
    ) -> Result<Vec<RespValue>, Error> {
        for (written, &endpoint) in used.iter().enumerate() {
            let write = self.connection(endpoint).and_then(|connection| {
                connection.write_selected(commands, |index| targets[index] == endpoint)
            });

            if let Err(error) = write {
                // The nodes already written have replies on their way; read them so those
                // sockets stay in step for the next command.
                self.drain(targets, &used[..written]);

                return Err(error);
            }
        }

        let mut replies = vec![RespValue::Null; commands.len()];
        let wait = if self.options.command_timeout.is_some() {
            pipeline_wait(commands)
        } else {
            Wait::None
        };

        for (read, &endpoint) in used.iter().enumerate() {
            let connection = self.connection(endpoint)?;

            connection.widen(wait)?;

            for (index, target) in targets.iter().enumerate() {
                if *target != endpoint {
                    continue;
                }

                match connection.read_reply() {
                    Ok(reply) => replies[index] = reply,
                    Err(error) => {
                        connection.narrow();
                        self.drain(targets, &used[read + 1..]);

                        return Err(error);
                    }
                }
            }

            connection.narrow();
        }

        Ok(replies)
    }

    /// Reads and drops the pending replies of `endpoints`, stopping at a node whose socket fails.
    fn drain(&mut self, targets: &[usize], endpoints: &[usize]) {
        for &endpoint in endpoints {
            let Ok(connection) = self.connection(endpoint) else {
                continue;
            };

            for _ in targets.iter().filter(|target| **target == endpoint) {
                if connection.read_reply().is_err() {
                    break;
                }
            }
        }
    }

    /// A pipeline with a command whose keys span slots: split commands are sent per slot inside
    /// the node groups and their replies merged at the command's index.
    fn execute_many_split(&mut self, commands: &[&[&str]]) -> Result<Vec<RespValue>, Error> {
        // Per node: where each reply goes, and the commands in pipeline order.
        let mut groups: HashMap<usize, (Vec<Destination>, Vec<Vec<String>>)> = HashMap::new();
        let mut splits: Vec<PendingSplit> = Vec::new();
        let mut replies = vec![RespValue::Null; commands.len()];

        for (index, command) in commands.iter().enumerate() {
            let name = command_name(command);
            let keys = command_keys(&name, command);
            let target = match slots_of(&keys) {
                Slots::None => self.seed_target()?,
                Slots::One(slot) => self.for_slot(slot)?,
                Slots::Many if listed(SPLIT, &name) => {
                    match split_parts(&name, command, &keys) {
                        Ok(Split { parts, positions }) => {
                            for (part, (slot, part_command)) in parts.into_iter().enumerate() {
                                let group = groups.entry(self.for_slot(slot)?).or_default();

                                group.0.push(Destination::Part {
                                    split: splits.len(),
                                    part,
                                });
                                group.1.push(part_command);
                            }

                            splits.push(PendingSplit {
                                index,
                                name,
                                key_count: keys.len(),
                                replies: vec![RespValue::Null; positions.len()],
                                positions,
                            });
                        }
                        Err(reply) => replies[index] = reply,
                    }

                    continue;
                }
                Slots::Many => {
                    replies[index] = cross_slot();

                    continue;
                }
            };
            let group = groups.entry(target).or_default();

            group.0.push(Destination::Whole(index));
            group
                .1
                .push(command.iter().map(|item| (*item).to_owned()).collect());
        }

        for (target, (destinations, batch)) in groups {
            let values = self.connection(target)?.send_many_owned(&batch)?;

            for ((destination, command), value) in destinations.into_iter().zip(&batch).zip(values)
            {
                let value = self.follow_moved(target, value, |connection, asking| {
                    connection.send_owned_with(command, asking)
                })?;

                match destination {
                    Destination::Whole(index) => replies[index] = value,
                    Destination::Part { split, part } => splits[split].replies[part] = value,
                }
            }
        }

        for split in splits {
            replies[split.index] = merge(
                &split.name,
                split.key_count,
                &split.positions,
                split.replies,
            );
        }

        Ok(replies)
    }

    pub(crate) fn run_replies(
        &mut self,
        arguments: &[&str],
        replies: usize,
    ) -> Result<Vec<RespValue>, Error> {
        let name = CommandName::of(arguments);
        let slot = if matches!(name.as_str(), "SSUBSCRIBE" | "SUNSUBSCRIBE") && arguments.len() > 1
        {
            hash_slot(arguments[1])
        } else {
            0
        };

        let target = self.for_slot(slot)?;

        self.subscriber = Some(target);
        let values = self.connection(target)?.send_replies(arguments, replies)?;

        if values.len() < replies {
            return Err(Error::Protocol("missing subscribe confirmation".to_owned()));
        }

        Ok(values)
    }

    pub(crate) fn read_message(&mut self) -> Result<RespValue, Error> {
        let target = self.subscriber.unwrap_or(self.seed);

        self.connection(target)?.read()
    }

    /// After `MONITOR`: the socket `read_message` reads waits for pushes with no timeout.
    pub(crate) fn monitor_without_timeout(&mut self) -> Result<(), Error> {
        let target = self.subscriber.unwrap_or(self.seed);

        self.connection(target)?.set_read_timeout(None)
    }

    pub(crate) fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        self.options.command_timeout = timeout;

        for endpoint in &mut self.endpoints {
            if let Some(connection) = endpoint.connection.as_mut() {
                connection.set_read_timeout(timeout)?;
            }
        }

        Ok(())
    }

    fn transaction(&mut self, name: &str, arguments: &[&str]) -> Result<RespValue, Error> {
        if let Some(pinned) = self.pinned {
            let reply = self.connection(pinned)?.send(arguments)?;

            if name == "EXEC" || name == "DISCARD" || (name == "UNWATCH" && !self.in_multi) {
                self.pinned = None;
                self.in_multi = false;
            } else if name == "MULTI" && !matches!(reply, RespValue::Error(_)) {
                self.in_multi = true;
            }

            return Ok(reply);
        }

        match name {
            "MULTI" => {
                return Ok(RespValue::Error(
                    "ERR MULTI calls can not be nested".to_owned(),
                ));
            }

            "WATCH" => {
                return Ok(RespValue::Error(
                    "ERR WATCH inside MULTI is not allowed".to_owned(),
                ));
            }

            "DISCARD" => {
                self.reset_transaction();

                return Ok(RespValue::Simple("OK".to_owned()));
            }

            "EXEC" => {
                let target = self.seed;
                let mut commands = vec![vec!["MULTI".to_owned()]];

                commands.extend(self.queued.iter().cloned());
                commands.push(arguments.iter().map(|item| (*item).to_owned()).collect());
                let replies = self.connection(target)?.send_many_owned(&commands)?;

                self.reset_transaction();

                return Ok(replies.into_iter().next_back().unwrap_or(RespValue::Null));
            }

            _ => {}
        }

        let keys = command_keys(name, arguments);

        if keys.is_empty() {
            self.queued
                .push(arguments.iter().map(|item| (*item).to_owned()).collect());

            return Ok(RespValue::Simple("QUEUED".to_owned()));
        }

        let owner = self.for_slot(hash_slot(keys[0]))?;
        let mut commands = vec![vec!["MULTI".to_owned()]];

        commands.extend(self.queued.iter().cloned());
        commands.push(arguments.iter().map(|item| (*item).to_owned()).collect());
        let replies = self.connection(owner)?.send_many_owned(&commands)?;

        self.queued.clear();
        self.multi_pending = false;

        if matches!(replies.first(), Some(RespValue::Error(_))) {
            return Ok(replies.into_iter().next().unwrap());
        }

        self.pinned = Some(owner);
        self.in_multi = true;

        Ok(replies.into_iter().next_back().unwrap_or(RespValue::Null))
    }

    fn reset_transaction(&mut self) {
        self.queued.clear();
        self.multi_pending = false;
        self.in_multi = false;
        self.pinned = None;
    }

    fn watch(&mut self, arguments: &[&str]) -> Result<RespValue, Error> {
        let keys = command_keys("WATCH", arguments);
        let owner = self.target(&keys)?;
        let reply = self.connection(owner)?.send(arguments)?;

        if !matches!(reply, RespValue::Error(_)) {
            self.pinned = Some(owner);
        }

        Ok(reply)
    }

    fn every_node(&mut self, name: &str, arguments: &[&str]) -> Result<RespValue, Error> {
        let mut replies = Vec::new();

        for node in 0..self.routes.len() {
            let endpoint = self.routes[node];

            self.open(endpoint, false)?;

            let reply = self.connection(endpoint)?.send(arguments)?;

            if let RespValue::Error(_) = reply {
                return Ok(reply);
            }

            replies.push(reply);
        }

        if name == "DBSIZE" {
            return Ok(RespValue::Integer(
                replies.iter().filter_map(RespValue::as_integer).sum(),
            ));
        }

        Ok(replies.into_iter().next().unwrap_or(RespValue::Null))
    }

    fn scan(&mut self, arguments: &[&str]) -> Result<RespValue, Error> {
        let nodes = self.topology.nodes.len() as u64;
        let cursor = arguments
            .get(1)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| Error::Protocol("ERR invalid cursor".to_owned()))?;
        let node = (cursor % nodes) as usize;
        let mut forwarded: Vec<String> = arguments.iter().map(|item| (*item).to_owned()).collect();

        forwarded[1] = (cursor / nodes).to_string();
        let endpoint = self.routes[node];

        self.open(endpoint, false)?;

        let reply = self.connection(endpoint)?.send_owned(&forwarded)?;
        let RespValue::Array(items) = &reply else {
            return Ok(reply);
        };

        if items.len() < 2 {
            return Ok(reply);
        }

        let server_next = items[0]
            .as_string()?
            .unwrap_or_else(|| "0".to_owned())
            .parse::<u64>()
            .unwrap_or(0);
        let client_next = if server_next != 0 {
            server_next
                .checked_mul(nodes)
                .and_then(|value| value.checked_add(node as u64))
                .ok_or_else(|| Error::Protocol("scan cursor overflow".to_owned()))?
        } else if (node as u64) + 1 < nodes {
            node as u64 + 1
        } else {
            0
        };

        Ok(RespValue::Array(vec![
            RespValue::Bulk(client_next.to_string().into_bytes()),
            items[1].clone(),
        ]))
    }

    fn split(&mut self, name: &str, arguments: &[&str], keys: &[&str]) -> Result<RespValue, Error> {
        let Split { parts, positions } = match split_parts(name, arguments, keys) {
            Ok(split) => split,
            Err(reply) => return Ok(reply),
        };
        let mut replies = Vec::with_capacity(parts.len());

        for (slot, command) in &parts {
            let target = self.slot_target(name, *slot)?;
            let command = borrowed(command);
            let mut reply = self.send_routed(target, &command)?;

            if self.options.read_policy != ReadPolicy::Primary
                && self.endpoints[target].replica
                && loading(&reply)
            {
                reply = self.read_again_from_primary(target, reply, |connection, asking| {
                    connection.send_with(&command, asking)
                })?;
            }

            // The first error stops the remaining parts, as one command would have failed.
            if matches!(reply, RespValue::Error(_)) {
                return Ok(reply);
            }

            replies.push(reply);
        }

        Ok(merge(name, keys.len(), &positions, replies))
    }

    fn send_routed(&mut self, target: usize, arguments: &[&str]) -> Result<RespValue, Error> {
        let reply = self.connection(target)?.send(arguments)?;

        self.follow_moved(target, reply, |connection, asking| {
            connection.send_with(arguments, asking)
        })
    }

    /// Follows the `MOVED`, `ASK` and `TRYAGAIN` replies of a reply already read for one command,
    /// sending it again with `send` (its flag asks for `ASKING` to go first). `owner` is the
    /// endpoint the command was routed to. `MOVED` and `ASK` share one budget of `MAX_REDIRECTS`
    /// so a slot map that keeps changing cannot loop forever: the next redirect is returned. An
    /// `ASK` is answered by the named node and leaves the slot map alone. `TRYAGAIN` sends the
    /// command to the slot's current owner again after a short sleep, at most
    /// `TRYAGAIN_BACKOFF.len()` times; the last `TRYAGAIN` is returned.
    fn follow_moved(
        &mut self,
        mut owner: usize,
        mut reply: RespValue,
        mut send: impl FnMut(&mut Connection, bool) -> Result<RespValue, Error>,
    ) -> Result<RespValue, Error> {
        let mut redirects = 0;
        let mut moves = 0;
        let mut retries = 0;

        loop {
            let RespValue::Error(text) = &reply else {
                return Ok(reply);
            };

            if text.starts_with("TRYAGAIN") {
                let Some(pause) = TRYAGAIN_BACKOFF.get(retries) else {
                    return Ok(reply);
                };

                retries += 1;
                std::thread::sleep(*pause);
                reply = send(self.connection(owner)?, false)?;

                continue;
            }

            if redirects == MAX_REDIRECTS {
                return Ok(reply);
            }

            if let Some((slot, host, port)) = redirect_target(text, "MOVED ") {
                redirects += 1;
                moves += 1;
                owner = self.redirect(slot, &host, port, moves)?;
                reply = send(self.connection(owner)?, false)?;
            } else if let Some((_, host, port)) = redirect_target(text, "ASK ") {
                redirects += 1;

                let target = self.ask_target(&host, port)?;

                reply = send(self.connection(target)?, true)?;
            } else {
                return Ok(reply);
            }
        }
    }

    /// The endpoint of the node an `ASK` names, opened on demand. The slot map is not touched: the
    /// node is only added to the known nodes when the client has never seen it.
    fn ask_target(&mut self, host: &str, port: u16) -> Result<usize, Error> {
        let node = match self.topology.node_index(host, port) {
            Some(node) => node,
            None => {
                let endpoint = self.endpoint_index(host, port);

                self.topology.nodes.push((host.to_owned(), port));
                self.topology.replicas.push(Vec::new());
                self.routes.push(endpoint);
                if !self.read_routes.is_empty() {
                    self.read_routes.push(None);
                }
                self.topology.nodes.len() - 1
            }
        };
        let endpoint = self.routes[node];

        self.open(endpoint, false)?;

        Ok(endpoint)
    }

    /// Applies one `MOVED` before the command is sent again to the named node. The whole slot map
    /// is reloaded first when that node is unknown to the client, and on the command's second and
    /// later redirects. Either way the slot then points at the named node, even when the reloaded
    /// map names another owner.
    fn redirect(
        &mut self,
        slot: u16,
        host: &str,
        port: u16,
        redirect: usize,
    ) -> Result<usize, Error> {
        let mut node = self.topology.node_index(host, port);

        if node.is_none() || redirect > 1 {
            self.refresh()?;
            node = self.topology.node_index(host, port);
        }

        let node = match node {
            Some(node) => node,
            None => {
                let endpoint = self.endpoint_index(host, port);

                self.topology.nodes.push((host.to_owned(), port));
                self.topology.replicas.push(Vec::new());
                self.routes.push(endpoint);
                if !self.read_routes.is_empty() {
                    self.read_routes.push(None);
                }
                self.topology.nodes.len() - 1
            }
        };

        self.topology.owners[usize::from(slot)] = Some(node);

        let endpoint = self.routes[node];

        self.open(endpoint, false)?;

        Ok(endpoint)
    }

    /// Reloads the slot map. Nodes with an open socket are asked first; when none of them
    /// answers (the usual case after a node went away), every other known node and then every
    /// seed is opened in turn, one connect attempt each, until one returns a slot map.
    fn refresh(&mut self) -> Result<(), Error> {
        let mut last_error = None;

        for index in 0..self.endpoints.len() {
            let Some(connection) = self.endpoints[index].connection.as_mut() else {
                continue;
            };

            if connection.is_broken() {
                continue;
            }

            match Self::ask_slots(connection) {
                Ok(topology) => {
                    self.install(topology);
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }

        let known = self.endpoints.len();

        for (host, port) in self.seeds.clone() {
            self.endpoint_index(&host, port);
        }

        for index in 0..self.endpoints.len() {
            let live = self.endpoints[index]
                .connection
                .as_ref()
                .is_some_and(|connection| !connection.is_broken());

            if live {
                continue;
            }

            let (host, port) = (
                self.endpoints[index].host.clone(),
                self.endpoints[index].port,
            );

            let mut connection = match Connection::open(&host, port, &self.options) {
                Ok(connection) => connection,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };

            let answer = Self::ask_slots(&mut connection);

            self.endpoints[index].connection = Some(connection);

            match answer {
                Ok(topology) => {
                    self.install(topology);
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }

        // Seeds that are not cluster nodes stay out of the endpoint list once they failed.
        if self.endpoints.len() > known {
            let mut index = self.endpoints.len();

            while index > known {
                index -= 1;

                if self.endpoints[index].connection.is_none() {
                    self.endpoints.remove(index);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Protocol("no cluster topology source".to_owned())))
    }

    fn ask_slots(connection: &mut Connection) -> Result<Topology, Error> {
        let reply = connection.send(&["CLUSTER", "SLOTS"])?;

        Topology::parse(&reply)
            .ok_or_else(|| Error::Protocol("invalid CLUSTER SLOTS topology".to_owned()))
    }

    fn target(&mut self, keys: &[&str]) -> Result<usize, Error> {
        if let Some(key) = keys.first() {
            return self.for_slot(hash_slot(key));
        }

        self.seed_target()
    }

    /// The seed node for commands without a key. Goes through `open` so a seed whose socket broke
    /// is opened again (when `reconnect` is on) instead of failing every keyless command for good.
    fn seed_target(&mut self) -> Result<usize, Error> {
        let error = match self.open(self.seed, false) {
            Ok(()) => return Ok(self.seed),
            Err(error) => error,
        };

        // The seed node is gone: reload the map from another known node and use the first node
        // of the new map that has an open socket.
        self.refresh().map_err(|_| error)?;

        for node in 0..self.routes.len() {
            let endpoint = self.routes[node];

            if self.open(endpoint, false).is_ok() {
                self.seed = endpoint;

                return Ok(endpoint);
            }
        }

        Err(closed_socket())
    }

    fn first_keyed_target(&mut self, commands: &[&[&str]]) -> Result<usize, Error> {
        for command in commands {
            let keys = command_keys(&command_name(command), command);

            if let Some(key) = keys.first() {
                return self.for_slot(hash_slot(key));
            }
        }

        self.seed_target()
    }

    /// The endpoint that owns `slot`, with an open connection. A node that cannot be reached
    /// makes the client reload the slot map once and try the owner it names.
    fn for_slot(&mut self, slot: u16) -> Result<usize, Error> {
        let endpoint = self.route(slot)?;

        if self.endpoints[endpoint]
            .connection
            .as_ref()
            .is_some_and(|connection| !connection.is_broken())
        {
            return Ok(endpoint);
        }

        match self.open(endpoint, false) {
            Ok(()) => Ok(endpoint),
            Err(error) => {
                self.refresh().map_err(|_| error)?;

                let endpoint = self.route(slot)?;

                self.open(endpoint, false)?;

                Ok(endpoint)
            }
        }
    }

    fn route(&self, slot: u16) -> Result<usize, Error> {
        self.topology
            .owners
            .get(usize::from(slot))
            .copied()
            .flatten()
            .map(|node| self.routes[node])
            .ok_or_else(|| Error::Protocol(format!("no cluster owner for slot {slot}")))
    }

    /// The endpoint for a node, with an open connection (see [`Self::open`]).
    #[cfg(test)]
    fn open_endpoint(&mut self, host: &str, port: u16, force: bool) -> Result<usize, Error> {
        let endpoint = self.endpoint_index(host, port);

        self.open(endpoint, force)?;

        Ok(endpoint)
    }

    /// Opens the endpoint's connection when it has none or its socket broke. A broken socket is
    /// replaced only when `reconnect` is on or `force` is set (what `Client::reconnect` does).
    fn open(&mut self, endpoint: usize, force: bool) -> Result<(), Error> {
        let broken = self.endpoints[endpoint]
            .connection
            .as_ref()
            .is_some_and(Connection::is_broken);

        if !broken && self.endpoints[endpoint].connection.is_some() {
            return Ok(());
        }

        if broken && !force && !self.options.reconnect {
            return Err(closed_socket());
        }

        let host = self.endpoints[endpoint].host.clone();
        let port = self.endpoints[endpoint].port;

        if broken {
            self.notify_lost(&host, port);
            self.endpoints[endpoint].connection = None;
        }

        let delay = self.reconnect_delay.clone();
        let mut opened = Connection::open_with_retry(&host, port, &self.options, delay.as_ref())?;

        // Redis replicas serve reads only to connections that sent READONLY; Ruvio replicas
        // serve them anyway and answer OK.
        if self.endpoints[endpoint].replica {
            opened.send(&["READONLY"])?;
        }

        self.endpoints[endpoint].connection = Some(opened);

        if broken {
            self.notify_restored(&host, port);
        }

        Ok(())
    }

    fn notify_lost(&self, host: &str, port: u16) {
        let notice = crate::client::ConnectionNotice {
            host: host.to_owned(),
            port,
            subscriber: false,
        };

        if let Some(hook) = self.on_lost.lock().expect("connection hook").as_ref() {
            hook(notice);
        }
    }

    fn notify_restored(&self, host: &str, port: u16) {
        let notice = crate::client::ConnectionNotice {
            host: host.to_owned(),
            port,
            subscriber: false,
        };

        if let Some(hook) = self.on_restored.lock().expect("connection hook").as_ref() {
            hook(notice);
        }
    }

    fn connection(&mut self, endpoint: usize) -> Result<&mut Connection, Error> {
        match self.endpoints.get_mut(endpoint) {
            Some(Endpoint {
                connection: Some(connection),
                ..
            }) => Ok(connection),
            Some(Endpoint { host, port, .. }) => {
                Err(Error::Protocol(format!("no connection for {host}:{port}")))
            }
            None => Err(Error::Protocol(format!(
                "no connection for endpoint {endpoint}"
            ))),
        }
    }
}

/// Server flag bits from the command table (see `scripts/gen_client_keys.py`).
const FLAG_READONLY: u8 = 2;
const FLAG_BLOCKING: u8 = 16;

/// Whether a command only reads and may go to a replica: flagged `readonly`, not blocking.
fn reads(name: &str) -> bool {
    COMMAND_KEY_RULES
        .binary_search_by(|(entry, ..)| (*entry).cmp(name))
        .is_ok_and(|index| {
            let flags = COMMAND_KEY_RULES[index].2;

            flags & FLAG_READONLY != 0 && flags & FLAG_BLOCKING == 0
        })
}

fn loading(reply: &RespValue) -> bool {
    matches!(reply, RespValue::Error(message) if message.starts_with("LOADING") || message.starts_with("MASTERDOWN"))
}

/// A number that differs between clients, to spread their reads over a shard's replicas.
fn random_index() -> usize {
    use std::hash::{BuildHasher, Hasher};

    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish() as usize
}

fn borrowed(command: &[String]) -> Vec<&str> {
    command.iter().map(String::as_str).collect()
}

fn spreads_to_every_node(arguments: &[&str]) -> bool {
    arguments.get(1).is_some_and(|subcommand| {
        matches!(
            subcommand.to_ascii_uppercase().as_str(),
            "LOAD" | "FLUSH" | "DELETE"
        )
    })
}

/// Where the keys of one command hash to.
enum Slots {
    None,
    One(u16),
    Many,
}

/// The slot of the first key, checked against the rest; a single key is hashed once.
fn slots_of(keys: &[&str]) -> Slots {
    let Some((first, rest)) = keys.split_first() else {
        return Slots::None;
    };
    let slot = hash_slot(first);

    if rest.iter().any(|key| hash_slot(key) != slot) {
        Slots::Many
    } else {
        Slots::One(slot)
    }
}

fn cross_slot() -> RespValue {
    RespValue::Error(CROSSSLOT.to_owned())
}

/// Where a pipeline reply goes: the command's own index, or one part of a split command.
#[derive(Clone, Copy)]
enum Destination {
    Whole(usize),
    Part { split: usize, part: usize },
}

/// A split command inside a pipeline, waiting for the replies of its parts.
struct PendingSplit {
    index: usize,
    name: String,
    key_count: usize,
    positions: Vec<Vec<usize>>,
    replies: Vec<RespValue>,
}

/// A split command cut per slot: each part's slot and command, and the key positions it carries.
#[derive(Debug, PartialEq)]
struct Split {
    parts: Vec<(u16, Vec<String>)>,
    positions: Vec<Vec<usize>>,
}

/// The per-slot parts of a split command (`MGET`, `MSET`, `DEL`, ...): each part's slot and
/// command, and the key positions each part carries. An `MSET` with a key but no value gets
/// the server's arity error instead.
fn split_parts(name: &str, arguments: &[&str], keys: &[&str]) -> Result<Split, RespValue> {
    let pairs = name == "MSET";

    // key value pairs after the name: an odd count is the caller's mistake, answered like the server would.
    if pairs && arguments.len().is_multiple_of(2) {
        return Err(RespValue::Error(
            "ERR wrong number of arguments for 'mset' command".to_owned(),
        ));
    }

    let mut by_slot: HashMap<u16, Vec<usize>> = HashMap::new();

    for (index, key) in keys.iter().enumerate() {
        by_slot.entry(hash_slot(key)).or_default().push(index);
    }

    let mut parts = Vec::with_capacity(by_slot.len());
    let mut positions = Vec::with_capacity(by_slot.len());

    for (slot, held) in by_slot {
        let mut command = vec![arguments[0].to_owned()];

        for position in &held {
            if pairs {
                command.push(arguments[1 + position * 2].to_owned());
                command.push(arguments[2 + position * 2].to_owned());
            } else {
                command.push(keys[*position].to_owned());
            }
        }

        parts.push((slot, command));
        positions.push(held);
    }

    Ok(Split { parts, positions })
}

/// One reply for a split command from its parts' replies: the first error, `OK` for `MSET`,
/// the values in key order for `MGET`, and the summed count for the rest.
fn merge(
    name: &str,
    key_count: usize,
    positions: &[Vec<usize>],
    replies: Vec<RespValue>,
) -> RespValue {
    if let Some(error) = replies
        .iter()
        .find(|reply| matches!(reply, RespValue::Error(_)))
    {
        return error.clone();
    }

    match name {
        "MSET" => RespValue::Simple("OK".to_owned()),
        "MGET" => {
            let mut values = vec![RespValue::Null; key_count];

            for (held, reply) in positions.iter().zip(replies) {
                let RespValue::Array(items) = reply else {
                    continue;
                };

                for (position, item) in held.iter().zip(items) {
                    values[*position] = item;
                }
            }

            RespValue::Array(values)
        }

        _ => RespValue::Integer(replies.iter().filter_map(RespValue::as_integer).sum()),
    }
}

/// The slot and node of a `MOVED` or `ASK` error text, given its prefix (`"MOVED "`). A slot
/// outside the slot range makes it malformed.
fn redirect_target(text: &str, prefix: &str) -> Option<(u16, String, u16)> {
    let mut fields = text.strip_prefix(prefix)?.split_whitespace();
    let slot = fields
        .next()?
        .parse()
        .ok()
        .filter(|slot| *slot < SLOT_COUNT)?;
    let (host, port) = fields.next()?.rsplit_once(':')?;

    Some((slot, host.to_owned(), port.parse().ok()?))
}

fn closed_socket() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "connection is closed",
    ))
}

impl Connection {
    pub(crate) fn open_with_retry(
        host: &str,
        port: u16,
        options: &crate::client::ClientOptions,
        custom: Option<&Arc<dyn Fn(u32) -> Option<Duration> + Send + Sync>>,
    ) -> Result<Self, Error> {
        let mut attempt = 0_u32;

        loop {
            attempt += 1;

            match Self::open(host, port, options) {
                Ok(connection) => return Ok(connection),
                Err(error) => match reconnect_delay(options, custom, attempt) {
                    Some(delay) => std::thread::sleep(delay),
                    None => return Err(error),
                },
            }
        }
    }
}

fn reconnect_delay(
    options: &crate::client::ClientOptions,
    custom: Option<&Arc<dyn Fn(u32) -> Option<Duration> + Send + Sync>>,
    attempt: u32,
) -> Option<Duration> {
    if !options.reconnect {
        return None;
    }

    if let Some(delay) = custom {
        return delay(attempt);
    }

    if attempt >= options.max_reconnect_attempts {
        return None;
    }

    let multiplier = 2_u32.saturating_pow(attempt.saturating_sub(1));
    let delay = options.reconnect_base_delay.saturating_mul(multiplier);

    Some(delay.min(options.reconnect_max_delay))
}

#[cfg(test)]
mod tests {
    use super::{ClusterRouter, Connection, Discovery, Topology, command_keys, hash_slot};
    use crate::{client::ClientOptions, value::RespValue};
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
        time::Duration,
    };

    fn slots(first: i64, last: i64) -> RespValue {
        RespValue::Array(vec![
            RespValue::Integer(first),
            RespValue::Integer(last),
            RespValue::Array(vec![
                RespValue::Bulk(b"127.0.0.1".to_vec()),
                RespValue::Integer(6379),
            ]),
        ])
    }

    #[test]
    fn topology_rejects_gaps_overlaps_and_invalid_boundaries() {
        for ranges in [
            vec![slots(0, 8191), slots(8193, 16383)],
            vec![slots(0, 8192), slots(8192, 16383)],
            vec![slots(0, 16384)],
            vec![slots(0, -1)],
            vec![slots(-1, 16383)],
        ] {
            assert!(Topology::parse(&RespValue::Array(ranges)).is_none());
        }
        let topology =
            Topology::parse(&RespValue::Array(vec![slots(8192, 16383), slots(0, 8191)])).unwrap();
        for slot in 0..super::SLOT_COUNT {
            assert_eq!(topology.owner(slot), Some(&("127.0.0.1".to_owned(), 6379)));
        }
    }

    #[test]
    fn disconnected_stale_owner_refreshes_from_live_seed_before_sending() {
        let seed = TcpListener::bind("127.0.0.1:0").unwrap();
        let fresh = TcpListener::bind("127.0.0.1:0").unwrap();
        let stale = TcpListener::bind("127.0.0.1:0").unwrap();
        let stale_port = stale.local_addr().unwrap().port();
        drop(stale);
        let fresh_port = fresh.local_addr().unwrap().port();
        let seed_port = seed.local_addr().unwrap().port();
        let seed_thread = thread::spawn(move || {
            let (mut socket, _) = crate::test_support::accept(&seed);
            let mut buffer = [0; 512];
            for port in [stale_port, fresh_port] {
                let _request_len = socket.read(&mut buffer).unwrap();
                socket
                    .write_all(
                        format!("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{port}\r\n")
                            .as_bytes(),
                    )
                    .unwrap();
            }
        });
        let fresh_thread = thread::spawn(move || {
            let (mut socket, _) = crate::test_support::accept(&fresh);
            let mut buffer = [0; 512];
            let _request_len = socket.read(&mut buffer).unwrap();
            socket.write_all(b"$5\r\nvalue\r\n").unwrap();
        });
        let options = ClientOptions {
            reconnect: false,
            connect_timeout: Duration::from_millis(100),
            ..ClientOptions::default()
        };
        let connection = Connection::open("127.0.0.1", seed_port, &options).unwrap();
        let Discovery::Cluster(mut router) =
            ClusterRouter::discover(connection, options, Vec::new()).unwrap()
        else {
            panic!("expected cluster topology");
        };
        assert_eq!(
            router.execute(&["GET", "key"]).unwrap(),
            RespValue::Bulk(b"value".to_vec())
        );
        seed_thread.join().unwrap();
        fresh_thread.join().unwrap();
    }

    #[test]
    fn hash_slot_matches_redis_and_honors_a_hash_tag() {
        assert_eq!(hash_slot("123456789"), 12_739);
        assert_eq!(hash_slot("somekey"), 11_058);
        assert_eq!(hash_slot("{user1000}.following"), 3_443);
        assert_eq!(hash_slot("user:{42}:name"), hash_slot("cart:{42}"));
    }

    #[test]
    fn command_keys_follow_the_redis_positions() {
        assert_eq!(command_keys("MGET", &["MGET", "a", "b"]), ["a", "b"]);
        assert_eq!(
            command_keys("MSET", &["MSET", "a", "1", "c", "2"]),
            ["a", "c"]
        );
        assert_eq!(
            command_keys("EVAL", &["EVAL", "return 1", "2", "k1", "k2", "arg"]),
            ["k1", "k2"]
        );
        assert_eq!(
            command_keys("LMPOP", &["LMPOP", "2", "a", "b", "LEFT"]),
            ["a", "b"]
        );
        assert_eq!(
            command_keys("BLMPOP", &["BLMPOP", "0", "1", "a", "LEFT"]),
            ["a"]
        );
        assert!(command_keys("PING", &["PING"]).is_empty());
        assert!(command_keys("CHANGES", &["CHANGES", "START"]).is_empty());
        assert_eq!(command_keys("LEASE", &["LEASE", "lock", "5000"]), ["lock"]);
        assert_eq!(
            command_keys("LIMIT", &["LIMIT", "login", "5", "60000"]),
            ["login"]
        );
        assert_eq!(command_keys("ONCE", &["ONCE", "pay", "60000"]), ["pay"]);
        assert_eq!(
            command_keys("IDEM", &["IDEM", "pay", "BEGIN", "fp", "owner", "60000"]),
            ["pay"]
        );
        assert_eq!(
            command_keys("XADD", &["XADD", "s", "DELAY", "5", "*", "f", "v"]),
            ["s"]
        );
        assert!(command_keys("PUBLISH", &["PUBLISH", "chan", "hi"]).is_empty());
        assert_eq!(
            command_keys("BITOP", &["BITOP", "AND", "dest", "a", "b"]),
            ["dest", "a", "b"]
        );
        assert_eq!(
            command_keys("SINTERCARD", &["SINTERCARD", "2", "a", "b", "LIMIT", "1"]),
            ["a", "b"]
        );
        assert_eq!(
            command_keys("JSON.MGET", &["JSON.MGET", "a", "b", "$.x"]),
            ["a", "b"]
        );
        assert_eq!(
            command_keys("XGROUP", &["XGROUP", "CREATE", "s", "g", "$"]),
            ["s"]
        );
        assert_eq!(command_keys("XINFO", &["XINFO", "STREAM", "s"]), ["s"]);
        assert!(command_keys("RUVIO", &["RUVIO", "OWNER"]).is_empty());
        assert!(
            command_keys("JSON.INDEX", &["JSON.INDEX", "CREATE", "i", "PREFIX", "u:"]).is_empty()
        );
        assert!(command_keys("JSON.FIND", &["JSON.FIND", "i", "VALUE", "x"]).is_empty());
        assert_eq!(
            command_keys("LPUSHCAP", &["LPUSHCAP", "recent", "10", "a"]),
            ["recent"]
        );
        assert_eq!(
            command_keys("DELEX", &["DELEX", "lock", "IFEQ", "t"]),
            ["lock"]
        );
        assert_eq!(
            command_keys("BF.MADD", &["BF.MADD", "bf", "a", "b"]),
            ["bf"]
        );
        assert_eq!(command_keys("WAIT", &["WAIT", "0", "10"]).len(), 0);
        assert_eq!(
            command_keys("SPUBLISH", &["SPUBLISH", "chan", "hi"]),
            ["chan"]
        );
    }

    /// The keys in `clients/testdata/key_vectors.json` are the ones the server routes by.
    /// SPUBLISH, SSUBSCRIBE and SUNSUBSCRIBE are skipped: they have no keys there, but this
    /// client routes them by channel through the same function.
    #[test]
    fn command_keys_match_the_shared_vectors() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata/key_vectors.json");
        let text = std::fs::read_to_string(&path).expect("key_vectors.json");
        let vectors: serde_json::Value =
            serde_json::from_str(&text).expect("key_vectors.json is JSON");
        let mut failures = std::collections::BTreeMap::new();
        let mut checked = 0;

        for entry in vectors["vectors"].as_array().expect("vectors") {
            let arguments: Vec<&str> = entry["args"]
                .as_array()
                .expect("args")
                .iter()
                .map(|argument| argument.as_str().expect("argument"))
                .collect();
            let name = super::command_name(&arguments);

            if matches!(name.as_str(), "SPUBLISH" | "SSUBSCRIBE" | "SUNSUBSCRIBE") {
                continue;
            }

            let mut expected: Vec<&str> = entry["keys"]
                .as_array()
                .expect("keys")
                .iter()
                .map(|key| key.as_str().expect("key"))
                .collect();
            let mut actual = command_keys(&name, &arguments);

            expected.sort_unstable();
            actual.sort_unstable();
            checked += 1;

            if actual != expected {
                failures
                    .entry(name)
                    .or_insert(format!("{arguments:?}: got {actual:?}, want {expected:?}"));
            }
        }

        assert!(checked > 1_000, "only {checked} vectors");
        assert!(
            failures.is_empty(),
            "{} commands route by the wrong keys:\n{}",
            failures.len(),
            failures
                .iter()
                .map(|(name, example)| format!("{name}: {example}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

#[cfg(test)]
mod more_tests {
    use super::*;
    use crate::value::RespValue;
    use std::io::Read;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread::{self, JoinHandle};

    fn strs(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_owned()).collect()
    }

    // --- hashing ---------------------------------------------------------------------------

    #[test]
    fn hash_tag_edge_cases() {
        // An empty tag, a missing close, or no open brace hashes the whole key.
        assert_eq!(hash_slot("{}abc"), hash_slot_bytes(b"{}abc"));
        assert_ne!(hash_slot("{}abc"), hash_slot("abc"));
        assert_eq!(hash_tag(b"{}abc"), b"{}abc");
        assert_eq!(hash_tag(b"{abc"), b"{abc");
        assert_eq!(hash_tag(b"abc}"), b"abc}");
        assert_eq!(hash_tag(b"plain"), b"plain");
        assert_eq!(hash_tag(b""), b"");
        // Only the first brace pair counts, and its first close ends the tag.
        assert_eq!(hash_tag(b"{{a}}"), b"{a");
        assert_eq!(hash_tag(b"x{a}{b}"), b"a");
        assert_eq!(hash_tag(b"}{a}"), b"a");
        // The first pair is empty, so a later pair is ignored.
        assert_eq!(hash_tag(b"{}{a}"), b"{}{a}");
        assert_eq!(hash_slot("a{user}b"), hash_slot("user"));
    }

    #[test]
    fn hash_tag_is_found_in_long_keys_across_block_boundaries() {
        for position in [0_usize, 1, 14, 15, 16, 17, 31, 32, 33, 47, 100] {
            let mut key = vec![b'x'; position];

            key.extend_from_slice(b"{tag}");
            key.extend(std::iter::repeat_n(b'y', 40));
            assert_eq!(hash_tag(&key), b"tag", "brace at {position}");

            let mut open_only = vec![b'x'; position];

            open_only.push(b'{');
            open_only.extend(std::iter::repeat_n(b'y', 40));
            assert_eq!(
                hash_tag(&open_only),
                &open_only[..],
                "unclosed at {position}"
            );
        }
    }

    #[test]
    fn find_byte_matches_a_plain_search() {
        let haystack: Vec<u8> = (0..200_u32).map(|i| (i % 251) as u8).collect();

        for needle in [0_u8, 1, 15, 16, 17, 100, 199, 250, 255] {
            for start in [0_usize, 1, 15, 16, 33] {
                let slice = &haystack[start..];

                assert_eq!(
                    find_byte(slice, needle),
                    slice.iter().position(|byte| *byte == needle),
                    "needle {needle} from {start}"
                );
            }
        }

        assert_eq!(find_byte(b"", b'{'), None);
        assert_eq!(find_byte(&[0xff; 40], 0xff), Some(0));
    }

    #[test]
    fn crc16_and_slots_stay_in_range() {
        assert_eq!(crc16(b""), 0);
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(hash_slot(""), 0);

        for index in 0..500 {
            assert!(hash_slot(&format!("key:{index}")) < SLOT_COUNT);
        }

        assert_eq!(
            hash_slot_bytes(&[0xff, 0xfe, 0x00, b'{', 0x80, b'}']),
            hash_slot_bytes(&[0x80])
        );
    }

    // --- key extraction ---------------------------------------------------------------------

    #[test]
    fn command_keys_for_short_and_keyless_commands() {
        assert!(command_keys("GET", &["GET"]).is_empty());
        assert!(command_keys("", &[]).is_empty());

        for name in [
            "ECHO", "INFO", "CONFIG", "CLIENT", "CLUSTER", "SELECT", "LATENCY", "SCAN",
        ] {
            assert!(command_keys(name, &[name, "x"]).is_empty(), "{name}");
        }

        assert_eq!(command_keys("GET", &["GET", "k"]), ["k"]);
        assert_eq!(command_keys("SET", &["SET", "k", "v", "EX", "5"]), ["k"]);
    }

    #[test]
    fn command_keys_for_multi_key_forms() {
        assert_eq!(
            command_keys("DEL", &["DEL", "a", "b", "c"]),
            ["a", "b", "c"]
        );
        assert_eq!(
            command_keys("SDIFFSTORE", &["SDIFFSTORE", "d", "a"]),
            ["d", "a"]
        );
        assert_eq!(
            command_keys("RENAME", &["RENAME", "a", "b", "extra"]),
            ["a", "b"]
        );
        assert_eq!(
            command_keys("COPY", &["COPY", "a", "b", "DB", "1"]),
            ["a", "b"]
        );
        assert_eq!(
            command_keys("LMOVE", &["LMOVE", "a", "b", "LEFT", "RIGHT"]),
            ["a", "b"]
        );
        assert_eq!(command_keys("RENAME", &["RENAME", "a"]), ["a"]);
        assert_eq!(command_keys("MSET", &["MSET", "a", "1", "b"]), ["a", "b"]);
        assert_eq!(
            command_keys("MSETNX", &["MSETNX", "a", "1", "b", "2"]),
            ["a", "b"]
        );
        assert_eq!(command_keys("BLPOP", &["BLPOP", "a", "b", "0"]), ["a", "b"]);
        assert_eq!(command_keys("BZPOPMIN", &["BZPOPMIN", "a", "0"]), ["a"]);
        assert!(command_keys("BRPOP", &["BRPOP", "0"]).is_empty());
        assert_eq!(
            command_keys("JSON.MGET", &["JSON.MGET", "a"]),
            Vec::<&str>::new()
        );
    }

    #[test]
    fn command_keys_with_counts() {
        assert_eq!(
            command_keys("EVALSHA", &["EVALSHA", "sha", "1", "k", "arg"]),
            ["k"]
        );
        assert_eq!(
            command_keys("FCALL", &["FCALL", "fn", "2", "a", "b", "x"]),
            ["a", "b"]
        );
        // The count promises more keys than there are: take what is there.
        assert_eq!(
            command_keys("EVAL", &["EVAL", "s", "5", "a", "b"]),
            ["a", "b"]
        );
        // A count that is not a number, or that is missing, means no keys.
        assert!(command_keys("EVAL", &["EVAL", "s", "many", "a"]).is_empty());
        assert!(command_keys("EVAL", &["EVAL", "s"]).is_empty());
        assert!(command_keys("ZINTER", &["ZINTER", "x", "a"]).is_empty());
        assert_eq!(
            command_keys("ZUNION", &["ZUNION", "2", "a", "b", "WEIGHTS", "1", "1"]),
            ["a", "b"]
        );
        assert_eq!(command_keys("ZDIFF", &["ZDIFF", "0"]), Vec::<&str>::new());
        assert_eq!(
            command_keys("ZUNIONSTORE", &["ZUNIONSTORE", "dst", "2", "a", "b"]),
            ["dst", "a", "b"]
        );
        assert_eq!(
            command_keys("ZINTERSTORE", &["ZINTERSTORE", "dst", "x"]),
            ["dst"]
        );
        assert_eq!(
            command_keys("SINTERCARD", &["SINTERCARD", "1", "a", "LIMIT", "3"]),
            ["a"]
        );
    }

    #[test]
    fn command_keys_for_streams_and_subcommands() {
        assert_eq!(
            command_keys(
                "XREAD",
                &["XREAD", "COUNT", "2", "STREAMS", "a", "b", "0", "0"]
            ),
            ["a", "b"]
        );
        assert_eq!(
            command_keys(
                "XREADGROUP",
                &["XREADGROUP", "GROUP", "g", "c", "streams", "a", "$"]
            ),
            ["a"]
        );
        // STREAMS missing, or with nothing after it.
        assert!(command_keys("XREAD", &["XREAD", "COUNT", "2"]).is_empty());
        assert!(command_keys("XREAD", &["XREAD", "STREAMS"]).is_empty());
        assert_eq!(command_keys("OBJECT", &["OBJECT", "ENCODING", "k"]), ["k"]);
        assert_eq!(command_keys("MEMORY", &["MEMORY", "USAGE", "k"]), ["k"]);
        // The key is the third argument; without one there is no key.
        assert!(command_keys("OBJECT", &["OBJECT", "HELP"]).is_empty());
        assert!(command_keys("XGROUP", &["XGROUP", "HELP"]).is_empty());
        assert!(command_keys("BITOP", &["BITOP", "NOT"]).is_empty());
        assert_eq!(command_keys("BITOP", &["BITOP", "AND", "d"]), ["d"]);
    }

    #[test]
    fn command_key_rules_are_sorted_for_binary_search() {
        assert!(
            COMMAND_KEY_RULES
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0)
        );
        // Not in the table: routed by the first argument.
        assert_eq!(
            command_keys("NOT-A-COMMAND", &["NOT-A-COMMAND", "k"]),
            ["k"]
        );
    }

    #[test]
    fn small_helpers() {
        assert_eq!(command_name(&[]), "");
        assert_eq!(command_name(&["get", "k"]), "GET");
        assert!(keyless("PING"));
        assert!(!keyless("GET"));
        assert!(!keyless("NOT-A-COMMAND"));
        assert!(spreads_to_every_node(&["SCRIPT", "flush"]));
        assert!(spreads_to_every_node(&["FUNCTION", "Delete"]));
        assert!(spreads_to_every_node(&["SCRIPT", "load"]));
        assert!(!spreads_to_every_node(&["SCRIPT", "EXISTS"]));
        assert!(!spreads_to_every_node(&["SCRIPT"]));
        assert!(matches!(slots_of(&[]), Slots::None));
        assert!(matches!(slots_of(&["{a}1", "{a}2"]), Slots::One(slot) if slot == hash_slot("a")));
        assert!(matches!(slots_of(&["{a}", "{b}"]), Slots::Many));
        assert!(matches!(slots_of(&["{a}", "{a}x", "{b}"]), Slots::Many));
        assert_eq!(cross_slot(), RespValue::Error(CROSSSLOT.to_owned()));
        assert_eq!(CommandName::of(&[]).as_str(), "");
        assert_eq!(CommandName::of(&["get", "k"]).as_str(), "GET");
        assert_eq!(CommandName::of(&["Json.Get", "k"]).as_str(), "JSON.GET");
        assert_eq!(CommandName::from_bytes(&[0xff, b'a']).as_str(), "\u{fffd}");
        let long = "x".repeat(40);
        assert_eq!(CommandName::of(&[long.as_str()]).as_str(), "X".repeat(40));
        assert_eq!(single_key("GET", &["get", "k"]), Some("k"));
        assert_eq!(single_key("NOT-A-COMMAND", &["x", "k"]), Some("k"));
        assert_eq!(single_key("SPUBLISH", &["spublish", "c", "m"]), Some("c"));
        assert_eq!(single_key("SSUBSCRIBE", &["ssubscribe", "c"]), None);
        assert_eq!(single_key("MGET", &["mget", "a", "b"]), None);
        assert_eq!(single_key("GET", &["get"]), None);
        assert_eq!(borrowed(&strs(&["a", "b"])), ["a", "b"]);
    }

    #[test]
    fn split_parts_group_keys_by_slot_and_merge_restores_key_order() {
        let Split { parts, positions } = split_parts(
            "MGET",
            &["MGET", "{a}1", "{b}1", "{a}2"],
            &["{a}1", "{b}1", "{a}2"],
        )
        .unwrap();
        let mut pairs: Vec<_> = parts.into_iter().zip(positions.clone()).collect();

        pairs.sort_by_key(|((_, command), _)| command[1].clone());
        assert_eq!(
            pairs[0].0,
            (hash_slot("a"), strs(&["MGET", "{a}1", "{a}2"]))
        );
        assert_eq!(pairs[0].1, [0, 2]);
        assert_eq!(pairs[1].0, (hash_slot("b"), strs(&["MGET", "{b}1"])));

        let bulk = |text: &str| RespValue::Bulk(text.as_bytes().to_vec());
        let replies = positions
            .iter()
            .map(|held| {
                RespValue::Array(
                    held.iter()
                        .map(|position| bulk(&position.to_string()))
                        .collect(),
                )
            })
            .collect();

        assert_eq!(
            merge("MGET", 3, &positions, replies),
            RespValue::Array(vec![bulk("0"), bulk("1"), bulk("2")])
        );

        let Split { parts, .. } =
            split_parts("MSET", &["MSET", "{a}", "1", "{b}", "2"], &["{a}", "{b}"]).unwrap();
        let mut commands: Vec<_> = parts.into_iter().map(|(_, command)| command).collect();

        commands.sort();
        assert_eq!(
            commands,
            [strs(&["MSET", "{a}", "1"]), strs(&["MSET", "{b}", "2"])]
        );
        assert_eq!(
            split_parts("MSET", &["MSET", "{a}", "1", "{b}"], &["{a}", "{b}"]),
            Err(RespValue::Error(
                "ERR wrong number of arguments for 'mset' command".to_owned()
            ))
        );

        let two = vec![vec![0], vec![1]];

        assert_eq!(
            merge("MSET", 2, &two, vec![RespValue::Simple("OK".into()); 2]),
            RespValue::Simple("OK".into())
        );
        assert_eq!(
            merge(
                "DEL",
                2,
                &two,
                vec![RespValue::Integer(2), RespValue::Integer(3)]
            ),
            RespValue::Integer(5)
        );
        assert_eq!(
            merge(
                "DEL",
                2,
                &two,
                vec![RespValue::Integer(1), RespValue::Error("ERR boom".into())]
            ),
            RespValue::Error("ERR boom".into())
        );
        // A part whose reply is not an array leaves its keys null.
        assert_eq!(
            merge(
                "MGET",
                2,
                &two,
                vec![RespValue::Integer(5), RespValue::Array(vec![bulk("x")])]
            ),
            RespValue::Array(vec![RespValue::Null, bulk("x")])
        );
    }

    #[test]
    fn moved_parsing() {
        let moved = |reply: &RespValue| match reply {
            RespValue::Error(text) => redirect_target(text, "MOVED "),
            _ => None,
        };
        let error = |text: &str| RespValue::Error(text.to_owned());

        assert_eq!(
            moved(&error("MOVED 3999 127.0.0.1:6381")),
            Some((3999, "127.0.0.1".into(), 6381))
        );
        assert_eq!(
            moved(&error("MOVED 1 [::1]:7000")),
            Some((1, "[::1]".into(), 7000))
        );
        assert_eq!(
            moved(&error("MOVED  16383   h:5 extra")),
            Some((16383, "h".into(), 5))
        );
        assert_eq!(moved(&error("MOVED 1")), None);
        assert_eq!(moved(&error("MOVED 16384 h:5")), None);
        assert_eq!(moved(&error("MOVED x h:5")), None);
        assert_eq!(moved(&error("MOVED 1 hostonly")), None);
        assert_eq!(moved(&error("MOVED 1 h:99999")), None);
        assert_eq!(moved(&error("MOVED 1 h:-1")), None);
        assert_eq!(moved(&error("moved 1 h:5")), None);
        assert_eq!(moved(&error("ASK 1 h:5")), None);
        assert_eq!(moved(&error("ERR MOVED 1 h:5")), None);
        assert_eq!(moved(&RespValue::Simple("MOVED 1 h:5".into())), None);
        assert_eq!(moved(&RespValue::Null), None);

        assert_eq!(
            redirect_target("ASK 3999 127.0.0.1:6381", "ASK "),
            Some((3999, "127.0.0.1".into(), 6381))
        );
        assert_eq!(redirect_target("ASK 16384 h:5", "ASK "), None);
        assert_eq!(redirect_target("MOVED 1 h:5", "ASK "), None);
    }

    #[test]
    fn reconnect_delays_double_up_to_the_cap() {
        let mut options = ClientOptions {
            reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_base_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_millis(350),
            ..ClientOptions::default()
        };

        assert_eq!(
            reconnect_delay(&options, None, 1),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            reconnect_delay(&options, None, 2),
            Some(Duration::from_millis(200))
        );
        assert_eq!(
            reconnect_delay(&options, None, 3),
            Some(Duration::from_millis(350))
        );
        assert_eq!(
            reconnect_delay(&options, None, 4),
            Some(Duration::from_millis(350))
        );
        assert_eq!(reconnect_delay(&options, None, 5), None);
        assert_eq!(reconnect_delay(&options, None, 50), None);

        // A custom function overrides the attempt limit and the built-in curve.
        let custom: Arc<dyn Fn(u32) -> Option<Duration> + Send + Sync> = Arc::new(|attempt| {
            (attempt < 100).then_some(Duration::from_millis(u64::from(attempt)))
        });

        assert_eq!(
            reconnect_delay(&options, Some(&custom), 50),
            Some(Duration::from_millis(50))
        );
        assert_eq!(reconnect_delay(&options, Some(&custom), 100), None);

        // Huge attempt numbers saturate instead of overflowing.
        options.max_reconnect_attempts = u32::MAX;
        options.reconnect_base_delay = Duration::from_secs(1);
        options.reconnect_max_delay = Duration::from_secs(2);
        assert_eq!(
            reconnect_delay(&options, None, 4_000_000_000),
            Some(Duration::from_secs(2))
        );

        // With reconnect off there is never a retry, custom function or not.
        options.reconnect = false;
        assert_eq!(reconnect_delay(&options, None, 1), None);
        assert_eq!(reconnect_delay(&options, Some(&custom), 1), None);
    }

    #[test]
    fn idle_wait_and_error_helpers() {
        use std::io::{Error as IoError, ErrorKind};

        assert!(idle_wait(&IoError::from(ErrorKind::TimedOut)));
        assert!(idle_wait(&IoError::from(ErrorKind::WouldBlock)));
        assert!(!idle_wait(&IoError::from(ErrorKind::UnexpectedEof)));
        assert!(matches!(
            throw_error(RespValue::Error("ERR x".into())),
            Err(Error::Server(message)) if message == "ERR x"
        ));
        assert_eq!(
            throw_error(RespValue::Integer(1)).unwrap(),
            RespValue::Integer(1)
        );
        assert!(matches!(closed_socket(), Error::Io(e) if e.kind() == ErrorKind::BrokenPipe));
    }

    // --- topology parsing -------------------------------------------------------------------

    fn range(first: i64, last: i64, host: RespValue, port: i64) -> RespValue {
        RespValue::Array(vec![
            RespValue::Integer(first),
            RespValue::Integer(last),
            RespValue::Array(vec![host, RespValue::Integer(port)]),
        ])
    }

    fn host(name: &str) -> RespValue {
        RespValue::Bulk(name.as_bytes().to_vec())
    }

    fn whole(host_value: RespValue, port: i64) -> RespValue {
        RespValue::Array(vec![range(0, 16_383, host_value, port)])
    }

    #[test]
    fn topology_accepts_replica_fields_and_dedupes_nodes() {
        let with_replica = RespValue::Array(vec![
            RespValue::Integer(0),
            RespValue::Integer(99),
            RespValue::Array(vec![host("h"), RespValue::Integer(1), host("node-id")]),
            RespValue::Array(vec![host("replica"), RespValue::Integer(2)]),
        ]);
        let second = range(100, 16_383, host("h"), 1);
        let topology = Topology::parse(&RespValue::Array(vec![second, with_replica])).unwrap();

        assert_eq!(topology.nodes, [("h".to_owned(), 1)]);
        assert_eq!(topology.replicas, [vec![("replica".to_owned(), 2)]]);
        assert_eq!(topology.owner(0), Some(&("h".to_owned(), 1)));
        assert_eq!(topology.owner(16_383), Some(&("h".to_owned(), 1)));
        assert_eq!(topology.owner(16_384), None);
    }

    #[test]
    fn topology_rejects_malformed_entries() {
        let valid = || range(0, 16_383, host("h"), 1);

        assert!(Topology::parse(&RespValue::Null).is_none());
        assert!(Topology::parse(&RespValue::Integer(1)).is_none());
        assert!(Topology::parse(&RespValue::Array(vec![])).is_none());
        assert!(Topology::parse(&RespValue::Array(vec![RespValue::Integer(1)])).is_none());
        assert!(Topology::parse(&RespValue::Array(vec![valid()])).is_some());
        // Too few fields.
        assert!(
            Topology::parse(&RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::Integer(16_383),
            ])]))
            .is_none()
        );
        // The node field is not an array, or is too short.
        assert!(
            Topology::parse(&RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::Integer(16_383),
                host("h"),
            ])]))
            .is_none()
        );
        assert!(
            Topology::parse(&RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::Integer(16_383),
                RespValue::Array(vec![host("h")]),
            ])]))
            .is_none()
        );
        // Host null or an array; port out of range or not an integer.
        assert!(Topology::parse(&whole(RespValue::Null, 1)).is_none());
        assert!(Topology::parse(&whole(RespValue::Array(vec![]), 1)).is_none());
        assert!(Topology::parse(&whole(host("h"), 65_536)).is_none());
        assert!(Topology::parse(&whole(host("h"), -1)).is_none());
        assert!(Topology::parse(&whole(host("h"), 65_535)).is_some());
        assert!(Topology::parse(&whole(host("h"), 0)).is_some());
        assert!(
            Topology::parse(&RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::Integer(16_383),
                RespValue::Array(vec![host("h"), host("1")]),
            ])]))
            .is_none()
        );
        // Slot bounds that are not integers.
        assert!(
            Topology::parse(&RespValue::Array(vec![RespValue::Array(vec![
                host("0"),
                RespValue::Integer(16_383),
                RespValue::Array(vec![host("h"), RespValue::Integer(1)]),
            ])]))
            .is_none()
        );
        assert!(
            Topology::parse(&RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::Null,
                RespValue::Array(vec![host("h"), RespValue::Integer(1)]),
            ])]))
            .is_none()
        );
        // Ends short of the last slot, and starts after slot 0.
        assert!(Topology::parse(&RespValue::Array(vec![range(0, 16_382, host("h"), 1)])).is_none());
        assert!(Topology::parse(&RespValue::Array(vec![range(1, 16_383, host("h"), 1)])).is_none());
        // An integer host is accepted as text (as_string), as the server never sends one.
        assert!(Topology::parse(&whole(RespValue::Integer(7), 1)).is_some());
    }

    // --- scripted sockets -------------------------------------------------------------------

    enum Step {
        /// Read one request, then write these bytes.
        Reply(Vec<u8>),
        /// Write these bytes without waiting for a request.
        Push(Vec<u8>),
        /// Wait until the test sends on (or drops) the paired sender, 10 s at most.
        Wait(mpsc::Receiver<()>),
        /// Read one request and close the socket.
        Close,
    }

    struct Script {
        port: u16,
        requests: Arc<Mutex<Vec<Vec<u8>>>>,
        handle: JoinHandle<()>,
    }

    impl Script {
        fn start(steps: Vec<Step>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let seen = Arc::clone(&requests);
            let handle = thread::spawn(move || {
                let (mut socket, _) = crate::test_support::accept(&listener);
                let mut buffer = [0_u8; 4096];

                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();

                for step in steps {
                    match step {
                        Step::Reply(bytes) => {
                            let read = socket.read(&mut buffer).unwrap();

                            seen.lock().unwrap().push(buffer[..read].to_vec());
                            socket.write_all(&bytes).unwrap();
                        }
                        Step::Push(bytes) => socket.write_all(&bytes).unwrap(),
                        Step::Wait(gate) => {
                            let _ = gate.recv_timeout(Duration::from_secs(10));
                        }
                        Step::Close => {
                            let read = socket.read(&mut buffer).unwrap();

                            seen.lock().unwrap().push(buffer[..read].to_vec());

                            return;
                        }
                    }
                }

                // Hold the socket open until the client is done with it.
                let _ = socket.read(&mut buffer);
            });

            Self {
                port,
                requests,
                handle,
            }
        }

        fn connect(&self) -> Connection {
            Connection::open("127.0.0.1", self.port, &ClientOptions::default()).unwrap()
        }

        fn finish(self) -> Vec<Vec<u8>> {
            // Dropping the client side lets the thread's final read return.
            let Self {
                requests, handle, ..
            } = self;

            handle.join().unwrap();

            requests.lock().unwrap().clone()
        }
    }

    /// A router on one primary, with `replica` listed as its replica.
    fn replicated_router(primary: &Script, replica: u16, policy: ReadPolicy) -> ClusterRouter {
        ClusterRouter::new(
            primary.connect(),
            Topology {
                owners: vec![Some(0); SLOT_COUNT as usize],
                nodes: vec![("127.0.0.1".to_owned(), primary.port)],
                replicas: vec![vec![("127.0.0.1".to_owned(), replica)]],
            },
            ClientOptions {
                read_policy: policy,
                reconnect_base_delay: Duration::ZERO,
                max_reconnect_attempts: 1,
                connect_timeout: Duration::from_millis(200),
                ..ClientOptions::default()
            },
            Vec::new(),
        )
    }

    fn sent(requests: &[Vec<u8>]) -> String {
        requests
            .iter()
            .map(|request| String::from_utf8_lossy(request).into_owned())
            .collect()
    }

    #[test]
    fn reads_go_to_a_replica_and_writes_and_mixed_pipelines_to_the_primary() {
        let primary = Script::start(vec![
            Step::Reply(b"+OK\r\n".to_vec()),
            Step::Reply(b"+OK\r\n$1\r\n1\r\n".to_vec()),
        ]);
        let replica = Script::start(vec![
            Step::Reply(b"+OK\r\n".to_vec()),
            Step::Reply(b"$1\r\nr\r\n".to_vec()),
            Step::Reply(b"$1\r\nr\r\n$1\r\ns\r\n".to_vec()),
        ]);
        let mut router = replicated_router(&primary, replica.port, ReadPolicy::PreferReplica);

        assert_eq!(
            router.execute(&["GET", "k"]).unwrap(),
            RespValue::Bulk(b"r".to_vec())
        );
        assert_eq!(
            router.execute(&["SET", "k", "v"]).unwrap(),
            RespValue::Simple("OK".to_owned())
        );
        assert_eq!(
            router
                .execute_many(&[&["GET", "k"], &["STRLEN", "k"]])
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            router
                .execute_many(&[&["SET", "a", "1"], &["GET", "a"]])
                .unwrap()[1],
            RespValue::Bulk(b"1".to_vec())
        );
        drop(router);

        let to_replica = sent(&replica.finish());
        let to_primary = sent(&primary.finish());

        assert!(
            to_replica.starts_with("*1\r\n$8\r\nREADONLY\r\n"),
            "{to_replica}"
        );
        assert!(
            to_replica.contains("$3\r\nGET\r\n$1\r\nk\r\n"),
            "{to_replica}"
        );
        assert!(to_replica.contains("STRLEN"), "{to_replica}");
        assert!(!to_replica.contains("SET"), "{to_replica}");
        assert!(
            to_primary.contains("$3\r\nSET\r\n$1\r\nk\r\n"),
            "{to_primary}"
        );
        assert!(
            to_primary.contains("$3\r\nGET\r\n$1\r\na\r\n"),
            "{to_primary}"
        );
    }

    #[test]
    fn an_unreachable_or_loading_replica_sends_reads_to_the_primary() {
        let closed = closed_port();
        let primary = Script::start(vec![Step::Reply(b"$1\r\np\r\n".to_vec())]);
        let mut router = replicated_router(&primary, closed, ReadPolicy::PreferReplica);

        assert_eq!(
            router.execute(&["GET", "k"]).unwrap(),
            RespValue::Bulk(b"p".to_vec())
        );
        drop(router);
        primary.finish();

        let primary = Script::start(vec![
            Step::Reply(b"$1\r\np\r\n".to_vec()),
            Step::Reply(b"$1\r\nq\r\n".to_vec()),
        ]);
        let replica = Script::start(vec![
            Step::Reply(b"+OK\r\n".to_vec()),
            Step::Reply(b"-LOADING Ruvio is loading the dataset in memory\r\n".to_vec()),
        ]);
        let mut router = replicated_router(&primary, replica.port, ReadPolicy::PreferReplica);

        assert_eq!(
            router.execute(&["GET", "k"]).unwrap(),
            RespValue::Bulk(b"p".to_vec())
        );
        // The loading replica is skipped until the slot map is reloaded.
        assert_eq!(
            router.execute(&["GET", "k"]).unwrap(),
            RespValue::Bulk(b"q".to_vec())
        );
        drop(router);
        replica.finish();
        primary.finish();
    }

    #[test]
    fn replica_policy_fails_without_a_reachable_replica() {
        let primary = Script::start(Vec::new());
        let mut router = replicated_router(&primary, closed_port(), ReadPolicy::Replica);

        assert!(router.execute(&["GET", "k"]).is_err());
        drop(router);
        primary.finish();

        let primary = Script::start(Vec::new());
        let mut router = ClusterRouter::new(
            primary.connect(),
            Topology {
                owners: vec![Some(0); SLOT_COUNT as usize],
                nodes: vec![("127.0.0.1".to_owned(), primary.port)],
                replicas: vec![Vec::new()],
            },
            ClientOptions {
                read_policy: ReadPolicy::Replica,
                ..ClientOptions::default()
            },
            Vec::new(),
        );

        assert!(matches!(
            router.execute(&["GET", "k"]),
            Err(Error::Protocol(message)) if message.contains("no replica")
        ));
        drop(router);
        primary.finish();
    }

    #[test]
    fn only_non_blocking_readonly_commands_count_as_reads() {
        assert!(reads("GET"));
        assert!(reads("HGETALL"));
        assert!(!reads("SET"));
        assert!(!reads("BLPOP"));
        assert!(!reads("EVAL"));
        assert!(!reads("NOSUCH"));
    }

    fn slots_to(port: u16) -> Vec<u8> {
        format!("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{port}\r\n").into_bytes()
    }

    #[test]
    fn open_sends_auth_with_and_without_a_user() {
        for (username, expected) in [
            (None, "*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n"),
            (Some("bob"), "*3\r\n$4\r\nAUTH\r\n$3\r\nbob\r\n$2\r\npw\r\n"),
        ] {
            let script = Script::start(vec![Step::Reply(b"+OK\r\n".to_vec())]);
            let options = ClientOptions {
                username: username.map(str::to_owned),
                password: Some("pw".to_owned()),
                ..ClientOptions::default()
            };
            let connection = Connection::open("127.0.0.1", script.port, &options).unwrap();

            drop(connection);
            assert_eq!(script.finish(), [expected.as_bytes()]);
        }
    }

    #[test]
    fn open_fails_when_auth_is_refused() {
        let script = Script::start(vec![Step::Reply(b"-WRONGPASS invalid\r\n".to_vec())]);
        let options = ClientOptions {
            password: Some("nope".to_owned()),
            ..ClientOptions::default()
        };
        let result = Connection::open("127.0.0.1", script.port, &options);

        assert!(matches!(result, Err(Error::Server(message)) if message == "WRONGPASS invalid"));
        script.finish();
    }

    #[test]
    fn open_reports_a_refused_connection_and_retry_gives_up() {
        let closed = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = closed.local_addr().unwrap().port();

        drop(closed);

        let mut options = ClientOptions {
            connect_timeout: Duration::from_millis(200),
            reconnect: false,
            ..ClientOptions::default()
        };

        assert!(matches!(
            Connection::open("127.0.0.1", port, &options),
            Err(Error::Io(_))
        ));
        // reconnect off: a single attempt.
        assert!(matches!(
            Connection::open_with_retry("127.0.0.1", port, &options, None),
            Err(Error::Io(_))
        ));

        options.reconnect = true;
        options.max_reconnect_attempts = 3;
        options.reconnect_base_delay = Duration::ZERO;
        options.reconnect_max_delay = Duration::ZERO;

        let attempts = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&attempts);
        let custom: Arc<dyn Fn(u32) -> Option<Duration> + Send + Sync> = Arc::new(move |attempt| {
            seen.lock().unwrap().push(attempt);
            (attempt < 4).then_some(Duration::ZERO)
        });

        assert!(Connection::open_with_retry("127.0.0.1", port, &options, Some(&custom)).is_err());
        assert_eq!(*attempts.lock().unwrap(), [1, 2, 3, 4]);
        assert!(Connection::open_with_retry("127.0.0.1", port, &options, None).is_err());
    }

    #[test]
    fn open_with_retry_succeeds_once_the_server_appears() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let options = ClientOptions {
            reconnect_base_delay: Duration::from_millis(1),
            ..ClientOptions::default()
        };

        let connection = Connection::open_with_retry("127.0.0.1", port, &options, None).unwrap();

        assert!(!connection.is_broken());
    }

    #[test]
    fn send_variants_write_and_read_the_right_number_of_replies() {
        let script = Script::start(vec![
            Step::Reply(b"+PONG\r\n".to_vec()),
            Step::Reply(b":1\r\n:2\r\n".to_vec()),
            Step::Reply(b"+A\r\n+B\r\n+C\r\n".to_vec()),
            Step::Reply(b"$2\r\nhi\r\n".to_vec()),
            Step::Reply(b"+X\r\n+Y\r\n".to_vec()),
        ]);
        let mut connection = script.connect();

        assert_eq!(
            connection.send(&["PING"]).unwrap(),
            RespValue::Simple("PONG".into())
        );
        assert_eq!(
            connection
                .send_replies(&["SUBSCRIBE", "a", "b"], 2)
                .unwrap(),
            [RespValue::Integer(1), RespValue::Integer(2)]
        );
        assert_eq!(
            connection
                .send_many(&[&["A"], &["B"], &["C"]])
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            connection.send_bytes(&[b"GET", &[0xff, 0x00]]).unwrap(),
            RespValue::Bulk(b"hi".to_vec())
        );
        assert_eq!(
            connection
                .send_many_owned(&[strs(&["X"]), strs(&["Y", "1"])])
                .unwrap(),
            [RespValue::Simple("X".into()), RespValue::Simple("Y".into())]
        );
        drop(connection);

        let requests = script.finish();

        assert_eq!(requests[0], b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(requests[3], b"*2\r\n$3\r\nGET\r\n$2\r\n\xff\x00\r\n");
    }

    #[test]
    fn send_owned_matches_send() {
        let script = Script::start(vec![Step::Reply(b"+OK\r\n".to_vec())]);
        let mut connection = script.connect();

        assert_eq!(
            connection.send_owned(&strs(&["SET", "k", "v"])).unwrap(),
            RespValue::Simple("OK".into())
        );
        drop(connection);
        assert_eq!(
            script.finish()[0],
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
        );
    }

    #[test]
    fn an_empty_command_is_refused_before_anything_is_written() {
        let script = Script::start(vec![Step::Reply(b"+PONG\r\n".to_vec())]);
        let mut connection = script.connect();

        assert!(matches!(connection.send(&[]), Err(Error::Protocol(_))));
        assert!(matches!(
            connection.send_bytes(&[]),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            connection.send_many(&[&["PING"], &[]]),
            Err(Error::Protocol(_))
        ));
        assert!(!connection.is_broken());
        // The scratch buffer is back and the socket still works.
        assert_eq!(
            connection.send(&["PING"]).unwrap(),
            RespValue::Simple("PONG".into())
        );
        drop(connection);
        assert_eq!(script.finish(), [b"*1\r\n$4\r\nPING\r\n"]);
    }

    #[test]
    fn a_closed_peer_marks_the_connection_broken() {
        let script = Script::start(vec![Step::Close]);
        let mut connection = script.connect();

        assert!(matches!(connection.send(&["PING"]), Err(Error::Io(_))));
        assert!(connection.is_broken());

        for _ in 0..2 {
            assert!(matches!(
                connection.send(&["PING"]),
                Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::BrokenPipe
            ));
        }

        assert!(matches!(
            connection.send_bytes(&[b"PING"]),
            Err(Error::Io(_))
        ));
        assert!(matches!(
            connection.send_replies(&["PING"], 1),
            Err(Error::Io(_))
        ));
        assert!(matches!(
            connection.send_many(&[&["PING"]]),
            Err(Error::Io(_))
        ));
        assert!(matches!(connection.read(), Err(Error::Io(_))));
        script.finish();
    }

    #[test]
    fn an_idle_read_timeout_does_not_break_the_connection() {
        let (release, gate) = mpsc::channel();
        let script = Script::start(vec![
            Step::Push(b"+one\r\n".to_vec()),
            Step::Wait(gate),
            Step::Push(b"+two\r\n".to_vec()),
        ]);
        let mut connection = script.connect();

        connection
            .set_read_timeout(Some(Duration::from_millis(80)))
            .unwrap();
        assert_eq!(connection.read().unwrap(), RespValue::Simple("one".into()));
        // Nothing arrives within the timeout: still usable.
        assert!(matches!(connection.read(), Err(Error::Io(e)) if idle_wait(&e)));
        assert!(!connection.is_broken());
        release.send(()).unwrap();
        connection
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert_eq!(connection.read().unwrap(), RespValue::Simple("two".into()));
        drop(connection);
        script.finish();
    }

    #[test]
    fn a_command_whose_reply_never_comes_marks_the_connection_broken() {
        // The script holds the socket open, without replying, until the checks are done.
        let (release, gate) = mpsc::channel();
        let script = Script::start(vec![Step::Wait(gate)]);
        let mut connection = script.connect();

        connection
            .set_read_timeout(Some(Duration::from_millis(60)))
            .unwrap();
        assert!(matches!(connection.send(&["PING"]), Err(Error::Io(e)) if idle_wait(&e)));
        assert!(connection.is_broken());
        drop(release);
        drop(connection);
        script.finish();
    }

    #[test]
    fn protocol_errors_in_a_reply_are_returned_and_leave_the_flag_alone() {
        let script = Script::start(vec![Step::Reply(b"?bad\r\n".to_vec())]);
        let mut connection = script.connect();

        assert!(matches!(
            connection.send(&["PING"]),
            Err(Error::Protocol(_))
        ));
        assert!(!connection.is_broken());
        drop(connection);
        script.finish();
    }

    // --- router internals over a scripted socket -------------------------------------------

    #[test]
    fn discover_treats_a_cluster_disabled_error_as_standalone() {
        let script = Script::start(vec![Step::Reply(
            b"-ERR cluster support disabled\r\n".to_vec(),
        )]);
        let result =
            ClusterRouter::discover(script.connect(), ClientOptions::default(), Vec::new())
                .unwrap();

        assert!(matches!(result, Discovery::Standalone(_)));
        drop(result);
        script.finish();
    }

    #[test]
    fn router_subscribe_path_and_message_reads() {
        // The node answers CLUSTER SLOTS with itself, then confirms two subscriptions at once.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut socket, _) = crate::test_support::accept(&listener);
            let mut buffer = [0_u8; 4096];

            let _ = socket.read(&mut buffer).unwrap();
            socket.write_all(&slots_to(port)).unwrap();
            let _ = socket.read(&mut buffer).unwrap();
            socket
                .write_all(b"*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n")
                .unwrap();
            socket
                .write_all(b"*3\r\n$7\r\nmessage\r\n$1\r\na\r\n$2\r\nhi\r\n")
                .unwrap();
            let _ = socket.read(&mut buffer).unwrap();
            socket
                .write_all(b"*3\r\n$10\r\nssubscribe\r\n$1\r\nc\r\n:1\r\n")
                .unwrap();
            let _ = socket.read(&mut buffer);
        });
        let connection = Connection::open("127.0.0.1", port, &ClientOptions::default()).unwrap();
        let Discovery::Cluster(mut router) =
            ClusterRouter::discover(connection, ClientOptions::default(), Vec::new()).unwrap()
        else {
            panic!("cluster expected");
        };

        assert_eq!(router.node_count(), 1);
        assert_eq!(
            router.subscription_endpoint(&["SSUBSCRIBE", "c"]),
            ("127.0.0.1".into(), port)
        );
        assert_eq!(
            router.subscription_endpoint(&["SUBSCRIBE", "c"]),
            ("127.0.0.1".into(), port)
        );
        assert_eq!(
            router.subscription_endpoint(&["ssubscribe"]),
            ("127.0.0.1".into(), port)
        );

        let values = router.run_replies(&["SUBSCRIBE", "a", "b"], 2).unwrap();

        assert_eq!(values.len(), 2);
        assert_eq!(
            router.read_message().unwrap(),
            RespValue::Array(vec![
                RespValue::Bulk(b"message".to_vec()),
                RespValue::Bulk(b"a".to_vec()),
                RespValue::Bulk(b"hi".to_vec()),
            ])
        );
        router
            .set_read_timeout(Some(Duration::from_millis(30)))
            .unwrap();
        assert!(matches!(router.read_message(), Err(Error::Io(_))));
        router.set_read_timeout(None).unwrap();
        assert_eq!(
            router.run_replies(&["SSUBSCRIBE", "c"], 1).unwrap().len(),
            1
        );
        drop(router);
        server.join().unwrap();
    }

    #[test]
    fn router_notifies_and_reopens_a_broken_endpoint() {
        let script = Script::start(vec![Step::Close]);
        let port = script.port;
        let mut router = ClusterRouter::new(
            script.connect(),
            Topology {
                owners: vec![Some(0); SLOT_COUNT as usize],
                nodes: vec![("127.0.0.1".to_owned(), port)],
                replicas: Vec::new(),
            },
            ClientOptions {
                reconnect_base_delay: Duration::ZERO,
                max_reconnect_attempts: 1,
                connect_timeout: Duration::from_millis(200),
                ..ClientOptions::default()
            },
            Vec::new(),
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let lost = Arc::clone(&events);
        let restored = Arc::clone(&events);

        router.share_hooks(
            Arc::new(Mutex::new(Some(Arc::new(
                move |notice: crate::client::ConnectionNotice| {
                    lost.lock().unwrap().push(("lost", notice.port));
                },
            )))),
            Arc::new(Mutex::new(Some(Arc::new(
                move |notice: crate::client::ConnectionNotice| {
                    restored.lock().unwrap().push(("restored", notice.port));
                },
            )))),
        );

        // The peer closes after reading the first request.
        assert!(matches!(router.execute(&["GET", "k"]), Err(Error::Io(_))));
        script.finish();

        // Nobody is listening any more: reopening fails after one attempt, and the lost hook has fired.
        let result = router.execute(&["GET", "k"]);

        assert!(matches!(result, Err(Error::Io(_))), "{result:?}");
        assert_eq!(*events.lock().unwrap(), [("lost", port)]);
    }

    #[test]
    fn with_reconnect_off_a_broken_endpoint_is_not_replaced() {
        let script = Script::start(vec![]);
        let mut router = router_for_topology(&script);
        router.options.reconnect = false;
        router.endpoints[0].connection.as_mut().unwrap().broken = true;
        assert!(matches!(
            router.open_endpoint("127.0.0.1", script.port, false),
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::BrokenPipe
        ));
        // Forcing it (what Client::reconnect does) opens a new socket... which this one-shot
        // script server cannot accept, so only check that the broken one was dropped first.
        router.options.connect_timeout = Duration::from_millis(100);
        router.options.max_reconnect_attempts = 1;
        let _ = router.open_endpoint("127.0.0.1", script.port, true);
        drop(router);
        script.finish();
    }

    #[test]
    fn for_slot_without_an_owner_is_a_protocol_error() {
        let script = Script::start(vec![]);
        let mut router = router_for_topology(&script);

        router.topology.owners[5] = None;
        assert!(matches!(
            router.for_slot(5),
            Err(Error::Protocol(message)) if message == "no cluster owner for slot 5"
        ));
        assert!(router.subscription_endpoint(&["SSUBSCRIBE", "x"]).1 > 0);
        drop(router);
        script.finish();
    }

    #[test]
    fn a_lost_node_reloads_the_slot_map_from_a_seed() {
        let gone = Script::start(vec![Step::Close]);
        let gone_port = gone.port;
        let live = TcpListener::bind("127.0.0.1:0").unwrap();
        let live_port = live.local_addr().unwrap().port();
        let live_thread = thread::spawn(move || {
            let (mut socket, _) = crate::test_support::accept(&live);
            let mut buffer = [0_u8; 512];

            for reply in [slots_to(live_port), b"$5\r\nvalue\r\n".to_vec()] {
                let _request_len = socket.read(&mut buffer).unwrap();

                socket.write_all(&reply).unwrap();
            }

            let _ = socket.read(&mut buffer);
        });
        let mut router = ClusterRouter::new(
            gone.connect(),
            Topology {
                owners: vec![Some(0); SLOT_COUNT as usize],
                nodes: vec![("127.0.0.1".to_owned(), gone_port)],
                replicas: Vec::new(),
            },
            ClientOptions {
                reconnect_base_delay: Duration::ZERO,
                max_reconnect_attempts: 1,
                connect_timeout: Duration::from_millis(200),
                ..ClientOptions::default()
            },
            vec![
                ("127.0.0.1".to_owned(), gone_port),
                ("127.0.0.1".to_owned(), live_port),
            ],
        );

        assert!(router.execute(&["GET", "key"]).is_err());
        gone.finish();

        assert_eq!(
            router.execute(&["GET", "key"]).unwrap(),
            RespValue::Bulk(b"value".to_vec())
        );
        assert_eq!(router.node_count(), 1);
        drop(router);
        live_thread.join().unwrap();
    }

    #[test]
    fn a_failed_reload_leaves_no_seed_endpoints_behind() {
        let gone = Script::start(vec![Step::Close]);
        let gone_port = gone.port;
        let mut router = ClusterRouter::new(
            gone.connect(),
            Topology {
                owners: vec![Some(0); SLOT_COUNT as usize],
                nodes: vec![("127.0.0.1".to_owned(), gone_port)],
                replicas: Vec::new(),
            },
            ClientOptions {
                reconnect_base_delay: Duration::ZERO,
                max_reconnect_attempts: 1,
                connect_timeout: Duration::from_millis(200),
                ..ClientOptions::default()
            },
            vec![("127.0.0.1".to_owned(), closed_port())],
        );

        assert!(router.execute(&["GET", "key"]).is_err());
        gone.finish();

        assert!(matches!(router.execute(&["GET", "key"]), Err(Error::Io(_))));
        assert_eq!(router.endpoints.len(), 1);
    }

    fn closed_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn router_for_topology(script: &Script) -> ClusterRouter {
        ClusterRouter::new(
            script.connect(),
            Topology {
                owners: vec![Some(0); SLOT_COUNT as usize],
                nodes: vec![("127.0.0.1".to_owned(), script.port)],
                replicas: Vec::new(),
            },
            ClientOptions::default(),
            Vec::new(),
        )
    }

    #[test]
    fn missing_connection_and_empty_refresh_sources_are_protocol_errors() {
        let script = Script::start(vec![]);
        let mut router = router_for_topology(&script);

        assert!(matches!(
            router.connection(7),
            Err(Error::Protocol(message)) if message == "no connection for endpoint 7"
        ));
        router.endpoint_index("nowhere", 1);
        assert!(matches!(
            router.connection(1),
            Err(Error::Protocol(message)) if message == "no connection for nowhere:1"
        ));
        router.endpoints.clear();
        assert!(matches!(
            router.refresh(),
            Err(Error::Protocol(message)) if message == "no cluster topology source"
        ));
        drop(router);
        script.finish();
    }

    #[test]
    fn router_for_helper_discovers_a_scripted_cluster() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut socket, _) = crate::test_support::accept(&listener);
            let mut buffer = [0_u8; 256];

            let _ = socket.read(&mut buffer).unwrap();
            socket.write_all(&slots_to(port)).unwrap();
            let _ = socket.read(&mut buffer);
        });
        let connection = Connection::open("127.0.0.1", port, &ClientOptions::default()).unwrap();
        let Discovery::Cluster(router) =
            ClusterRouter::discover(connection, ClientOptions::default(), Vec::new()).unwrap()
        else {
            panic!("cluster expected");
        };

        assert_eq!(router.node_count(), 1);
        drop(router);
        server.join().unwrap();
    }
}
