use std::{
    collections::{HashMap, VecDeque},
    io::{self},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use tracing::warn;
use wire::Wire;

use crate::{
    KvShard, OwnedMutation,
    persistence::{
        CheckpointManager, FrameCursor, FrameRead, PersistenceConfig, PersistenceError, WalSink,
        apply_mutation, decode_record, read_frames, read_frames_resuming,
    },
    protocol::ProtocolLimits,
};

pub const HANDSHAKE_MAGIC: &[u8; 8] = b"RUVREP01";
/// Handshake versions this build speaks; the version also names the request: resume from the
/// replica's log, resync from a snapshot at a sequence, or resync an incomplete image.
pub const VERSION_RESUME: u16 = 15;
pub const VERSION_RESYNC: u16 = 16;
pub const VERSION_RESYNC_INCOMPLETE: u16 = 17;
const SUPPORTED_VERSIONS: std::ops::RangeInclusive<u16> =
    VERSION_RESUME..=VERSION_RESYNC_INCOMPLETE;
/// Versions of builds whose handshake and reply are this build's, but whose stream has no
/// control frames. Read in full, then refused in this build's reply format.
const NO_CONTROL_VERSIONS: std::ops::RangeInclusive<u16> = 12..=14;
/// Versions of builds whose handshake ended after the password and whose reply carried no
/// history id. They are refused in their own reply format so they log the refusal.
const LEGACY_VERSIONS: std::ops::RangeInclusive<u16> = 1..=5;
const LEGACY_REPLY_LEN: usize = 8 + 2 + 8 + CHECKSUM_LEN;
/// Versions of builds whose handshake and reply carried a history id but no epoch. Refused in
/// their own formats, like the legacy ones.
const HISTORY_ONLY_VERSIONS: std::ops::RangeInclusive<u16> = 6..=8;
const HISTORY_ONLY_TAIL: usize = HISTORY_LEN + 4;
const HISTORY_ONLY_REPLY_LEN: usize = 8 + 2 + 8 + HISTORY_LEN + CHECKSUM_LEN;
/// Versions of builds whose handshake carried an epoch but no client address. Their reply
/// is this build's reply without the address that follows it.
const EPOCH_ONLY_VERSIONS: std::ops::RangeInclusive<u16> = 9..=11;
const EPOCH_ONLY_TAIL: usize = HISTORY_LEN + 4 + 8;
const VERSION_MISMATCH: &str =
    "primary and replicas must run the same Ruvio version (replication handshake version mismatch)";
pub const READONLY_LINE: &[u8] = b"READONLY You can't write against a read only replica.";
pub const FENCED_LINE: &[u8] =
    b"READONLY This primary was replaced: a replica has followed a newer epoch.";
pub const NOREPLICAS_LINE: &[u8] = b"NOREPLICAS Not enough good replicas to write.";
/// A replica that has nothing new to acknowledge repeats its last acknowledgement this
/// often, so a primary can tell a quiet replica from a lost one (`min-replicas-max-lag`).
pub const ACK_HEARTBEAT: Duration = Duration::from_secs(1);
/// A replica silent for longer than this (ten missed heartbeats) is no longer listed for
/// reads, even while its connection looks open: a host that lost power sends no reset.
pub const LISTED_REPLICA_LAG: Duration = Duration::from_secs(10);

/// Whether a shard takes writes, and the error it answers when it does not. Read for every
/// write command as one byte, like the role check it replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteGate {
    Open,
    /// A replica: writes come only from its primary.
    Replica,
    /// A primary that learned a newer epoch exists: another primary replaced it.
    Fenced,
    /// A primary with fewer recently acknowledging replicas than `min-replicas-to-write`.
    NoReplicas,
}

impl WriteGate {
    /// The error a write gets, or `None` while writes are taken.
    pub fn refusal(self) -> Option<&'static [u8]> {
        match self {
            Self::Open => None,
            Self::Replica => Some(READONLY_LINE),
            Self::Fenced => Some(FENCED_LINE),
            Self::NoReplicas => Some(NOREPLICAS_LINE),
        }
    }
}
pub const UNCOPIED_LINE: &[u8] = b"ERR replication does not copy this command";
pub const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(30);
pub const QUEUE_FRAMES: usize = 1024;
pub const QUEUE_BYTES: usize = 16 * 1024 * 1024;
const SHIPPER_READ_BYTES: usize = 16 * 1024 * 1024;
pub const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// First reconnect pause. Unit tests start replicas before primaries and must not wait a second.
#[cfg(not(test))]
const RETRY_START: Duration = RETRY_INTERVAL;
#[cfg(test)]
const RETRY_START: Duration = Duration::from_millis(20);
pub const SHIPPER_POLL: Duration = Duration::from_millis(10);
pub const MAX_REPLICAS: usize = 16;
/// Pause after an accept error (such as running out of file descriptors) before retrying.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(100);

const HANDSHAKE_PREFIX: usize = 24;
const PASSWORD_LEN_OFFSET: usize = 22;
/// Identity of one data directory's write history.
pub type HistoryId = [u8; HISTORY_LEN];

pub const HISTORY_LEN: usize = 16;
/// History id (16), shard count (4), epoch (8), client port (2) and client host length (2)
/// follow the password; the host and the checksum close the frame.
const HANDSHAKE_TAIL: usize = HISTORY_LEN + 4 + 8 + 2 + 2;
const HANDSHAKE_MIN: usize = HANDSHAKE_PREFIX + HANDSHAKE_TAIL + CHECKSUM_LEN;
/// The reply up to its first checksum; every reply to a current handshake is followed by
/// the primary's client address (see [`encode_address`]).
const REPLY_LEN: usize = 8 + 2 + 8 + HISTORY_LEN + 8 + CHECKSUM_LEN;
/// Client port (2), host length (2) and checksum (4) of an address frame, around the host.
const ADDRESS_FRAME_MIN: usize = 2 + 2 + CHECKSUM_LEN;
const CHECKSUM_LEN: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum HandshakeStatus {
    Ok = 1,
    Gap = 2,
    Auth = 3,
    Shard = 4,
    Version = 5,
    Ahead = 6,
    Local = 7,
    Snapshot = 8,
    /// The replica follows another write history than this primary's.
    History = 9,
    /// The other side has seen a newer epoch: this primary was replaced.
    Stale = 10,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handshake {
    pub version: u16,
    pub shard_id: u32,
    pub after_sequence: u64,
    pub password: Vec<u8>,
    /// History of the primary this replica last followed; all zero when it has none.
    pub history: HistoryId,
    pub shard_count: u32,
    /// Highest epoch this replica has seen; a primary below it was replaced.
    pub epoch: u64,
    /// Where clients reach this replica shard; `None` keeps it out of the primary's topology.
    pub announce: Option<Endpoint>,
}

/// Where clients reach one shard of a node: the host and the shard's own client port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

/// Port, host length and host of an optional endpoint; port 0 and no host stand for none.
fn put_endpoint(bytes: &mut Vec<u8>, endpoint: Option<&Endpoint>) -> Result<(), ()> {
    let (port, host) = endpoint.map_or((0, &b""[..]), |endpoint| {
        (endpoint.port, endpoint.host.as_bytes())
    });
    let Ok(host_len) = u16::try_from(host.len()) else {
        return Err(());
    };

    put_u16(bytes, port);
    put_u16(bytes, host_len);
    bytes.extend_from_slice(host);
    Ok(())
}

/// Longest host a node announces; longer ones are not announced.
pub const MAX_ANNOUNCED_HOST: usize = 255;

/// Whether `host` may be written into `CLUSTER NODES` and `INFO`: a host name or an IP
/// address (with an IPv6 zone or brackets), nothing that could split a line or a field.
pub fn announceable_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= MAX_ANNOUNCED_HOST
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".-_:%[]".contains(&byte))
}

/// An announced endpoint; a host that is not [`announceable_host`] is treated as no
/// announcement, so a peer cannot write arbitrary text into the cluster answers.
fn read_endpoint(port: u16, host: &[u8]) -> Result<Option<Endpoint>, FrameError> {
    if port == 0 || host.is_empty() {
        return Ok(None);
    }

    let host = std::str::from_utf8(host).map_err(|_| "client host is not UTF-8")?;

    if !announceable_host(host) {
        warn!(
            host = host
                .chars()
                .take(64)
                .collect::<String>()
                .escape_debug()
                .to_string(),
            "announced client host is not a host name or address; not listed"
        );

        return Ok(None);
    }

    Ok(Some(Endpoint {
        host: host.to_owned(),
        port,
    }))
}

/// The frame that follows every reply to a current handshake: the primary shard's client
/// address, with its own checksum.
pub fn encode_address(endpoint: Option<&Endpoint>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ADDRESS_FRAME_MIN + 64);

    // A host too long for the frame is announced as none.
    if put_endpoint(&mut bytes, endpoint).is_err() {
        bytes.clear();
        let _ = put_endpoint(&mut bytes, None);
    }

    append_checksum(&mut bytes);
    bytes
}

pub fn decode_address(bytes: &[u8]) -> Result<Option<Endpoint>, FrameError> {
    if bytes.len() < ADDRESS_FRAME_MIN
        || bytes.len() != ADDRESS_FRAME_MIN + usize::from(read_u16(bytes, 2))
    {
        return Err("address frame length");
    }

    if !checksum_matches(bytes) {
        return Err("address frame checksum mismatch");
    }

    read_endpoint(read_u16(bytes, 0), &bytes[4..bytes.len() - CHECKSUM_LEN])
}

pub fn encode_handshake(handshake: &Handshake) -> Result<Vec<u8>, ()> {
    let Ok(password_len) = u16::try_from(handshake.password.len()) else {
        return Err(());
    };

    let mut bytes = Vec::with_capacity(HANDSHAKE_MIN + handshake.password.len() + 64);

    bytes.extend_from_slice(HANDSHAKE_MAGIC);
    put_u16(&mut bytes, handshake.version);
    put_u32(&mut bytes, handshake.shard_id);
    put_u64(&mut bytes, handshake.after_sequence);
    put_u16(&mut bytes, password_len);
    bytes.extend_from_slice(&handshake.password);
    bytes.extend_from_slice(&handshake.history);
    put_u32(&mut bytes, handshake.shard_count);
    put_u64(&mut bytes, handshake.epoch);
    put_endpoint(&mut bytes, handshake.announce.as_ref())?;
    append_checksum(&mut bytes);
    Ok(bytes)
}

/// Why a handshake or reply frame was refused; it ends up in the connection's error.
pub type FrameError = &'static str;

pub fn decode_handshake(bytes: &[u8]) -> Result<Handshake, FrameError> {
    if bytes.len() < HANDSHAKE_MIN {
        return Err("short handshake");
    }

    if &bytes[..HANDSHAKE_MAGIC.len()] != HANDSHAKE_MAGIC {
        return Err("bad handshake magic");
    }

    let password_len = usize::from(read_u16(bytes, PASSWORD_LEN_OFFSET));
    let tail = HANDSHAKE_PREFIX + password_len;

    if bytes.len() < HANDSHAKE_MIN + password_len {
        return Err("handshake length does not match its password length");
    }

    let host_len = usize::from(read_u16(bytes, tail + HANDSHAKE_TAIL - 2));

    if bytes.len() != HANDSHAKE_MIN + password_len + host_len {
        return Err("handshake length does not match its password and host lengths");
    }

    if !checksum_matches(bytes) {
        return Err("handshake checksum mismatch");
    }

    let version = read_u16(bytes, 8);
    let shard_id = read_u32(bytes, 10);
    let after_sequence = read_u64(bytes, 14);
    let password = bytes[HANDSHAKE_PREFIX..tail].to_vec();
    let mut history = [0; HISTORY_LEN];

    history.copy_from_slice(&bytes[tail..tail + HISTORY_LEN]);
    let shard_count = read_u32(bytes, tail + HISTORY_LEN);
    let epoch = read_u64(bytes, tail + HISTORY_LEN + 4);
    let port = read_u16(bytes, tail + HISTORY_LEN + 12);
    let host_at = tail + HANDSHAKE_TAIL;
    let announce = read_endpoint(port, &bytes[host_at..host_at + host_len])?;

    Ok(Handshake {
        version,
        shard_id,
        after_sequence,
        password,
        history,
        shard_count,
        epoch,
        announce,
    })
}

pub fn encode_reply(status: HandshakeStatus, next_sequence: u64) -> Vec<u8> {
    encode_reply_with_history(status, next_sequence, &Origin::NONE)
}

pub fn encode_reply_with_history(
    status: HandshakeStatus,
    next_sequence: u64,
    origin: &Origin,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(REPLY_LEN);

    bytes.extend_from_slice(HANDSHAKE_MAGIC);
    put_u16(&mut bytes, status as u16);
    put_u64(&mut bytes, next_sequence);
    bytes.extend_from_slice(&origin.history);
    put_u64(&mut bytes, origin.epoch);
    append_checksum(&mut bytes);
    bytes
}

/// A refusal in the reply format of builds with a history id but no epoch.
fn encode_history_only_reply(status: HandshakeStatus) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HISTORY_ONLY_REPLY_LEN);

    bytes.extend_from_slice(HANDSHAKE_MAGIC);
    put_u16(&mut bytes, status as u16);
    put_u64(&mut bytes, 0);
    bytes.extend_from_slice(&[0; HISTORY_LEN]);
    append_checksum(&mut bytes);
    bytes
}

/// A refusal in the reply format of builds before the history id: magic, status, sequence,
/// checksum.
fn encode_legacy_reply(status: HandshakeStatus) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(LEGACY_REPLY_LEN);

    bytes.extend_from_slice(HANDSHAKE_MAGIC);
    put_u16(&mut bytes, status as u16);
    put_u64(&mut bytes, 0);
    append_checksum(&mut bytes);
    bytes
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn decode_reply(bytes: &[u8]) -> Result<(HandshakeStatus, u64), FrameError> {
    decode_reply_with_history(bytes).map(|(status, next, _)| (status, next))
}

pub fn decode_reply_with_history(
    bytes: &[u8],
) -> Result<(HandshakeStatus, u64, Origin), FrameError> {
    if bytes.len() != REPLY_LEN {
        return Err("handshake reply length");
    }

    if &bytes[..HANDSHAKE_MAGIC.len()] != HANDSHAKE_MAGIC {
        return Err("bad handshake reply magic");
    }

    if !checksum_matches(bytes) {
        return Err("handshake reply checksum mismatch");
    }

    let status = status_from_u16(read_u16(bytes, 8))?;
    let next_sequence = read_u64(bytes, 10);
    let mut history = [0; HISTORY_LEN];

    history.copy_from_slice(&bytes[18..18 + HISTORY_LEN]);
    let epoch = read_u64(bytes, 18 + HISTORY_LEN);

    Ok((status, next_sequence, Origin { history, epoch }))
}

/// Write-history identity of `data_dir`: random, created with the directory, so a wiped or
/// restored directory is a different history even if its sequence numbers line up.
pub fn load_or_create_history(data_dir: &Path) -> io::Result<HistoryId> {
    let path = data_dir.join("HISTORY");

    if let Some(id) = read_history_file(&path) {
        return Ok(id);
    }

    std::fs::create_dir_all(data_dir)?;
    let id = random_history_id();
    let staged = data_dir.join(format!(
        "HISTORY.{}.tmp",
        id.iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ));

    std::fs::write(&staged, id)?;
    // Write access: Windows refuses to flush a read-only handle.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&staged)?
        .sync_all()?;
    // Linking never replaces: when another shard created the file first, its id wins.
    let linked = std::fs::hard_link(&staged, &path);

    let _ = std::fs::remove_file(&staged);
    #[cfg(unix)]
    std::fs::File::open(data_dir)?.sync_all()?;

    match linked {
        Ok(()) => Ok(id),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            read_history_file(&path).ok_or(error)
        }
        Err(error) => Err(error),
    }
}

/// Name of the file holding the history shard `shard_id`'s log continues. Shards resync on
/// their own, so each keeps its own record.
pub fn upstream_file_name(shard_id: u32) -> String {
    format!("UPSTREAM_{shard_id:04}")
}

/// A primary's write history and epoch, as its replicas see them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Origin {
    pub history: HistoryId,
    pub epoch: u64,
}

impl Origin {
    pub const NONE: Self = Self {
        history: [0; HISTORY_LEN],
        epoch: 0,
    };
}

/// The history one primary shard writes, and the history it forked from when a replica was
/// promoted: replicas of the old primary that did not pass the fork continue from their log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lineage {
    pub origin: Origin,
    /// History this one continues up to `fork_sequence`; zeros when it has none.
    pub parent: HistoryId,
    pub fork_sequence: u64,
}

impl Lineage {
    /// A history of its own with no parent: a new primary, or one written by an older build.
    pub fn root(history: HistoryId) -> Self {
        Self {
            origin: Origin { history, epoch: 0 },
            parent: [0; HISTORY_LEN],
            fork_sequence: 0,
        }
    }

    /// Whether a replica that followed `history` through `after` can continue from this log.
    /// A replica past the fork holds records this primary never saw and must take a copy.
    pub fn continues(&self, history: &HistoryId, after: u64) -> bool {
        *history == self.origin.history
            || (self.parent != [0; HISTORY_LEN]
                && *history == self.parent
                && after <= self.fork_sequence)
    }
}

const LINEAGE_LEN: usize = HISTORY_LEN * 2 + 8 + 8;

/// Name of the file holding primary shard `shard_id`'s lineage.
pub fn lineage_file_name(shard_id: u32) -> String {
    format!("LINEAGE_{shard_id:04}")
}

/// The lineage primary shard `shard_id` writes, deciding it on the shard's first start as a
/// writer. A shard whose last role was replica (it has an upstream record and no lineage of
/// its own) is being promoted: it forks a new history from the one it followed, at
/// `last_sequence`, one epoch above it. A shard with neither continues the directory's
/// `HISTORY`, as older builds did.
pub fn resolve_lineage(data_dir: &Path, shard_id: u32, last_sequence: u64) -> io::Result<Lineage> {
    let path = data_dir.join(lineage_file_name(shard_id));

    if let Some(lineage) = read_lineage_file(&path) {
        return Ok(lineage);
    }

    let lineage = match read_origin_file(&data_dir.join(upstream_file_name(shard_id))) {
        Some(followed) => Lineage {
            origin: Origin {
                history: random_history_id(),
                epoch: followed.epoch.saturating_add(1),
            },
            parent: followed.history,
            fork_sequence: last_sequence,
        },
        None => Lineage::root(load_or_create_history(data_dir)?),
    };

    let mut bytes = Vec::with_capacity(LINEAGE_LEN);

    bytes.extend_from_slice(&lineage.origin.history);
    bytes.extend_from_slice(&lineage.parent);
    put_u64(&mut bytes, lineage.fork_sequence);
    put_u64(&mut bytes, lineage.origin.epoch);
    write_durably(&path, &bytes)?;

    Ok(lineage)
}

/// The lineage writer shard `shard_id` recorded, without deciding one; `None` for a shard that
/// never replicated (its log is the root history, epoch zero).
pub fn read_lineage(data_dir: &Path, shard_id: u32) -> Option<Lineage> {
    read_lineage_file(&data_dir.join(lineage_file_name(shard_id)))
}

fn read_lineage_file(path: &Path) -> Option<Lineage> {
    let bytes = std::fs::read(path).ok()?;

    if bytes.len() != LINEAGE_LEN {
        return None;
    }

    let mut history = [0; HISTORY_LEN];
    let mut parent = [0; HISTORY_LEN];

    history.copy_from_slice(&bytes[..HISTORY_LEN]);
    parent.copy_from_slice(&bytes[HISTORY_LEN..2 * HISTORY_LEN]);

    Some(Lineage {
        origin: Origin {
            history,
            epoch: read_u64(&bytes, 2 * HISTORY_LEN + 8),
        },
        parent,
        fork_sequence: read_u64(&bytes, 2 * HISTORY_LEN),
    })
}

/// History of the primary shard `shard_id` of this replica last synchronised from; zeros when
/// unknown, which makes the primary refuse a resume and send a full copy.
///
/// A directory copied from a primary has no upstream record but carries the primary's own
/// `HISTORY`, which is what it continues. A directory with the older shared `UPSTREAM` file
/// cannot tell which shard that record was written for, so it claims nothing: each shard
/// takes one full copy and records its own id.
#[cfg(test)]
pub fn upstream_history(data_dir: &Path, shard_id: u32) -> HistoryId {
    upstream_origin(data_dir, shard_id).history
}

/// History and epoch of the primary shard `shard_id` last synchronised from (see
/// [`upstream_history`]); the epoch is zero when unknown.
pub fn upstream_origin(data_dir: &Path, shard_id: u32) -> Origin {
    read_origin_file(&data_dir.join(upstream_file_name(shard_id)))
        .or_else(|| {
            if data_dir.join("UPSTREAM").exists() {
                None
            } else {
                read_history_file(&data_dir.join("HISTORY"))
                    .map(|history| Origin { history, epoch: 0 })
            }
        })
        .unwrap_or(Origin::NONE)
}

/// Records that shard `shard_id`'s log now continues `origin`'s history. Called only once the
/// log really does: on a resume the primary accepted, or after a snapshot from it is
/// installed. The shard no longer writes a history of its own, so its lineage goes: started as
/// a writer again, it forks from `origin`.
pub fn record_upstream_history(data_dir: &Path, shard_id: u32, origin: &Origin) -> io::Result<()> {
    let path = data_dir.join(upstream_file_name(shard_id));

    if read_origin_file(&path) != Some(*origin) {
        let mut bytes = Vec::with_capacity(HISTORY_LEN + 8);

        bytes.extend_from_slice(&origin.history);
        put_u64(&mut bytes, origin.epoch);
        write_durably(&path, &bytes)?;
    }

    match std::fs::remove_file(data_dir.join(lineage_file_name(shard_id))) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// An upstream record: a history id, followed by the epoch in files this build writes.
fn read_origin_file(path: &Path) -> Option<Origin> {
    let bytes = std::fs::read(path).ok()?;
    let mut history = [0; HISTORY_LEN];

    match bytes.len() {
        HISTORY_LEN => {
            history.copy_from_slice(&bytes);
            Some(Origin { history, epoch: 0 })
        }
        len if len == HISTORY_LEN + 8 => {
            history.copy_from_slice(&bytes[..HISTORY_LEN]);
            Some(Origin {
                history,
                epoch: read_u64(&bytes, HISTORY_LEN),
            })
        }
        _ => None,
    }
}

fn read_history_file(path: &Path) -> Option<HistoryId> {
    let bytes = std::fs::read(path).ok()?;

    <HistoryId>::try_from(bytes.as_slice()).ok()
}

pub(crate) fn write_durably(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");

    std::fs::write(&temporary, bytes)?;
    // Write access: Windows refuses to flush a read-only handle.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&temporary)?
        .sync_all()?;
    std::fs::rename(&temporary, path)?;

    if let Some(directory) = path.parent() {
        #[cfg(unix)]
        std::fs::File::open(directory)?.sync_all()?;
        #[cfg(not(unix))]
        let _ = directory;
    }

    Ok(())
}

fn random_history_id() -> HistoryId {
    use std::hash::{BuildHasher, Hasher};

    let mut id = [0; HISTORY_LEN];

    for chunk in id.chunks_mut(8) {
        // `RandomState` is seeded from the operating system for each instance.
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();

        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_nanos()),
        );
        hasher.write_u32(std::process::id());
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }

    if id == [0; HISTORY_LEN] {
        id[0] = 1;
    }

    id
}

#[derive(Debug, PartialEq, Eq)]
pub enum ApplyFailure {
    BadFrame,
    NeedsSnapshot,
    /// The frame's sequence is already in this replica's log: an earlier link queued it and
    /// the next link, which resumes from the applied cursor, sent it again.
    Applied,
    Sequence,
    Shard,
    Persist(String),
}

pub fn accept_frame(
    wal: &mut WalSink,
    frame: &[u8],
    limits: ProtocolLimits,
) -> Result<(u8, crate::OwnedMutation), ApplyFailure> {
    let record = decode_record(frame, wal.shard_id(), limits).map_err(decode_failure)?;

    if record.sequence != wal.last_sequence() + 1 {
        return Err(ApplyFailure::Sequence);
    }

    wal.append_frame(frame).map_err(persist_failure)?;
    Ok((record.database, record.mutation))
}

/// Refuse a delta whose prerequisite JSON document is no longer available locally.
/// Only a newer primary snapshot can safely recover the document: an expired
/// deadline does not prove that a later PERSIST left the document absent.
pub(crate) fn accept_replica_frame(
    shard: &mut KvShard,
    wal: &mut WalSink,
    _checkpoints: &CheckpointManager,
    _config: &PersistenceConfig,
    frame: &[u8],
    limits: ProtocolLimits,
) -> Result<(u8, OwnedMutation, bool), ApplyFailure> {
    let record = decode_record(frame, wal.shard_id(), limits).map_err(decode_failure)?;
    // The link resumes from the last applied sequence, while frames an earlier link already
    // queued may still be waiting in the shard's queue: the new link resends those. Under one
    // history a sequence names one record, so the copy is already in the log.
    if record.sequence <= wal.last_sequence() {
        return Err(ApplyFailure::Applied);
    }
    if record.sequence != wal.last_sequence() + 1 {
        return Err(ApplyFailure::Sequence);
    }

    let missing = match &record.mutation {
        OwnedMutation::Transaction { mutations } => {
            transaction_needs_snapshot(shard, record.database, mutations)
        }
        mutation => {
            let previous = shard.active_database();
            shard.enter(record.database);
            let missing = needs_snapshot(shard, mutation);
            shard.enter(previous);
            missing
        }
    };
    if missing {
        return Err(ApplyFailure::NeedsSnapshot);
    }

    wal.append_frame(frame).map_err(persist_failure)?;
    Ok((record.database, record.mutation, false))
}

/// Track only key kinds, never clone documents or apply edits during the WAL preflight.
/// The map is local to transactions with JSON/TTL prerequisites; ordinary frames
/// retain their single decode-and-append path.
struct TransactionKinds {
    changed: HashMap<(u8, Vec<u8>), Option<crate::store::ValueKind>>,
    original: [Option<u8>; 256],
}

impl TransactionKinds {
    fn new() -> Self {
        Self {
            changed: HashMap::new(),
            original: std::array::from_fn(|db| Some(db as u8)),
        }
    }

    fn kind(&self, shard: &mut KvShard, db: u8, key: &[u8]) -> Option<crate::store::ValueKind> {
        if let Some(kind) = self.changed.get(&(db, key.to_vec())) {
            return *kind;
        }
        let original = self.original[db as usize]?;
        shard.enter(original);
        shard.value_kind(key)
    }

    fn set(&mut self, db: u8, key: &[u8], kind: Option<crate::store::ValueKind>) {
        self.changed.insert((db, key.to_vec()), kind);
    }

    fn clear_database(&mut self, db: u8) {
        self.changed.retain(|(database, _), _| *database != db);
        self.original[db as usize] = None;
    }

    fn swap(&mut self, first: u8, second: u8) {
        self.original.swap(first as usize, second as usize);
        let mut changed = HashMap::with_capacity(self.changed.len());
        for ((db, key), kind) in self.changed.drain() {
            let db = if db == first {
                second
            } else if db == second {
                first
            } else {
                db
            };
            changed.insert((db, key), kind);
        }
        self.changed = changed;
    }
}

fn transaction_needs_snapshot(shard: &mut KvShard, db: u8, mutations: &[OwnedMutation]) -> bool {
    use crate::store::ValueKind;

    if !mutations.iter().any(|mutation| {
        matches!(
            mutation,
            OwnedMutation::JsonSetPath { .. }
                | OwnedMutation::JsonMergePath { .. }
                | OwnedMutation::JsonDelPath { .. }
                | OwnedMutation::Persist { .. }
                | OwnedMutation::ExpireAt { .. }
        )
    }) {
        return false;
    }

    let previous = shard.active_database();
    let mut kinds = TransactionKinds::new();
    let mut missing = false;
    for mutation in mutations {
        match mutation {
            OwnedMutation::JsonSetPath { key, .. }
            | OwnedMutation::JsonMergePath { key, .. }
            | OwnedMutation::JsonDelPath { key, .. } => {
                missing = kinds.kind(shard, db, key) != Some(ValueKind::Json);
            }
            OwnedMutation::Persist { key } => {
                missing = kinds.kind(shard, db, key).is_none();
            }
            // A replica keeps keys past its own clock until the primary deletes them, so
            // a deadline for a key it does not hold is a divergence.
            OwnedMutation::ExpireAt { key, .. } => {
                missing = kinds.kind(shard, db, key).is_none();
            }
            // A replica applies frames on a paused clock and keeps a key past its deadline
            // until the primary deletes it, so a value with a deadline always leaves a key.
            OwnedMutation::SetBytesExpiring { key, .. }
            | OwnedMutation::SetBytes { key, .. }
            | OwnedMutation::SetRange { key, .. }
            | OwnedMutation::IdempotencyPut { key, .. }
            | OwnedMutation::SetInteger { key, .. } => kinds.set(db, key, Some(ValueKind::Bytes)),
            OwnedMutation::JsonPut { key, .. } => kinds.set(db, key, Some(ValueKind::Json)),
            OwnedMutation::SetVersioned { key, .. } => {
                kinds.set(db, key, Some(ValueKind::Versioned))
            }
            OwnedMutation::LeasePut { key, .. } => kinds.set(db, key, Some(ValueKind::Lease)),
            OwnedMutation::LimitPut { key, .. } => kinds.set(db, key, Some(ValueKind::Limit)),
            OwnedMutation::OncePut { key, .. } => kinds.set(db, key, Some(ValueKind::Once)),
            OwnedMutation::ListPushFront { key, values }
            | OwnedMutation::ListPushBack { key, values }
                if !values.is_empty() =>
            {
                kinds.set(db, key, Some(ValueKind::List));
            }
            OwnedMutation::SetAdd { key, members } if !members.is_empty() => {
                kinds.set(db, key, Some(ValueKind::Set));
            }
            OwnedMutation::SetStore { key, members } => {
                kinds.set(db, key, (!members.is_empty()).then_some(ValueKind::Set));
            }
            OwnedMutation::HashSet { key, pairs } if !pairs.is_empty() => {
                kinds.set(db, key, Some(ValueKind::Hash));
            }
            OwnedMutation::SortedSetAdd { key, pairs } if !pairs.is_empty() => {
                kinds.set(db, key, Some(ValueKind::SortedSet));
            }
            OwnedMutation::BloomReserve { key, .. }
            | OwnedMutation::BloomAdd { key, .. }
            | OwnedMutation::BloomAddMany { key, .. } => {
                kinds.set(db, key, Some(ValueKind::BloomFilter));
            }
            OwnedMutation::StreamAppend { key, .. } => kinds.set(db, key, Some(ValueKind::Stream)),
            OwnedMutation::StreamGroupCreate {
                key,
                mkstream: true,
                ..
            } => {
                kinds.set(db, key, Some(ValueKind::Stream));
            }
            OwnedMutation::SemaphoreAcquire { key, .. } => {
                kinds.set(db, key, Some(ValueKind::Semaphore));
            }
            OwnedMutation::Delete { keys } => {
                for key in keys {
                    kinds.set(db, key, None);
                }
            }
            OwnedMutation::Rename {
                source,
                destination,
            } => {
                let kind = kinds.kind(shard, db, source);
                missing = kind.is_none();
                kinds.set(db, source, None);
                kinds.set(db, destination, kind);
            }
            OwnedMutation::MoveKey { key, destination } => {
                let kind = kinds.kind(shard, db, key);
                missing = kind.is_none() || kinds.kind(shard, *destination, key).is_some();
                kinds.set(db, key, None);
                kinds.set(*destination, key, kind);
            }
            OwnedMutation::FlushDatabase => kinds.clear_database(db),
            OwnedMutation::FlushAll => {
                kinds.changed.clear();
                kinds.original.fill(None);
            }
            OwnedMutation::SwapDatabases { first, second } => kinds.swap(*first, *second),
            // These operations can remove the final member. Treat their result as
            // absent rather than risk accepting a later path/TTL dependency.
            OwnedMutation::ListPopFront { key }
            | OwnedMutation::ListPopBack { key }
            | OwnedMutation::ListTrim { key, .. }
            | OwnedMutation::ListRemove { key, .. }
            | OwnedMutation::SetRemove { key, .. }
            | OwnedMutation::HashDelete { key, .. }
            | OwnedMutation::SortedSetRemove { key, .. }
            | OwnedMutation::StreamTrim { key, .. }
            | OwnedMutation::StreamTrimMinId { key, .. }
            | OwnedMutation::SemaphoreRelease { key, .. } => kinds.set(db, key, None),
            OwnedMutation::SetMove {
                source,
                destination,
                ..
            } => {
                kinds.set(db, source, None);
                kinds.set(db, destination, Some(ValueKind::Set));
            }
            // An empty push or add creates nothing, and a group without MKSTREAM needs a
            // stream that already exists.
            OwnedMutation::ListPushFront { .. }
            | OwnedMutation::ListPushBack { .. }
            | OwnedMutation::SetAdd { .. }
            | OwnedMutation::HashSet { .. }
            | OwnedMutation::SortedSetAdd { .. }
            | OwnedMutation::StreamGroupCreate { .. } => {}
            // These change a value in place: they never create or remove a key. A replica
            // keeps fields past their deadline until the primary deletes them.
            OwnedMutation::ListSet { .. }
            | OwnedMutation::ListInsert { .. }
            | OwnedMutation::HashFieldExpire { .. }
            | OwnedMutation::HashFieldPersist { .. }
            | OwnedMutation::StreamDelete { .. }
            | OwnedMutation::StreamSetId { .. }
            | OwnedMutation::StreamClaim { .. }
            | OwnedMutation::StreamReclaim { .. }
            | OwnedMutation::StreamForceClaim { .. }
            | OwnedMutation::StreamAck { .. }
            | OwnedMutation::StreamGroupDestroy { .. }
            | OwnedMutation::StreamGroupSetId { .. }
            | OwnedMutation::StreamGroupDelConsumer { .. }
            | OwnedMutation::StreamGroupCreateConsumer { .. }
            | OwnedMutation::StreamHold { .. }
            | OwnedMutation::StreamGroupAdvance { .. } => {}
            // A transaction record never nests another one, nor a slot move's records
            // (written on their own, and never while replication is on).
            OwnedMutation::Transaction { .. }
            | OwnedMutation::SlotCopy { .. }
            | OwnedMutation::SlotsMoved { .. }
            | OwnedMutation::SlotsIncoming { .. }
            | OwnedMutation::SlotsGiven { .. } => {}
        }
        if missing {
            break;
        }
    }
    shard.enter(previous);
    missing
}

fn needs_snapshot(shard: &mut KvShard, mutation: &OwnedMutation) -> bool {
    match mutation {
        // The primary records a rename or move only for a key it holds: a replica without
        // it has diverged and would fail the apply.
        OwnedMutation::Rename { source, .. } => shard.value_kind(source).is_none(),
        OwnedMutation::MoveKey { key, destination } => {
            if shard.value_kind(key).is_none() {
                return true;
            }
            let current = shard.active_database();
            shard.enter(*destination);
            let taken = shard.value_kind(key).is_some();
            shard.enter(current);
            taken
        }
        OwnedMutation::JsonSetPath { key, .. }
        | OwnedMutation::JsonMergePath { key, .. }
        | OwnedMutation::JsonDelPath { key, .. } => {
            shard.value_kind(key) != Some(crate::store::ValueKind::Json)
        }
        OwnedMutation::Persist { key } | OwnedMutation::ExpireAt { key, .. } => {
            shard.value_kind(key).is_none()
        }
        // Every other record applies whether or not the replica holds the key.
        OwnedMutation::IdempotencyPut { .. }
        | OwnedMutation::SetBytes { .. }
        | OwnedMutation::SetBytesExpiring { .. }
        | OwnedMutation::JsonPut { .. }
        | OwnedMutation::SetInteger { .. }
        | OwnedMutation::Delete { .. }
        | OwnedMutation::ListPushFront { .. }
        | OwnedMutation::ListPopFront { .. }
        | OwnedMutation::ListPushBack { .. }
        | OwnedMutation::ListPopBack { .. }
        | OwnedMutation::ListTrim { .. }
        | OwnedMutation::SetRange { .. }
        | OwnedMutation::ListSet { .. }
        | OwnedMutation::ListInsert { .. }
        | OwnedMutation::ListRemove { .. }
        | OwnedMutation::SetAdd { .. }
        | OwnedMutation::SetRemove { .. }
        | OwnedMutation::SetStore { .. }
        | OwnedMutation::SetMove { .. }
        | OwnedMutation::HashSet { .. }
        | OwnedMutation::HashDelete { .. }
        | OwnedMutation::HashFieldExpire { .. }
        | OwnedMutation::HashFieldPersist { .. }
        | OwnedMutation::SemaphoreAcquire { .. }
        | OwnedMutation::SemaphoreRelease { .. }
        | OwnedMutation::SortedSetAdd { .. }
        | OwnedMutation::SortedSetRemove { .. }
        | OwnedMutation::BloomReserve { .. }
        | OwnedMutation::BloomAdd { .. }
        | OwnedMutation::BloomAddMany { .. }
        | OwnedMutation::StreamAppend { .. }
        | OwnedMutation::StreamDelete { .. }
        | OwnedMutation::StreamTrim { .. }
        | OwnedMutation::StreamTrimMinId { .. }
        | OwnedMutation::StreamSetId { .. }
        | OwnedMutation::StreamGroupCreate { .. }
        | OwnedMutation::StreamClaim { .. }
        | OwnedMutation::StreamReclaim { .. }
        | OwnedMutation::StreamForceClaim { .. }
        | OwnedMutation::StreamAck { .. }
        | OwnedMutation::StreamGroupDestroy { .. }
        | OwnedMutation::StreamGroupSetId { .. }
        | OwnedMutation::StreamGroupDelConsumer { .. }
        | OwnedMutation::StreamGroupCreateConsumer { .. }
        | OwnedMutation::StreamHold { .. }
        | OwnedMutation::StreamGroupAdvance { .. }
        | OwnedMutation::FlushDatabase
        | OwnedMutation::FlushAll
        | OwnedMutation::SwapDatabases { .. }
        | OwnedMutation::LeasePut { .. }
        | OwnedMutation::SetVersioned { .. }
        | OwnedMutation::LimitPut { .. }
        | OwnedMutation::OncePut { .. }
        | OwnedMutation::SlotCopy { .. }
        | OwnedMutation::SlotsMoved { .. }
        | OwnedMutation::SlotsIncoming { .. }
        | OwnedMutation::SlotsGiven { .. }
        | OwnedMutation::Transaction { .. } => false,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn apply_frame(
    shard: &mut KvShard,
    wal: &mut WalSink,
    frame: &[u8],
    limits: ProtocolLimits,
) -> Result<(), ApplyFailure> {
    let (database, mutation) = accept_frame(wal, frame, limits)?;

    apply_mutation(shard, database, &mutation).map_err(persist_failure)
}

pub const PUBLISH_MAGIC: &[u8; 8] = b"RUVPUB01";
pub const ACK_MAGIC: &[u8; 8] = b"RUVACK01";
pub const CONTROL_MAGIC: &[u8; 8] = b"RUVCTL01";
const ACK_LEN: usize = 16;

pub fn encode_ack(sequence: u64) -> [u8; ACK_LEN] {
    let mut bytes = [0; ACK_LEN];

    bytes[..8].copy_from_slice(ACK_MAGIC);
    bytes[8..].copy_from_slice(&sequence.to_le_bytes());
    bytes
}

pub fn decode_ack(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != ACK_LEN || &bytes[..8] != ACK_MAGIC {
        return None;
    }

    Some(u64::from_le_bytes(bytes[8..].try_into().ok()?))
}

/// One connected replica of a shard: what it acknowledged, and where clients reach it.
#[derive(Clone)]
struct AckSlot {
    sequence: u64,
    at: std::time::Instant,
    /// False until the replica's first acknowledgement, which comes once any full copy is
    /// installed: only then does it serve reads.
    acknowledged: bool,
    endpoint: Option<Endpoint>,
}

/// A replica a shard ships to that serves reads, as `CLUSTER SLOTS`, `ROLE` and `INFO` show it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaPeer {
    pub endpoint: Endpoint,
    /// Highest sequence the replica reported durable.
    pub sequence: u64,
    /// Time since its last acknowledgement.
    pub lag: Duration,
}

/// Durable sequences reported by connected replicas and the addresses they announced, and
/// on a replica the address of the primary it follows. `SET` does not read this.
pub struct DurableAcks {
    /// Per connected replica: the sequence it reported and when it last reported.
    slots: Mutex<Vec<Option<AckSlot>>>,
    /// Where clients reach the primary this shard follows, from the last accepted handshake.
    primary: Mutex<Option<Endpoint>>,
    notify: tokio::sync::Notify,
}

impl DurableAcks {
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(Vec::new()),
            primary: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub fn reserve(&self, index: usize) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        if slots.len() <= index {
            slots.resize(index + 1, None);
        }

        slots[index] = Some(AckSlot {
            sequence: 0,
            at: std::time::Instant::now(),
            acknowledged: false,
            endpoint: None,
        });
    }

    /// Records where clients reach the replica in slot `index`.
    pub fn announce(&self, index: usize, endpoint: Option<Endpoint>) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        if let Some(Some(slot)) = slots.get_mut(index) {
            slot.endpoint = endpoint;
        }
    }

    pub fn observe(&self, index: usize, sequence: u64) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        if slots.len() <= index {
            slots.resize(index + 1, None);
        }

        let slot = slots[index].get_or_insert(AckSlot {
            sequence: 0,
            at: std::time::Instant::now(),
            acknowledged: false,
            endpoint: None,
        });
        let current = slot.sequence;

        slot.sequence = sequence.max(current);
        slot.at = std::time::Instant::now();
        slot.acknowledged = true;
        if sequence > current {
            self.notify.notify_waiters();
        }
    }

    /// Connected replicas that announced an address and acknowledged within
    /// [`LISTED_REPLICA_LAG`], in slot order.
    pub fn replicas(&self) -> Vec<ReplicaPeer> {
        let slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        slots
            .iter()
            .flatten()
            .filter(|slot| slot.acknowledged && slot.at.elapsed() <= LISTED_REPLICA_LAG)
            .filter_map(|slot| {
                Some(ReplicaPeer {
                    endpoint: slot.endpoint.clone()?,
                    sequence: slot.sequence,
                    lag: slot.at.elapsed(),
                })
            })
            .collect()
    }

    pub fn set_primary(&self, endpoint: Option<Endpoint>) {
        *self
            .primary
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = endpoint;
    }

    /// Where clients reach the primary this shard last followed, if it announced itself.
    pub fn primary(&self) -> Option<Endpoint> {
        self.primary
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn clear(&self, index: usize) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        if let Some(slot) = slots.get_mut(index) {
            *slot = None;
            self.notify.notify_waiters();
        }
    }

    /// How many connected replicas have synced `target` to disk.
    pub fn reached(&self, target: u64) -> u64 {
        let slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        slots
            .iter()
            .filter(|slot| slot.as_ref().is_some_and(|slot| slot.sequence >= target))
            .count() as u64
    }

    /// How many connected replicas acknowledged within `max_lag`.
    pub fn fresh(&self, max_lag: Duration) -> usize {
        let slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());

        slots
            .iter()
            .filter(|slot| {
                slot.as_ref()
                    .is_some_and(|slot| slot.at.elapsed() <= max_lag)
            })
            .count()
    }

    pub async fn notified(&self) {
        self.notify.notified().await;
    }
}

pub fn encode_copied_publish(sharded: bool, channel: &[u8], message: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();

    body.extend_from_slice(PUBLISH_MAGIC);
    body.push(u8::from(sharded));
    body.extend_from_slice(&(u32::try_from(channel.len()).unwrap_or(u32::MAX)).to_le_bytes());
    body.extend_from_slice(channel);
    body.extend_from_slice(&(u32::try_from(message.len()).unwrap_or(u32::MAX)).to_le_bytes());
    body.extend_from_slice(message);

    while 4 + body.len() + 4 < 32 {
        body.push(0);
    }

    let mut frame = Vec::with_capacity(4 + body.len() + 4);
    let total = u32::try_from(4 + body.len() + 4).unwrap_or(u32::MAX);

    frame.extend_from_slice(&total.to_le_bytes());
    frame.extend_from_slice(&body);
    let checksum = crc32c::crc32c(&frame);

    frame.extend_from_slice(&checksum.to_le_bytes());
    frame
}

/// What a primary shard tells its replicas besides its log: the highest sequence a durable
/// reply was released for, and whether a durable reply is waiting now.
#[derive(Debug, Default)]
pub struct DurableReplies {
    committed: AtomicU64,
    waiting: AtomicU32,
}

impl DurableReplies {
    pub fn committed(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    pub fn commit(&self, sequence: u64) {
        self.committed.fetch_max(sequence, Ordering::AcqRel);
    }

    /// Forgets what was committed: the shard follows another primary, and its log is no
    /// longer the one those sequence numbers named.
    pub fn reset(&self) {
        self.committed.store(0, Ordering::Release);
    }

    /// While at least one durable reply waits, replicas sync and acknowledge every batch.
    pub fn begin_wait(&self) {
        self.waiting.fetch_add(1, Ordering::AcqRel);
    }

    pub fn end_wait(&self) {
        self.waiting.fetch_sub(1, Ordering::AcqRel);
    }

    fn state(&self) -> (u64, bool) {
        (
            self.committed.load(Ordering::Acquire),
            self.waiting.load(Ordering::Acquire) != 0,
        )
    }
}

pub fn encode_control(committed: u64, ack_each: bool) -> Vec<u8> {
    let mut frame = Vec::with_capacity(32);

    frame.extend_from_slice(&32u32.to_le_bytes());
    frame.extend_from_slice(CONTROL_MAGIC);
    frame.extend_from_slice(&committed.to_le_bytes());
    frame.push(u8::from(ack_each));
    frame.resize(28, 0);
    let checksum = crc32c::crc32c(&frame);

    frame.extend_from_slice(&checksum.to_le_bytes());
    frame
}

/// `(committed, ack_each)` of a control frame; `None` for any other frame.
pub fn decode_control(frame: &[u8]) -> Option<(u64, bool)> {
    if frame.len() != 32 || &frame[4..12] != CONTROL_MAGIC {
        return None;
    }

    if crc32c::crc32c(&frame[..28]) != u32::from_le_bytes(frame[28..].try_into().ok()?) {
        return None;
    }

    Some((
        u64::from_le_bytes(frame[12..20].try_into().ok()?),
        frame[20] == 1,
    ))
}

pub fn decode_copied_publish(frame: &[u8]) -> Option<(bool, Vec<u8>, Vec<u8>)> {
    if frame.len() < 32 || u32::from_le_bytes(frame[..4].try_into().ok()?) as usize != frame.len() {
        return None;
    }

    if &frame[4..12] != PUBLISH_MAGIC {
        return None;
    }

    let checksum_at = frame.len() - 4;

    if crc32c::crc32c(&frame[..checksum_at])
        != u32::from_le_bytes(frame[checksum_at..].try_into().ok()?)
    {
        return None;
    }

    let sharded = frame[12] == 1;
    let channel_len = u32::from_le_bytes(frame[13..17].try_into().ok()?) as usize;
    let channel_at: usize = 17;
    let channel_end = channel_at.checked_add(channel_len)?;

    if channel_end + 4 > checksum_at {
        return None;
    }

    let message_len =
        u32::from_le_bytes(frame[channel_end..channel_end + 4].try_into().ok()?) as usize;
    let message_at = channel_end + 4;
    let message_end = message_at.checked_add(message_len)?;

    if message_end > checksum_at {
        return None;
    }

    Some((
        sharded,
        frame[channel_at..channel_end].to_vec(),
        frame[message_at..message_end].to_vec(),
    ))
}

pub struct PublishFanout {
    senders: Mutex<Vec<SyncSender<Vec<u8>>>>,
}

impl PublishFanout {
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(Vec::new()),
        }
    }

    pub fn register(&self) -> std::sync::mpsc::Receiver<Vec<u8>> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(64);

        self.senders
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(sender);
        receiver
    }

    pub fn send(&self, frame: Vec<u8>) {
        let mut senders = self
            .senders
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        senders.retain(|sender| match sender.try_send(frame.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => true,
            Err(TrySendError::Disconnected(_)) => false,
        });
    }
}

fn decode_failure(error: PersistenceError) -> ApplyFailure {
    match error {
        PersistenceError::Corruption(message) if message == "WAL record shard mismatch" => {
            ApplyFailure::Shard
        }

        _ => ApplyFailure::BadFrame,
    }
}

fn persist_failure(error: PersistenceError) -> ApplyFailure {
    ApplyFailure::Persist(error.to_string())
}

fn status_from_u16(value: u16) -> Result<HandshakeStatus, FrameError> {
    match value {
        1 => Ok(HandshakeStatus::Ok),
        2 => Ok(HandshakeStatus::Gap),
        3 => Ok(HandshakeStatus::Auth),
        4 => Ok(HandshakeStatus::Shard),
        5 => Ok(HandshakeStatus::Version),
        6 => Ok(HandshakeStatus::Ahead),
        7 => Ok(HandshakeStatus::Local),
        8 => Ok(HandshakeStatus::Snapshot),
        9 => Ok(HandshakeStatus::History),
        10 => Ok(HandshakeStatus::Stale),
        _ => Err("unknown handshake reply status"),
    }
}

fn checksum_matches(bytes: &[u8]) -> bool {
    let checksum_at = bytes.len() - CHECKSUM_LEN;
    let expected = read_u32(bytes, checksum_at);

    crc32c::crc32c(&bytes[..checksum_at]) == expected
}

fn append_checksum(bytes: &mut Vec<u8>) {
    let checksum = crc32c::crc32c(bytes);

    put_u32(bytes, checksum);
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub struct Shipment {
    pub listen: SocketAddr,
    pub shard_id: u32,
    pub published: Arc<AtomicU64>,
    pub data_dir: PathBuf,
    pub requirepass: Vec<u8>,
    pub limits: ProtocolLimits,
    pub max_replicas: usize,
    pub backlog_floor: Arc<AtomicU64>,
    pub replica_count: Arc<AtomicU64>,
    pub acks: Arc<DurableAcks>,
    pub fanout: Arc<PublishFanout>,
    pub snapshot_max_bytes: u64,
    pub shard_count: u32,
    pub lineage: Lineage,
    /// Highest epoch a refused replica has followed; above `lineage`'s, this primary was
    /// replaced and its shard stops taking writes.
    pub newer_epoch: Arc<AtomicU64>,
    /// Request a coordinated checkpoint after a rejected delta; never used by ordinary streams.
    pub snapshot_requests: Arc<AtomicU64>,
    /// Where clients reach this primary shard, sent to every replica with the reply.
    pub announce: Option<Endpoint>,
    /// Wraps each accepted replica connection in TLS; none leaves the link in cleartext.
    pub tls: Option<Arc<crate::tls::TlsAcceptor>>,
    /// Sent to every replica in control frames when it changes.
    pub durable_replies: Arc<DurableReplies>,
}

struct ReplicaProgress {
    floor: Arc<AtomicU64>,
    count: Arc<AtomicU64>,
    acks: Arc<DurableAcks>,
    cursors: Mutex<Vec<Option<u64>>>,
}

impl ReplicaProgress {
    fn new(floor: Arc<AtomicU64>, count: Arc<AtomicU64>, acks: Arc<DurableAcks>) -> Self {
        Self {
            floor,
            count,
            acks,
            cursors: Mutex::new(Vec::new()),
        }
    }

    fn hold(self: &Arc<Self>, cursor: u64) -> CursorHold {
        let mut cursors = self
            .cursors
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let index = cursors
            .iter()
            .position(|slot| slot.is_none())
            .unwrap_or_else(|| {
                cursors.push(None);
                cursors.len() - 1
            });

        cursors[index] = Some(cursor);
        self.acks.reserve(index);
        publish_floor(&self.floor, &self.count, &cursors);
        CursorHold {
            progress: Arc::clone(self),
            index,
        }
    }
}

struct CursorHold {
    progress: Arc<ReplicaProgress>,
    index: usize,
}

impl CursorHold {
    fn advance(&self, cursor: u64) {
        let mut cursors = self
            .progress
            .cursors
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if let Some(slot) = cursors.get_mut(self.index) {
            *slot = Some(cursor);
        }

        publish_floor(&self.progress.floor, &self.progress.count, &cursors);
    }

    fn ack(&self, sequence: u64) {
        self.progress.acks.observe(self.index, sequence);
    }

    fn announce(&self, endpoint: Option<Endpoint>) {
        self.progress.acks.announce(self.index, endpoint);
    }
}

impl Drop for CursorHold {
    fn drop(&mut self) {
        let mut cursors = self
            .progress
            .cursors
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if let Some(slot) = cursors.get_mut(self.index) {
            *slot = None;
        }

        self.progress.acks.clear(self.index);
        publish_floor(&self.progress.floor, &self.progress.count, &cursors);
    }
}

fn publish_floor(floor: &AtomicU64, count: &AtomicU64, cursors: &[Option<u64>]) {
    let connected = cursors.iter().flatten().copied();
    let mut retained = u64::MAX;
    let mut replicas = 0u64;

    for cursor in connected {
        retained = retained.min(cursor);
        replicas = replicas.saturating_add(1);
    }

    floor.store(retained, Ordering::Release);
    count.store(replicas, Ordering::Release);
}

pub struct ReplicationThread {
    stop: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<TcpStream>>>,
    join: Option<JoinHandle<()>>,
}

impl ReplicationThread {
    /// The thread that reads from the primary, so the shard can wake it once it has made
    /// room in the inbound queue.
    pub fn reader(&self) -> Option<thread::Thread> {
        self.join.as_ref().map(|join| join.thread().clone())
    }
}

impl Drop for ReplicationThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);

        if let Ok(mut slot) = self.wake.lock()
            && let Some(stream) = slot.take()
        {
            let _ = stream.shutdown(Shutdown::Both);
        }

        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

pub fn spawn_primary(shipment: Shipment) -> io::Result<ReplicationThread> {
    spawn_primary_on(TcpListener::bind(shipment.listen)?, shipment)
}

/// [`spawn_primary`] on a listener already bound (to `shipment.listen` or any address).
pub fn spawn_primary_on(
    listener: TcpListener,
    shipment: Shipment,
) -> io::Result<ReplicationThread> {
    listener.set_nonblocking(true)?;
    let stop = Arc::new(AtomicBool::new(false));
    let shipment = Arc::new(shipment);
    let thread_stop = Arc::clone(&stop);
    let join = thread::Builder::new()
        .name(format!("ruvio-ship-{}", shipment.shard_id))
        .spawn(move || accept_replicas(listener, shipment, thread_stop))?;

    Ok(ReplicationThread {
        stop,
        wake: Arc::new(Mutex::new(None)),
        join: Some(join),
    })
}

pub enum ReplicaDelivery {
    Frame(Vec<u8>),
    Snapshot {
        shard: Vec<u8>,
        hot: Option<Vec<u8>>,
        catchup_through: u64,
        /// The primary's history: the replica records it once the snapshot is installed.
        history: Origin,
    },
    CaughtUp(u64),
}

/// What the replica reader tells the rest of the process about its connection.
#[derive(Debug, Default)]
pub struct LinkStatus {
    up: AtomicBool,
    last_io_millis: AtomicU64,
    /// Set from the moment a full copy is queued until the shard has installed it and
    /// caught up: the replica then sends no acknowledgement, so its primary neither lists
    /// it for reads nor counts it for `WAIT` and `min-replicas-to-write`.
    copying: AtomicBool,
    /// Highest sequence the primary released a durable reply for.
    committed: AtomicU64,
    /// The primary has a durable reply waiting: sync and acknowledge every batch.
    ack_each: AtomicBool,
}

impl LinkStatus {
    pub(crate) fn set_copying(&self, copying: bool) {
        self.copying.store(copying, Ordering::Release);
    }

    pub fn is_copying(&self) -> bool {
        self.copying.load(Ordering::Acquire)
    }

    fn note_control(&self, committed: u64, ack_each: bool) {
        self.committed.fetch_max(committed, Ordering::AcqRel);
        self.ack_each.store(ack_each, Ordering::Release);
    }

    pub fn committed(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    pub fn ack_each(&self) -> bool {
        self.ack_each.load(Ordering::Acquire)
    }

    fn mark_up(&self) {
        self.up.store(true, Ordering::Release);
        self.touch();
    }

    pub(crate) fn mark_down(&self) {
        self.up.store(false, Ordering::Release);
    }

    fn touch(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis() as u64);

        self.last_io_millis.store(now, Ordering::Release);
    }

    pub fn is_up(&self) -> bool {
        self.up.load(Ordering::Acquire)
    }

    /// Seconds since the last frame arrived, or `None` when nothing ever did.
    pub fn seconds_since_io(&self) -> Option<u64> {
        let last = self.last_io_millis.load(Ordering::Acquire);

        (last != 0).then(|| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_millis() as u64);

            now.saturating_sub(last) / 1000
        })
    }
}

pub struct ReplicaLink {
    pub primary: SocketAddr,
    pub primary_name: Option<(String, u16)>,
    pub shard_id: u32,
    pub wal: Arc<Mutex<WalSink>>,
    pub inbound: SyncSender<ReplicaDelivery>,
    pub password: Vec<u8>,
    pub limits: ProtocolLimits,
    pub snapshot_max_bytes: u64,
    /// Sequence of the first rejected frame, or zero when ordinary streaming is safe.
    pub resync_sequence: Arc<AtomicU64>,
    pub data_dir: PathBuf,
    pub shard_count: u32,
    pub status: Arc<LinkStatus>,
    /// Woken after every delivery so the shard applies frames as they arrive instead of
    /// on its next timer tick.
    pub wake_shard: Arc<tokio::sync::Notify>,
    /// Where clients reach this replica shard, sent with the handshake.
    pub announce: Option<Endpoint>,
    /// Records the primary's client address from each accepted handshake.
    pub peers: Arc<DurableAcks>,
    /// Wraps the dialed connection in TLS; none leaves the link in cleartext.
    pub tls: Option<Arc<crate::tls::ReplicaTlsClient>>,
}

pub fn spawn_replica_reader(link: ReplicaLink) -> ReplicationThread {
    let stop = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(Mutex::new(None));
    let thread_stop = Arc::clone(&stop);
    let thread_wake = Arc::clone(&wake);
    let join = thread::Builder::new()
        .name(format!("ruvio-replica-{}", link.shard_id))
        .spawn(move || {
            let mut outstanding = VecDeque::new();
            let mut delay = RETRY_START;

            while !thread_stop.load(Ordering::Acquire) {
                let (retry, steady) =
                    follow_primary(&link, &mut outstanding, &thread_stop, &thread_wake);

                if !retry {
                    break;
                }

                sleep_unless_stopped(&thread_stop, delay);

                if steady {
                    delay = RETRY_START;
                } else {
                    delay = delay.saturating_mul(2).min(RETRY_BACKOFF_MAX);
                }
            }

            warn!(
                shard_id = link.shard_id,
                "replication replica reader stopping"
            );
        })
        .expect("replica reader thread");

    ReplicationThread {
        stop,
        wake,
        join: Some(join),
    }
}

fn sleep_unless_stopped(stop: &AtomicBool, total: Duration) {
    let slice = Duration::from_millis(50);
    let mut slept = Duration::ZERO;

    while slept < total {
        if stop.load(Ordering::Acquire) {
            return;
        }

        let pause = slice.min(total - slept);

        thread::sleep(pause);
        slept += pause;
    }
}

fn accept_replicas(listener: TcpListener, shipment: Arc<Shipment>, stop: Arc<AtomicBool>) {
    // An idle listener waits in the kernel for a replica instead of waking every poll.
    let accept = || match listener.accept() {
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            crate::runtime::wait_for_connection(&listener, REPLICA_ACCEPT_WAIT);
            listener.accept()
        }
        accepted => accepted,
    };

    accept_replicas_from(accept, shipment, stop);
}

/// How long an idle replica listener waits for a connection between looks at stop.
const REPLICA_ACCEPT_WAIT: Duration = Duration::from_millis(100);

fn accept_replicas_from(
    mut accept: impl FnMut() -> io::Result<(TcpStream, SocketAddr)>,
    shipment: Arc<Shipment>,
    stop: Arc<AtomicBool>,
) {
    let progress = Arc::new(ReplicaProgress::new(
        Arc::clone(&shipment.backlog_floor),
        Arc::clone(&shipment.replica_count),
        Arc::clone(&shipment.acks),
    ));
    let mut connections = Vec::new();
    let mut accept_warnings = crate::admission::WarnLimiter::new(Duration::from_secs(1));

    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }

        match accept() {
            Ok((stream, _)) => {
                connections.retain(|handle: &JoinHandle<()>| !handle.is_finished());

                if connections.len() >= shipment.max_replicas {
                    warn!(
                        shard_id = shipment.shard_id,
                        limit = shipment.max_replicas,
                        "replication replica limit reached"
                    );
                    let _ = stream.shutdown(Shutdown::Both);

                    continue;
                }

                let shipment = Arc::clone(&shipment);
                let progress = Arc::clone(&progress);
                let connection_stop = Arc::clone(&stop);
                let handle =
                    thread::Builder::new()
                        .name("ruvio-ship-conn".into())
                        .spawn(move || {
                            let _ = stream.set_nonblocking(false);

                            let Some(acceptor) = shipment.tls.as_ref() else {
                                let _ =
                                    serve_replica(stream, &shipment, &progress, &connection_stop);

                                return;
                            };

                            // A peer that never finishes the handshake must not hold this
                            // thread: bound it, then hand the stream to the protocol, which
                            // sets its own timeouts.
                            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                            let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));

                            match acceptor.handshake(stream) {
                                Ok(secure) => {
                                    let _ = serve_replica(
                                        secure,
                                        &shipment,
                                        &progress,
                                        &connection_stop,
                                    );
                                }
                                Err(error) => {
                                    warn!(%error, "replication tls handshake failed");
                                }
                            }
                        });

                if let Ok(handle) = handle {
                    connections.push(handle);
                }
            }

            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(SHIPPER_POLL);
            }

            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            // Out of descriptors or memory, or an aborted connection: the listener is still
            // fine, so wait and accept again instead of shutting replication down for good.
            Err(error) => {
                if accept_warnings.allow(std::time::Instant::now()) {
                    warn!(
                        shard_id = shipment.shard_id,
                        %error,
                        suppressed = accept_warnings.take_suppressed(),
                        "replication accept failed; retrying"
                    );
                }

                thread::sleep(ACCEPT_ERROR_PAUSE);
            }
        }

        connections.retain(|handle: &JoinHandle<()>| !handle.is_finished());
    }

    for handle in connections {
        let _ = handle.join();
    }
}

fn serve_replica(
    mut stream: impl Wire,
    shipment: &Shipment,
    progress: &Arc<ReplicaProgress>,
    stop: &AtomicBool,
) -> io::Result<()> {
    if stop.load(Ordering::Acquire) {
        return Ok(());
    }

    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(SHIPPER_POLL));
    let handshake = match read_handshake(&mut stream) {
        Ok(Incoming::Handshake(handshake)) => handshake,
        Ok(Incoming::Unsupported(version)) => {
            warn!(
                shard_id = shipment.shard_id,
                replica_version = version,
                primary_versions = ?SUPPORTED_VERSIONS,
                "replica refused: {VERSION_MISMATCH}"
            );
            let reply = if LEGACY_VERSIONS.contains(&version) {
                encode_legacy_reply(HandshakeStatus::Version)
            } else if HISTORY_ONLY_VERSIONS.contains(&version) {
                encode_history_only_reply(HandshakeStatus::Version)
            } else {
                encode_reply(HandshakeStatus::Version, 0)
            };

            stream.write_all(&reply)?;

            return stream.flush();
        }

        Err(error) => {
            if error.kind() == io::ErrorKind::InvalidData {
                warn!(
                    shard_id = shipment.shard_id,
                    %error,
                    "malformed replication handshake; {VERSION_MISMATCH}"
                );
            }

            return Ok(());
        }
    };

    if handshake.shard_id != shipment.shard_id {
        refuse_handshake(&mut stream, HandshakeStatus::Shard)?;

        return Ok(());
    }

    if !crate::security::constant_time_eq(&handshake.password, &shipment.requirepass) {
        refuse_handshake(&mut stream, HandshakeStatus::Auth)?;

        return Ok(());
    }

    if handshake.shard_count != shipment.shard_count {
        refuse_handshake(&mut stream, HandshakeStatus::Shard)?;

        return Ok(());
    }

    let forced_snapshot = handshake.version != VERSION_RESUME;
    let incomplete = handshake.version == VERSION_RESYNC_INCOMPLETE;

    // A replica that has seen a newer epoch follows a primary that replaced this one.
    if handshake.epoch > shipment.lineage.origin.epoch {
        warn!(
            shard_id = shipment.shard_id,
            epoch = shipment.lineage.origin.epoch,
            replica_epoch = handshake.epoch,
            "a replica has seen a newer primary; this one was replaced"
        );
        shipment
            .newer_epoch
            .fetch_max(handshake.epoch, Ordering::AcqRel);
        refuse_handshake(&mut stream, HandshakeStatus::Stale)?;

        return Ok(());
    }

    // A replica that resumes from its own log must have followed this very history, or the
    // one it forked from no further than the fork; a wiped, restored or unrelated primary
    // can reuse the same sequence numbers.
    if !forced_snapshot
        && handshake.after_sequence > 0
        && !shipment
            .lineage
            .continues(&handshake.history, handshake.after_sequence)
    {
        refuse_handshake(&mut stream, HandshakeStatus::History)?;

        return Ok(());
    }

    let through = shipment.published.load(Ordering::Acquire);
    let hold = progress.hold(handshake.after_sequence);

    hold.announce(reachable_endpoint(
        handshake.announce,
        stream.peer_addr().ok().map(|peer| peer.ip()),
    ));
    if forced_snapshot
        && (handshake.after_sequence > through
            || (!incomplete && handshake.after_sequence == through))
    {
        refuse_handshake(&mut stream, HandshakeStatus::Gap)?;
        return Ok(());
    }
    // Where the shipper stopped in the log, so each chunk continues there.
    let mut reader = FrameCursor::default();
    let read = if forced_snapshot {
        Ok(FrameRead::Gap)
    } else {
        read_frames_resuming(
            &mut reader,
            &shipment.data_dir,
            shipment.shard_id,
            handshake.after_sequence,
            through,
            shipment.limits,
            SHIPPER_READ_BYTES,
        )
    };

    let frames = match read {
        Ok(FrameRead::Frames(frames)) => {
            if frames.is_empty()
                && handshake.after_sequence == through
                && !caught_up_cursor_is_retained(shipment, handshake.after_sequence, through)
            {
                refuse_handshake(&mut stream, HandshakeStatus::Gap)?;

                return Ok(());
            }

            frames
        }

        Ok(FrameRead::Gap) => {
            let Some(resync) = crate::persistence::load_resync_bytes(
                &shipment.data_dir,
                shipment.limits,
                shipment.snapshot_max_bytes,
                shipment.shard_id,
                shipment.shard_count,
                handshake.after_sequence + u64::from(forced_snapshot && !incomplete),
                through,
            )
            .unwrap_or(None) else {
                if forced_snapshot {
                    shipment.snapshot_requests.fetch_add(1, Ordering::AcqRel);
                }
                refuse_handshake(&mut stream, HandshakeStatus::Gap)?;

                return Ok(());
            };
            let last_sequence = resync.last_sequence;
            hold.advance(last_sequence);
            let copied_publishes = shipment.fanout.register();

            if !write_live(
                &mut stream,
                &accepted_reply(HandshakeStatus::Snapshot, through, shipment),
                stop,
            )? {
                return Ok(());
            }

            if !write_snapshot_blob(&mut stream, &resync.shard, stop)? {
                return Ok(());
            }

            if shipment.shard_id == 0 {
                let hot = resync.hot.as_deref().unwrap_or(&[]);

                if !write_snapshot_blob(&mut stream, hot, stop)? {
                    return Ok(());
                }
            }

            drop(resync);

            let Some(last_sent) = stream_frames_through(
                &mut stream,
                shipment,
                &hold,
                stop,
                &mut reader,
                last_sequence,
                through,
            )?
            else {
                return Ok(());
            };

            return stream_new_frames(
                &mut stream,
                shipment,
                &hold,
                &copied_publishes,
                stop,
                &mut reader,
                last_sent,
            );
        }

        Ok(FrameRead::Ahead) => {
            refuse_handshake(&mut stream, HandshakeStatus::Ahead)?;

            return Ok(());
        }

        Ok(FrameRead::Local) | Err(_) => {
            refuse_handshake(&mut stream, HandshakeStatus::Local)?;

            return Ok(());
        }
    };

    let next_sequence = handshake.after_sequence.saturating_add(1);
    let copied_publishes = shipment.fanout.register();

    if !write_live(
        &mut stream,
        &accepted_reply(HandshakeStatus::Ok, next_sequence, shipment),
        stop,
    )? {
        return Ok(());
    }

    let mut last_sent = handshake.after_sequence;

    for frame in &frames {
        if !write_live(&mut stream, frame, stop)? {
            return Ok(());
        }

        last_sent = frame_sequence(frame);
        hold.advance(last_sent);
    }

    let _ = stream.flush();

    stream_new_frames(
        &mut stream,
        shipment,
        &hold,
        &copied_publishes,
        stop,
        &mut reader,
        last_sent,
    )
}

/// The address a peer announced, with its host replaced by the address of the connection
/// when it announced a loopback host but is not on this machine: a node left at the default
/// `advertised-host` is still listed where clients can reach it.
fn reachable_endpoint(
    announced: Option<Endpoint>,
    peer: Option<std::net::IpAddr>,
) -> Option<Endpoint> {
    let mut endpoint = announced?;
    let loopback = endpoint.host.eq_ignore_ascii_case("localhost")
        || endpoint
            .host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());

    if loopback && let Some(peer) = peer.filter(|peer| !peer.is_loopback()) {
        endpoint.host = peer.to_string();
    }

    Some(endpoint)
}

fn caught_up_cursor_is_retained(shipment: &Shipment, after_sequence: u64, through: u64) -> bool {
    if after_sequence == 0 {
        return true;
    }

    if let Ok(FrameRead::Frames(_)) = read_frames(
        &shipment.data_dir,
        shipment.shard_id,
        after_sequence - 1,
        after_sequence,
        shipment.limits,
    ) {
        return true;
    }

    matches!(
        crate::persistence::load_resync_bytes(
            &shipment.data_dir,
            shipment.limits,
            shipment.snapshot_max_bytes,
            shipment.shard_id,
            shipment.shard_count,
            after_sequence,
            through,
        ),
        Ok(Some(resync)) if resync.last_sequence == after_sequence
    )
}

/// Reads the replica's acknowledgements. False once the replica has hung up or the
/// connection failed: an idle primary writes nothing that would find out, so without this
/// the connection would hold one of the replica slots for good.
fn poll_acks(stream: &mut dyn Wire, hold: &CursorHold, pending: &mut Vec<u8>) -> bool {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(1)));
    let mut scratch = [0u8; 64];

    loop {
        match stream.read(&mut scratch) {
            Ok(0) => return false,
            Ok(read) => pending.extend_from_slice(&scratch[..read]),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return false,
        }
    }

    while pending.len() >= ACK_LEN {
        let Some(sequence) = decode_ack(&pending[..ACK_LEN]) else {
            pending.clear();

            return true;
        };

        pending.drain(..ACK_LEN);
        hold.ack(sequence);
    }

    true
}

struct AckSend {
    /// While it reports a copy in progress, nothing is sent.
    status: Arc<LinkStatus>,
    last: u64,
    /// When `last` was sent: it is sent again after [`ACK_HEARTBEAT`].
    sent_at: std::time::Instant,
    offset: usize,
    bytes: [u8; ACK_LEN],
    queued: bool,
}

impl AckSend {
    fn poll(&mut self, stream: &mut dyn Wire, wal: &Mutex<WalSink>) {
        // The log of a replica still copying belongs to the history it is replacing.
        if !self.queued && self.status.is_copying() {
            return;
        }

        if !self.queued {
            let durable = wal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .durable()
                .load(Ordering::Acquire);

            if durable == self.last && self.sent_at.elapsed() < ACK_HEARTBEAT {
                return;
            }

            self.bytes = encode_ack(durable);
            self.offset = 0;
            self.queued = true;
        }

        match stream.write(&self.bytes[self.offset..]) {
            Ok(0) | Err(_) => {}

            Ok(written) => {
                self.offset += written;

                if self.offset == self.bytes.len() {
                    self.last = decode_ack(&self.bytes).unwrap_or(self.last);
                    self.sent_at = std::time::Instant::now();
                    self.queued = false;
                }
            }
        }
    }
}

fn stream_frames_through(
    stream: &mut dyn Wire,
    shipment: &Shipment,
    hold: &CursorHold,
    stop: &AtomicBool,
    reader: &mut FrameCursor,
    mut cursor: u64,
    through: u64,
) -> io::Result<Option<u64>> {
    while cursor < through {
        let frames = match read_frames_resuming(
            reader,
            &shipment.data_dir,
            shipment.shard_id,
            cursor,
            through,
            shipment.limits,
            SHIPPER_READ_BYTES,
        ) {
            Ok(FrameRead::Frames(frames)) if !frames.is_empty() => frames,
            _ => return Ok(None),
        };
        for frame in frames {
            if !write_live(stream, &frame, stop)? {
                return Ok(None);
            }
            cursor = frame_sequence(&frame);
            hold.advance(cursor);
        }
    }
    Ok(Some(cursor))
}

fn stream_new_frames(
    stream: &mut dyn Wire,
    shipment: &Shipment,
    hold: &CursorHold,
    copied_publishes: &Receiver<Vec<u8>>,
    stop: &AtomicBool,
    reader: &mut FrameCursor,
    mut last_sent: u64,
) -> io::Result<()> {
    let mut ack_bytes = Vec::new();
    // What this replica was last told; a new connection starts from nothing committed.
    let mut control = (0, false);

    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }

        if !poll_acks(stream, hold, &mut ack_bytes) {
            return Ok(());
        }

        let current = shipment.durable_replies.state();

        if current != control {
            if !write_live(stream, &encode_control(current.0, current.1), stop)? {
                return Ok(());
            }

            control = current;
            let _ = stream.flush();
        }

        while let Ok(frame) = copied_publishes.try_recv() {
            if !write_live(stream, &frame, stop)? {
                return Ok(());
            }
        }

        let through = shipment.published.load(Ordering::Acquire);

        if through <= last_sent {
            thread::sleep(SHIPPER_POLL);

            continue;
        }

        let Some(sent) =
            stream_frames_through(stream, shipment, hold, stop, reader, last_sent, through)?
        else {
            return Ok(());
        };
        last_sent = sent;
        let _ = stream.flush();
    }
}

pub(crate) fn follow_primary(
    link: &ReplicaLink,
    outstanding: &mut VecDeque<(u64, usize)>,
    stop: &AtomicBool,
    wake: &Mutex<Option<TcpStream>>,
) -> (bool, bool) {
    if stop.load(Ordering::Acquire) {
        return (false, false);
    }

    let addresses = match dial_addresses(link) {
        Ok(addresses) => addresses,
        Err(error) => {
            warn!(%error, "replication dial failed");

            return (!stop.load(Ordering::Acquire), false);
        }
    };

    // A name can resolve to several addresses (`localhost` to ::1 and 127.0.0.1); the primary
    // may listen on only one of them, so each is tried in the resolver's order.
    let mut dialed = Err(io::Error::from(io::ErrorKind::NotFound));

    for address in &addresses {
        dialed = TcpStream::connect_timeout(address, Duration::from_millis(200));
        if dialed.is_ok() {
            break;
        }
    }
    let mut stream = match dialed {
        Ok(stream) => stream,
        Err(error) => {
            warn!(%error, primary = ?addresses, "replication dial failed");

            return (!stop.load(Ordering::Acquire), false);
        }
    };

    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

    if let Ok(cloned) = stream.try_clone() {
        *wake.lock().unwrap_or_else(|error| error.into_inner()) = Some(cloned);
    }

    let (retry, steady) = match &link.tls {
        None => read_primary(&mut stream, link, outstanding, stop),
        Some(client) => {
            let name = match &link.primary_name {
                Some((host, _)) => host.clone(),
                None => link.primary.ip().to_string(),
            };

            match client.connect(&name, stream) {
                Ok(mut secure) => read_primary(&mut secure, link, outstanding, stop),
                Err(error) => {
                    warn!(%error, primary = ?addresses, "replication tls handshake failed");

                    (!stop.load(Ordering::Acquire), false)
                }
            }
        }
    };

    link.status.mark_down();
    *wake.lock().unwrap_or_else(|error| error.into_inner()) = None;

    if stop.load(Ordering::Acquire) {
        return (false, steady);
    }

    (retry, steady)
}

fn dial_addresses(link: &ReplicaLink) -> io::Result<Vec<SocketAddr>> {
    let Some((host, port)) = &link.primary_name else {
        return Ok(vec![link.primary]);
    };

    let port = u16::try_from(link.shard_id)
        .ok()
        .and_then(|shard| port.checked_add(shard))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "replication port plus shard id is above 65535",
            )
        })?;
    let addresses: Vec<SocketAddr> = (host.as_str(), port).to_socket_addrs()?.collect();

    if addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "replication primary did not resolve",
        ));
    }

    Ok(addresses)
}

struct CatchupReconnect<'a> {
    link: &'a ReplicaLink,
    through: Option<u64>,
}

impl Drop for CatchupReconnect<'_> {
    fn drop(&mut self) {
        if self.through.is_some() && self.link.resync_sequence.load(Ordering::Acquire) == 0 {
            let next = self
                .link
                .wal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .last_sequence()
                .saturating_add(1);
            self.link.resync_sequence.store(next, Ordering::Release);
        }
    }
}

fn read_primary(
    stream: &mut dyn Wire,
    link: &ReplicaLink,
    outstanding: &mut VecDeque<(u64, usize)>,
    stop: &AtomicBool,
) -> (bool, bool) {
    let requested = link.resync_sequence.load(Ordering::Acquire);
    let after_sequence = if requested == u64::MAX {
        0 // A persisted incomplete image has no trustworthy resume cursor.
    } else {
        let wal = link.wal.lock().unwrap_or_else(|error| error.into_inner());
        wal.published().load(Ordering::Acquire)
    };
    if requested != 0 {
        outstanding.clear();
    }

    let followed = upstream_origin(&link.data_dir, link.shard_id);
    let handshake = Handshake {
        version: if requested == 0 {
            VERSION_RESUME
        } else if requested == u64::MAX {
            VERSION_RESYNC_INCOMPLETE
        } else {
            VERSION_RESYNC
        },
        shard_id: link.shard_id,
        after_sequence,
        password: link.password.clone(),
        history: followed.history,
        shard_count: link.shard_count,
        epoch: followed.epoch,
        announce: link.announce.clone(),
    };

    let Ok(encoded) = encode_handshake(&handshake) else {
        warn!("replication handshake password exceeds 65535 bytes");

        return (true, false);
    };

    if stream.write_all(&encoded).is_err() {
        return (true, false);
    }

    let (status, snapshot_through, primary_history, primary_endpoint) = match read_reply(stream) {
        Ok(reply) => reply,
        Err(error) => {
            // A primary of another build cannot parse this handshake and closes the link (or
            // answers in a format this build cannot read); say so instead of retrying silently.
            warn!(
                shard_id = link.shard_id,
                %error,
                "no valid reply to the replication handshake; {VERSION_MISMATCH}, and the \
                 replication password and address must match"
            );

            return (true, false);
        }
    };

    if status == HandshakeStatus::Version {
        warn!(
            shard_id = link.shard_id,
            replica_version = handshake.version,
            "primary refused this replica: {VERSION_MISMATCH}"
        );

        return (true, false);
    }

    if status == HandshakeStatus::Stale {
        warn!(
            shard_id = link.shard_id,
            epoch = followed.epoch,
            "the primary was replaced by a newer one; not following it"
        );

        return (true, false);
    }

    // A primary below the epoch this replica already followed is an old, replaced one.
    if matches!(status, HandshakeStatus::Ok | HandshakeStatus::Snapshot)
        && primary_history.epoch < followed.epoch
    {
        warn!(
            shard_id = link.shard_id,
            epoch = followed.epoch,
            primary_epoch = primary_history.epoch,
            "the primary is older than the one this replica followed; not following it"
        );

        return (true, false);
    }

    if matches!(status, HandshakeStatus::Ok | HandshakeStatus::Snapshot) {
        link.peers.set_primary(reachable_endpoint(
            primary_endpoint,
            stream.peer_addr().ok().map(|peer| peer.ip()),
        ));
    }

    if status == HandshakeStatus::History {
        // This replica followed another history: its log cannot be continued, so take a
        // full copy of the primary instead.
        warn!("primary has a different write history; requesting a full resync");
        link.resync_sequence.store(u64::MAX, Ordering::Release);

        return (true, false);
    }

    // A resume continues the log the primary just vouched for. A snapshot's history is
    // recorded only after it is installed: a crash while it downloads must not leave the old
    // log claiming the new history.
    if status == HandshakeStatus::Ok
        && let Err(error) = record_upstream_history(&link.data_dir, link.shard_id, &primary_history)
    {
        warn!(%error, "could not record the primary's write history");

        return (true, false);
    }
    let mut catchup = CatchupReconnect {
        link,
        through: None,
    };

    if requested != 0 && status != HandshakeStatus::Snapshot {
        return (true, false);
    }
    if status == HandshakeStatus::Snapshot {
        let shard = match read_snapshot_blob(stream, link.snapshot_max_bytes) {
            Ok(bytes) => bytes,
            Err(_) => return (true, false),
        };

        let hot = if link.shard_id == 0 {
            let bytes = match read_snapshot_blob(stream, link.snapshot_max_bytes) {
                Ok(bytes) => bytes,
                Err(_) => return (true, false),
            };

            (!bytes.is_empty()).then_some(bytes)
        } else {
            None
        };

        let snapshot_sequence = match crate::persistence::decode_snapshot_blob(
            &shard,
            link.limits,
            link.snapshot_max_bytes,
        ) {
            Ok(checkpoint) if checkpoint.last_sequence <= snapshot_through => {
                checkpoint.last_sequence
            }
            _ => return (true, false),
        };
        // Before the shard sees the copy, so not one acknowledgement of the log it replaces
        // goes out in between.
        link.status.set_copying(true);
        if link
            .inbound
            .send(ReplicaDelivery::Snapshot {
                shard,
                hot,
                catchup_through: snapshot_through,
                history: primary_history,
            })
            .is_err()
        {
            return (true, false);
        }
        link.wake_shard.notify_one();
        if snapshot_sequence == snapshot_through {
            if link
                .inbound
                .send(ReplicaDelivery::CaughtUp(snapshot_through))
                .is_err()
            {
                return (true, false);
            }
            link.wake_shard.notify_one();
        } else {
            catchup.through = Some(snapshot_through);
        }
        if requested != 0 {
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while link.resync_sequence.load(Ordering::Acquire) != 0 {
                if stop.load(Ordering::Acquire) {
                    return (false, false);
                }
                if std::time::Instant::now() >= deadline {
                    return (true, false);
                }
                thread::sleep(SHIPPER_POLL);
            }
        }
    } else if status != HandshakeStatus::Ok {
        warn!(?status, "replication handshake rejected");

        return (true, false);
    }

    // Blocking with a short timeout: a frame is read the moment it arrives, and the
    // timeout still lets the loop send acks and notice stop or a resync request.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(SHIPPER_POLL));
    let _ = stream.set_write_timeout(Some(SHIPPER_POLL));
    let mut acks = AckSend {
        status: Arc::clone(&link.status),
        last: 0,
        sent_at: std::time::Instant::now(),
        offset: 0,
        bytes: [0; ACK_LEN],
        queued: false,
    };

    acks.poll(stream, &link.wal);
    // The primary repeats what a durable reply needs after every handshake.
    link.status.note_control(0, false);
    link.status.mark_up();
    // Frames carry records written under the primary's limits, not this replica's; but
    // none larger than this replica's own log segment can be stored, so a declared length
    // past it is refused before it is buffered.
    let segment_bytes = link
        .wal
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .segment_bytes();
    let mut reader = FrameReader {
        pending: Vec::new(),
        max_frame_bytes: usize::try_from(segment_bytes)
            .unwrap_or(usize::MAX)
            .min(crate::persistence::WAL_RECORD_MAX_BYTES),
    };

    loop {
        if !wait_for_queue_room(link, outstanding, stop) {
            return (!stop.load(Ordering::Acquire), true);
        }

        let frame =
            match reader.next_frame(stream, stop, &link.resync_sequence, &mut acks, &link.wal) {
                Ok(Some(frame)) => frame,
                Ok(None) => return (false, true),
                Err(error) => {
                    if error.kind() == io::ErrorKind::InvalidData {
                        warn!(%error, "replication frame rejected");
                    }

                    return (!stop.load(Ordering::Acquire), true);
                }
            };

        link.status.touch();

        if let Some((committed, ack_each)) = decode_control(&frame) {
            link.status.note_control(committed, ack_each);
            if ack_each {
                link.wake_shard.notify_one();
            }
            acks.poll(stream, &link.wal);

            continue;
        }

        let sequence = if decode_copied_publish(&frame).is_some() {
            None
        } else {
            Some(frame_sequence(&frame))
        };
        if !enqueue_frame(link, outstanding, frame, stop) {
            return (!stop.load(Ordering::Acquire), true);
        }
        if let (Some(sequence), Some(through)) = (sequence, catchup.through)
            && sequence >= through
        {
            if link
                .inbound
                .send(ReplicaDelivery::CaughtUp(sequence))
                .is_err()
            {
                return (true, false);
            }
            link.wake_shard.notify_one();
            catchup.through = None;
        }

        acks.poll(stream, &link.wal);
    }
}

struct FrameReader {
    pending: Vec<u8>,
    max_frame_bytes: usize,
}

impl FrameReader {
    fn next_frame(
        &mut self,
        stream: &mut dyn Wire,
        stop: &AtomicBool,
        resync_sequence: &AtomicU64,
        acks: &mut AckSend,
        wal: &Mutex<WalSink>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut scratch = [0; 8192];

        loop {
            if resync_sequence.load(Ordering::Acquire) != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "snapshot resync requested",
                ));
            }
            if let Some(frame) = self.take_frame()? {
                return Ok(Some(frame));
            }

            if stop.load(Ordering::Acquire) {
                return Ok(None);
            }

            match stream.read(&mut scratch) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "replication closed",
                    ));
                }

                Ok(read) => self.pending.extend_from_slice(&scratch[..read]),
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::TimedOut =>
                {
                    acks.poll(stream, wal);
                }

                Err(error) => return Err(error),
            }
        }
    }

    fn take_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.pending.len() < 4 {
            return Ok(None);
        }

        let length = u32::from_le_bytes(self.pending[..4].try_into().unwrap()) as usize;

        if !(32..=self.max_frame_bytes).contains(&length) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wal frame length",
            ));
        }

        if self.pending.len() < length {
            return Ok(None);
        }

        Ok(Some(self.pending.drain(..length).collect()))
    }
}

fn wait_for_queue_room(
    link: &ReplicaLink,
    outstanding: &mut VecDeque<(u64, usize)>,
    stop: &AtomicBool,
) -> bool {
    loop {
        if stop.load(Ordering::Acquire) || link.resync_sequence.load(Ordering::Acquire) != 0 {
            return false;
        }

        let queued_bytes = reclaim_applied(link, outstanding);

        if outstanding.len() < QUEUE_FRAMES
            && (outstanding.is_empty() || queued_bytes < QUEUE_BYTES)
        {
            return true;
        }

        // The shard unparks this thread after it drains; the timeout covers stop.
        link.wake_shard.notify_one();
        thread::park_timeout(SHIPPER_POLL);
    }
}

fn reclaim_applied(link: &ReplicaLink, outstanding: &mut VecDeque<(u64, usize)>) -> usize {
    let applied = {
        let wal = link.wal.lock().unwrap_or_else(|error| error.into_inner());

        wal.last_sequence()
    };

    while outstanding
        .front()
        .is_some_and(|(sequence, _)| *sequence <= applied)
    {
        outstanding.pop_front();
    }

    outstanding.iter().map(|(_, len)| *len).sum()
}

fn enqueue_frame(
    link: &ReplicaLink,
    outstanding: &mut VecDeque<(u64, usize)>,
    mut frame: Vec<u8>,
    stop: &AtomicBool,
) -> bool {
    let sequence = frame_sequence(&frame);
    let len = frame.len();

    loop {
        if stop.load(Ordering::Acquire) || link.resync_sequence.load(Ordering::Acquire) != 0 {
            return false;
        }

        match link.inbound.try_send(ReplicaDelivery::Frame(frame)) {
            Ok(()) => {
                outstanding.push_back((sequence, len));
                link.wake_shard.notify_one();

                return true;
            }

            Err(TrySendError::Full(ReplicaDelivery::Frame(returned))) => {
                frame = returned;
                // The shard unparks this thread after it drains; the timeout covers stop.
                link.wake_shard.notify_one();
                thread::park_timeout(SHIPPER_POLL);
            }

            Err(
                TrySendError::Full(ReplicaDelivery::Snapshot { .. } | ReplicaDelivery::CaughtUp(_))
                | TrySendError::Disconnected(_),
            ) => return false,
        }
    }
}

fn frame_sequence(frame: &[u8]) -> u64 {
    crate::persistence::record_sequence(frame).unwrap_or(u64::MAX)
}

enum Incoming {
    Handshake(Handshake),
    /// A handshake of another build, by its version.
    Unsupported(u16),
}

fn read_handshake(stream: &mut dyn Wire) -> io::Result<Incoming> {
    let mut prefix = [0; HANDSHAKE_PREFIX];

    stream.read_exact(&mut prefix)?;

    if &prefix[..HANDSHAKE_MAGIC.len()] != HANDSHAKE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad handshake magic",
        ));
    }

    let password_len = usize::from(u16::from_le_bytes([prefix[22], prefix[23]]));
    let version = read_u16(&prefix, 8);

    // Decided before the rest is read: other builds send a different tail, and waiting for
    // bytes that never come would stall both sides until the read timeout.
    if !SUPPORTED_VERSIONS.contains(&version) && !NO_CONTROL_VERSIONS.contains(&version) {
        // Consume what older builds send after the prefix so the peer reads the refusal
        // instead of a reset: the legacy ones end with the password, the history-only ones
        // add a history id and shard count.
        let rest_len = if LEGACY_VERSIONS.contains(&version) {
            Some(password_len + CHECKSUM_LEN)
        } else if HISTORY_ONLY_VERSIONS.contains(&version) {
            Some(password_len + HISTORY_ONLY_TAIL + CHECKSUM_LEN)
        } else if EPOCH_ONLY_VERSIONS.contains(&version) {
            Some(password_len + EPOCH_ONLY_TAIL + CHECKSUM_LEN)
        } else {
            None
        };

        if let Some(rest_len) = rest_len {
            let mut rest = vec![0; rest_len];

            let _ = stream.read_exact(&mut rest);
        }

        return Ok(Incoming::Unsupported(version));
    }
    let mut bytes = prefix.to_vec();

    bytes.resize(HANDSHAKE_PREFIX + password_len + HANDSHAKE_TAIL, 0);
    stream.read_exact(&mut bytes[HANDSHAKE_PREFIX..])?;
    // The client host and the checksum follow the fixed tail.
    let host_len = usize::from(read_u16(&bytes, bytes.len() - 2));
    let read = bytes.len();

    bytes.resize(read + host_len + CHECKSUM_LEN, 0);
    stream.read_exact(&mut bytes[read..])?;

    if NO_CONTROL_VERSIONS.contains(&version) {
        return Ok(Incoming::Unsupported(version));
    }

    decode_handshake(&bytes)
        .map(Incoming::Handshake)
        .map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, reason))
}

/// A fake primary's side of the handshake, for tests outside this module.
#[cfg(test)]
pub(crate) fn read_current_handshake(stream: &mut dyn Wire) -> Handshake {
    match read_handshake(stream) {
        Ok(Incoming::Handshake(handshake)) => handshake,
        _ => panic!("replica sent no current handshake"),
    }
}

pub fn read_snapshot_blob(stream: &mut dyn Wire, maximum: u64) -> io::Result<Vec<u8>> {
    let mut length_bytes = [0; 8];

    stream.read_exact(&mut length_bytes)?;
    let length = u64::from_le_bytes(length_bytes);

    if length > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot exceeds configured size limit",
        ));
    }

    let length = usize::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "snapshot length"))?;

    let mut bytes = vec![0; length];

    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

/// The primary's reply and the client address that follows it. A version refusal comes
/// from a build that may not send the address, so nothing more is read after one.
fn read_reply(
    stream: &mut dyn Wire,
) -> io::Result<(HandshakeStatus, u64, Origin, Option<Endpoint>)> {
    let invalid = |reason| io::Error::new(io::ErrorKind::InvalidData, reason);
    let mut bytes = [0; REPLY_LEN];

    stream.read_exact(&mut bytes)?;
    let (status, next, origin) = decode_reply_with_history(&bytes).map_err(invalid)?;

    if status == HandshakeStatus::Version {
        return Ok((status, next, origin, None));
    }

    let mut address = vec![0; 4];

    stream.read_exact(&mut address)?;
    let host_len = usize::from(read_u16(&address, 2));

    address.resize(ADDRESS_FRAME_MIN + host_len, 0);
    stream.read_exact(&mut address[4..])?;
    let endpoint = decode_address(&address).map_err(invalid)?;

    Ok((status, next, origin, endpoint))
}

fn refuse_handshake(stream: &mut dyn Wire, status: HandshakeStatus) -> io::Result<()> {
    warn!(?status, "replication handshake rejected");
    let mut reply = encode_reply(status, 0);

    reply.extend_from_slice(&encode_address(None));
    stream.write_all(&reply)?;
    stream.flush()
}

/// A reply to a current handshake that the primary follows with its log: the reply and the
/// primary shard's client address.
fn accepted_reply(status: HandshakeStatus, next_sequence: u64, shipment: &Shipment) -> Vec<u8> {
    let mut reply = encode_reply_with_history(status, next_sequence, &shipment.lineage.origin);

    reply.extend_from_slice(&encode_address(shipment.announce.as_ref()));
    reply
}

fn write_snapshot_blob(stream: &mut dyn Wire, bytes: &[u8], stop: &AtomicBool) -> io::Result<bool> {
    let mut payload = (bytes.len() as u64).to_le_bytes().to_vec();

    payload.extend_from_slice(bytes);
    write_live(stream, &payload, stop)
}

fn write_live(stream: &mut dyn Wire, bytes: &[u8], stop: &AtomicBool) -> io::Result<bool> {
    let mut offset = 0;
    let mut last_progress = std::time::Instant::now();

    while offset < bytes.len() {
        if stop.load(Ordering::Acquire) {
            return Ok(false);
        }

        match stream.write(&bytes[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "replication write closed",
                ));
            }

            Ok(written) => {
                offset += written;
                last_progress = std::time::Instant::now();
            }
            Err(error)
                if error.kind() == io::ErrorKind::TimedOut
                    || error.kind() == io::ErrorKind::WouldBlock =>
            {
                if last_progress.elapsed() >= Duration::from_secs(30) {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "replica stopped accepting data",
                    ));
                }
            }

            Err(error) => return Err(error),
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests;
mod wire;
