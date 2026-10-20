//! The coordinators' replicated state on openraft: which member of each replication group is
//! primary, at which epoch, the slot ranges moved between groups, and the highest sequence
//! committed per group and shard (what a failover must keep). Log, vote and snapshot are small JSON files written with temp file,
//! fsync and rename, like Ruvio's other durable files; RPCs are length-prefixed JSON frames on
//! the coordinator's own port.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{self, Cursor, Write},
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftNetwork,
    RaftNetworkFactory, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
    error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use serde::{Deserialize, Serialize};

use super::config::{Group, Member};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// A change to the replicated state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// `member` (a member's client address, `host:port`) is the group's primary from `epoch`
    /// on. Applied only above the epoch already recorded, so a retry is harmless.
    SetPrimary {
        group: String,
        member: String,
        epoch: u64,
        /// The committed sequences that hold under the new primary, decided when it was chosen
        /// (an unclean promotion drops the ones it lacks). Absent on the first record of a
        /// group and in entries older coordinators wrote: the values carry over unchanged.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<Commit>,
    },
    /// A slot range begins to move between two groups; refused while another move runs.
    BeginMove(Migration),
    /// The target holds the whole range: from now on the map gives it the range.
    CommitMove { id: String },
    /// The move ended before its commit (cancelled, failed, or a failover interrupted it).
    EndMove { id: String },
    /// Committed sequences the leader saw rise since the last record (0 for a shard that did
    /// not). Coordinators older than this request cannot read a log that holds it: all
    /// coordinators are upgraded together.
    Commit(BTreeMap<String, Raised>),
    /// The group's members from now on: the configured ones and the replicas discovered
    /// following its primary. Applied only while the recorded list is still `previous` (the
    /// one the leader started from), holds no address twice and holds the current primary.
    SetMembers {
        group: String,
        previous: Option<Vec<Member>>,
        members: Vec<Member>,
    },
    /// `COORDINATOR FORGET`: `member` (a client address) leaves the group's recorded list and
    /// is not discovered again. Refused for the current primary.
    Forget { group: String, member: String },
}

/// One group's committed sequences that rose under the primary of `epoch`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Raised {
    pub epoch: u64,
    /// Per shard, the new committed sequence, or 0 where it did not rise; no trailing zeros.
    pub committed: Vec<u64>,
}

/// A group's committed sequences under one primary epoch, per shard: a failover keeps them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// The primary's epoch the values belong to.
    pub epoch: u64,
    /// The highest committed sequence seen, per shard.
    pub committed: Vec<u64>,
    /// Per shard, what replicas may still report from a primary whose history an unclean
    /// failover dropped (a replica keeps the highest value it was ever told); replica reports
    /// up to it are ignored.
    pub stale: Vec<u64>,
}

/// Raises `values[shard]` to at least `value`, growing `values` as needed.
pub fn raise(values: &mut Vec<u64>, shard: usize, value: u64) {
    if values.len() <= shard {
        values.resize(shard + 1, 0);
    }
    values[shard] = values[shard].max(value);
}

impl Commit {
    /// Takes in the record `other`: a newer epoch replaces these values (the promotion decided
    /// which hold), the same epoch raises the committed ones shard by shard, an older one
    /// changes nothing. The stale values are set only by a promotion, so within one epoch every
    /// coordinator holds the same ones.
    pub fn merge(&mut self, other: &Commit) {
        if other.epoch > self.epoch {
            *self = other.clone();
        } else if other.epoch == self.epoch {
            for (shard, &value) in other.committed.iter().enumerate() {
                if value > 0 {
                    raise(&mut self.committed, shard, value);
                }
            }
        }
    }

    /// The values that hold once `promoted`, which holds `durable` per shard, is primary at
    /// `epoch`: those it lacks are gone, and replicas that keep reporting them are ignored.
    pub fn after_promotion(&self, durable: &[u64], epoch: u64) -> Commit {
        let mut next = Commit {
            epoch,
            ..self.clone()
        };

        for (shard, committed) in next.committed.iter_mut().enumerate() {
            if durable.get(shard).copied().unwrap_or(0) < *committed {
                raise(&mut next.stale, shard, *committed);
                *committed = 0;
            }
        }
        next
    }

    /// Per shard, the committed sequence where it is above `recorded`'s and 0 elsewhere, without
    /// trailing zeros: empty when nothing rose or the epochs differ.
    pub fn raised_over(&self, recorded: &Commit) -> Vec<u64> {
        if self.epoch != recorded.epoch {
            return Vec::new();
        }
        let mut raised: Vec<u64> = self
            .committed
            .iter()
            .enumerate()
            .map(|(shard, &value)| {
                if value > recorded.committed.get(shard).copied().unwrap_or(0) {
                    value
                } else {
                    0
                }
            })
            .collect();

        while raised.last() == Some(&0) {
            raised.pop();
        }
        raised
    }
}

/// Every group's recorded committed sequences.
pub type Recorded = BTreeMap<String, Commit>;

/// A move between groups the coordinators agreed to start.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Migration {
    pub id: String,
    pub first: u16,
    pub last: u16,
    pub from: String,
    pub to: String,
    /// The target group's shard that receives the range.
    pub shard: usize,
    /// Both groups' primary epochs when it began: a failover of either ends it.
    pub from_epoch: u64,
    pub to_epoch: u64,
}

/// A range that moved to `group`'s shard `shard`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovedRange {
    pub first: u16,
    pub last: u16,
    pub group: String,
    pub shard: usize,
}

/// Every committed move, oldest first, and the one in progress.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Moves {
    pub moved: Vec<MovedRange>,
    pub current: Option<Migration>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = bool,
);

pub type NodeId = u64;
pub type Raft = openraft::Raft<TypeConfig>;

/// The primary of one replication group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Primary {
    /// The member's client address (`host:port`), as the group lists it.
    pub member: String,
    pub epoch: u64,
}

impl Primary {
    /// Where the primary is in `group`'s members; `None` when it is not listed.
    pub fn index(&self, group: &Group) -> Option<usize> {
        group
            .members
            .iter()
            .position(|member| member.address.to_string() == self.member)
    }
}

/// Every group's primary, as the coordinators agreed on it.
pub type Primaries = BTreeMap<String, Primary>;

fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

/// Writes `bytes` to `path` through a synced temp file and a rename.
fn write_durably(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)?;

    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, path)?;
    #[cfg(unix)]
    std::fs::File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()?;
    Ok(())
}

// ---- log and vote ----------------------------------------------------------------------

#[derive(Default, Serialize, Deserialize)]
struct LogFile {
    last_purged: Option<LogId<NodeId>>,
    entries: Vec<Entry<TypeConfig>>,
}

struct LogInner {
    dir: PathBuf,
    last_purged: Option<LogId<NodeId>>,
    entries: BTreeMap<u64, Entry<TypeConfig>>,
    vote: Option<Vote<NodeId>>,
}

impl LogInner {
    /// Rewrites the whole log. The coordinators' log is a few small entries per failover, and
    /// snapshots purge it, so the file stays small.
    fn persist(&self) -> io::Result<()> {
        let file = LogFile {
            last_purged: self.last_purged,
            entries: self.entries.values().cloned().collect(),
        };

        write_durably(
            &self.dir.join("raft-log.json"),
            &serde_json::to_vec(&file).map_err(io_error)?,
        )
    }
}

/// The Raft log and vote, kept in memory and on disk.
#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Mutex<LogInner>>,
}

impl LogStore {
    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let file: LogFile = match std::fs::read(dir.join("raft-log.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io_error)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => LogFile::default(),
            Err(error) => return Err(error),
        };
        let vote = match std::fs::read(dir.join("raft-vote.json")) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes).map_err(io_error)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(LogInner {
                dir: dir.to_path_buf(),
                last_purged: file.last_purged,
                entries: file
                    .entries
                    .into_iter()
                    .map(|entry| (entry.log_id.index, entry))
                    .collect(),
                vote,
            })),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LogInner> {
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        Ok(self
            .lock()
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.lock();
        let last = inner
            .entries
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(inner.last_purged);

        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.lock();
        let bytes = serde_json::to_vec(vote).map_err(|error| StorageIOError::write_vote(&error))?;

        write_durably(&inner.dir.join("raft-vote.json"), &bytes)
            .map_err(|error| StorageIOError::write_vote(&error))?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.lock().vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.lock();

        for entry in entries {
            inner.entries.insert(entry.log_id.index, entry);
        }
        let result = inner.persist();

        callback.log_io_completed(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|error| io::Error::new(error.kind(), error.to_string())),
        );
        result.map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.lock();

        inner.entries.retain(|index, _| *index < log_id.index);
        inner
            .persist()
            .map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.lock();

        inner.entries.retain(|index, _| *index > log_id.index);
        inner.last_purged = Some(log_id);
        inner
            .persist()
            .map_err(|error| StorageIOError::write_logs(&error).into())
    }
}

// ---- state machine ---------------------------------------------------------------------

#[derive(Clone, Default, Serialize, Deserialize)]
struct MachineState {
    last_applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
    primaries: Primaries,
    /// Absent in snapshots written before moves between groups existed.
    #[serde(default)]
    moves: Moves,
    /// Absent in snapshots written before committed sequences were recorded.
    #[serde(default)]
    commits: Recorded,
    /// Each group's members once the leader recorded them; absent until then (the file's
    /// list holds).
    #[serde(default)]
    members: BTreeMap<String, Vec<Member>>,
    /// Per group, the client addresses forgotten with `COORDINATOR FORGET`.
    #[serde(default)]
    forgotten: BTreeMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// The agreed primaries. Readers (the failover loop, the Sentinel API) take a copy through
/// [`StateMachine::primaries`]; only Raft applies changes.
#[derive(Clone)]
pub struct StateMachine {
    dir: PathBuf,
    state: Arc<RwLock<MachineState>>,
    snapshot_count: Arc<Mutex<u64>>,
}

impl StateMachine {
    /// Opens the state as of the last snapshot; Raft applies the log entries after it.
    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let state = match std::fs::read(dir.join("raft-snapshot.json")) {
            Ok(bytes) => {
                let stored: StoredSnapshot = serde_json::from_slice(&bytes).map_err(io_error)?;

                serde_json::from_slice(&stored.data).map_err(io_error)?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => MachineState::default(),
            Err(error) => return Err(error),
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            state: Arc::new(RwLock::new(state)),
            snapshot_count: Arc::new(Mutex::new(0)),
        })
    }

    pub fn primaries(&self) -> Primaries {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .primaries
            .clone()
    }

    pub fn moves(&self) -> Moves {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .moves
            .clone()
    }

    /// `configured` with its members as recorded: the recorded list, then any member of the
    /// file it does not hold (added to the file since). The file's list until one is recorded.
    pub fn group(&self, configured: &Group) -> Group {
        self.group_and_record(configured).0
    }

    /// [`StateMachine::group`] and the recorded list it was built from, read together.
    pub fn group_and_record(&self, configured: &Group) -> (Group, Option<Vec<Member>>) {
        let state = self.state.read().unwrap_or_else(|error| error.into_inner());
        let mut group = configured.clone();
        let record = state.members.get(&configured.name).cloned();

        if let Some(recorded) = &record {
            let mut members = recorded.clone();

            for member in &configured.members {
                if !members.iter().any(|known| known.address == member.address) {
                    members.push(member.clone());
                }
            }
            group.members = members;
        }
        (group, record)
    }

    /// The client addresses forgotten in `group`.
    pub fn forgotten(&self, group: &str) -> Vec<String> {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .forgotten
            .get(group)
            .cloned()
            .unwrap_or_default()
    }

    pub fn commits(&self) -> Recorded {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .commits
            .clone()
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join("raft-snapshot.json")
    }
}

/// Applies a move request; returns whether it changed anything. A commit applies only
/// while both groups still have the primaries the move began with.
pub fn apply_move(moves: &mut Moves, primaries: &Primaries, request: &Request) -> bool {
    match request {
        Request::BeginMove(migration) => {
            if moves.current.is_some() {
                return false;
            }
            moves.current = Some(migration.clone());
            true
        }
        Request::CommitMove { id } => {
            let unchanged = |group: &String, epoch: u64| {
                primaries
                    .get(group)
                    .is_some_and(|primary| primary.epoch == epoch)
            };
            let Some(migration) = moves.current.take_if(|current| {
                current.id == *id
                    && unchanged(&current.from, current.from_epoch)
                    && unchanged(&current.to, current.to_epoch)
            }) else {
                return false;
            };

            moves.moved.push(MovedRange {
                first: migration.first,
                last: migration.last,
                group: migration.to,
                shard: migration.shard,
            });
            true
        }
        Request::EndMove { id } => moves.current.take_if(|current| current.id == *id).is_some(),
        Request::SetPrimary { .. }
        | Request::Commit(_)
        | Request::SetMembers { .. }
        | Request::Forget { .. } => false,
    }
}

/// A `SetPrimary` naming a member forgotten since the leader chose it: refused, so a forget
/// that won a race with a failover never leaves the group with a primary it does not list.
pub fn forgotten_primary(forgotten: &BTreeMap<String, Vec<String>>, request: &Request) -> bool {
    let Request::SetPrimary { group, member, .. } = request else {
        return false;
    };
    forgotten
        .get(group)
        .is_some_and(|forgotten| forgotten.contains(member))
}

/// Applies a members record or a forget; returns whether it changed anything. The checks
/// read only the state, so every coordinator decides the same.
pub fn apply_members(
    members: &mut BTreeMap<String, Vec<Member>>,
    forgotten: &mut BTreeMap<String, Vec<String>>,
    primaries: &Primaries,
    request: &Request,
) -> bool {
    let primary = |group: &String| primaries.get(group).map(|primary| primary.member.clone());

    match request {
        Request::SetMembers {
            group,
            previous,
            members: listed,
        } => {
            let twice = listed.iter().enumerate().any(|(index, member)| {
                listed[..index]
                    .iter()
                    .any(|other| other.address == member.address)
            });
            let holds_primary = primary(group).is_none_or(|primary| {
                listed
                    .iter()
                    .any(|member| member.address.to_string() == primary)
            });

            if members.get(group) != previous.as_ref() || twice || !holds_primary {
                return false;
            }
            // Listing a forgotten member takes it back: the leader lists one only when the
            // file names it again (discovery skips forgotten members).
            let mut changed = false;

            if let Some(forgotten) = forgotten.get_mut(group) {
                let before = forgotten.len();

                forgotten.retain(|address| {
                    !listed
                        .iter()
                        .any(|member| member.address.to_string() == *address)
                });
                changed = forgotten.len() != before;
            }
            if members.get(group) != Some(listed) {
                members.insert(group.clone(), listed.clone());
                changed = true;
            }
            changed
        }
        Request::Forget { group, member } => {
            if primary(group).as_ref() == Some(member) {
                return false;
            }
            let known = forgotten.entry(group.clone()).or_default();
            let mut changed = false;

            if !known.contains(member) {
                known.push(member.clone());
                changed = true;
            }
            if let Some(listed) = members.get_mut(group) {
                let before = listed.len();

                listed.retain(|known| known.address.to_string() != *member);
                changed |= listed.len() != before;
            }
            changed
        }
        _ => false,
    }
}

/// After a `SetPrimary` that applied: the group's committed sequences under the new primary.
pub fn apply_promotion(commits: &mut Recorded, request: &Request) {
    let Request::SetPrimary {
        group,
        epoch,
        commit,
        ..
    } = request
    else {
        return;
    };
    let entry = commits.entry(group.clone()).or_default();
    let kept = commit.clone().unwrap_or_else(|| std::mem::take(entry));

    *entry = Commit {
        epoch: *epoch,
        ..kept
    };
}

/// Applies a commit record: raises the shards it names, for groups whose primary is still of
/// the record's epoch. Returns whether it changed anything.
pub fn apply_commit(commits: &mut Recorded, primaries: &Primaries, request: &Request) -> bool {
    let Request::Commit(groups) = request else {
        return false;
    };
    let mut changed = false;

    for (group, raised) in groups {
        if primaries.get(group).map(|primary| primary.epoch) != Some(raised.epoch) {
            continue;
        }
        let entry = commits.entry(group.clone()).or_default();

        if entry.epoch != raised.epoch {
            // State from before commits were recorded: start the group's record here.
            *entry = Commit {
                epoch: raised.epoch,
                ..Commit::default()
            };
        }
        for (shard, &value) in raised.committed.iter().enumerate() {
            if value > entry.committed.get(shard).copied().unwrap_or(0) {
                raise(&mut entry.committed, shard, value);
                changed = true;
            }
        }
    }
    changed
}

/// Applies one request; returns whether it changed anything.
pub fn apply_request(primaries: &mut Primaries, request: &Request) -> bool {
    match request {
        Request::BeginMove(_)
        | Request::CommitMove { .. }
        | Request::EndMove { .. }
        | Request::Commit(_)
        | Request::SetMembers { .. }
        | Request::Forget { .. } => false,
        Request::SetPrimary {
            group,
            member,
            epoch,
            ..
        } => {
            if primaries
                .get(group)
                .is_some_and(|current| current.epoch >= *epoch)
            {
                return false;
            }
            primaries.insert(
                group.clone(),
                Primary {
                    member: member.clone(),
                    epoch: *epoch,
                },
            );
            true
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let data = serde_json::to_vec(&state)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let count = {
            let mut count = self
                .snapshot_count
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            *count += 1;
            *count
        };
        let meta = SnapshotMeta {
            last_log_id: state.last_applied,
            last_membership: state.membership.clone(),
            snapshot_id: format!(
                "{}-{count}",
                state.last_applied.map_or(0, |log_id| log_id.index)
            ),
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        write_durably(
            &self.snapshot_path(),
            &serde_json::to_vec(&stored)
                .map_err(|error| StorageIOError::write_snapshot(None, &error))?,
        )
        .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state = self.state.read().unwrap_or_else(|error| error.into_inner());

        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<bool>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let mut replies = Vec::new();

        for entry in entries {
            state.last_applied = Some(entry.log_id);
            replies.push(match entry.payload {
                EntryPayload::Blank => false,
                EntryPayload::Normal(request) => {
                    let state = &mut *state;

                    if forgotten_primary(&state.forgotten, &request) {
                        false
                    } else if apply_request(&mut state.primaries, &request) {
                        apply_promotion(&mut state.commits, &request);
                        true
                    } else {
                        apply_move(&mut state.moves, &state.primaries, &request)
                            || apply_commit(&mut state.commits, &state.primaries, &request)
                            || apply_members(
                                &mut state.members,
                                &mut state.forgotten,
                                &state.primaries,
                                &request,
                            )
                    }
                }
                EntryPayload::Membership(membership) => {
                    state.membership = StoredMembership::new(Some(entry.log_id), membership);
                    false
                }
            });
        }

        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let installed: MachineState = serde_json::from_slice(&data)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };

        write_durably(
            &self.snapshot_path(),
            &serde_json::to_vec(&stored)
                .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?,
        )
        .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        *self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner()) = installed;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match std::fs::read(self.snapshot_path()) {
            Ok(bytes) => {
                let stored: StoredSnapshot = serde_json::from_slice(&bytes)
                    .map_err(|error| StorageIOError::read_snapshot(None, &error))?;

                Ok(Some(Snapshot {
                    meta: stored.meta,
                    snapshot: Box::new(Cursor::new(stored.data)),
                }))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StorageIOError::read_snapshot(None, &error).into()),
        }
    }
}

// ---- network ---------------------------------------------------------------------------

/// First byte of a coordinator-to-coordinator connection; a RESP client starts with `*`.
pub const RPC_MAGIC: u8 = b'R';
const MAX_FRAME: u32 = 64 * 1024 * 1024;

/// A call from one coordinator to another.
#[derive(Serialize, Deserialize)]
pub enum RpcRequest {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    Snapshot(InstallSnapshotRequest<TypeConfig>),
    /// How long ago this coordinator last heard from `member` (a client address) of
    /// `group`, in milliseconds; `None` for never.
    Observe {
        group: String,
        member: String,
    },
    /// [`RpcRequest::Observe`], and whether the primary lease of `member` ended by this
    /// coordinator's own settings. A yes promises no grant to it until the leader confirms it
    /// again ([`RpcRequest::Confirm`]). Coordinators before the lease cannot decode it; the
    /// leader then falls back to `Observe`.
    Leases {
        group: String,
        member: String,
    },
    /// To the leader: the primary of `group`, read after any failover decision in progress
    /// and once a majority confirmed the leader.
    Confirm {
        group: String,
    },
}

#[derive(Serialize, Deserialize)]
pub enum RpcResponse {
    Append(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    Snapshot(Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>),
    Observe(Option<u64>),
    Leases {
        heard: Option<u64>,
        /// The lease ended by this coordinator's settings, or it grants none.
        ended: bool,
        /// This coordinator grants leases.
        lease_on: bool,
    },
    /// `None` when the leader could not confirm.
    Confirmed(Option<Primary>),
}

/// What a coordinator sends on a call: the shared secret, then the request.
#[derive(Serialize, Deserialize)]
pub struct Envelope {
    pub secret: String,
    pub request: RpcRequest,
}

pub async fn write_frame<T: Serialize>(stream: &mut TcpStream, value: &T) -> io::Result<()> {
    let body = serde_json::to_vec(value).map_err(io_error)?;
    let length = u32::try_from(body.len()).map_err(io_error)?;

    stream.write_all(&length.to_le_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

pub async fn read_frame<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> io::Result<T> {
    let mut length = [0; 4];

    stream.read_exact(&mut length).await?;
    let length = u32::from_le_bytes(length);

    if length > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut body = vec![0; length as usize];

    stream.read_exact(&mut body).await?;
    serde_json::from_slice(&body).map_err(io_error)
}

/// One call to the coordinator at `address`.
pub async fn call(
    address: &str,
    secret: &str,
    request: RpcRequest,
    wait: std::time::Duration,
) -> io::Result<RpcResponse> {
    tokio::time::timeout(wait, async {
        let mut stream = TcpStream::connect(address).await?;

        stream.set_nodelay(true)?;
        stream.write_all(&[RPC_MAGIC]).await?;
        write_frame(
            &mut stream,
            &Envelope {
                secret: secret.to_owned(),
                request,
            },
        )
        .await?;
        read_frame(&mut stream).await
    })
    .await
    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

/// Opens clients to the other coordinators; `secret` goes with every call.
pub struct Network {
    pub secret: Arc<str>,
}

pub struct PeerClient {
    target: NodeId,
    address: String,
    secret: Arc<str>,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = PeerClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        PeerClient {
            target,
            address: node.addr.clone(),
            secret: Arc::clone(&self.secret),
        }
    }
}

type RpcResult<T, E = openraft::error::Infallible> =
    Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>;

impl PeerClient {
    async fn send(
        &self,
        request: RpcRequest,
        option: &RPCOption,
    ) -> Result<RpcResponse, Unreachable> {
        call(&self.address, &self.secret, request, option.hard_ttl())
            .await
            .map_err(|error| Unreachable::new(&error))
    }
}

impl RaftNetwork<TypeConfig> for PeerClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<NodeId>> {
        match self.send(RpcRequest::Append(rpc), &option).await? {
            RpcResponse::Append(result) => {
                result.map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
            }
            _ => Err(Unreachable::new(&io_error("unexpected reply")).into()),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<NodeId>, InstallSnapshotError> {
        match self.send(RpcRequest::Snapshot(rpc), &option).await? {
            RpcResponse::Snapshot(result) => {
                result.map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
            }
            _ => Err(Unreachable::new(&io_error("unexpected reply")).into()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<NodeId>> {
        match self.send(RpcRequest::Vote(rpc), &option).await? {
            RpcResponse::Vote(result) => {
                result.map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
            }
            _ => Err(Unreachable::new(&io_error("unexpected reply")).into()),
        }
    }
}

/// The longest a confirmed leadership is trusted, and the longest one confirmation may take.
/// Below the 1 s minimum election timeout and openraft's 2 s leader lease (a follower that
/// answered the confirmation votes for nobody else within the lease), so no other leader can
/// have been elected while a confirmation is still trusted.
const LEADERSHIP_WINDOW: Duration = Duration::from_secs(1);

/// Whether this coordinator leads with a majority behind it, asked before every action on the
/// data nodes. openraft 0.9 keeps a leader that cannot reach the other coordinators leading
/// (it steps down only on a higher vote or a membership change), so `current_leader()` alone
/// lets a cut-off leader keep acting on its old record while the majority elects another.
/// A confirmation (`ensure_linearizable`: a heartbeat a majority answers) is trusted for one
/// window, so every caller in that window shares one round of heartbeats.
pub struct Leadership {
    id: NodeId,
    window: Duration,
    state: tokio::sync::Mutex<Confirmation>,
}

impl Leadership {
    /// Confirmations trusted for one `ping_interval` (the leader's tick), at most 1 s.
    pub fn new(id: NodeId, ping_interval: Duration) -> Self {
        Self {
            id,
            window: ping_interval.min(LEADERSHIP_WINDOW),
            state: tokio::sync::Mutex::default(),
        }
    }

    /// True when this coordinator is the leader and a majority confirmed it within the window.
    /// Never waits longer than the window.
    pub async fn confirm(&self, raft: &Raft) -> bool {
        let (leader, term) = {
            let metrics = raft.metrics();
            let metrics = metrics.borrow();

            (metrics.current_leader, metrics.current_term)
        };
        // One caller confirms at a time; the others then find its answer.
        let mut state = self.state.lock().await;

        if leader != Some(self.id) {
            *state = Confirmation::default();
            return false;
        }
        if state.fresh(term, self.window) {
            return true;
        }
        let started = Instant::now();
        let confirmed = matches!(
            tokio::time::timeout(self.window, raft.ensure_linearizable()).await,
            Ok(Ok(_))
        );

        match state.record(term, started, confirmed) {
            Some(true) => {
                tracing::info!(
                    term,
                    "a majority of coordinators confirms this leader again; acting on the data nodes"
                )
            }
            Some(false) => {
                tracing::warn!(
                    term,
                    "this leader cannot reach a majority of coordinators; not acting on the data nodes until it can"
                )
            }
            None => {}
        }
        confirmed
    }
}

/// The last confirmation of one leader.
#[derive(Debug, Default)]
struct Confirmation {
    /// When the last successful confirmation began, in which term.
    at: Option<(u64, Instant)>,
    /// The last confirmation failed (logged once, until one succeeds).
    lost: bool,
}

impl Confirmation {
    fn fresh(&self, term: u64, window: Duration) -> bool {
        self.at
            .is_some_and(|(known, at)| known == term && at.elapsed() < window)
    }

    /// Takes in one confirmation's outcome; `Some(confirmed)` when it changes from the last
    /// (a leader's first success is not a change).
    fn record(&mut self, term: u64, started: Instant, confirmed: bool) -> Option<bool> {
        self.at = confirmed.then_some((term, started));
        let changed = self.lost == confirmed;

        self.lost = !confirmed;
        changed.then_some(confirmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_leadership_confirmation_is_trusted_for_one_window_of_its_term() {
        let window = Duration::from_secs(60);
        let mut confirmation = Confirmation::default();

        assert!(!confirmation.fresh(1, window));
        // The first success is not logged; it is trusted in its term only.
        assert_eq!(confirmation.record(1, Instant::now(), true), None);
        assert!(confirmation.fresh(1, window));
        assert!(!confirmation.fresh(2, window));
        // Older than the window: confirmed again.
        let old = Instant::now() - Duration::from_millis(20);
        confirmation.record(1, old, true);
        assert!(!confirmation.fresh(1, Duration::from_millis(10)));

        // A failure is logged once, not on every tick, and nothing is trusted meanwhile.
        assert_eq!(confirmation.record(1, Instant::now(), false), Some(false));
        assert!(!confirmation.fresh(1, window));
        assert_eq!(confirmation.record(1, Instant::now(), false), None);
        assert_eq!(confirmation.record(1, Instant::now(), false), None);
        // The majority answers again: logged once.
        assert_eq!(confirmation.record(1, Instant::now(), true), Some(true));
        assert_eq!(confirmation.record(1, Instant::now(), true), None);
        assert!(confirmation.fresh(1, window));
    }

    #[test]
    fn recorded_members_replace_the_files_and_keep_members_added_to_it_since() {
        let member = |port: u16| Member {
            address: super::super::config::Endpoint::parse(&format!("127.0.0.1:{port}")).unwrap(),
            replication: super::super::config::Endpoint::parse(&format!("127.0.0.1:2{port}"))
                .unwrap(),
        };
        let configured = Group {
            name: "main".into(),
            members: vec![member(6379), member(6380)],
            shards: None,
            slots: Vec::new(),
        };
        let dir = tempfile::tempdir().unwrap();
        let machine = StateMachine::open(dir.path()).unwrap();

        // Nothing recorded: the file's list.
        assert_eq!(machine.group(&configured), configured);
        let mut members = BTreeMap::new();
        let mut forgotten = BTreeMap::new();
        let mut primaries = Primaries::new();

        primaries.insert(
            "main".into(),
            Primary {
                member: "127.0.0.1:6380".into(),
                epoch: 2,
            },
        );
        macro_rules! apply {
            ($request:expr) => {
                apply_members(&mut members, &mut forgotten, &primaries, $request)
            };
        }
        let found = Request::SetMembers {
            group: "main".into(),
            previous: None,
            members: vec![member(6379), member(6380), member(6381)],
        };

        // A list without the primary, or with an address twice, is refused.
        assert!(!apply!(&Request::SetMembers {
            group: "main".into(),
            previous: None,
            members: vec![member(6379), member(6381)],
        }));
        assert!(!apply!(&Request::SetMembers {
            group: "main".into(),
            previous: None,
            members: vec![member(6379), member(6380), member(6381), member(6381)],
        }));
        assert!(apply!(&found));
        // Built from a list that changed since: refused.
        assert!(!apply!(&found));
        // The primary is not forgotten; another member is, once.
        assert!(!apply!(&Request::Forget {
            group: "main".into(),
            member: "127.0.0.1:6380".into(),
        }));
        let forget = Request::Forget {
            group: "main".into(),
            member: "127.0.0.1:6381".into(),
        };

        assert!(apply!(&forget));
        assert!(!apply!(&forget));
        // A forgotten member is not made primary until it is listed again.
        assert!(forgotten_primary(
            &forgotten,
            &Request::SetPrimary {
                group: "main".into(),
                member: "127.0.0.1:6381".into(),
                epoch: 3,
                commit: None,
            }
        ));
        assert_eq!(forgotten["main"], ["127.0.0.1:6381"]);
        assert_eq!(members["main"].len(), 2);
        // Listed again (the file names it): no longer forgotten.
        assert!(apply!(&Request::SetMembers {
            group: "main".into(),
            previous: Some(vec![member(6379), member(6380)]),
            members: vec![member(6379), member(6380), member(6381)],
        }));
        assert!(forgotten["main"].is_empty());
        machine.state.write().unwrap().members = members;
        let listed = |group: &Group| -> Vec<u16> {
            group
                .members
                .iter()
                .map(|member| member.address.port)
                .collect()
        };

        assert_eq!(listed(&machine.group(&configured)), [6379, 6380, 6381]);
        // A member added to the file later is kept after the recorded ones.
        let mut grown = configured.clone();

        grown.members.push(member(6390));
        assert_eq!(listed(&machine.group(&grown)), [6379, 6380, 6381, 6390]);
        assert_eq!(
            Primary {
                member: "127.0.0.1:6381".into(),
                epoch: 1
            }
            .index(&machine.group(&configured)),
            Some(2)
        );
    }

    #[test]
    fn set_primary_applies_only_above_the_recorded_epoch() {
        let mut primaries = Primaries::new();
        let set = |member: usize, epoch| Request::SetPrimary {
            group: "main".into(),
            member: format!("127.0.0.1:{}", 6379 + member),
            epoch,
            commit: None,
        };

        assert!(apply_request(&mut primaries, &set(0, 1)));
        assert!(apply_request(&mut primaries, &set(2, 2)));
        // A retried or late request at an older epoch changes nothing.
        assert!(!apply_request(&mut primaries, &set(1, 2)));
        assert!(!apply_request(&mut primaries, &set(1, 1)));
        assert_eq!(
            primaries["main"],
            Primary {
                member: "127.0.0.1:6381".into(),
                epoch: 2
            }
        );
    }

    #[test]
    fn one_move_runs_at_a_time_and_only_its_own_id_ends_it() {
        let mut moves = Moves::default();
        let mut primaries = Primaries::new();

        for group in ["a", "b"] {
            primaries.insert(
                group.into(),
                Primary {
                    member: "127.0.0.1:6379".into(),
                    epoch: 1,
                },
            );
        }
        let begin = |id: &str| {
            Request::BeginMove(Migration {
                id: id.into(),
                first: 100,
                last: 199,
                from: "a".into(),
                to: "b".into(),
                shard: 0,
                from_epoch: 1,
                to_epoch: 1,
            })
        };

        assert!(apply_move(&mut moves, &primaries, &begin("m1")));
        assert!(!apply_move(&mut moves, &primaries, &begin("m2")));
        assert!(!apply_move(
            &mut moves,
            &primaries,
            &Request::CommitMove { id: "m2".into() }
        ));
        assert!(apply_move(
            &mut moves,
            &primaries,
            &Request::CommitMove { id: "m1".into() }
        ));
        // A retried commit or a late end changes nothing.
        assert!(!apply_move(
            &mut moves,
            &primaries,
            &Request::CommitMove { id: "m1".into() }
        ));
        assert!(!apply_move(
            &mut moves,
            &primaries,
            &Request::EndMove { id: "m1".into() }
        ));
        assert_eq!(
            moves.moved,
            [MovedRange {
                first: 100,
                last: 199,
                group: "b".into(),
                shard: 0
            }]
        );
        assert!(apply_move(&mut moves, &primaries, &begin("m2")));
        assert!(apply_move(
            &mut moves,
            &primaries,
            &Request::EndMove { id: "m2".into() }
        ));
        assert_eq!(moves.current, None);
        assert_eq!(moves.moved.len(), 1);

        // A failover of either group after the move began keeps it from committing.
        assert!(apply_move(&mut moves, &primaries, &begin("m3")));
        primaries.get_mut("b").unwrap().epoch = 2;
        assert!(!apply_move(
            &mut moves,
            &primaries,
            &Request::CommitMove { id: "m3".into() }
        ));
        assert_eq!(moves.moved.len(), 1);
    }

    #[test]
    fn a_promotion_sets_the_commit_record_and_raises_apply_only_to_its_epoch() {
        let mut primaries = Primaries::new();
        let mut recorded = Recorded::new();
        fn apply(primaries: &mut Primaries, recorded: &mut Recorded, request: Request) -> bool {
            if apply_request(primaries, &request) {
                apply_promotion(recorded, &request);
                true
            } else {
                apply_commit(recorded, primaries, &request)
            }
        }
        let raise = |epoch, committed: &[u64]| {
            Request::Commit(BTreeMap::from([(
                "main".to_owned(),
                Raised {
                    epoch,
                    committed: committed.to_vec(),
                },
            )]))
        };
        let promote = |member: usize, epoch, commit| Request::SetPrimary {
            group: "main".into(),
            member: format!("127.0.0.1:{}", 6379 + member),
            epoch,
            commit,
        };

        // The first record of a group carries nothing.
        assert!(apply(&mut primaries, &mut recorded, promote(0, 1, None)));
        assert!(apply(&mut primaries, &mut recorded, raise(1, &[20, 30])));
        // Retried or late: nothing changes.
        assert!(!apply(&mut primaries, &mut recorded, raise(1, &[20, 30])));
        assert!(!apply(&mut primaries, &mut recorded, raise(1, &[10])));
        assert!(apply(&mut primaries, &mut recorded, raise(1, &[25])));
        assert_eq!(recorded["main"].committed, [25, 30]);

        // An unclean promotion decided what holds under epoch 2.
        let kept = recorded["main"].after_promotion(&[25, 12], 2);
        assert_eq!(kept.committed, [25, 0]);
        assert_eq!(kept.stale, [0, 30]);
        assert!(apply(
            &mut primaries,
            &mut recorded,
            promote(2, 2, Some(kept.clone()))
        ));
        assert_eq!(recorded["main"], kept);
        // A raise of the old epoch arriving late changes nothing.
        assert!(!apply(&mut primaries, &mut recorded, raise(1, &[0, 90])));
        assert_eq!(recorded["main"], kept);

        // A promotion without a decision (written by an older coordinator) keeps the values.
        assert!(apply(&mut primaries, &mut recorded, promote(1, 3, None)));
        assert_eq!(
            recorded["main"],
            Commit {
                epoch: 3,
                ..kept.clone()
            }
        );
        // A retried promotion changes nothing, not even the record.
        assert!(!apply(
            &mut primaries,
            &mut recorded,
            promote(0, 3, Some(Commit::default()))
        ));
        assert_eq!(recorded["main"].epoch, 3);
        assert_eq!(recorded["main"].committed, [25, 0]);
    }

    #[test]
    fn only_the_shards_that_rose_are_sent() {
        let recorded = Commit {
            epoch: 4,
            committed: vec![10, 20, 30],
            stale: vec![],
        };
        let seen = Commit {
            committed: vec![10, 21, 30, 5],
            ..recorded.clone()
        };

        assert_eq!(seen.raised_over(&recorded), [0, 21, 0, 5]);
        // Nothing rose past the record: nothing to send.
        assert!(recorded.raised_over(&seen).is_empty());
        // Another epoch: the promotion's record comes first.
        let older = Commit {
            epoch: 3,
            ..seen.clone()
        };
        assert!(older.raised_over(&recorded).is_empty());
    }

    #[test]
    fn a_snapshot_without_commit_records_still_loads() {
        let state: MachineState = serde_json::from_str(
            r#"{"last_applied":null,"membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}},"primaries":{}}"#,
        )
        .unwrap();

        assert!(state.commits.is_empty());
    }

    /// The cost of recording committed sequences for 8 groups of 64 shards: bytes per entry
    /// and the time to rewrite and sync the whole log once, at the log length the snapshot
    /// policy allows. Run by hand:
    /// `cargo test --features coordinator --lib -- --ignored --nocapture commit_record_cost`.
    #[test]
    #[ignore]
    fn commit_record_cost() {
        const GROUPS: usize = 8;
        const SHARDS: u64 = 64;
        let group = |index: usize| format!("group-{index}");
        let full = Commit {
            epoch: 7,
            committed: (0..SHARDS).map(|shard| 1_000_000_000 + shard).collect(),
            stale: Vec::new(),
        };
        // The entry an earlier draft wrote: every group's whole record.
        let whole: Recorded = (0..GROUPS)
            .map(|index| (group(index), full.clone()))
            .collect();
        let whole_bytes = serde_json::to_vec(&serde_json::json!({ "Commit": whole }))
            .unwrap()
            .len();
        // `rose` shards of each group rose, the last ones (the most zeros before them).
        let raised = |rose: u64| {
            Request::Commit(
                (0..GROUPS)
                    .map(|index| {
                        let raised = Raised {
                            epoch: 7,
                            committed: (0..SHARDS)
                                .map(|shard| {
                                    if shard >= SHARDS - rose {
                                        1_000_000_000 + shard
                                    } else {
                                        0
                                    }
                                })
                                .collect(),
                        };

                        (group(index), raised)
                    })
                    .collect(),
            )
        };
        // An entry of about the earlier size, to time a log of such entries.
        let padded = Request::SetPrimary {
            group: "g".into(),
            member: "127.0.0.1:6379".into(),
            epoch: 7,
            commit: Some(Commit {
                epoch: 7,
                committed: (0..GROUPS as u64 * SHARDS)
                    .map(|shard| 1_000_000_000 + shard)
                    .collect(),
                stale: Vec::new(),
            }),
        };
        let entry = |index: u64, request: &Request| Entry::<TypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(request.clone()),
        };
        let bytes = |request: &Request| serde_json::to_vec(&entry(1, request)).unwrap().len();
        let time = |entries: u64, request: &Request| {
            let dir = tempfile::tempdir().unwrap();
            let store = LogStore::open(dir.path()).unwrap();
            let mut inner = store.lock();

            for index in 1..=entries {
                inner.entries.insert(index, entry(index, request));
            }
            inner.persist().unwrap();
            let rounds = 20;
            let started = std::time::Instant::now();

            for round in 0..rounds {
                inner
                    .entries
                    .insert(entries + 1 + round, entry(entries + 1 + round, request));
                inner.persist().unwrap();
            }
            let size = std::fs::metadata(dir.path().join("raft-log.json"))
                .unwrap()
                .len();

            (started.elapsed() / rounds as u32, size)
        };

        println!("earlier entry, whole records: {whole_bytes} bytes");
        println!("padded stand-in for it: {} bytes", bytes(&padded));
        for shards in [1, 8, 64] {
            println!(
                "new entry, {shards} shard(s) rose per group: {} bytes",
                bytes(&raised(shards))
            );
        }
        let (per, size) = time(1100, &padded);
        println!("before: 1100 entries of the earlier size: {per:?} per append, log {size} bytes");
        for shards in [8, 64] {
            let (per, size) = time(120, &raised(shards));
            println!(
                "after: 120 entries, {shards} shards rose per group: {per:?} per append, log {size} bytes"
            );
        }
    }

    #[test]
    fn the_log_and_vote_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        runtime.block_on(async {
            let mut store = LogStore::open(dir.path()).unwrap();
            let vote = Vote::new(3, 1);

            store.save_vote(&vote).await.unwrap();
            let mut reopened = LogStore::open(dir.path()).unwrap();

            assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
            assert!(
                reopened
                    .get_log_state()
                    .await
                    .unwrap()
                    .last_log_id
                    .is_none()
            );
        });
    }
}
