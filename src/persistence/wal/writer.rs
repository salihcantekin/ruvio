//! The shard's WAL writer thread. The shard encodes records, keeps every logical cursor and
//! hands the bytes, segment changes and file removals to this thread in order, so its own
//! thread never waits for the disk on the write path. A disk that stops answering for a few
//! seconds then holds only this thread: the shard keeps answering reads, health checks and
//! replication, `everysec` writes keep being acknowledged until `WAL_WRITER_QUEUE_BYTES` are
//! waiting, and `always` replies wait for their sync as before.
//!
//! What a log must see on disk first (a sync, a truncation, recovery after a failure) waits
//! for the queue to empty (`Writer::drain`) and then works on [`Physical`] directly, as the
//! shard did before; those paths are rare or already wait for the disk.

use std::collections::VecDeque;
use std::sync::{Condvar, MutexGuard};
use std::thread::JoinHandle;

use super::*;

/// Bytes handed to the writer and not written yet before the shard waits for the disk
/// itself, as it did for every write before the writer thread.
const WAL_WRITER_QUEUE_BYTES: u64 = 64 * 1024 * 1024;
/// Write buffers kept for reuse, and the largest one kept.
const SPARE_BUFFERS: usize = 8;
const SPARE_BUFFER_BYTES: usize = 256 * 1024;
/// Room for queued operations before the queue grows.
const OPS_CAPACITY: usize = 16;

/// Work on the files that runs in order with the writes.
pub(super) type Task = Box<dyn FnOnce(&mut Physical, &WriterShared) + Send>;

enum Op {
    Write {
        bytes: Vec<u8>,
        last_sequence: u64,
        /// A test's IO faults armed on the shard thread for this write (the write itself, the
        /// sync of the preallocation fill it asks for).
        #[cfg(test)]
        fault: (bool, bool),
    },
    Task(Task),
}

#[derive(Default)]
struct Queue {
    ops: VecDeque<Op>,
    bytes: u64,
    busy: bool,
    stopped: bool,
    /// The writer waits for work and needs a wake.
    idle: bool,
    /// Threads waiting for the writer's progress: woken after every batch.
    waiters: u32,
    spare: Vec<Vec<u8>>,
}

/// The open segment as it is on disk. Owned by the writer thread while it works; anyone else
/// drains the queue first.
pub(super) struct Physical {
    pub(super) file: Option<Arc<File>>,
    /// Bytes written to `file`; it is not in append mode, so a preallocated tail does not move it.
    pub(super) write_pos: u64,
    /// Size the segment file has been extended to by `fallocate`.
    pub(super) prealloc_end: u64,
    pub(super) prealloc: bool,
    pub(super) prefill: Prefill,
    pub(super) segment_first_sequence: u64,
    pub(super) segment_bytes: u64,
    /// Last sequence whose record is in the file.
    pub(super) written: u64,
    /// A write or a segment change failed: later records are kept in `unwritten`, in order,
    /// until the shard's recovery writes them.
    pub(super) failed: Option<String>,
    pub(super) unwritten: Vec<u8>,
    /// Sequence of the first record in `unwritten`; meaningful only while it is non-empty.
    pub(super) unwritten_first: u64,
    pub(super) data_dir: PathBuf,
    pub(super) shard_id: u32,
}

pub(super) struct WriterShared {
    queue: Mutex<Queue>,
    /// The writer waits here for work.
    work: Condvar,
    /// Everyone else waits here for the writer's progress.
    progress: Condvar,
    physical: Mutex<Physical>,
    written: AtomicU64,
    /// The last sequence the shard published; readers see it once it is written too.
    publish_target: AtomicU64,
    published: Arc<AtomicU64>,
    pub(super) durable: Arc<DurableSequence>,
    pub(super) sync_error: Arc<SyncError>,
    failed: AtomicBool,
    failure: Mutex<Option<String>>,
    #[cfg(test)]
    data_dir: PathBuf,
}

pub(super) struct Writer {
    shared: Arc<WriterShared>,
    thread: Option<JoinHandle<()>>,
}

impl Writer {
    /// A writer for `physical`; `spawn` is false for a log that never writes (no thread).
    pub(super) fn new(
        physical: Physical,
        published: Arc<AtomicU64>,
        durable: Arc<DurableSequence>,
        sync_error: Arc<SyncError>,
        spawn: bool,
    ) -> io::Result<Self> {
        let written = physical.written;
        let shard_id = physical.shard_id;
        #[cfg(test)]
        let data_dir = physical.data_dir.clone();
        let shared = Arc::new(WriterShared {
            queue: Mutex::new(Queue {
                // Two buffers, one filling while the writer writes the other, cover the steady
                // state. Untouched capacity is not resident.
                spare: if spawn {
                    (0..2)
                        .map(|_| Vec::with_capacity(SPARE_BUFFER_BYTES))
                        .collect()
                } else {
                    Vec::new()
                },
                // The shard and the writer swap this with the writer's own queue; both start
                // with room so a push on the shard does not allocate.
                ops: VecDeque::with_capacity(if spawn { OPS_CAPACITY } else { 0 }),
                ..Queue::default()
            }),
            work: Condvar::new(),
            progress: Condvar::new(),
            physical: Mutex::new(physical),
            written: AtomicU64::new(written),
            publish_target: AtomicU64::new(published.load(Ordering::Acquire)),
            published,
            durable,
            sync_error,
            failed: AtomicBool::new(false),
            failure: Mutex::new(None),
            #[cfg(test)]
            data_dir,
        });
        #[cfg(test)]
        if spawn {
            WRITERS
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(Arc::downgrade(&shared));
        }
        let thread = if spawn {
            let worker = Arc::clone(&shared);

            Some(
                std::thread::Builder::new()
                    .name(format!("ruvio-wal-writer-{shard_id}"))
                    .spawn(move || run(&worker))?,
            )
        } else {
            None
        };

        Ok(Self { shared, thread })
    }

    /// Hands `bytes` (records up to `last_sequence`) to the writer.
    pub(super) fn write(&self, bytes: &[u8], last_sequence: u64) -> io::Result<()> {
        #[cfg(test)]
        let fault = (
            super::crash::take_io_fault(super::crash::CrashSite::Append).is_some(),
            super::crash::take_io_fault(super::crash::CrashSite::PrefillSync).is_some(),
        );
        let mut guard = self.wait_for_space(bytes.len() as u64)?;
        let queue = &mut *guard;

        // A write the writer has not taken yet absorbs this one: the shard and the writer then
        // swap two warm buffers instead of one per append, and the steady state allocates nothing.
        #[cfg(test)]
        let joins = fault == (false, false);
        #[cfg(not(test))]
        let joins = true;
        if joins
            && let Some(Op::Write {
                bytes: queued,
                last_sequence: queued_last,
                #[cfg(test)]
                    fault: (false, false),
            }) = queue.ops.back_mut()
            && (queued.capacity() - queued.len() >= bytes.len() || queue.spare.is_empty())
        {
            queued.extend_from_slice(bytes);
            *queued_last = last_sequence;
            queue.bytes += bytes.len() as u64;
            return Ok(());
        }
        let mut buffer = queue
            .spare
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(SPARE_BUFFER_BYTES.max(bytes.len())));

        buffer.extend_from_slice(bytes);
        self.push(
            queue,
            Op::Write {
                bytes: buffer,
                last_sequence,
                #[cfg(test)]
                fault,
            },
            bytes.len() as u64,
        );
        Ok(())
    }

    /// Runs `task` on the writer thread after everything handed over before it.
    pub(super) fn task(&self, task: Task) -> io::Result<()> {
        let mut queue = self.wait_for_space(0)?;

        self.push(&mut queue, Op::Task(task), 0);
        Ok(())
    }

    fn wait_for_space(&self, bytes: u64) -> io::Result<MutexGuard<'_, Queue>> {
        let mut queue = self.shared.lock_queue();

        if queue.bytes > 0 && queue.bytes.saturating_add(bytes) > WAL_WRITER_QUEUE_BYTES {
            #[cfg(test)]
            note_writer_wait(&self.shared);
            queue.waiters += 1;
            while queue.bytes > 0
                && queue.bytes.saturating_add(bytes) > WAL_WRITER_QUEUE_BYTES
                && !queue.stopped
            {
                queue = self.shared.wait_progress(queue);
            }
            queue.waiters -= 1;
        }
        if queue.stopped || self.thread.is_none() {
            return Err(io::Error::other("WAL writer stopped"));
        }

        Ok(queue)
    }

    fn push(&self, queue: &mut Queue, op: Op, bytes: u64) {
        queue.bytes += bytes;
        queue.ops.push_back(op);
        if queue.idle {
            queue.idle = false;
            self.shared.work.notify_one();
        }
    }

    /// Waits until everything handed over is on its way to disk or kept for recovery.
    pub(super) fn drain(&self) {
        let mut queue = self.shared.lock_queue();

        if queue.ops.is_empty() && !queue.busy {
            return;
        }
        #[cfg(test)]
        note_writer_wait(&self.shared);
        queue.waiters += 1;
        while (!queue.ops.is_empty() || queue.busy) && !queue.stopped {
            queue = self.shared.wait_progress(queue);
        }
        queue.waiters -= 1;
    }

    /// The open segment as on disk. Drain first: the writer holds it while it works.
    pub(super) fn physical(&self) -> MutexGuard<'_, Physical> {
        self.shared.lock_physical()
    }

    /// The error the writer stopped on, while it has not resumed.
    pub(super) fn failure_message(&self) -> Option<String> {
        if !self.shared.failed.load(Ordering::Acquire) {
            return None;
        }

        self.shared
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .or_else(|| Some("WAL write failed".into()))
    }

    pub(super) fn has_failure(&self) -> bool {
        self.shared.failed.load(Ordering::Acquire)
    }

    /// The shard published up to `sequence`: readers see it once it is written.
    pub(super) fn publish(&self, sequence: u64) {
        self.shared
            .publish_target
            .fetch_max(sequence, Ordering::SeqCst);
        let written = self.shared.written.load(Ordering::SeqCst);

        self.shared
            .published
            .fetch_max(sequence.min(written), Ordering::Release);
    }

    /// Every record up to `sequence` is in the file and published (a new base, after a drain).
    pub(super) fn reset_cursors(&self, sequence: u64) {
        self.shared.written.store(sequence, Ordering::SeqCst);
        self.shared.publish_target.store(sequence, Ordering::SeqCst);
        self.shared.published.store(sequence, Ordering::Release);
    }

    /// The writer, for a sync that must cover records up to a sequence.
    pub(super) fn source(&self) -> Arc<WriterShared> {
        Arc::clone(&self.shared)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        {
            let mut queue = self.shared.lock_queue();

            queue.stopped = true;
            self.shared.work.notify_all();
            self.shared.progress.notify_all();
        }
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            tracing::error!("WAL writer panicked");
        }
    }
}

impl WriterShared {
    fn lock_queue(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(super) fn lock_physical(&self) -> MutexGuard<'_, Physical> {
        self.physical
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn wait_progress<'a>(&self, queue: MutexGuard<'a, Queue>) -> MutexGuard<'a, Queue> {
        self.progress
            .wait(queue)
            .unwrap_or_else(|error| error.into_inner())
    }

    fn note_written(&self, sequence: u64) {
        self.written.store(sequence, Ordering::SeqCst);
        let target = self.publish_target.load(Ordering::SeqCst);

        self.published
            .fetch_max(sequence.min(target), Ordering::Release);
    }

    pub(super) fn note_rewritten(&self, sequence: u64) {
        self.failed.store(false, Ordering::Release);
        *self
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
        self.note_written(sequence);
    }

    fn report_failure(&self, message: String) {
        self.failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_or_insert(message);
        self.failed.store(true, Ordering::Release);
    }

    /// Waits until the records up to `sequence` are in the file, then syncs it. A record the
    /// writer could not write is not synced: the shard picks up that failure on its own.
    pub(super) fn sync_through(&self, sequence: u64) -> super::sync_worker::SyncOutcome {
        use super::sync_worker::SyncOutcome;

        if self.written.load(Ordering::Acquire) < sequence {
            let mut queue = self.lock_queue();

            queue.waiters += 1;
            while self.written.load(Ordering::Acquire) < sequence
                && !self.failed.load(Ordering::Acquire)
                && !queue.stopped
                && (queue.busy || !queue.ops.is_empty())
            {
                queue = self.wait_progress(queue);
            }
            queue.waiters -= 1;
        }

        let physical = self.lock_physical();

        if physical.written < sequence {
            return SyncOutcome::NotWritten(
                physical
                    .failed
                    .clone()
                    .unwrap_or_else(|| "WAL records were not written".into()),
            );
        }
        let Some(file) = physical.file.clone() else {
            return SyncOutcome::NotWritten("WAL-backed profile has no open segment".into());
        };

        drop(physical);
        if self.sync_error.poisoned() {
            return SyncOutcome::Failed(io::Error::other("an earlier WAL fsync failed"));
        }
        match file.sync_data() {
            // A failure noted while this sync ran may be the same lost write-back.
            Ok(()) if self.sync_error.poisoned() => {
                SyncOutcome::Failed(io::Error::other("an earlier WAL fsync failed"))
            }
            Ok(()) => SyncOutcome::Synced,
            Err(error) => {
                self.sync_error.poison();
                SyncOutcome::Failed(error)
            }
        }
    }
}

impl super::sync_worker::SyncSource for WriterShared {
    fn sync_through(&self, sequence: u64) -> super::sync_worker::SyncOutcome {
        WriterShared::sync_through(self, sequence)
    }
}

impl Physical {
    /// Writes `bytes` at the end of the open segment. On failure the cursor stays where it
    /// was: recovery cuts whatever the failed write left behind.
    pub(super) fn write_at_end(&mut self, bytes: &[u8]) -> io::Result<()> {
        let Some(file) = &self.file else {
            return Err(io::Error::other("WAL-backed profile has no open segment"));
        };
        let end = self.write_pos + bytes.len() as u64;

        // Preallocated *and zero-filled* space: a sync of blocks that already hold data needs
        // no extent conversion or size change, which made `fdatasync` ~2.4x faster (2.0 ->
        // 0.8 ms per sync) on ext4.
        if self.prealloc {
            let cap = self.segment_bytes.max(end);

            self.prefill.reserve(
                file,
                &mut self.prealloc_end,
                &mut self.prealloc,
                end,
                cap,
                self.write_pos,
            );
        }

        #[cfg(test)]
        if let Some(error) = injected_write_fault(&self.data_dir, self.shard_id) {
            self.prealloc_end = self.write_pos;
            return Err(error);
        }
        #[cfg(test)]
        {
            count_write(&self.data_dir);
            stall_write(&self.data_dir);
        }
        let written = super::crash::hit_write_at(
            super::crash::CrashSite::Append,
            file,
            bytes,
            self.write_pos,
        );

        if written.is_ok() {
            self.write_pos = end;
        } else {
            // Keep whatever a failed write left behind (recovery classifies it); only a clean
            // tail is trimmed.
            self.prealloc_end = self.write_pos;
        }

        written
    }

    fn write_records(&mut self, shared: &WriterShared, bytes: &[u8], last_sequence: u64) {
        if self.failed.is_some() {
            self.keep_unwritten(bytes);
            return;
        }

        match self.write_at_end(bytes) {
            Ok(()) => {
                self.written = last_sequence;
                shared.note_written(last_sequence);
            }
            Err(error) => {
                tracing::error!(
                    shard = self.shard_id,
                    %error,
                    "WAL write failed; writes are refused until the disk accepts them again"
                );
                self.keep_unwritten(bytes);
                self.fail(shared, error.to_string());
            }
        }
    }

    fn keep_unwritten(&mut self, bytes: &[u8]) {
        if self.unwritten.is_empty() {
            self.unwritten_first = self.written + 1;
        }
        self.unwritten.extend_from_slice(bytes);
    }

    pub(super) fn fail(&mut self, shared: &WriterShared, message: String) {
        self.failed.get_or_insert_with(|| message.clone());
        shared.report_failure(message);
    }

    /// Drop the preallocated, never-written tail so a closed segment has its exact size.
    pub(super) fn shrink_to_written(&mut self) {
        // A fill still running would write past the new end.
        self.prefill.finish(&mut self.prealloc_end);

        if self.prealloc_end > self.write_pos {
            if let Some(file) = &self.file {
                let _ = file.set_len(self.write_pos);
            }
            self.prealloc_end = self.write_pos;
        }
    }

    /// Closes the open segment, which holds the records up to `last_sequence`, and opens the
    /// next one at `first_sequence`. A failed log keeps its segment: recovery opens a new one.
    pub(super) fn rotate(
        &mut self,
        shared: &WriterShared,
        first_sequence: u64,
        last_sequence: u64,
        #[cfg(test)] faults: (bool, bool),
    ) {
        if self.failed.is_some() {
            return;
        }

        self.shrink_to_written();
        if let Some(file) = &self.file {
            #[cfg(test)]
            let synced = match faults.0 {
                true => Err(io::Error::other("injected sync failure")),
                false => {
                    delay_sync(&self.data_dir);
                    file.sync_data()
                }
            };
            #[cfg(not(test))]
            let synced = file.sync_data();

            if let Err(error) = synced {
                // Sticky, as for any sync of the log (see `WalSink::sync_failed`).
                let message = error.to_string();

                tracing::error!(
                    shard = self.shard_id,
                    %error,
                    "WAL fsync failed; writes are refused until restart"
                );
                shared.sync_error.report(message.clone());
                self.fail(shared, message);
                return;
            }
        }
        if !shared.sync_error.poisoned() {
            shared
                .durable
                .fetch_max(last_sequence.min(self.written), Ordering::Release);
        }

        #[cfg(test)]
        if faults.1 {
            super::crash::arm_io_fault(super::crash::CrashSite::Rotate);
        }
        let directory = shard_wal_directory(&self.data_dir, self.shard_id);

        match create_segment(&directory, self.shard_id, first_sequence) {
            Ok((file, _, first_sequence, file_length)) => {
                self.file = Some(Arc::new(file));
                self.segment_first_sequence = first_sequence;
                self.write_pos = file_length;
                self.prealloc_end = file_length;
                self.prefill.reset();
            }
            Err(error) => {
                tracing::error!(shard = self.shard_id, %error, "opening a WAL segment failed");
                self.fail(shared, error.to_string());
            }
        }
    }

    /// Cuts the last `length` bytes of the log, whether written or still kept unwritten.
    pub(super) fn forget_last(&mut self, length: u64) {
        let kept = self.unwritten.len() as u64;
        let from_kept = length.min(kept);

        self.unwritten.truncate((kept - from_kept) as usize);
        let length = length - from_kept;

        if length == 0 {
            return;
        }

        let position = self.write_pos.saturating_sub(length);

        self.write_pos = position;
        // If the file cannot be shortened, overwrite the records with zeros instead: replay
        // then sees an unwritten preallocated tail.
        self.prefill.finish(&mut self.prealloc_end);
        if let Some(file) = &self.file {
            if file.set_len(position).is_ok() {
                self.prealloc_end = position;
            } else {
                let zeros = vec![0u8; usize::try_from(length).unwrap_or(0)];
                let _ = write_all_at(file, &zeros, position);
            }
        }
    }
}

/// Stops the queue if the writer thread panics, so nobody waits for it forever: the shard
/// then sees a failed log and refuses writes, as for any write failure.
struct StopOnPanic<'a>(&'a WriterShared);

impl Drop for StopOnPanic<'_> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        self.0.report_failure("WAL writer panicked".into());

        let mut queue = self.0.lock_queue();

        queue.stopped = true;
        queue.busy = false;
        self.0.progress.notify_all();
    }
}

fn run(shared: &WriterShared) {
    let _stop_on_panic = StopOnPanic(shared);
    let mut batch = VecDeque::with_capacity(OPS_CAPACITY);
    let mut scratch = Vec::new();
    let mut done = Vec::new();

    loop {
        {
            let mut queue = shared.lock_queue();

            for mut buffer in done.drain(..) {
                if queue.spare.len() < SPARE_BUFFERS && Vec::capacity(&buffer) <= SPARE_BUFFER_BYTES
                {
                    Vec::clear(&mut buffer);
                    queue.spare.push(buffer);
                }
            }
            while queue.ops.is_empty() {
                if queue.stopped {
                    queue.busy = false;
                    shared.progress.notify_all();
                    return;
                }
                queue.idle = true;
                queue = shared
                    .work
                    .wait(queue)
                    .unwrap_or_else(|error| error.into_inner());
            }
            queue.idle = false;
            queue.busy = true;
            std::mem::swap(&mut batch, &mut queue.ops);
        }

        let mut bytes_done = 0u64;

        {
            let mut physical = shared.lock_physical();

            while let Some(op) = batch.pop_front() {
                match op {
                    Op::Write {
                        bytes,
                        mut last_sequence,
                        #[cfg(test)]
                        fault,
                    } => {
                        bytes_done += bytes.len() as u64;
                        #[cfg(test)]
                        let single = fault != (false, false);
                        #[cfg(not(test))]
                        let single = false;

                        // Writes handed over one after another go out as one `write`.
                        if !single && matches!(batch.front(), Some(Op::Write { .. })) {
                            scratch.clear();
                            scratch.extend_from_slice(&bytes);
                            done.push(bytes);
                            while let Some(Op::Write { .. }) = batch.front() {
                                #[cfg(test)]
                                if !matches!(
                                    batch.front(),
                                    Some(Op::Write {
                                        fault: (false, false),
                                        ..
                                    })
                                ) {
                                    break;
                                }
                                let Some(Op::Write {
                                    bytes,
                                    last_sequence: last,
                                    ..
                                }) = batch.pop_front()
                                else {
                                    break;
                                };

                                bytes_done += bytes.len() as u64;
                                scratch.extend_from_slice(&bytes);
                                last_sequence = last;
                                done.push(bytes);
                            }
                            physical.write_records(shared, &scratch, last_sequence);
                            if scratch.capacity() > 4 * SPARE_BUFFER_BYTES {
                                scratch = Vec::new();
                            }
                        } else {
                            #[cfg(test)]
                            {
                                if fault.0 {
                                    super::crash::arm_io_fault(super::crash::CrashSite::Append);
                                }
                                if fault.1 {
                                    super::crash::arm_io_fault(
                                        super::crash::CrashSite::PrefillSync,
                                    );
                                }
                            }
                            physical.write_records(shared, &bytes, last_sequence);
                            done.push(bytes);
                        }
                    }
                    Op::Task(task) => task(&mut physical, shared),
                }
            }
        }

        let mut queue = shared.lock_queue();

        queue.bytes -= bytes_done;
        queue.busy = false;
        if queue.waiters > 0 {
            shared.progress.notify_all();
        }
    }
}

/// Data directories whose WAL writes wait while listed (a disk that stopped answering), on
/// the writer thread. A write waits at most 30 s, so a test that forgets to release the
/// directory fails on its own assertions instead of hanging.
#[cfg(test)]
pub(crate) static STALLED_WRITE_DIRS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(in crate::persistence) fn stall_write(data_dir: &Path) {
    let deadline = Instant::now() + std::time::Duration::from_secs(30);
    let stalled = || {
        STALLED_WRITE_DIRS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .any(|dir| dir == data_dir)
    };

    while stalled() && Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// A thread waits for the writer of a log in a data directory: on a shard's thread that
/// counts as that thread waiting for the disk (see `SlowSyncDir::on_shard_thread`).
#[cfg(test)]
fn note_writer_wait(shared: &WriterShared) {
    note_sync_wait(&shared.data_dir);
}

/// Every writer a test started, for `settle_wal_writers`.
#[cfg(test)]
static WRITERS: Mutex<Vec<std::sync::Weak<WriterShared>>> = Mutex::new(Vec::new());

/// Waits until the WAL writers of `data_dir` wrote everything handed to them (or kept it after
/// a failed write): a test that copies the directory as a kill would leave it, or counts its
/// writes, does so once the records the server acknowledged reached the kernel.
#[cfg(test)]
pub(crate) fn settle_wal_writers(data_dir: &Path) {
    let writers: Vec<_> = {
        let mut writers = WRITERS.lock().unwrap_or_else(|error| error.into_inner());

        writers.retain(|writer| writer.strong_count() > 0);
        writers
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .filter(|writer| writer.data_dir == data_dir)
            .collect()
    };

    for writer in writers {
        let mut queue = writer.lock_queue();

        queue.waiters += 1;
        while (!queue.ops.is_empty() || queue.busy) && !queue.stopped {
            queue = writer.wait_progress(queue);
        }
        queue.waiters -= 1;
    }
}
