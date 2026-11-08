use std::{
    fs::File,
    io,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Instant,
};

use tokio::sync::Notify;

use super::PersistenceError;

/// What a sync covers: a file, or the WAL writer, which first writes the records it holds.
pub(crate) trait SyncSource: Send + Sync {
    fn sync_through(&self, sequence: u64) -> SyncOutcome;
}

pub(crate) enum SyncOutcome {
    Synced,
    /// The records were not written (the writer failed first): nothing was synced, and the
    /// log picks up the write failure on its own. Not a failed sync.
    NotWritten(String),
    Failed(io::Error),
}

impl SyncSource for File {
    fn sync_through(&self, _sequence: u64) -> SyncOutcome {
        match self.sync_data() {
            Ok(()) => SyncOutcome::Synced,
            Err(error) => SyncOutcome::Failed(error),
        }
    }
}

struct Job {
    file: Arc<dyn SyncSource>,
    sequence: u64,
}

#[derive(Default)]
struct State {
    // Single coalescing slot: a newer sequence replaces an older pending job, so no batching
    // delay is added.
    pending: Option<Job>,
    requested: u64,
    busy: bool,
    stopped: bool,
    failure: Option<String>,
    /// The last job whose records were not written, and why: barriers up to it fail until
    /// the log recovers (`clear_not_written`).
    not_written: Option<(u64, String)>,
    #[cfg(test)]
    paused: bool,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    ready: Notify,
    durable: Arc<super::DurableSequence>,
    syncs: AtomicU64,
    sync_nanos: AtomicU64,
    unavailable: AtomicBool,
    /// `State::not_written`'s sequence, for a barrier's check without the lock.
    not_written_through: AtomicU64,
    #[cfg(test)]
    fail_sync: std::sync::atomic::AtomicBool,
    /// The log's data directory, for `SLOW_SYNC_DIRS`.
    #[cfg(test)]
    data_dir: Mutex<Option<std::path::PathBuf>>,
}

/// One active sync and one coalescing slot, independent of connection count.
pub(crate) struct SyncWorker {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub(crate) struct SyncBarrier {
    shared: Arc<Shared>,
    sequence: u64,
}

impl SyncWorker {
    pub(crate) fn new(durable: Arc<super::DurableSequence>, shard: u32) -> io::Result<Self> {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            ready: Notify::new(),
            durable,
            syncs: AtomicU64::new(0),
            sync_nanos: AtomicU64::new(0),
            unavailable: AtomicBool::new(false),
            not_written_through: AtomicU64::new(0),
            #[cfg(test)]
            fail_sync: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            data_dir: Mutex::new(None),
        });
        let worker = Arc::clone(&shared);
        let thread = thread::Builder::new()
            .name(format!("ruvio-sync-{shard}"))
            .spawn(move || run(worker))?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }

    #[cfg(test)]
    pub(crate) fn set_data_dir_for_test(&self, data_dir: &std::path::Path) {
        *self
            .shared
            .data_dir
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(data_dir.to_path_buf());
    }

    pub(crate) fn submit(
        &self,
        file: Arc<dyn SyncSource>,
        sequence: u64,
    ) -> Result<SyncBarrier, PersistenceError> {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        check_failure(&state)?;
        if sequence > state.requested && sequence > self.shared.durable.load(Ordering::Acquire) {
            state.requested = sequence;
            state.pending = Some(Job { file, sequence });
            self.shared.changed.notify_one();
        }
        Ok(SyncBarrier {
            shared: Arc::clone(&self.shared),
            sequence,
        })
    }

    pub(crate) fn check(&self) -> Result<(), PersistenceError> {
        self.shared.check()
    }

    pub(crate) fn unavailable(&self) -> bool {
        self.shared.unavailable.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn fail_sync_for_test(&self) {
        self.shared.fail_sync.store(true, Ordering::Release);
    }

    pub(crate) fn drain(&self) -> Result<(), PersistenceError> {
        // A test never waits this long for one sync: past it, a lost wake or a worker a failed
        // test left paused fails that test instead of hanging the run (every WAL test drains
        // on drop).
        #[cfg(test)]
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        #[cfg(test)]
        if state.busy || state.pending.is_some() {
            let data_dir = self
                .shared
                .data_dir
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(data_dir) = data_dir {
                super::wal::note_sync_wait(&data_dir);
            }
        }
        while state.busy || state.pending.is_some() {
            check_failure(&state)?;
            #[cfg(test)]
            {
                let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                    return Err(PersistenceError::Failed(
                        "WAL sync worker did not drain within 30 s".into(),
                    ));
                };
                state = self
                    .shared
                    .changed
                    .wait_timeout(state, left)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            #[cfg(not(test))]
            {
                state = self
                    .shared
                    .changed
                    .wait(state)
                    .unwrap_or_else(|e| e.into_inner());
            }
        }
        check_failure(&state)
    }

    pub(crate) fn reset(&self, sequence: u64) -> Result<(), PersistenceError> {
        self.drain()?;
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .requested = sequence;
        self.clear_not_written();
        Ok(())
    }

    /// The log wrote again what its writer could not: later barriers wait for their sync.
    pub(crate) fn clear_not_written(&self) {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());

        state.not_written = None;
        // A job for those records may have been dropped as not written: ask again.
        state.requested = 0;
        self.shared.not_written_through.store(0, Ordering::Release);
    }

    pub(crate) fn stats(&self) -> (u64, u64) {
        (
            self.shared.syncs.load(Ordering::Relaxed),
            self.shared.sync_nanos.load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    pub(crate) fn pause(&self, paused: bool) {
        self.pause_handle()(paused);
    }

    #[cfg(test)]
    pub(crate) fn pause_handle(&self) -> impl Fn(bool) + Send + 'static {
        let shared = Arc::clone(&self.shared);
        move |paused| {
            shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .paused = paused;
            shared.changed.notify_all();
        }
    }
}

impl SyncBarrier {
    pub(crate) async fn wait(&self) -> Result<(), PersistenceError> {
        loop {
            // Register before checking the cursor: notify_waiters stores no permit.
            let notified = self.shared.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.shared.check()?;
            if self.shared.durable.load(Ordering::Acquire) >= self.sequence {
                return Ok(());
            }
            self.shared.check_written(self.sequence)?;
            notified.await;
        }
    }

    /// `wait` for a thread outside the runtime (a checkpoint's commit thread).
    pub(crate) fn wait_blocking(&self) -> Result<(), PersistenceError> {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());

        loop {
            check_failure(&state)?;
            if self.shared.durable.load(Ordering::Acquire) >= self.sequence {
                return Ok(());
            }
            if let Some((through, message)) = &state.not_written
                && *through >= self.sequence
            {
                return Err(PersistenceError::Failed(message.clone()));
            }
            // A job ends with a notification; the timeout only bounds a missed one.
            state = self
                .shared
                .changed
                .wait_timeout(state, std::time::Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

impl Shared {
    /// Fails a barrier whose records the writer could not write.
    fn check_written(&self, sequence: u64) -> Result<(), PersistenceError> {
        if self.not_written_through.load(Ordering::Acquire) < sequence {
            return Ok(());
        }

        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());

        match &state.not_written {
            Some((through, message)) if *through >= sequence => {
                Err(PersistenceError::Failed(message.clone()))
            }
            _ => Ok(()),
        }
    }

    fn check(&self) -> Result<(), PersistenceError> {
        if !self.unavailable.load(Ordering::Acquire) {
            return Ok(());
        }
        check_failure(&self.state.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

fn check_failure(state: &State) -> Result<(), PersistenceError> {
    if let Some(error) = &state.failure {
        return Err(PersistenceError::Failed(error.clone()));
    }
    if state.stopped {
        return Err(PersistenceError::Failed("WAL sync worker stopped".into()));
    }
    Ok(())
}

struct ExitGuard(Arc<Shared>);

impl Drop for ExitGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.stopped && state.failure.is_none() {
            state.failure = Some("WAL sync worker exited unexpectedly".into());
        }
        state.busy = false;
        state.pending = None;
        self.0.unavailable.store(true, Ordering::Release);
        self.0.changed.notify_all();
        self.0.ready.notify_waiters();
    }
}

fn run(shared: Arc<Shared>) {
    let _exit = ExitGuard(Arc::clone(&shared));
    loop {
        let job = {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            while state.pending.is_none() && !state.stopped {
                state = shared
                    .changed
                    .wait(state)
                    .unwrap_or_else(|e| e.into_inner());
            }
            if state.stopped {
                return;
            }
            state.busy = true;
            #[cfg(test)]
            while state.paused {
                state = shared
                    .changed
                    .wait(state)
                    .unwrap_or_else(|e| e.into_inner());
            }
            state.pending.take().expect("notified sync job")
        };
        let started = Instant::now();
        super::crash::hit(super::crash::CrashSite::Sync);
        #[cfg(test)]
        let result = if shared.fail_sync.load(Ordering::Acquire) {
            SyncOutcome::Failed(io::Error::other("injected worker sync failure"))
        } else {
            let data_dir = shared
                .data_dir
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(data_dir) = data_dir {
                super::wal::delay_sync(&data_dir);
            }
            job.file.sync_through(job.sequence)
        };
        #[cfg(not(test))]
        let result = job.file.sync_through(job.sequence);
        shared.syncs.fetch_add(1, Ordering::Relaxed);
        shared.sync_nanos.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.busy = false;
        let result = match result {
            SyncOutcome::Synced => Ok(()),
            SyncOutcome::NotWritten(message) => {
                // Not this worker's failure: the log refuses writes until it recovers, and
                // the barriers up to these records fail.
                let through = state
                    .not_written
                    .as_ref()
                    .map_or(job.sequence, |(through, _)| (*through).max(job.sequence));

                state.not_written = Some((through, message));
                shared.not_written_through.store(through, Ordering::Release);
                // Let a later request for the same records reach the worker again.
                state.requested = state.requested.min(job.sequence.saturating_sub(1));
                shared.changed.notify_all();
                shared.ready.notify_waiters();
                continue;
            }
            SyncOutcome::Failed(error) => Err(error),
        };
        match result {
            Ok(()) => {
                // Release pairs with the Acquire loads in submit and wait. Only the captured
                // sequence is published; later bytes may be synced but are not assumed durable.
                shared.durable.fetch_max(job.sequence, Ordering::Release);
            }
            Err(error) => {
                // Sticky: after a failed sync no later completion may advance the durable cursor.
                tracing::error!(%error, sequence = job.sequence, "WAL sync worker failed");
                state.failure = Some(error.to_string());
                state.pending = None;
                shared.unavailable.store(true, Ordering::Release);
            }
        }
        shared.changed.notify_all();
        shared.ready.notify_waiters();
        if state.failure.is_some() {
            return;
        }
    }
}

impl Drop for SyncWorker {
    fn drop(&mut self) {
        if let Err(error) = self.drain() {
            tracing::error!(%error, "WAL sync worker drain failed");
        }
        {
            let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            state.stopped = true;
            // A worker still paused (by a test that failed first) would never see the stop.
            #[cfg(test)]
            {
                state.paused = false;
            }
            self.shared.unavailable.store(true, Ordering::Release);
            self.shared.changed.notify_all();
            self.shared.ready.notify_waiters();
        }
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            tracing::error!("WAL sync worker panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Runs `future` on `runtime`, failing the test after 10 s instead of hanging.
    fn block_on_bounded<F: std::future::Future>(
        runtime: &tokio::runtime::Runtime,
        future: F,
    ) -> F::Output {
        runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), future)
                .await
                .expect("test wait exceeded 10 s")
        })
    }

    #[test]
    fn barriers_cover_multiple_waiters_and_repeated_sequences() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"record").unwrap();
        let file = Arc::new(file);
        let durable = Arc::new(super::super::DurableSequence::new(0));
        let worker = SyncWorker::new(Arc::clone(&durable), 0).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        block_on_bounded(&runtime, async {
            let first = worker.submit(file.clone(), 1).unwrap();
            let repeated = worker.submit(file.clone(), 1).unwrap();
            let later = worker.submit(file.clone(), 2).unwrap();
            let (a, b, c) = tokio::join!(first.wait(), repeated.wait(), later.wait());
            a.unwrap();
            b.unwrap();
            c.unwrap();
        });
        worker.drain().unwrap();
        assert_eq!(durable.load(Ordering::Acquire), 2);
        assert!((1..=2).contains(&worker.stats().0));
    }

    #[test]
    fn failure_is_permanent_and_does_not_advance_durable_cursor() {
        let file = Arc::new(tempfile::tempfile().unwrap());
        let durable = Arc::new(super::super::DurableSequence::new(0));
        let worker = SyncWorker::new(Arc::clone(&durable), 0).unwrap();
        worker.shared.fail_sync.store(true, Ordering::Release);
        let barrier = worker.submit(file.clone(), 1).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(block_on_bounded(&runtime, barrier.wait()).is_err());
        assert!(worker.submit(file, 2).is_err());
        assert!(worker.drain().is_err());
        assert_eq!(durable.load(Ordering::Acquire), 0);
    }

    #[test]
    fn paused_worker_coalesces_without_blocking_the_executor_or_losing_wakes() {
        let file = Arc::new(tempfile::tempfile().unwrap());
        let durable = Arc::new(super::super::DurableSequence::new(0));
        let worker = SyncWorker::new(Arc::clone(&durable), 0).unwrap();
        worker.pause(true);
        let first = worker.submit(file.clone(), 1).unwrap();
        for sequence in 2..=1000 {
            worker.submit(file.clone(), sequence).unwrap();
        }
        assert_eq!(durable.load(Ordering::Acquire), 0);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        block_on_bounded(&runtime, async {
            let later = worker.submit(file.clone(), 1000).unwrap();
            let release = async {
                tokio::task::yield_now().await;
                assert_eq!(durable.load(Ordering::Acquire), 0);
                worker.pause(false);
            };
            let (a, b, _) = tokio::join!(first.wait(), later.wait(), release);
            a.unwrap();
            b.unwrap();
        });
        assert_eq!(worker.stats().0, 1);
        assert_eq!(durable.load(Ordering::Acquire), 1000);
    }

    #[test]
    fn cancelled_waiter_does_not_cancel_sync_or_other_waiters() {
        let file = Arc::new(tempfile::tempfile().unwrap());
        let durable = Arc::new(super::super::DurableSequence::new(0));
        let worker = SyncWorker::new(Arc::clone(&durable), 0).unwrap();
        worker.pause(true);
        let first = worker.submit(file.clone(), 1).unwrap();
        let other = worker.submit(file, 1).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        block_on_bounded(&runtime, async {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(10), first.wait())
                    .await
                    .is_err()
            );
            assert_eq!(durable.load(Ordering::Acquire), 0);
            worker.pause(false);
            other.wait().await.unwrap();
        });
        assert_eq!(durable.load(Ordering::Acquire), 1);
    }
}
