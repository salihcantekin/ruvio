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

struct Job {
    file: Arc<File>,
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
    #[cfg(test)]
    fail_sync: std::sync::atomic::AtomicBool,
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
            #[cfg(test)]
            fail_sync: std::sync::atomic::AtomicBool::new(false),
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

    pub(crate) fn submit(
        &self,
        file: Arc<File>,
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
        Ok(())
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
            notified.await;
        }
    }
}

impl Shared {
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
            Err(io::Error::other("injected worker sync failure"))
        } else {
            job.file.sync_data()
        };
        #[cfg(not(test))]
        let result = job.file.sync_data();
        shared.syncs.fetch_add(1, Ordering::Relaxed);
        shared.sync_nanos.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.busy = false;
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
            let first = worker.submit(Arc::clone(&file), 1).unwrap();
            let repeated = worker.submit(Arc::clone(&file), 1).unwrap();
            let later = worker.submit(Arc::clone(&file), 2).unwrap();
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
        let barrier = worker.submit(Arc::clone(&file), 1).unwrap();
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
        let first = worker.submit(Arc::clone(&file), 1).unwrap();
        for sequence in 2..=1000 {
            worker.submit(Arc::clone(&file), sequence).unwrap();
        }
        assert_eq!(durable.load(Ordering::Acquire), 0);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        block_on_bounded(&runtime, async {
            let later = worker.submit(Arc::clone(&file), 1000).unwrap();
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
        let first = worker.submit(Arc::clone(&file), 1).unwrap();
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
