//! The last WAL sequence synced to disk, and a wake-up for the threads that follow it.

use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, Thread},
    time::Duration,
};

/// Last sequence synced to disk. Replication ships through it, so a replica shipper that
/// has sent everything parks in [`wait_past`](Self::wait_past) and an advance unparks it,
/// instead of the shipper finding new records on its next poll.
///
/// With nobody waiting an advance costs one atomic read-modify-write and one load.
#[derive(Debug, Default)]
pub struct DurableSequence {
    sequence: AtomicU64,
    /// Threads inside `wait_past`; read before taking the `waiting` lock.
    parked: AtomicUsize,
    waiting: Mutex<Vec<Thread>>,
}

impl DurableSequence {
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence: AtomicU64::new(sequence),
            ..Self::default()
        }
    }

    pub fn load(&self, order: Ordering) -> u64 {
        self.sequence.load(order)
    }

    // The writers below use SeqCst read-modify-writes, and `wait_past` a SeqCst
    // increment and reload: either the writer sees the waiter in `parked`, or the waiter
    // sees the new sequence. The `order` argument only keeps the `AtomicU64` call shape.

    pub fn store(&self, sequence: u64, _order: Ordering) {
        if self.sequence.swap(sequence, Ordering::SeqCst) < sequence {
            self.wake();
        }
    }

    pub fn fetch_max(&self, sequence: u64, _order: Ordering) -> u64 {
        let previous = self.sequence.fetch_max(sequence, Ordering::SeqCst);

        if previous < sequence {
            self.wake();
        }
        previous
    }

    /// Park the calling thread until the sequence passes `seen`, an advance or another
    /// unpark wakes it, or `timeout` elapses. Callers recheck what they wait for.
    pub fn wait_past(&self, seen: u64, timeout: Duration) {
        let me = thread::current();

        self.lock().push(me.clone());
        self.parked.fetch_add(1, Ordering::SeqCst);

        if self.sequence.load(Ordering::SeqCst) <= seen {
            thread::park_timeout(timeout);
        }

        self.parked.fetch_sub(1, Ordering::SeqCst);
        let mut waiting = self.lock();

        if let Some(at) = waiting.iter().position(|thread| thread.id() == me.id()) {
            waiting.swap_remove(at);
        }
    }

    fn wake(&self) {
        if self.parked.load(Ordering::SeqCst) == 0 {
            return;
        }

        for thread in self.lock().iter() {
            thread.unpark();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Thread>> {
        self.waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Instant};

    use super::*;

    #[test]
    fn an_advance_wakes_a_waiter_before_its_timeout() {
        let durable = Arc::new(DurableSequence::new(5));
        let waiter = {
            let durable = Arc::clone(&durable);
            thread::spawn(move || {
                let begun = Instant::now();
                // Spurious unparks are allowed: wait like a shipper does.
                while durable.load(Ordering::Acquire) <= 5 {
                    durable.wait_past(5, Duration::from_secs(30));
                }
                begun.elapsed()
            })
        };

        // Advance only once the waiter is inside `wait_past`, so the wake is what ends it.
        while durable.parked.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }
        durable.store(6, Ordering::Release);

        assert!(waiter.join().unwrap() < Duration::from_secs(20));
    }

    #[test]
    fn a_waiter_behind_the_sequence_does_not_park() {
        let durable = DurableSequence::new(9);
        let begun = Instant::now();

        durable.wait_past(3, Duration::from_secs(30));

        assert!(begun.elapsed() < Duration::from_secs(20));
        assert!(durable.lock().is_empty());
        assert_eq!(durable.parked.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fetch_max_never_moves_back() {
        let durable = DurableSequence::new(4);

        assert_eq!(durable.fetch_max(2, Ordering::Release), 4);
        assert_eq!(durable.load(Ordering::Acquire), 4);
        assert_eq!(durable.fetch_max(7, Ordering::Release), 4);
        assert_eq!(durable.load(Ordering::Acquire), 7);
    }
}
