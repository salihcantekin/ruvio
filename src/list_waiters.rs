use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::poll_fn,
    rc::Rc,
    task::{Poll, Waker},
    time::Instant,
};

pub type EncodedReplySlot = Rc<ReplySlot>;

/// Where a parked client's reply lands. Filling it wakes the client's task, so a parked
/// client costs nothing until it is served or times out.
#[derive(Default)]
pub struct ReplySlot {
    payload: RefCell<Option<Vec<u8>>>,
    waker: Cell<Option<Waker>>,
    /// The last WAL record of the write that served this client, when it wrote one (see
    /// `ListWaiterSink::served_by`).
    served_by: Cell<Option<u64>>,
}

impl std::fmt::Debug for ReplySlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplySlot")
            .field("payload", &self.payload)
            .finish_non_exhaustive()
    }
}

impl ReplySlot {
    pub fn fill(&self, payload: Vec<u8>) {
        *self.payload.borrow_mut() = Some(payload);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub fn mark_served_by(&self, sequence: Option<u64>) {
        self.served_by.set(sequence);
    }

    pub fn served_by(&self) -> Option<u64> {
        self.served_by.get()
    }

    pub fn take(&self) -> Option<Vec<u8>> {
        self.payload.borrow_mut().take()
    }

    /// Resolves with the reply once the slot is filled.
    pub async fn filled(&self) -> Vec<u8> {
        poll_fn(|context| match self.take() {
            Some(payload) => Poll::Ready(payload),
            None => {
                self.waker.set(Some(context.waker().clone()));
                Poll::Pending
            }
        })
        .await
    }
}

pub const DEFAULT_MAX_LIST_WAITERS: usize = 1_024;

#[derive(Debug)]
enum WaitKind {
    List,
    /// `BZPOPMIN` (`max` false) or `BZPOPMAX` (`max` true).
    Sorted {
        max: bool,
    },
    Stream {
        cursors: Vec<(u64, u64)>,
    },
    Group {
        group: Vec<u8>,
        consumer: Vec<u8>,
        cursor: (u64, u64),
        noack: bool,
    },
}

pub struct BlockedGroup {
    pub group: Vec<u8>,
    pub consumer: Vec<u8>,
    pub noack: bool,
}

#[derive(Debug)]
pub struct ListWaiter<R> {
    #[allow(dead_code)]
    pub id: u64,
    pub keys: Vec<Vec<u8>>,
    kind: WaitKind,
    pub deadline: Option<Instant>,
    pub reply: R,
    pub timeout: Vec<u8>,
}

pub struct GroupRegistration {
    pub key: Vec<u8>,
    pub group: Vec<u8>,
    pub consumer: Vec<u8>,
    pub cursor: (u64, u64),
    pub noack: bool,
}

/// A client parked by `BLPOP`/`BRPOP`, `BZPOPMIN`/`BZPOPMAX`, `XREAD BLOCK`, or `XREADGROUP BLOCK`.
pub enum ParkRequest {
    List {
        keys: Vec<Vec<u8>>,
        timeout_ms: Option<u64>,
        /// `Some(max)` for a sorted-set pop. `None` for a list pop.
        sorted: Option<bool>,
    },
    Stream {
        keys: Vec<Vec<u8>>,
        cursors: Vec<(u64, u64)>,
        timeout_ms: Option<u64>,
    },
    Group {
        key: Vec<u8>,
        group: Vec<u8>,
        consumer: Vec<u8>,
        cursor: (u64, u64),
        noack: bool,
        timeout_ms: Option<u64>,
    },
}

#[derive(Debug)]
pub struct ListWaiters<R> {
    max_waiters: usize,
    next_id: u64,
    waiters: VecDeque<ListWaiter<R>>,
    /// No deadline is earlier than this, so `expire` skips its scan until then.
    soonest: Option<Instant>,
    /// Deliveries taken while a write was not yet recorded, with their replies.
    held: Vec<(ListWaiter<R>, Vec<u8>)>,
    /// Where each open hold starts in `held`; holds nest (a push inside `MULTI`).
    hold_marks: Vec<usize>,
    /// The last WAL record of the write now handing out elements (see
    /// `ListWaiterSink::served_by`).
    serving_sequence: Option<u64>,
    /// Clients served by a write that logged no record (all its elements went to them).
    unlogged_deliveries: u64,
}

impl<R> ListWaiters<R> {
    pub fn new(max_waiters: usize) -> Self {
        Self {
            max_waiters: max_waiters.max(1),
            next_id: 1,
            waiters: VecDeque::new(),
            soonest: None,
            held: Vec::new(),
            hold_marks: Vec::new(),
            serving_sequence: None,
            unlogged_deliveries: 0,
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.waiters.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }

    pub fn register(
        &mut self,
        keys: Vec<Vec<u8>>,
        deadline: Option<Instant>,
        reply: R,
        timeout: Vec<u8>,
    ) -> Result<u64, R> {
        self.register_kind(keys, WaitKind::List, deadline, reply, timeout)
    }

    pub fn register_sorted(
        &mut self,
        keys: Vec<Vec<u8>>,
        max: bool,
        deadline: Option<Instant>,
        reply: R,
        timeout: Vec<u8>,
    ) -> Result<u64, R> {
        self.register_kind(keys, WaitKind::Sorted { max }, deadline, reply, timeout)
    }

    fn register_kind(
        &mut self,
        keys: Vec<Vec<u8>>,
        kind: WaitKind,
        deadline: Option<Instant>,
        reply: R,
        timeout: Vec<u8>,
    ) -> Result<u64, R> {
        if self.waiters.len() >= self.max_waiters {
            return Err(reply);
        }

        let id = self.next_id;

        self.next_id = self.next_id.saturating_add(1);
        self.note_deadline(deadline);
        self.waiters.push_back(ListWaiter {
            id,
            keys,
            kind,
            deadline,
            reply,
            timeout,
        });

        Ok(id)
    }

    pub fn register_stream(
        &mut self,
        keys: Vec<Vec<u8>>,
        cursors: Vec<(u64, u64)>,
        deadline: Option<Instant>,
        reply: R,
        timeout: Vec<u8>,
    ) -> Result<u64, R> {
        if self.waiters.len() >= self.max_waiters {
            return Err(reply);
        }

        let id = self.next_id;

        self.next_id = self.next_id.saturating_add(1);
        self.note_deadline(deadline);
        self.waiters.push_back(ListWaiter {
            id,
            keys,
            kind: WaitKind::Stream { cursors },
            deadline,
            reply,
            timeout,
        });

        Ok(id)
    }

    pub fn register_group(
        &mut self,
        registration: GroupRegistration,
        deadline: Option<Instant>,
        reply: R,
        timeout: Vec<u8>,
    ) -> Result<u64, R> {
        if self.waiters.len() >= self.max_waiters {
            return Err(reply);
        }

        let id = self.next_id;

        self.next_id = self.next_id.saturating_add(1);
        self.note_deadline(deadline);
        self.waiters.push_back(ListWaiter {
            id,
            keys: vec![registration.key],
            kind: WaitKind::Group {
                group: registration.group,
                consumer: registration.consumer,
                cursor: registration.cursor,
                noack: registration.noack,
            },
            deadline,
            reply,
            timeout,
        });

        Ok(id)
    }

    pub fn park(
        &mut self,
        request: ParkRequest,
        reply: R,
        timeout: Vec<u8>,
    ) -> Result<(u64, Option<Instant>), R> {
        let deadline = match &request {
            ParkRequest::List { timeout_ms, .. }
            | ParkRequest::Stream { timeout_ms, .. }
            | ParkRequest::Group { timeout_ms, .. } => {
                timeout_ms.map(|millis| Instant::now() + std::time::Duration::from_millis(millis))
            }
        };

        let id = match request {
            ParkRequest::List {
                keys, sorted: None, ..
            } => self.register(keys, deadline, reply, timeout)?,
            ParkRequest::List {
                keys,
                sorted: Some(max),
                ..
            } => self.register_sorted(keys, max, deadline, reply, timeout)?,
            ParkRequest::Stream { keys, cursors, .. } => {
                self.register_stream(keys, cursors, deadline, reply, timeout)?
            }

            ParkRequest::Group {
                key,
                group,
                consumer,
                cursor,
                noack,
                ..
            } => self.register_group(
                GroupRegistration {
                    key,
                    group,
                    consumer,
                    cursor,
                    noack,
                },
                deadline,
                reply,
                timeout,
            )?,
        };

        Ok((id, deadline))
    }

    #[allow(dead_code)]
    pub fn cancel(&mut self, id: u64) -> Option<R> {
        let index = self.waiters.iter().position(|waiter| waiter.id == id)?;

        Some(self.waiters.remove(index).expect("index checked").reply)
    }

    pub fn take_for_key(&mut self, key: &[u8]) -> Option<ListWaiter<R>> {
        let index = self.waiters.iter().position(|waiter| {
            matches!(waiter.kind, WaitKind::List)
                && waiter.keys.iter().any(|blocked| blocked == key)
        })?;

        self.waiters.remove(index)
    }

    /// `Some(max)` for the oldest sorted-set waiter on `key`.
    pub fn oldest_sorted(&self, key: &[u8]) -> Option<bool> {
        self.waiters.iter().find_map(|waiter| match waiter.kind {
            WaitKind::Sorted { max } if waiter.keys.iter().any(|blocked| blocked == key) => {
                Some(max)
            }
            _ => None,
        })
    }

    pub fn take_sorted_for_key(&mut self, key: &[u8]) -> Option<ListWaiter<R>> {
        let index = self.waiters.iter().position(|waiter| {
            matches!(waiter.kind, WaitKind::Sorted { .. })
                && waiter.keys.iter().any(|blocked| blocked == key)
        })?;

        self.waiters.remove(index)
    }

    pub fn take_stream_for_append(
        &mut self,
        key: &[u8],
        millis: u64,
        sequence: u64,
    ) -> Option<ListWaiter<R>> {
        let index = self.waiters.iter().position(|waiter| {
            let WaitKind::Stream { cursors } = &waiter.kind else {
                return false;
            };

            waiter
                .keys
                .iter()
                .zip(cursors)
                .any(|(blocked, (cursor_ms, cursor_seq))| {
                    blocked == key
                        && (millis > *cursor_ms || (millis == *cursor_ms && sequence > *cursor_seq))
                })
        })?;

        self.waiters.remove(index)
    }

    pub fn oldest_blocked_group(
        &self,
        key: &[u8],
        millis: u64,
        sequence: u64,
        claimed: &[Vec<u8>],
    ) -> Option<BlockedGroup> {
        self.waiters.iter().find_map(|waiter| {
            let WaitKind::Group {
                group,
                consumer,
                cursor,
                noack,
            } = &waiter.kind
            else {
                return None;
            };

            let newer = millis > cursor.0 || (millis == cursor.0 && sequence > cursor.1);

            if newer
                && waiter.keys.iter().any(|blocked| blocked == key)
                && !claimed.iter().any(|name| name == group)
            {
                Some(BlockedGroup {
                    group: group.clone(),
                    consumer: consumer.clone(),
                    noack: *noack,
                })
            } else {
                None
            }
        })
    }

    pub fn take_group(&mut self, key: &[u8], group: &[u8]) -> Option<ListWaiter<R>> {
        let index = self.waiters.iter().position(|waiter| {
            let WaitKind::Group { group: name, .. } = &waiter.kind else {
                return false;
            };

            name == group && waiter.keys.iter().any(|blocked| blocked == key)
        })?;

        self.waiters.remove(index)
    }

    pub fn expire(&mut self, now: Instant) -> Vec<(R, Vec<u8>)> {
        let mut expired = Vec::new();

        if self.soonest.is_none_or(|soonest| soonest > now) {
            return expired;
        }

        self.soonest = None;
        let mut index = 0;

        while index < self.waiters.len() {
            let timed_out = self.waiters[index]
                .deadline
                .is_some_and(|deadline| deadline <= now);

            if timed_out {
                let waiter = self.waiters.remove(index).expect("index checked");

                expired.push((waiter.reply, waiter.timeout));
            } else {
                self.note_deadline(self.waiters[index].deadline);
                index += 1;
            }
        }

        expired
    }

    fn note_deadline(&mut self, deadline: Option<Instant>) {
        if let Some(deadline) = deadline {
            self.soonest = Some(
                self.soonest
                    .map_or(deadline, |soonest| soonest.min(deadline)),
            );
        }
    }

    /// From now until the matching [`Self::release`], served waiters keep their reply
    /// here instead of getting it: a write hands data to blocked clients only once its
    /// record is durable.
    pub fn hold(&mut self) {
        self.hold_marks.push(self.held.len());
    }

    pub fn set_serving_sequence(&mut self, sequence: Option<u64>) {
        self.serving_sequence = sequence;
    }

    /// The serving write's record, reset so a later write that sets none does not reuse it.
    pub fn take_serving_sequence(&mut self) -> Option<u64> {
        self.serving_sequence.take()
    }

    /// A delivery completed for a write whose last record is `served_by`.
    pub fn note_delivered(&mut self, served_by: Option<u64>) {
        if served_by.is_none() {
            self.unlogged_deliveries += 1;
        }
    }

    /// How many deliveries so far came from a write that logged no record (see
    /// `take_serving_sequence`); a caller compares two readings.
    pub fn unlogged_deliveries(&self) -> u64 {
        self.unlogged_deliveries
    }

    pub fn holding(&self) -> bool {
        !self.hold_marks.is_empty()
    }

    pub fn hold_delivery(&mut self, waiter: ListWaiter<R>, payload: Vec<u8>) {
        self.held.push((waiter, payload));
    }

    /// Ends the innermost hold. Delivered, its replies are returned for sending once no
    /// outer hold remains; otherwise its waiters are parked again, still first in line.
    pub fn release(&mut self, delivered: bool) -> Vec<(R, Vec<u8>)> {
        let Some(mark) = self.hold_marks.pop() else {
            return Vec::new();
        };

        if !delivered {
            for (waiter, _) in self.held.drain(mark..).rev() {
                if let Some(deadline) = waiter.deadline {
                    self.soonest = Some(
                        self.soonest
                            .map_or(deadline, |soonest| soonest.min(deadline)),
                    );
                }
                self.waiters.push_front(waiter);
            }

            return Vec::new();
        }

        if self.holding() {
            return Vec::new();
        }

        self.held
            .drain(..)
            .map(|(waiter, payload)| (waiter.reply, payload))
            .collect()
    }

    /// Removes every waiter blocked on a key `select` picks and returns its reply slot.
    pub fn take_where(&mut self, select: impl Fn(&[u8]) -> bool) -> Vec<R> {
        let (taken, kept): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.waiters)
            .into_iter()
            .partition(|waiter| waiter.keys.iter().any(|key| select(key)));

        self.waiters = kept;
        taken.into_iter().map(|waiter| waiter.reply).collect()
    }

    /// Every parked waiter with its timeout reply, for shutdown.
    pub fn drain_all(&mut self) -> Vec<(R, Vec<u8>)> {
        self.soonest = None;
        self.waiters
            .drain(..)
            .map(|waiter| (waiter.reply, waiter.timeout))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn serves_the_oldest_waiter_for_a_key_and_enforces_the_bound() {
        let mut waiters = ListWaiters::new(2);

        assert!(
            waiters
                .register(vec![b"a".to_vec()], None, 1, Vec::new())
                .is_ok()
        );
        assert!(
            waiters
                .register(vec![b"a".to_vec(), b"b".to_vec()], None, 2, Vec::new())
                .is_ok()
        );
        assert_eq!(
            waiters.register(vec![b"c".to_vec()], None, 3, Vec::new()),
            Err(3)
        );

        let first = waiters.take_for_key(b"a").unwrap();

        assert_eq!(first.reply, 1);
        let second = waiters.take_for_key(b"b").unwrap();

        assert_eq!(second.reply, 2);
        assert!(waiters.is_empty());
    }

    #[test]
    fn list_and_sorted_waiters_do_not_take_each_other() {
        let mut waiters = ListWaiters::new(4);

        waiters
            .register(vec![b"k".to_vec()], None, 1, Vec::new())
            .unwrap();
        waiters
            .register_sorted(vec![b"k".to_vec()], true, None, 2, Vec::new())
            .unwrap();
        assert_eq!(waiters.oldest_sorted(b"k"), Some(true));
        assert_eq!(waiters.take_sorted_for_key(b"k").unwrap().reply, 2);
        assert!(waiters.take_sorted_for_key(b"k").is_none());
        assert_eq!(waiters.take_for_key(b"k").unwrap().reply, 1);
    }

    #[test]
    fn expires_only_due_waiters() {
        let mut waiters = ListWaiters::new(8);
        let now = Instant::now();

        waiters
            .register(vec![b"a".to_vec()], Some(now), 1, b"*-1\r\n".to_vec())
            .unwrap();
        waiters
            .register(
                vec![b"b".to_vec()],
                Some(now + Duration::from_secs(5)),
                2,
                b"*-1\r\n".to_vec(),
            )
            .unwrap();
        assert_eq!(waiters.expire(now), vec![(1, b"*-1\r\n".to_vec())]);
        assert_eq!(waiters.len(), 1);
    }

    struct CountingWaker(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for CountingWaker {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn filling_a_reply_slot_wakes_its_waiting_task() {
        let counter = std::sync::Arc::new(CountingWaker(Default::default()));
        let waker = Waker::from(std::sync::Arc::clone(&counter));
        let mut context = std::task::Context::from_waker(&waker);
        let slot = ReplySlot::default();
        let mut filled = std::pin::pin!(slot.filled());

        assert!(filled.as_mut().poll(&mut context).is_pending());
        assert_eq!(counter.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        slot.fill(b"reply".to_vec());
        assert_eq!(counter.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            filled.as_mut().poll(&mut context),
            Poll::Ready(b"reply".to_vec())
        );
    }

    #[test]
    fn expiring_waiters_skips_the_scan_until_a_deadline_is_due() {
        let mut waiters = ListWaiters::new(8);
        let now = Instant::now();

        waiters
            .register(vec![b"a".to_vec()], None, 1, Vec::new())
            .unwrap();
        assert_eq!(waiters.soonest, None);
        waiters
            .register(
                vec![b"b".to_vec()],
                Some(now + Duration::from_secs(5)),
                2,
                Vec::new(),
            )
            .unwrap();
        waiters
            .register(
                vec![b"c".to_vec()],
                Some(now + Duration::from_secs(1)),
                3,
                Vec::new(),
            )
            .unwrap();
        assert_eq!(waiters.soonest, Some(now + Duration::from_secs(1)));
        assert!(waiters.expire(now).is_empty());
        assert_eq!(waiters.expire(now + Duration::from_secs(2)).len(), 1);
        assert_eq!(waiters.soonest, Some(now + Duration::from_secs(5)));
        assert_eq!(waiters.drain_all().len(), 2);
    }
}
