//! `SLOWLOG`: the most recent commands that took longer than a threshold.
//!
//! Only commands that go through the generic dispatcher are timed; the GET/SET/INCR/PING
//! fast path never pays for a clock read. Each entry keeps the arguments as Redis does (at most
//! 32, each cut to 128 bytes); they are read from the frame only when the command was slow.

use std::collections::VecDeque;

/// Bytes of the first key kept in an entry.
pub const KEY_PREVIEW_BYTES: usize = 128;

#[derive(Debug, Clone)]
pub struct SlowEntry {
    pub id: u64,
    pub unix_secs: u64,
    pub micros: u64,
    pub args: Vec<Vec<u8>>,
    pub addr: String,
    pub name: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct SlowLog {
    entries: VecDeque<SlowEntry>,
    next_id: u64,
}

impl SlowLog {
    pub fn push(&mut self, mut entry: SlowEntry, max_len: usize) {
        if max_len == 0 {
            return;
        }

        entry.id = self.next_id;
        self.next_id += 1;
        self.entries.push_front(entry);
        self.entries.truncate(max_len);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Newest first.
    pub fn newest(&self, count: usize) -> impl Iterator<Item = &SlowEntry> {
        self.entries.iter().take(count)
    }

    /// Drops the oldest entries past `max_len` (`CONFIG SET slowlog-max-len`).
    pub fn trim(&mut self, max_len: usize) {
        self.entries.truncate(max_len);
    }

    pub fn reset(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(micros: u64) -> SlowEntry {
        SlowEntry {
            id: 0,
            unix_secs: 0,
            micros,
            args: vec![b"GET".to_vec()],
            addr: String::new(),
            name: Vec::new(),
        }
    }

    #[test]
    fn keeps_the_newest_entries_and_numbers_them_from_zero() {
        let mut log = SlowLog::default();

        for micros in 1..=5 {
            log.push(entry(micros), 3);
        }

        let kept: Vec<(u64, u64)> = log.newest(10).map(|e| (e.id, e.micros)).collect();

        assert_eq!(kept, vec![(4, 5), (3, 4), (2, 3)]);
        assert_eq!(log.len(), 3);

        log.reset();
        assert!(log.is_empty());
        log.push(entry(9), 3);
        assert_eq!(log.newest(1).next().map(|e| e.id), Some(5));
    }

    #[test]
    fn a_zero_length_log_records_nothing() {
        let mut log = SlowLog::default();

        log.push(entry(1), 0);
        assert!(log.is_empty());
    }
}
