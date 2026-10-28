//! Key event gate. A zero mask or zero listeners returns before any allocation.

pub const KEYSPACE: u32 = 1 << 0;
pub const KEYEVENT: u32 = 1 << 1;
pub const GENERIC: u32 = 1 << 2;
pub const STRING: u32 = 1 << 3;
pub const EXPIRED: u32 = 1 << 4;
pub const EVICTED: u32 = 1 << 5;

const CLASS_MASK: u32 = GENERIC | STRING | EXPIRED | EVICTED;
const KIND_MASK: u32 = KEYSPACE | KEYEVENT;

const KEYSPACE_PREFIX: &[u8] = b"__keyspace@";
const KEYEVENT_PREFIX: &[u8] = b"__keyevent@";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyEvent {
    Set,
    Del,
    Expired,
    Evicted,
}

impl KeyEvent {
    pub fn name(self) -> &'static [u8] {
        match self {
            Self::Set => b"set",
            Self::Del => b"del",
            Self::Expired => b"expired",
            Self::Evicted => b"evicted",
        }
    }

    fn class(self) -> u32 {
        match self {
            Self::Set => STRING,
            Self::Del => GENERIC,
            Self::Expired => EXPIRED,
            Self::Evicted => EVICTED,
        }
    }
}

#[derive(Debug, Default)]
pub struct NotifyState {
    mask: u32,
    listeners: u32,
    /// Some connection tracks keys for client-side caching.
    tracking: bool,
    /// `NOTIFY_ON` when both a channel kind and a listener are present, `TRACKING_ON` while
    /// keys are tracked. Zero keeps every write to one test.
    gate: u32,
    pending: Vec<(u8, Box<[u8]>, KeyEvent)>,
    /// Keys removed by expiry or eviction while tracking is on: a write names its own keys
    /// for invalidation, these deletes happen beside it.
    dropped: Vec<(u8, Box<[u8]>)>,
}

const NOTIFY_ON: u32 = 1 << 0;
const TRACKING_ON: u32 = 1 << 1;

impl NotifyState {
    pub fn set_mask(&mut self, mask: u32) {
        self.mask = mask;
        self.refresh();
    }

    pub fn set_listeners(&mut self, listeners: u32) {
        self.listeners = listeners;
        self.refresh();
    }

    pub fn mask(&self) -> u32 {
        self.mask
    }

    pub fn set_tracking(&mut self, tracking: bool) {
        self.tracking = tracking;
        self.refresh();
    }

    #[inline]
    pub fn is_idle(&self) -> bool {
        self.pending.is_empty() && self.dropped.is_empty()
    }

    #[inline]
    pub fn record(&mut self, database: u8, key: &[u8], event: KeyEvent) {
        if self.gate == 0 {
            return;
        }

        if self.gate & NOTIFY_ON != 0 && self.mask & event.class() != 0 {
            self.pending
                .push((database, key.to_vec().into_boxed_slice(), event));
        }
        if self.gate & TRACKING_ON != 0 && matches!(event, KeyEvent::Expired | KeyEvent::Evicted) {
            self.dropped.push((database, key.into()));
        }
    }

    pub fn take_dropped(&mut self) -> Vec<(u8, Box<[u8]>)> {
        std::mem::take(&mut self.dropped)
    }

    pub fn take(&mut self) -> Vec<(u8, Box<[u8]>, KeyEvent)> {
        std::mem::take(&mut self.pending)
    }

    fn refresh(&mut self) {
        self.gate = if self.mask & KIND_MASK != 0 && self.listeners != 0 {
            NOTIFY_ON
        } else {
            0
        } | if self.tracking { TRACKING_ON } else { 0 };
    }
}

pub fn parse_mask(value: &str) -> Result<u32, String> {
    let mut mask = 0u32;

    for byte in value.bytes() {
        match byte {
            b'K' => mask |= KEYSPACE,
            b'E' => mask |= KEYEVENT,
            b'g' => mask |= GENERIC,
            b'$' => mask |= STRING,
            b'x' => mask |= EXPIRED,
            b'e' => mask |= EVICTED,
            b'A' => mask |= CLASS_MASK,
            _ => {
                return Err(format!(
                    "unknown notify-keyspace-events flag '{}'",
                    byte as char
                ));
            }
        }
    }

    if mask & KIND_MASK == 0 {
        return Ok(0);
    }

    Ok(mask)
}

pub fn is_notify_channel(channel: &[u8]) -> bool {
    channel.starts_with(KEYSPACE_PREFIX) || channel.starts_with(KEYEVENT_PREFIX)
}

pub struct Notice {
    pub channel: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Channel and payload pairs for a key; the channel names the key's database.
pub fn notices(mask: u32, database: u8, key: &[u8], event: KeyEvent) -> Vec<Notice> {
    let db = database.to_string();
    let mut out = Vec::with_capacity(2);

    if mask & KEYSPACE != 0 {
        let mut channel = Vec::with_capacity(KEYSPACE_PREFIX.len() + db.len() + 3 + key.len());

        channel.extend_from_slice(KEYSPACE_PREFIX);
        channel.extend_from_slice(db.as_bytes());
        channel.extend_from_slice(b"__:");
        channel.extend_from_slice(key);
        out.push(Notice {
            channel,
            payload: event.name().to_vec(),
        });
    }

    if mask & KEYEVENT != 0 {
        let name = event.name();
        let mut channel = Vec::with_capacity(KEYEVENT_PREFIX.len() + db.len() + 3 + name.len());

        channel.extend_from_slice(KEYEVENT_PREFIX);
        channel.extend_from_slice(db.as_bytes());
        channel.extend_from_slice(b"__:");
        channel.extend_from_slice(name);
        out.push(Notice {
            channel,
            payload: key.to_vec(),
        });
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_or_kindless_mask_stays_closed() {
        assert_eq!(parse_mask("").unwrap(), 0);
        assert_eq!(parse_mask("A").unwrap(), 0);
        assert_eq!(parse_mask("gx").unwrap(), 0);
        assert!(parse_mask("Q").is_err());
    }

    #[test]
    fn closed_gate_records_nothing() {
        let mut state = NotifyState::default();

        state.set_mask(parse_mask("Eg").unwrap());
        state.record(0, b"foo", KeyEvent::Del);
        assert!(state.take().is_empty());

        state.set_listeners(1);
        state.set_mask(0);
        state.record(0, b"foo", KeyEvent::Del);
        assert!(state.take().is_empty());
    }

    #[test]
    fn open_gate_records_only_the_enabled_class() {
        let mut state = NotifyState::default();

        state.set_mask(parse_mask("Eg").unwrap());
        state.set_listeners(1);
        state.record(0, b"foo", KeyEvent::Set);
        state.record(2, b"foo", KeyEvent::Del);
        let pending = state.take();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 2);
        assert_eq!(pending[0].1.as_ref(), b"foo");
        assert_eq!(pending[0].2, KeyEvent::Del);
        let frames = notices(parse_mask("Eg").unwrap(), 0, b"foo", KeyEvent::Del);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].channel, b"__keyevent@0__:del");
        assert_eq!(frames[0].payload, b"foo");
    }

    #[test]
    fn notices_name_the_database_of_the_key() {
        let frames = notices(parse_mask("KEg").unwrap(), 3, b"foo", KeyEvent::Del);

        assert_eq!(frames[0].channel, b"__keyspace@3__:foo");
        assert_eq!(frames[0].payload, b"del");
        assert_eq!(frames[1].channel, b"__keyevent@3__:del");
        assert_eq!(frames[1].payload, b"foo");
        assert!(is_notify_channel(b"__keyspace@3__:foo"));
    }
}
