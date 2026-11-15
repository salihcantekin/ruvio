use super::arena_table::ArenaTable;
use super::intset::{IntSet, format_i64, parse_canonical_i64};
use super::*;
use std::borrow::Cow;

pub(super) const COLLECTION_FLAT_LIMIT: usize = 128;

pub(super) fn flat_count(bytes: &[u8]) -> usize {
    if bytes.len() < 2 {
        return 0;
    }

    u16::from_le_bytes([bytes[0], bytes[1]]) as usize
}

pub(super) fn seal(bytes: Vec<u8>) -> Vec<u8> {
    let mut bytes = bytes;

    bytes.shrink_to_fit();
    bytes
}

pub(super) fn encode_set(members: &[impl AsRef<[u8]>]) -> Vec<u8> {
    let mut bytes = Vec::new();

    bytes.extend_from_slice(&(members.len() as u16).to_le_bytes());

    for member in members {
        let member = member.as_ref();

        bytes.extend_from_slice(&(member.len() as u16).to_le_bytes());
        bytes.extend_from_slice(member);
    }

    seal(bytes)
}

pub(super) fn decode_set(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut members = Vec::with_capacity(flat_count(bytes));
    let mut at = 2usize;

    for _ in 0..flat_count(bytes) {
        let len = u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;

        at += 2;
        members.push(bytes[at..at + len].to_vec());
        at += len;
    }

    members
}

/// `take` members of `set` drawn uniformly: distinct ones (at most all) when `distinct`,
/// otherwise independent draws. Cost follows `take` for every encoding but the spilled
/// table, which has no bucket access and is walked once.
///
/// Independent draws are capped at [`MAX_RANDOM_REPLY_BYTES`]: `-1000000` on a set holding
/// a 16 MiB member used to copy that member a million times before the reply limit was
/// applied, so the reply now stops growing once the budget is spent.
fn sample_members(set: SetView<'_>, take: usize, distinct: bool) -> Vec<Vec<u8>> {
    let width = set.len();

    if width == 0 || take == 0 {
        return Vec::new();
    }

    let positions: Vec<usize> = if distinct {
        distinct_positions(width, take)
    } else {
        (0..take).map(|_| random_below(width)).collect()
    };
    // A distinct draw copies each member at most once, so it cannot exceed the collection.
    let budget = if distinct {
        usize::MAX
    } else {
        MAX_RANDOM_REPLY_BYTES
    };
    let mut picked = Vec::with_capacity(positions.len().min(width));
    let mut bytes = 0usize;

    match set {
        SetView::Bytes(raw) => sample_flat(raw, &positions, budget, &mut picked, &mut bytes),
        SetView::Value(value) => match &value.body {
            SetBody::Flat(raw) => sample_flat(raw, &positions, budget, &mut picked, &mut bytes),
            SetBody::Int(ints) => {
                let mut digits = [0u8; 20];

                for at in positions {
                    let member = format_i64(ints.get(at), &mut digits);

                    if !push_member(&mut picked, &mut bytes, member, budget) {
                        break;
                    }
                }
            }
            SetBody::Arena(table) => {
                table.sample(take, distinct, |member, _| {
                    let _ = push_member(&mut picked, &mut bytes, member, budget);
                });
            }
            SetBody::Table(members) => {
                let members: Vec<&[u8]> = members.iter().map(|member| &**member).collect();

                for at in positions {
                    if !push_member(&mut picked, &mut bytes, members[at], budget) {
                        break;
                    }
                }
            }
        },
    }

    picked
}

fn sample_flat(raw: &[u8], positions: &[usize], budget: usize, picked: &mut Vec<Vec<u8>>, bytes: &mut usize) {
    let members: Vec<&[u8]> = SetMembers {
        bytes: raw,
        at: 2,
        left: flat_count(raw),
    }
    .collect();

    for &at in positions {
        if !push_member(picked, bytes, members[at], budget) {
            break;
        }
    }
}

/// Pushes a copy of `member` unless it would take the reply past `budget` bytes.
fn push_member(picked: &mut Vec<Vec<u8>>, bytes: &mut usize, member: &[u8], budget: usize) -> bool {
    if bytes.saturating_add(member.len()) > budget {
        return false;
    }

    *bytes += member.len();
    picked.push(member.to_vec());
    true
}

pub(super) fn set_contains(bytes: &[u8], member: &[u8]) -> bool {
    let mut at = 2usize;

    for _ in 0..flat_count(bytes) {
        let len = u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;

        at += 2;

        if &bytes[at..at + len] == member {
            return true;
        }

        at += len;
    }

    false
}

/// Members of one set, borrowed from the store. Iteration order is unspecified.
#[derive(Clone)]
pub struct MemberStream<'a> {
    view: Option<SetView<'a>>,
}

impl MemberStream<'_> {
    pub fn len(&self) -> usize {
        self.view.as_ref().map_or(0, SetView::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn visit<E>(&self, visit: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        match &self.view {
            Some(view) => view.visit(visit),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for MemberStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemberStream({} members)", self.len())
    }
}

impl PartialEq for MemberStream<'_> {
    fn eq(&self, other: &Self) -> bool {
        let collect = |stream: &Self| {
            let mut all: Vec<Vec<u8>> = Vec::new();
            let _ = stream.visit::<()>(|member| {
                all.push(member.to_vec());
                Ok(())
            });
            all.sort();
            all
        };

        collect(self) == collect(other)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum SetView<'a> {
    Bytes(&'a [u8]),
    Value(&'a SetValue),
}

impl<'a> SetView<'a> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Bytes(bytes) => flat_count(bytes),
            Self::Value(set) => set.len(),
        }
    }

    pub(crate) fn contains(&self, member: &[u8]) -> bool {
        match self {
            Self::Bytes(bytes) => set_contains(bytes, member),
            Self::Value(set) => set.contains(member),
        }
    }

    pub(crate) fn visit<E>(&self, mut visit: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        match self {
            Self::Bytes(bytes) => {
                for member in (SetMembers {
                    bytes,
                    at: 2,
                    left: flat_count(bytes),
                }) {
                    visit(member)?;
                }

                Ok(())
            }
            Self::Value(set) => set.visit(visit),
        }
    }

    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = Cow<'a, [u8]>> + 'a> {
        match self {
            Self::Bytes(bytes) => Box::new(
                SetMembers {
                    bytes,
                    at: 2,
                    left: flat_count(bytes),
                }
                .map(Cow::Borrowed),
            ),
            Self::Value(set) => set.iter(),
        }
    }
}

pub(super) struct SetMembers<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) at: usize,
    pub(super) left: usize,
}

impl<'a> Iterator for SetMembers<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }

        let len = u16::from_le_bytes([self.bytes[self.at], self.bytes[self.at + 1]]) as usize;

        self.at += 2;
        let member = &self.bytes[self.at..self.at + len];

        self.at += len;
        self.left -= 1;
        Some(member)
    }
}

pub(super) fn inplace_set_remove(buf: &mut [u8], member: &[u8]) -> bool {
    if buf.len() < 2 {
        return false;
    }

    let used = set_used_len(buf);
    let mut at = 2usize;

    for _ in 0..flat_count(buf) {
        let start = at;
        let len = u16::from_le_bytes([buf[at], buf[at + 1]]) as usize;
        let stored = &buf[at + 2..at + 2 + len];
        let end = at + 2 + len;

        if stored == member {
            buf.copy_within(end..used, start);
            let count = (flat_count(buf) - 1) as u16;

            buf[..2].copy_from_slice(&count.to_le_bytes());

            return true;
        }

        at = end;
    }

    false
}

pub(super) fn inplace_set_insert(buf: &mut [u8], member: &[u8]) -> Option<bool> {
    if !member_fits_flat(member) || flat_count(buf) >= COLLECTION_FLAT_LIMIT {
        return None;
    }

    let shift = 2 + member.len();
    let mut at = 2usize;
    let mut insert_at = None;

    for _ in 0..flat_count(buf) {
        let len = u16::from_le_bytes([buf[at], buf[at + 1]]) as usize;
        let stored = &buf[at + 2..at + 2 + len];

        if stored == member {
            return Some(false);
        }

        if insert_at.is_none() && member < stored {
            insert_at = Some(at);
        }

        at += 2 + len;
    }

    let used = at;
    let insert_at = insert_at.unwrap_or(used);

    if used + shift > buf.len() {
        return None;
    }

    buf.copy_within(insert_at..used, insert_at + shift);
    buf[insert_at..insert_at + 2].copy_from_slice(&(member.len() as u16).to_le_bytes());
    buf[insert_at + 2..insert_at + shift].copy_from_slice(member);
    let count = (flat_count(buf) + 1) as u16;

    buf[..2].copy_from_slice(&count.to_le_bytes());
    Some(true)
}

pub(super) fn member_fits_flat(member: &[u8]) -> bool {
    u16::try_from(member.len()).is_ok()
}

pub(super) fn set_used_len(bytes: &[u8]) -> usize {
    let mut at = 2usize;

    for _ in 0..flat_count(bytes) {
        let len = u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;

        at += 2 + len;
    }

    at
}

/// `Flat` holds up to `COLLECTION_FLAT_LIMIT` members of any bytes, sorted. `Int` holds up
/// to `INTSET_MAX_ENTRIES` canonical integers as a sorted array. Larger or mixed sets use
/// `Arena`: member bytes in one arena, a table of `u32` offsets. `Table` is only the
/// escape hatch for an arena that has run out of `u32` offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SetBody {
    Flat(Vec<u8>),
    Int(IntSet),
    Arena(Box<ArenaTable<false>>),
    Table(Box<AHashSet<Box<[u8]>>>),
}

impl Default for SetBody {
    fn default() -> Self {
        Self::Flat(Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SetValue {
    pub(super) body: SetBody,
    pub(super) payload_bytes: usize,
}

impl SetValue {
    pub(super) fn insert(&mut self, member: &[u8], intset_max: usize) -> bool {
        if self.contains(member) {
            return false;
        }

        let len = self.len();

        match &mut self.body {
            SetBody::Flat(_) if len >= COLLECTION_FLAT_LIMIT || !member_fits_flat(member) => {
                self.promote(Some(member), intset_max);
            }
            SetBody::Int(set) => match parse_canonical_i64(member) {
                Some(value) if len < intset_max => {
                    set.insert(value);
                    self.payload_bytes = self.payload_bytes.saturating_add(member.len());

                    return true;
                }
                _ => self.promote(Some(member), intset_max),
            },
            _ => {}
        }

        self.payload_bytes = self.payload_bytes.saturating_add(member.len());

        match &mut self.body {
            SetBody::Flat(bytes) => {
                if bytes.len() < 2 {
                    bytes.resize(2, 0);
                }

                let extra = 2 + member.len();
                let used = set_used_len(bytes);

                ensure_flat_room(bytes, used, extra);
                inplace_set_insert(bytes, member).expect("flat set reserved room");
                let used = set_used_len(bytes);

                bytes.truncate(used);
            }

            SetBody::Arena(table) => {
                if table.can_insert(member.len(), 0) {
                    table
                        .insert_new(member, &[])
                        .expect("room checked before the insert");
                } else {
                    let mut spilled: AHashSet<Box<[u8]>> =
                        table.iter().map(|(m, _)| m.into()).collect();

                    spilled.insert(member.into());
                    self.body = SetBody::Table(Box::new(spilled));
                }
            }

            SetBody::Table(members) => {
                members.insert(member.into());
            }

            SetBody::Int(set) => {
                let value = parse_canonical_i64(member)
                    .expect("promote keeps an integer set only for an integer member");

                set.insert(value);
            }
        }

        true
    }

    pub(super) fn remove(&mut self, member: &[u8]) -> bool {
        let removed = match &mut self.body {
            SetBody::Flat(bytes) => {
                if bytes.len() < 2 || !inplace_set_remove(bytes, member) {
                    return false;
                }

                let used = set_used_len(bytes);

                bytes.truncate(used);
                true
            }

            SetBody::Int(set) => {
                if !parse_canonical_i64(member).is_some_and(|value| set.remove(value)) {
                    return false;
                }

                true
            }

            SetBody::Arena(table) => {
                if table.remove(member).is_none() {
                    return false;
                }

                true
            }

            SetBody::Table(members) => {
                if !members.remove(member) {
                    return false;
                }

                if let Some(target) = reclaim_capacity(members.len(), members.capacity()) {
                    members.shrink_to(target);
                }

                true
            }
        };

        if removed {
            self.payload_bytes = self.payload_bytes.saturating_sub(member.len());
        }

        if removed
            && !matches!(self.body, SetBody::Flat(_))
            && self.len() <= COLLECTION_FLAT_LIMIT / 2
        {
            self.demote();
        }

        removed
    }

    pub(super) fn contains(&self, member: &[u8]) -> bool {
        match &self.body {
            SetBody::Flat(bytes) => set_contains(bytes, member),
            SetBody::Int(set) => {
                parse_canonical_i64(member).is_some_and(|value| set.contains(value))
            }
            SetBody::Arena(table) => table.contains(member),
            SetBody::Table(members) => members.contains(member),
        }
    }

    pub(super) fn len(&self) -> usize {
        match &self.body {
            SetBody::Flat(bytes) => flat_count(bytes),
            SetBody::Int(set) => set.len(),
            SetBody::Arena(table) => table.len(),
            SetBody::Table(members) => members.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn capacity_bytes(&self) -> usize {
        match &self.body {
            SetBody::Flat(bytes) => bytes.capacity(),
            SetBody::Int(set) => set.capacity_bytes(),
            SetBody::Arena(table) => table.capacity_bytes().saturating_sub(self.payload_bytes),
            SetBody::Table(members) => members
                .capacity()
                .saturating_mul(size_of::<Box<[u8]>>().saturating_add(1)),
        }
    }

    pub(super) fn iter(&self) -> Box<dyn Iterator<Item = Cow<'_, [u8]>> + '_> {
        match &self.body {
            SetBody::Flat(bytes) => Box::new(
                SetMembers {
                    bytes,
                    at: 2,
                    left: flat_count(bytes),
                }
                .map(Cow::Borrowed),
            ),
            SetBody::Int(set) => Box::new(set.iter().map(|value| {
                let mut buf = [0u8; 20];

                Cow::Owned(format_i64(value, &mut buf).to_vec())
            })),
            SetBody::Arena(table) => {
                Box::new(table.iter().map(|(member, _)| Cow::Borrowed(member)))
            }
            SetBody::Table(members) => Box::new(members.iter().map(|m| Cow::Borrowed(m.as_ref()))),
        }
    }

    /// Calls `visit` for every member without allocating.
    pub(super) fn visit<E>(&self, mut visit: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        match &self.body {
            SetBody::Flat(bytes) => {
                for member in (SetMembers {
                    bytes,
                    at: 2,
                    left: flat_count(bytes),
                }) {
                    visit(member)?;
                }
            }
            SetBody::Int(set) => {
                let mut buf = [0u8; 20];

                for value in set.iter() {
                    visit(format_i64(value, &mut buf))?;
                }
            }
            SetBody::Arena(table) => {
                for (member, _) in table.iter() {
                    visit(member)?;
                }
            }
            SetBody::Table(members) => {
                for member in members.iter() {
                    visit(member)?;
                }
            }
        }

        Ok(())
    }

    /// Moves a flat or integer set to the arena table, or to the integer array when every
    /// member and the incoming one are canonical integers and few enough.
    pub(super) fn promote(&mut self, incoming: Option<&[u8]>, intset_max: usize) {
        let members: Vec<Vec<u8>> = match &self.body {
            SetBody::Flat(flat) => decode_set(flat),
            SetBody::Int(set) => set
                .iter()
                .map(|value| format_i64(value, &mut [0u8; 20]).to_vec())
                .collect(),
            _ => return,
        };

        if matches!(self.body, SetBody::Flat(_)) && members.len() < intset_max {
            let ints: Option<Vec<i64>> = members
                .iter()
                .map(Vec::as_slice)
                .chain(incoming)
                .map(parse_canonical_i64)
                .collect();

            if let Some(ints) = ints {
                self.body = SetBody::Int(IntSet::from_values(ints.into_iter().take(members.len())));
                return;
            }
        }

        let mut table = ArenaTable::<false>::with_capacity(members.len() * 2);

        for member in &members {
            table
                .insert_new(member, &[])
                .expect("a small set fits the arena");
        }

        self.body = SetBody::Arena(Box::new(table));
    }

    pub(super) fn demote(&mut self) {
        let mut members: Vec<Vec<u8>> = self.iter().map(Cow::into_owned).collect();

        if members.iter().any(|member| !member_fits_flat(member)) {
            return;
        }

        members.sort();
        self.body = SetBody::Flat(encode_set(&members));
    }

    pub(super) fn slab_bytes(&self) -> Option<&[u8]> {
        let SetBody::Flat(bytes) = &self.body else {
            return None;
        };

        let used = set_used_len(bytes);

        (used <= SLAB_SLOT).then_some(&bytes[..used])
    }

    pub(super) fn from_flat(bytes: &[u8]) -> Self {
        let used = set_used_len(bytes).min(bytes.len());
        let bytes = bytes[..used].to_vec();
        let payload_bytes = SetMembers {
            bytes: &bytes,
            at: 2,
            left: flat_count(&bytes),
        }
        .map(<[u8]>::len)
        .sum();

        Self {
            body: SetBody::Flat(bytes),
            payload_bytes,
        }
    }

    pub(super) fn from_members(
        members: impl IntoIterator<Item = impl AsRef<[u8]>>,
        intset_max: usize,
    ) -> Self {
        let mut set = Self::default();

        for member in members {
            set.insert(member.as_ref(), intset_max);
        }

        set
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub fn set_add<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        members: &[T],
    ) -> Result<usize, WrongType> {
        self.remove_expired(key);
        let intset_max = self.collection_limits.set_max_intset_entries;
        let before = self.entry_charge(key);
        let mut added = 0usize;
        let mut index = 0usize;

        while index < members.len() {
            match self.entries.touch_slab(key, TAG_SET_SLAB, |buf| {
                inplace_set_insert(buf, members[index].as_ref())
            }) {
                SlabTouch::Done(Some(true)) => added += 1,
                SlabTouch::Done(Some(false)) => {}
                SlabTouch::Wrong | SlabTouch::Absent => {
                    if matches!(
                        self.entries.touch_slab(key, TAG_SET_SLAB, |_| Some(false)),
                        SlabTouch::Absent
                    ) && self.entries.create_slab(key, TAG_SET_SLAB)
                    {
                        continue;
                    }

                    added += self
                        .edit_set(key, true, |set| {
                            members[index..]
                                .iter()
                                .filter(|member| set.insert(member.as_ref(), intset_max))
                                .count()
                        })?
                        .unwrap_or(0);

                    index = members.len();

                    continue;
                }

                SlabTouch::Done(None) => {
                    let extra = 2 + members[index].as_ref().len();
                    let min_len = match self.entries.lookup(key) {
                        Some(IndexRef::SetSlab(bytes)) => set_used_len(bytes) + extra,
                        _ => SLAB_SLOT + 1,
                    };

                    if self.entries.enlarge_slab(key, min_len) {
                        continue;
                    }

                    let deadline = self.deadline_id(key);
                    let bytes = self.entries.take_slab(key).unwrap_or_default();
                    let mut set = SetValue::from_flat(&bytes);

                    added += members[index..]
                        .iter()
                        .filter(|member| set.insert(member.as_ref(), intset_max))
                        .count();

                    if !set.is_empty() {
                        self.insert_entry(key, Entry::fresh(Value::Set(Box::new(set))));
                        self.restore_deadline(key, deadline);
                    }

                    index = members.len();

                    continue;
                }
            }

            index += 1;
        }

        if added > 0 {
            self.reconcile_entry_charge(before, key);
        }
        Ok(added)
    }

    pub fn set_contains(&self, key: &[u8], member: &[u8]) -> Result<bool, WrongType> {
        Ok(self.live_set(key)?.is_some_and(|set| set.contains(member)))
    }

    pub fn set_remove<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        members: &[T],
    ) -> Result<usize, WrongType> {
        if self.remove_expired(key) {
            return Ok(0);
        }

        let before = self.entry_charge(key);
        let removed = self
            .edit_set(key, false, |set| {
                members
                    .iter()
                    .filter(|member| set.remove(member.as_ref()))
                    .count()
            })?
            .unwrap_or(0);

        self.reconcile_entry_charge(before, key);
        Ok(removed)
    }

    pub fn set_card(&self, key: &[u8]) -> Result<usize, WrongType> {
        Ok(self.live_set(key)?.map_or(0, |set| set.len()))
    }

    /// A borrowed, unordered view of a set's members for streaming a reply.
    pub fn set_member_stream(&self, key: &[u8]) -> Result<MemberStream<'_>, WrongType> {
        Ok(MemberStream {
            view: self.live_set(key)?,
        })
    }

    pub fn set_members(&self, key: &[u8]) -> Result<Vec<Cow<'_, [u8]>>, WrongType> {
        let Some(set) = self.live_set(key)? else {
            return Ok(Vec::new());
        };

        let mut members: Vec<Cow<'_, [u8]>> = set.iter().collect();

        members.sort_unstable();
        Ok(members)
    }

    /// One SSCAN page. An arena set is walked by hash home (see
    /// `ArenaTable::scan_buckets`); a small set (packed or integers) comes back whole with
    /// cursor 0, as in Redis. A set that spilled to the boxed table is paged in hash order
    /// (see `scan_by_hash`). Each cursor kind restarts the walk when handed another's.
    #[allow(clippy::type_complexity)]
    pub fn set_scan(
        &self,
        key: &[u8],
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> Result<(u64, Vec<Cow<'_, [u8]>>), WrongType> {
        let Some(set) = self.live_set(key)? else {
            return Ok((0, Vec::new()));
        };

        let matches =
            |member: &[u8]| pattern.is_none_or(|p| crate::protocol::glob_match(p, member));

        if let SetView::Value(value) = set {
            match &value.body {
                SetBody::Arena(table) => {
                    let mut page: Vec<Cow<'_, [u8]>> = Vec::new();
                    let next = table.scan_buckets(cursor, count.max(1), |member, _| {
                        if matches(member) {
                            page.push(Cow::Borrowed(member));
                        }
                    });

                    return Ok((next, page));
                }
                SetBody::Table(members) => {
                    let hasher = members.hasher();

                    return Ok(scan_by_hash(
                        members
                            .iter()
                            .map(|member| Cow::Borrowed(member.as_ref()))
                            .filter(|member| matches(member)),
                        |member| hasher.hash_one(member.as_ref()),
                        cursor,
                        count,
                    ));
                }
                SetBody::Flat(_) | SetBody::Int(_) => {}
            }
        }

        Ok((0, set.iter().filter(|member| matches(member)).collect()))
    }

    /// `SRANDMEMBER`: a positive `count` is distinct members, a negative one allows
    /// repeats. Every member is equally likely on every draw.
    pub(crate) fn set_random_members(
        &self,
        key: &[u8],
        count: Option<i64>,
    ) -> Result<Vec<Vec<u8>>, WrongType> {
        let Some(set) = self.live_set(key)? else {
            return Ok(Vec::new());
        };

        let width = set.len();
        if width == 0 {
            return Ok(Vec::new());
        }

        let (take, distinct) = match count {
            None => (1, false),
            Some(0) => return Ok(Vec::new()),
            Some(count) if count > 0 => (usize::try_from(count).unwrap_or(usize::MAX), true),
            Some(count) => (
                usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX),
                false,
            ),
        };

        Ok(sample_members(set, take, distinct))
    }

    /// `SPOP` candidates: `count` distinct members drawn at random (all when the set is
    /// smaller). The caller removes them.
    pub(crate) fn set_pop_members(
        &self,
        key: &[u8],
        count: usize,
    ) -> Result<Vec<Vec<u8>>, WrongType> {
        let Some(set) = self.live_set(key)? else {
            return Ok(Vec::new());
        };

        Ok(sample_members(set, count, true))
    }

    pub fn set_store<T: AsRef<[u8]>>(&mut self, key: &[u8], members: &[T]) -> usize {
        self.remove_expired(key);

        if members.is_empty() {
            let before = self.entry_charge(key);

            if let Some(entry) = self.remove_entry(key) {
                self.note_key(key, crate::notify::KeyEvent::Del);
                self.free_entry(key, entry);
            }

            self.reconcile_entry_charge(before, key);

            return 0;
        }

        let unique = SetValue::from_members(members, self.collection_limits.set_max_intset_entries);
        let count = unique.len();
        let before = self.entry_charge(key);

        if let Some(previous) = self.insert_entry(key, Entry::fresh(Value::Set(Box::new(unique)))) {
            self.free_entry(key, previous);
        }

        self.reconcile_entry_charge(before, key);
        count
    }

    pub fn set_move(
        &mut self,
        source: &[u8],
        destination: &[u8],
        member: &[u8],
    ) -> Result<bool, WrongType> {
        if source == destination {
            return self.set_contains(source, member);
        }

        self.remove_expired(source);
        self.remove_expired(destination);

        if self.live_set(destination).is_err() {
            return Err(WrongType);
        }

        let Some(set) = self.live_set(source)? else {
            return Ok(false);
        };

        if !set.contains(member) {
            return Ok(false);
        }

        self.set_remove(source, &[member])
            .expect("source type was validated");
        self.set_add(destination, &[member])
            .expect("destination type was validated");
        Ok(true)
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub(super) fn edit_set<R>(
        &mut self,
        key: &[u8],
        create: bool,
        edit: impl FnOnce(&mut SetValue) -> R,
    ) -> Result<Option<R>, WrongType> {
        // A slab record becomes an object for the edit and goes back afterwards when it fits.
        self.entries.promote_collection(key);

        if let Some(IndexRef::Object(_)) = self.entries.lookup(key) {
            let Some(entry) = self.entries.object_mut(key) else {
                return Ok(None);
            };

            let Value::Set(set) = &mut entry.value else {
                return Err(WrongType);
            };

            let result = edit(set);

            if set.is_empty() {
                self.remove_emptied(key);
            } else {
                self.entries.demote_collection(key);
            }

            return Ok(Some(result));
        }

        let mut set = match self.entries.lookup(key) {
            None => {
                if !create {
                    return Ok(None);
                }

                SetValue::default()
            }

            Some(_) => return Err(WrongType),
        };

        let result = edit(&mut set);

        if set.is_empty() {
            self.remove_emptied(key);
        } else {
            self.insert_entry(key, Entry::fresh(Value::Set(Box::new(set))));
        }

        Ok(Some(result))
    }

    pub(crate) fn live_set(&self, key: &[u8]) -> Result<Option<SetView<'_>>, WrongType> {
        self.live_view(
            key,
            |value| match value {
                Value::Set(set) => Some(SetView::Value(set)),
                _ => None,
            },
            |record| match record {
                IndexRef::SetSlab(bytes) => Some(SetView::Bytes(bytes)),
                _ => None,
            },
        )
    }
}
