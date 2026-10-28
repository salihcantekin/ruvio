use super::*;

mod btree;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct Score(pub(super) f64);

impl Eq for Score {}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

pub(super) fn write_score(bytes: &mut Vec<u8>, score: Score) {
    let mut buf = [0u8; 9];
    let len = put_score(&mut buf, score);

    bytes.extend_from_slice(&buf[..len]);
}

pub(super) fn compact_score(score: f64) -> Option<i32> {
    if !score.is_finite() || score.fract() != 0.0 {
        return None;
    }

    if score < f64::from(i32::MIN) || score > f64::from(i32::MAX) {
        return None;
    }

    Some(score as i32)
}

pub(super) fn read_score(bytes: &[u8], at: &mut usize) -> Score {
    let kind = bytes[*at];

    *at += 1;

    if kind & 0x80 != 0 {
        return Score(f64::from(kind & 0x7f));
    }

    if kind == 0 {
        let mut raw = [0u8; 4];

        raw.copy_from_slice(&bytes[*at..*at + 4]);
        *at += 4;

        return Score(f64::from(i32::from_le_bytes(raw)));
    }

    if kind == 2 {
        let mut raw = [0u8; 8];

        raw[..7].copy_from_slice(&bytes[*at..*at + 7]);
        *at += 7;

        return Score(u64::from_le_bytes(raw) as f64);
    }

    let mut raw = [0u8; 8];

    raw.copy_from_slice(&bytes[*at..*at + 8]);
    *at += 8;
    Score(f64::from_le_bytes(raw))
}

pub(super) fn encode_sorted<T: AsRef<[u8]>>(entries: &[(Score, T)]) -> Vec<u8> {
    let mut bytes = Vec::new();

    bytes.extend_from_slice(&(entries.len() as u16).to_le_bytes());

    for (score, member) in entries {
        let member = member.as_ref();

        write_score(&mut bytes, *score);
        bytes.extend_from_slice(&(member.len() as u16).to_le_bytes());
        bytes.extend_from_slice(member);
    }

    seal(bytes)
}

pub(super) fn decode_sorted(bytes: &[u8]) -> Vec<(Score, Vec<u8>)> {
    let mut entries = Vec::with_capacity(flat_count(bytes));
    let mut at = 2usize;

    for _ in 0..flat_count(bytes) {
        let score = read_score(bytes, &mut at);
        let len = u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;

        at += 2;
        entries.push((score, bytes[at..at + len].to_vec()));
        at += len;
    }

    entries
}

pub(super) struct SortedEntries<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) at: usize,
    pub(super) left: usize,
}

impl<'a> SortedEntries<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            at: 2,
            left: flat_count(bytes),
        }
    }
}

impl<'a> Iterator for SortedEntries<'a> {
    type Item = (Score, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }

        let score = read_score(self.bytes, &mut self.at);
        let len = u16::from_le_bytes([self.bytes[self.at], self.bytes[self.at + 1]]) as usize;

        self.at += 2;
        let member = &self.bytes[self.at..self.at + len];

        self.at += len;
        self.left -= 1;
        Some((score, member))
    }
}

pub(super) fn ensure_flat_room(bytes: &mut Vec<u8>, used: usize, extra: usize) {
    if bytes.len() < 2 {
        bytes.resize(2, 0);
    }

    let need = used.saturating_add(extra);

    if bytes.capacity() < need {
        let slack = extra.saturating_mul(4);
        let target = need.saturating_add(slack);
        let additional = target.saturating_sub(bytes.len());

        bytes.reserve_exact(additional);
    }

    if bytes.len() < need {
        bytes.resize(need, 0);
    }
}

pub(super) fn inplace_sorted_remove(buf: &mut [u8], member: &[u8]) -> bool {
    if buf.len() < 2 {
        return false;
    }

    let used = sorted_used_len(buf);
    let mut at = 2usize;

    for _ in 0..flat_count(buf) {
        let start = at;
        let _ = read_score(buf, &mut at);
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

pub(super) fn score_encoding_len(score: Score) -> usize {
    if compact_score(score.0).is_some_and(|score| (0..64).contains(&score)) {
        1
    } else if compact_score(score.0).is_some() {
        5
    } else if wide_score(score.0).is_some() {
        8
    } else {
        9
    }
}

pub(super) fn wide_score(score: f64) -> Option<u64> {
    if compact_score(score).is_some() || !score.is_finite() || score < 0.0 || score.fract() != 0.0 {
        return None;
    }

    if score >= (1u64 << 56) as f64 {
        return None;
    }

    let bits = score as u64;

    (bits as f64 == score).then_some(bits)
}

pub(super) fn put_score(buf: &mut [u8], score: Score) -> usize {
    if let Some(small) = compact_score(score.0).filter(|score| (0..64).contains(score)) {
        buf[0] = 0x80 | small as u8;

        return 1;
    }

    if let Some(wide) = compact_score(score.0) {
        buf[0] = 0;
        buf[1..5].copy_from_slice(&wide.to_le_bytes());

        return 5;
    }

    if let Some(bits) = wide_score(score.0) {
        buf[0] = 2;
        buf[1..8].copy_from_slice(&bits.to_le_bytes()[..7]);

        return 8;
    }

    buf[0] = 1;
    buf[1..9].copy_from_slice(&score.0.to_le_bytes());
    9
}

pub(super) enum FlatEdit {
    Done(bool),
    NeedRoom,
    Promote,
}

pub(super) fn inplace_sorted_insert(buf: &mut [u8], score: Score, member: &[u8]) -> FlatEdit {
    if !member_fits_flat(member) || buf.len() < 2 {
        return FlatEdit::Promote;
    }

    let mut at = 2usize;
    let mut insert_at = None;
    let mut found = None;

    for _ in 0..flat_count(buf) {
        let start = at;
        let stored_score = read_score(buf, &mut at);
        let len = u16::from_le_bytes([buf[at], buf[at + 1]]) as usize;
        let stored = &buf[at + 2..at + 2 + len];
        let end = at + 2 + len;

        if stored == member {
            found = Some((start, end, stored_score));
        } else if insert_at.is_none()
            && stored_score
                .cmp(&score)
                .then_with(|| stored.cmp(member))
                .is_gt()
        {
            insert_at = Some(start);
        }

        at = end;
    }

    let mut used = at;
    let shift;
    let insert_point;

    if let Some((start, end, stored_score)) = found {
        if stored_score == score {
            return FlatEdit::Done(false);
        }

        let new_len = score_encoding_len(score) + 2 + member.len();
        let old_len = end - start;
        let new_used = used - old_len + new_len;

        if new_used > buf.len() {
            return FlatEdit::NeedRoom;
        }

        let dest = insert_at.unwrap_or(used);
        let dest = if dest >= end { dest - old_len } else { dest };

        buf.copy_within(end..used, start);
        let count = (flat_count(buf) - 1) as u16;

        buf[..2].copy_from_slice(&count.to_le_bytes());
        used -= old_len;
        insert_point = dest;
        shift = new_len;
    } else {
        if flat_count(buf) >= COLLECTION_FLAT_LIMIT {
            return FlatEdit::Promote;
        }

        shift = score_encoding_len(score) + 2 + member.len();
        insert_point = insert_at.unwrap_or(used);

        if used + shift > buf.len() {
            return FlatEdit::NeedRoom;
        }
    }

    buf.copy_within(insert_point..used, insert_point + shift);
    let cursor = insert_point + put_score(&mut buf[insert_point..], score);

    buf[cursor..cursor + 2].copy_from_slice(&(member.len() as u16).to_le_bytes());
    buf[cursor + 2..cursor + 2 + member.len()].copy_from_slice(member);
    let count = (flat_count(buf) + 1) as u16;

    buf[..2].copy_from_slice(&count.to_le_bytes());
    FlatEdit::Done(found.is_none())
}

/// A large sorted set: each member is one arena block `score f64 LE ‖ member`,
/// `lookup` maps the member hash to the block offset, and `tree` orders the offsets by
/// `(score, member)` with subtree counts, so rank, select and seek are `O(log N)`.
#[derive(Debug, Clone)]
pub(super) struct IndexedSorted {
    arena: crate::arena::Arena,
    lookup: crate::hashbrown::HashTable<u32>,
    hasher: ahash::RandomState,
    tree: btree::CountedTree,
}

impl PartialEq for IndexedSorted {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl IndexedSorted {
    pub(super) fn new() -> Self {
        let mut lookup = crate::hashbrown::HashTable::new();

        // See `ArenaTable::with_capacity`: a stale ZSCAN cursor must not match this table.
        lookup.seed_epoch(sample_seed() as u32);

        Self {
            arena: crate::arena::Arena::new(),
            lookup,
            hasher: ahash::RandomState::new(),
            tree: btree::CountedTree::new(),
        }
    }

    fn hash(&self, member: &[u8]) -> u64 {
        self.hasher.hash_one(member)
    }

    fn member_at(&self, off: u32) -> &[u8] {
        btree::block_member(self.arena.get(off))
    }

    fn score_at(&self, off: u32) -> Score {
        Score(btree::block_score(self.arena.get(off)))
    }

    fn find(&self, member: &[u8]) -> Option<u32> {
        let hash = self.hash(member);

        self.lookup
            .find(hash, |off| self.member_at(*off) == member)
            .copied()
    }

    pub(super) fn score(&self, member: &[u8]) -> Option<Score> {
        self.find(member).map(|off| self.score_at(off))
    }

    pub(super) fn len(&self) -> usize {
        self.tree.len()
    }

    pub(super) fn insert(&mut self, score: Score, member: &[u8]) -> Result<bool, InsertError> {
        if let Some(off) = self.find(member) {
            let old = self.score_at(off);

            if old.0 == score.0 {
                return Ok(false);
            }

            self.tree.remove(&self.arena, old.0, member);
            self.arena.get_mut(off)[..btree::SCORE_LEN].copy_from_slice(&score.0.to_le_bytes());
            self.tree.insert(&self.arena, score.0, off);

            return Ok(false);
        }

        // The arena addresses 2 GiB of chunks per set; a full one refuses the member once
        // a compaction has given back what the dead blocks held.
        let score_bytes = score.0.to_le_bytes();
        let off = match self.arena.alloc(&[&score_bytes, member]) {
            Some(off) => off,
            None if self.arena.reclaimable() > 0 => {
                self.compact();
                self.arena
                    .alloc(&[&score_bytes, member])
                    .ok_or(InsertError::Full)?
            }
            None => return Err(InsertError::Full),
        };
        let hash = self.hash(member);
        let (arena, hasher) = (&self.arena, &self.hasher);

        self.lookup.insert_unique(hash, off, |stored| {
            hasher.hash_one(btree::block_member(arena.get(*stored)))
        });
        self.tree.insert(&self.arena, score.0, off);
        Ok(true)
    }

    /// True when every member of `members` not already in the set gets an arena block.
    /// Compacts first when the dead blocks are what stands in the way.
    pub(super) fn make_room<'m>(
        &mut self,
        members: impl Iterator<Item = &'m [u8]> + Clone,
    ) -> bool {
        // Counting every member as new skips the lookups whenever the arena is far from full.
        if self.arena.fits(
            members
                .clone()
                .map(|member| btree::SCORE_LEN + member.len()),
        ) {
            return true;
        }

        if self.new_members_fit(members.clone()) {
            return true;
        }

        if self.arena.reclaimable() == 0 {
            return false;
        }

        self.compact();
        self.new_members_fit(members)
    }

    fn new_members_fit<'m>(&self, members: impl Iterator<Item = &'m [u8]>) -> bool {
        self.arena.fits(
            members
                .filter(|member| self.find(member).is_none())
                .map(|member| btree::SCORE_LEN + member.len()),
        )
    }

    pub(super) fn remove(&mut self, member: &[u8]) -> bool {
        let hash = self.hash(member);
        let Ok(entry) = self.lookup.find_entry(hash, |off| {
            btree::block_member(self.arena.get(*off)) == member
        }) else {
            return false;
        };
        let (off, _) = entry.remove();
        let score = btree::block_score(self.arena.get(off));

        self.tree.remove(&self.arena, score, member);
        self.arena.free(off);

        if self.arena.should_compact() {
            self.compact();
        }

        true
    }

    /// Rewrites the arena in key order and rebuilds the lookup. `O(N)`, amortised over
    /// the removals that made half the arena dead.
    fn compact(&mut self) {
        self.tree.compact(&mut self.arena);
        self.lookup.clear();

        let (arena, hasher) = (&self.arena, &self.hasher);

        for (_, off) in self.tree.iter() {
            let hash = hasher.hash_one(btree::block_member(arena.get(off)));

            self.lookup.insert_unique(hash, off, |stored| {
                hasher.hash_one(btree::block_member(arena.get(*stored)))
            });
        }
    }

    pub(super) fn rank_forward(&self, member: &[u8]) -> Option<usize> {
        let off = self.find(member)?;

        self.tree.rank(&self.arena, self.score_at(off).0, member)
    }

    fn entry(&self, pos: btree::Pos) -> SortedMemberScore<'_> {
        let (score, off) = self.tree.get(pos);

        (self.member_at(off), score)
    }

    /// Every `(member, score)` in order.
    pub(super) fn iter(&self) -> impl Iterator<Item = SortedMemberScore<'_>> + '_ {
        self.tree
            .iter()
            .map(|(score, off)| (self.member_at(off), score))
    }

    /// `take` entries from forward rank `start`, walking backwards when `reverse`.
    fn walk(&self, start: usize, take: usize, reverse: bool) -> Vec<SortedMemberScore<'_>> {
        let mut out = Vec::with_capacity(take);
        let mut pos = self.tree.select(start);

        while out.len() < take && !pos.is_end() {
            out.push(self.entry(pos));
            pos = if reverse {
                self.tree.prev(pos)
            } else {
                self.tree.next(pos)
            };
        }

        out
    }

    /// Calls `visit` on the entries inside the score bounds, in order, until it returns false.
    fn visit_scores<'a>(
        &'a self,
        min: crate::protocol::ScoreBound,
        max: crate::protocol::ScoreBound,
        mut visit: impl FnMut(&'a [u8], f64) -> bool,
    ) {
        let mut pos = self.tree.seek(|score| !min.accepts_min(score));

        while !pos.is_end() {
            let (member, score) = self.entry(pos);

            if !max.accepts_max(score) || !visit(member, score) {
                return;
            }

            pos = self.tree.next(pos);
        }
    }

    /// The first `count` entries from the low end (the high end when `max`), in that order.
    fn visit_ends(&self, count: usize, max: bool, mut visit: impl FnMut(&[u8], f64)) {
        let len = self.len();
        let take = count.min(len);

        if take == 0 {
            return;
        }

        let mut pos = self.tree.select(if max { len - 1 } else { 0 });

        for _ in 0..take {
            let (member, score) = self.entry(pos);

            visit(member, score);
            pos = if max {
                self.tree.prev(pos)
            } else {
                self.tree.next(pos)
            };
        }
    }

    /// Visits members by lookup home bucket, about `count`, from where `cursor` left off;
    /// same cursor and guarantee as `ArenaTable::scan_buckets`. Rescoring a member moves
    /// it in the tree but not in the lookup, so a scan cannot skip it.
    pub(super) fn scan_buckets<'a>(
        &'a self,
        cursor: u64,
        count: usize,
        mut visit: impl FnMut(&'a [u8], f64),
    ) -> u64 {
        home_cursor_out(self.lookup.scan_homes(
            home_cursor_in(cursor),
            count,
            scan_budget(count),
            &|off| self.hash(self.member_at(*off)),
            &mut |off| visit(self.member_at(*off), self.score_at(*off).0),
        ))
    }

    pub(super) fn capacity_bytes(&self, payload: usize) -> usize {
        self.arena
            .capacity()
            .saturating_sub(payload)
            .saturating_add(self.lookup.capacity().saturating_mul(size_of::<u32>() + 1))
            .saturating_add(self.tree.capacity_bytes())
            .saturating_add(size_of::<Self>())
    }
}

/// Up to this many members are looked up in a flat set by scanning it.
const FEW_MEMBERS: usize = 4;

/// True when the members of the flat encoding `bytes` plus `members` fit one fresh arena.
/// Rescoring members of a flat set never promotes it, so only new members count.
fn flat_fits_fresh_arena<'m>(
    bytes: &[u8],
    members: impl Iterator<Item = &'m [u8]> + Clone,
) -> bool {
    let block = |member: &[u8]| btree::SCORE_LEN + member.len();

    if bytes.len() < 2 {
        return crate::arena::Arena::new().fits(members.map(block));
    }

    // A few members scan the small flat set directly; many build its member set once.
    let stored: Option<AHashSet<&[u8]>> = members.clone().nth(FEW_MEMBERS).map(|_| {
        SortedEntries::new(bytes)
            .map(|(_, member)| member)
            .collect()
    });
    let is_stored = |member: &[u8]| match &stored {
        Some(stored) => stored.contains(member),
        None => SortedEntries::new(bytes).any(|(_, held)| held == member),
    };
    let mut new_members = members.filter(|member| !is_stored(member)).peekable();

    if new_members.peek().is_none() {
        return true;
    }

    let held = SortedEntries::new(bytes).map(|(_, member)| block(member));

    crate::arena::Arena::new().fits(held.chain(new_members.map(block)))
}

pub(super) fn sorted_used_len(bytes: &[u8]) -> usize {
    let mut at = 2usize;

    for _ in 0..flat_count(bytes) {
        let _ = read_score(bytes, &mut at);
        let len = u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;

        at += 2 + len;
    }

    at
}

/// [`IndexedSorted::visit_ends`] for a flat encoding, which only reads forwards: the high
/// end skips to the last `count` entries and, for more than one, gathers them to reverse.
fn flat_visit_ends(bytes: &[u8], count: usize, max: bool, mut visit: impl FnMut(&[u8], f64)) {
    if !max {
        SortedEntries::new(bytes)
            .take(count)
            .for_each(|(score, member)| visit(member, score.0));
        return;
    }

    let length = flat_count(bytes);
    let take = count.min(length);
    let mut tail = SortedEntries::new(bytes).skip(length - take);

    if take == 1 {
        if let Some((score, member)) = tail.next() {
            visit(member, score.0);
        }

        return;
    }

    let tail: Vec<_> = tail.collect();

    tail.iter()
        .rev()
        .for_each(|(score, member)| visit(member, score.0));
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum SortedBody {
    Flat(Vec<u8>),
    Indexed(Box<IndexedSorted>),
}

impl Default for SortedBody {
    fn default() -> Self {
        Self::Flat(Vec::new())
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct SortedSet {
    pub(super) body: SortedBody,
    /// Member bytes held by this set; each member is stored once in either form.
    pub(super) payload_bytes: usize,
}

pub(super) type SortedMemberScore<'a> = (&'a [u8], f64);

pub(super) enum SortedView<'a> {
    Bytes(&'a [u8]),
    Value(&'a SortedSet),
}

impl<'a> SortedView<'a> {
    pub(super) fn card(&self) -> usize {
        match self {
            Self::Bytes(bytes) => flat_count(bytes),
            Self::Value(set) => set.card(),
        }
    }

    /// The first `count` entries from the low end (the high end when `max`), in the order
    /// `range_entries(0, count - 1, max)` lists them.
    pub(super) fn visit_ends(&self, count: usize, max: bool, visit: impl FnMut(&[u8], f64)) {
        match self {
            Self::Bytes(bytes) => flat_visit_ends(bytes, count, max, visit),
            Self::Value(set) => match &set.body {
                SortedBody::Flat(bytes) => flat_visit_ends(bytes, count, max, visit),
                SortedBody::Indexed(index) => index.visit_ends(count, max, visit),
            },
        }
    }

    pub(super) fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            Self::Bytes(bytes) => SortedEntries::new(bytes)
                .find_map(|(score, stored)| (stored == member).then_some(score.0)),
            Self::Value(set) => set.score(member),
        }
    }

    pub(super) fn rank(&self, member: &[u8], reverse: bool) -> Option<usize> {
        match self {
            Self::Bytes(bytes) => {
                let len = flat_count(bytes);
                let rank = SortedEntries::new(bytes).position(|(_, stored)| stored == member)?;

                Some(if reverse { len - rank - 1 } else { rank })
            }

            Self::Value(set) => set.rank(member, reverse),
        }
    }

    pub(super) fn range_entries(
        &self,
        start: isize,
        stop: isize,
        reverse: bool,
    ) -> Vec<SortedMemberScore<'a>> {
        match self {
            Self::Value(set) => set.range_entries(start, stop, reverse),
            Self::Bytes(bytes) => {
                let length = flat_count(bytes);
                let Some((start, stop)) = normalize_range(length, start, stop) else {
                    return Vec::new();
                };

                let take = stop - start + 1;
                let mut entries = Vec::with_capacity(take);

                if reverse {
                    let from = length - 1 - stop;
                    let until = length - start;

                    for (index, (score, member)) in SortedEntries::new(bytes).enumerate() {
                        if index >= until {
                            break;
                        }

                        if index >= from {
                            entries.push((member, score.0));
                        }
                    }

                    entries.reverse();
                    entries
                } else {
                    for (index, (score, member)) in SortedEntries::new(bytes).enumerate() {
                        if index > stop {
                            break;
                        }

                        if index >= start {
                            entries.push((member, score.0));
                        }
                    }

                    entries
                }
            }
        }
    }

    pub(super) fn range_by_score(
        &self,
        min: crate::protocol::ScoreBound,
        max: crate::protocol::ScoreBound,
    ) -> Vec<SortedMemberScore<'a>> {
        let accept = |score: f64| min.accepts_min(score) && max.accepts_max(score);

        match self {
            Self::Value(set) => set.range_by_score(min, max),
            Self::Bytes(bytes) => SortedEntries::new(bytes)
                .filter(|(score, _)| accept(score.0))
                .map(|(score, member)| (member, score.0))
                .collect(),
        }
    }

    pub(super) fn range_by_lex(
        &self,
        min: &crate::protocol::LexBound<'_>,
        max: &crate::protocol::LexBound<'_>,
    ) -> Vec<SortedMemberScore<'a>> {
        match self {
            Self::Value(set) => set.range_by_lex(min, max),
            Self::Bytes(bytes) => sort_lex(
                SortedEntries::new(bytes)
                    .filter(|(_, member)| lex_in_range(member, min, max))
                    .map(|(score, member)| (member, score.0))
                    .collect(),
            ),
        }
    }

    pub(super) fn visit_scores(
        &self,
        min: crate::protocol::ScoreBound,
        max: crate::protocol::ScoreBound,
        mut visit: impl FnMut(&[u8], f64) -> bool,
    ) {
        match self {
            Self::Value(set) => set.visit_scores(min, max, visit),
            Self::Bytes(bytes) => {
                for (score, member) in SortedEntries::new(bytes) {
                    if !min.accepts_min(score.0) {
                        continue;
                    }

                    if !max.accepts_max(score.0) {
                        break;
                    }

                    if !visit(member, score.0) {
                        return;
                    }
                }
            }
        }
    }
}

impl SortedSet {
    pub(super) fn insert(&mut self, score: f64, member: &[u8]) -> Result<bool, InsertError> {
        if score.is_nan() {
            return Err(InsertError::InvalidScore);
        }

        let score = Score(score);

        if self.is_flat() {
            if !member_fits_flat(member) {
                let added = self.score_of(member).is_none();

                if !added {
                    self.remove_member(member);
                }

                self.promote();

                return self.insert_indexed(score, member);
            }

            match self.insert_flat(score, member) {
                FlatEdit::Done(added) => return Ok(added),
                FlatEdit::Promote => self.promote(),
                FlatEdit::NeedRoom => unreachable!("flat sorted set reserved room"),
            }
        }

        self.insert_indexed(score, member)
    }

    /// True when inserting `members` cannot fail with [`InsertError::Full`]; see
    /// [`IndexedSorted::make_room`]. A flat set may be promoted on the way, so its members
    /// are counted into a fresh arena together with the new ones.
    pub(super) fn make_room<'m>(
        &mut self,
        members: impl Iterator<Item = &'m [u8]> + Clone,
    ) -> bool {
        match &mut self.body {
            SortedBody::Indexed(index) => index.make_room(members),
            SortedBody::Flat(bytes) => flat_fits_fresh_arena(bytes, members),
        }
    }

    pub(super) fn insert_flat(&mut self, score: Score, member: &[u8]) -> FlatEdit {
        let SortedBody::Flat(bytes) = &mut self.body else {
            unreachable!("flat sorted set");
        };

        if bytes.len() < 2 {
            bytes.resize(2, 0);
        }

        let extra = score_encoding_len(score) + 2 + member.len();
        let mut edit = inplace_sorted_insert(bytes, score, member);

        if matches!(edit, FlatEdit::NeedRoom) {
            let used = sorted_used_len(bytes);

            ensure_flat_room(bytes, used, extra);
            edit = inplace_sorted_insert(bytes, score, member);
        }

        if let FlatEdit::Done(added) = edit {
            let used = sorted_used_len(bytes);

            bytes.truncate(used);

            if added {
                self.payload_bytes = self.payload_bytes.saturating_add(member.len());
            }
        }

        edit
    }

    pub(super) fn remove(&mut self, member: &[u8]) -> bool {
        let removed = self.remove_member(member);

        if removed && !self.is_flat() && self.card() <= COLLECTION_FLAT_LIMIT / 2 {
            self.demote();
        }

        removed
    }

    pub(super) fn score(&self, member: &[u8]) -> Option<f64> {
        self.score_of(member).map(|score| score.0)
    }

    pub(super) fn card(&self) -> usize {
        match &self.body {
            SortedBody::Flat(bytes) => flat_count(bytes),
            SortedBody::Indexed(index) => index.len(),
        }
    }

    pub(super) fn rank(&self, member: &[u8], reverse: bool) -> Option<usize> {
        let rank = match &self.body {
            SortedBody::Flat(bytes) => {
                SortedEntries::new(bytes).position(|(_, stored)| stored == member)?
            }

            SortedBody::Indexed(index) => index.rank_forward(member)?,
        };

        let len = self.card();

        Some(if reverse { len - rank - 1 } else { rank })
    }

    pub(super) fn is_flat(&self) -> bool {
        matches!(self.body, SortedBody::Flat(_))
    }

    pub(super) fn slab_bytes(&self) -> Option<&[u8]> {
        let SortedBody::Flat(bytes) = &self.body else {
            return None;
        };

        let used = sorted_used_len(bytes);

        (used <= SLAB_SLOT).then_some(&bytes[..used])
    }

    pub(super) fn from_flat(bytes: &[u8]) -> Self {
        let used = sorted_used_len(bytes).min(bytes.len());
        let bytes = bytes[..used].to_vec();
        let payload_bytes = SortedEntries::new(&bytes)
            .map(|(_, member)| member.len())
            .sum();

        Self {
            body: SortedBody::Flat(bytes),
            payload_bytes,
        }
    }

    pub(super) fn score_of(&self, member: &[u8]) -> Option<Score> {
        match &self.body {
            SortedBody::Flat(bytes) => SortedEntries::new(bytes)
                .find_map(|(score, stored)| (stored == member).then_some(score)),
            SortedBody::Indexed(index) => index.score(member),
        }
    }

    pub(super) fn remove_member(&mut self, member: &[u8]) -> bool {
        match &mut self.body {
            SortedBody::Flat(bytes) => {
                if bytes.len() < 2 || !inplace_sorted_remove(bytes, member) {
                    return false;
                }

                self.payload_bytes = self.payload_bytes.saturating_sub(member.len());
                let used = sorted_used_len(bytes);

                bytes.truncate(used);
                true
            }

            SortedBody::Indexed(index) => {
                if !index.remove(member) {
                    return false;
                }

                self.payload_bytes = self.payload_bytes.saturating_sub(member.len());
                true
            }
        }
    }

    pub(super) fn insert_indexed(
        &mut self,
        score: Score,
        member: &[u8],
    ) -> Result<bool, InsertError> {
        let SortedBody::Indexed(index) = &mut self.body else {
            unreachable!("indexed sorted set");
        };

        let added = index.insert(score, member)?;

        if added {
            self.payload_bytes = self.payload_bytes.saturating_add(member.len());
        }

        Ok(added)
    }

    pub(super) fn promote(&mut self) {
        let SortedBody::Flat(flat) = std::mem::take(&mut self.body) else {
            return;
        };

        let entries = decode_sorted(&flat);
        let mut index = IndexedSorted::new();

        for (score, member) in entries {
            index
                .insert(score, &member)
                .expect("a flat set fits a fresh arena");
        }

        self.body = SortedBody::Indexed(Box::new(index));
    }

    pub(super) fn demote(&mut self) {
        let SortedBody::Indexed(index) = std::mem::take(&mut self.body) else {
            return;
        };

        let entries: Vec<_> = index
            .iter()
            .map(|(member, score)| (Score(score), member))
            .collect();

        self.body = SortedBody::Flat(encode_sorted(&entries));
    }

    pub(super) fn collection_overhead(&self) -> usize {
        let index = match &self.body {
            SortedBody::Flat(bytes) => bytes.capacity(),
            SortedBody::Indexed(index) => index.capacity_bytes(self.payload_bytes),
        };

        index.saturating_add(size_of::<SortedSet>())
    }

    pub(super) fn increment(&mut self, member: &[u8], delta: f64) -> Result<f64, InsertError> {
        if delta.is_nan() {
            return Err(InsertError::InvalidScore);
        }

        let next = self.score(member).unwrap_or(0.0) + delta;

        if next.is_nan() {
            return Err(InsertError::InvalidScore);
        }

        self.insert(next, member)?;
        Ok(next)
    }

    /// Visits every `(member, score)` in rank order, as `range_entries(0, -1, false)` lists
    /// them, without collecting them.
    pub(super) fn for_each_entry(&self, mut visit: impl FnMut(&[u8], f64)) {
        match &self.body {
            SortedBody::Flat(bytes) => {
                SortedEntries::new(bytes).for_each(|(score, member)| visit(member, score.0));
            }
            SortedBody::Indexed(index) => index
                .iter()
                .for_each(|(member, score)| visit(member, score)),
        }
    }

    pub(super) fn range_entries(
        &self,
        start: isize,
        stop: isize,
        reverse: bool,
    ) -> Vec<SortedMemberScore<'_>> {
        let length = self.card();
        let Some((start, stop)) = normalize_range(length, start, stop) else {
            return Vec::new();
        };

        let take = stop - start + 1;

        match &self.body {
            SortedBody::Flat(bytes) => {
                let mut entries: Vec<_> = SortedEntries::new(bytes).collect();

                if reverse {
                    entries.reverse();
                }

                entries
                    .into_iter()
                    .skip(start)
                    .take(take)
                    .map(|(score, member)| (member, score.0))
                    .collect()
            }

            SortedBody::Indexed(index) => {
                let first = if reverse { length - 1 - start } else { start };

                index.walk(first, take, reverse)
            }
        }
    }

    pub(super) fn range_by_score(
        &self,
        min: crate::protocol::ScoreBound,
        max: crate::protocol::ScoreBound,
    ) -> Vec<SortedMemberScore<'_>> {
        let accept = |score: f64| min.accepts_min(score) && max.accepts_max(score);

        match &self.body {
            SortedBody::Flat(bytes) => SortedEntries::new(bytes)
                .filter(|(score, _)| accept(score.0))
                .map(|(score, member)| (member, score.0))
                .collect(),
            SortedBody::Indexed(index) => {
                let mut ordered = Vec::new();

                index.visit_scores(min, max, |member, score| {
                    ordered.push((member, score));
                    true
                });

                ordered
            }
        }
    }

    pub(super) fn range_by_lex(
        &self,
        min: &crate::protocol::LexBound<'_>,
        max: &crate::protocol::LexBound<'_>,
    ) -> Vec<SortedMemberScore<'_>> {
        let ordered = match &self.body {
            SortedBody::Flat(bytes) => SortedEntries::new(bytes)
                .filter(|(_, member)| lex_in_range(member, min, max))
                .map(|(score, member)| (member, score.0))
                .collect(),
            SortedBody::Indexed(index) => index
                .iter()
                .filter(|(member, _)| lex_in_range(member, min, max))
                .collect(),
        };

        sort_lex(ordered)
    }

    pub(super) fn visit_scores(
        &self,
        min: crate::protocol::ScoreBound,
        max: crate::protocol::ScoreBound,
        mut visit: impl FnMut(&[u8], f64) -> bool,
    ) {
        match &self.body {
            SortedBody::Flat(bytes) => {
                for (score, member) in SortedEntries::new(bytes) {
                    if !min.accepts_min(score.0) {
                        continue;
                    }

                    if !max.accepts_max(score.0) {
                        break;
                    }

                    if !visit(member, score.0) {
                        return;
                    }
                }
            }

            SortedBody::Indexed(index) => index.visit_scores(min, max, visit),
        }
    }
}

pub(super) fn lex_in_range(
    member: &[u8],
    min: &crate::protocol::LexBound<'_>,
    max: &crate::protocol::LexBound<'_>,
) -> bool {
    min.accepts_min(member) && max.accepts_max(member)
}

pub(super) fn sort_lex<'a>(mut entries: Vec<SortedMemberScore<'a>>) -> Vec<SortedMemberScore<'a>> {
    entries.sort_by(|left, right| left.0.cmp(right.0));

    entries
}

pub(super) fn normalize_index(length: usize, index: isize) -> Option<usize> {
    let length = isize::try_from(length).ok()?;
    let index = if index < 0 { length + index } else { index };

    (index >= 0 && index < length).then_some(index as usize)
}

pub(super) fn normalize_range(length: usize, start: isize, stop: isize) -> Option<(usize, usize)> {
    if length == 0 {
        return None;
    }

    let length = isize::try_from(length).ok()?;
    let start = if start < 0 { length + start } else { start }.max(0);

    let stop = if stop < 0 { length + stop } else { stop }.min(length - 1);

    (start <= stop).then_some((start as usize, stop as usize))
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// True when adding `members` to the sorted set at `key` cannot run out of arena
    /// space, so a caller can refuse the write before recording it. A missing or
    /// non-sorted key is checked as a fresh set (`replace` checks as if `key` were
    /// deleted first). May compact the set's arena, which changes no member.
    pub fn sorted_set_make_room<'m>(
        &mut self,
        key: &[u8],
        replace: bool,
        members: impl Iterator<Item = &'m [u8]> + Clone,
    ) -> bool {
        if replace {
            return flat_fits_fresh_arena(&[], members);
        }

        match self.entries.lookup(key) {
            Some(IndexRef::Object(_)) => {}
            Some(IndexRef::SortedSlab(bytes)) => return flat_fits_fresh_arena(bytes, members),
            _ => return flat_fits_fresh_arena(&[], members),
        }

        let Some(entry) = self.entries.object_mut(key) else {
            return true;
        };
        let before = EntryCharge::for_entry(key, entry);
        let Value::SortedSet(set) = &mut entry.value else {
            return flat_fits_fresh_arena(&[], members);
        };
        let fits = set.make_room(members);
        let after = EntryCharge::for_entry(key, entry);

        if after != before {
            self.apply_entry_charge(before, after, key);
        }

        fits
    }

    pub fn sorted_set_add(
        &mut self,
        key: &[u8],
        score: f64,
        member: &[u8],
    ) -> Result<bool, SortedSetError> {
        self.sorted_set_add_pairs(key, &[(score, member)])
            .map(|added| added == 1)
    }

    pub fn sorted_set_add_pairs<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        pairs: &[(f64, T)],
    ) -> Result<usize, SortedSetError> {
        if pairs.iter().any(|(score, _)| score.is_nan()) {
            return Err(SortedSetError::InvalidScore);
        }

        // Callers check `sorted_set_make_room` first; a `Full` here still leaves the set
        // stored and charged with the members inserted before it.
        self.remove_expired(key);
        let before = self.entry_charge(key);
        let mut added = 0usize;
        let mut index = 0usize;
        let mut failed = None;

        while index < pairs.len() {
            let (score, member) = &pairs[index];

            match self.entries.touch_slab(key, TAG_ZSET_SLAB, |buf| {
                match inplace_sorted_insert(buf, Score(*score), member.as_ref()) {
                    FlatEdit::Done(inserted) => Some(inserted),
                    FlatEdit::NeedRoom | FlatEdit::Promote => None,
                }
            }) {
                SlabTouch::Done(Some(true)) => added += 1,
                SlabTouch::Done(Some(false)) => {}
                SlabTouch::Wrong | SlabTouch::Absent => {
                    if matches!(
                        self.entries.touch_slab(key, TAG_ZSET_SLAB, |_| Some(false)),
                        SlabTouch::Absent
                    ) && self.entries.create_slab(key, TAG_ZSET_SLAB)
                    {
                        continue;
                    }

                    let edited = self.edit_sorted(key, true, |set| {
                        let mut added = 0;

                        for (score, member) in &pairs[index..] {
                            match set.insert(*score, member.as_ref()) {
                                Ok(true) => added += 1,
                                Ok(false) => {}
                                Err(error) => return Ok((added, Some(error.into()))),
                            }
                        }

                        Ok::<_, std::convert::Infallible>((added, None))
                    });

                    match edited {
                        Ok(Some(Ok((more, error)))) => {
                            added += more;
                            failed = error;
                        }
                        Ok(None) => {}
                        Err(WrongType) => failed = Some(SortedSetError::WrongType),
                    }

                    index = pairs.len();

                    continue;
                }

                SlabTouch::Done(None) => {
                    let extra = score_encoding_len(Score(*score)) + 2 + member.as_ref().len();
                    let min_len = match self.entries.lookup(key) {
                        Some(IndexRef::SortedSlab(bytes)) => sorted_used_len(bytes) + extra,
                        _ => SLAB_SLOT + 1,
                    };

                    if self.entries.enlarge_slab(key, min_len) {
                        continue;
                    }

                    let deadline = self.deadline_id(key);
                    let bytes = self.entries.take_slab(key).unwrap_or_default();
                    let mut set = SortedSet::from_flat(&bytes);

                    // The slab is out of the index now, so an error still puts the set back.
                    for (score, member) in &pairs[index..] {
                        match set.insert(*score, member.as_ref()) {
                            Ok(true) => added += 1,
                            Ok(false) => {}
                            Err(error) => {
                                failed = Some(error.into());
                                break;
                            }
                        }
                    }

                    if set.card() > 0 {
                        self.insert_entry(key, Entry::fresh(Value::SortedSet(Box::new(set))));
                        self.restore_deadline(key, deadline);
                    }

                    index = pairs.len();

                    continue;
                }
            }

            index += 1;
        }

        self.reconcile_entry_charge(before, key);

        match failed {
            Some(error) => Err(error),
            None => Ok(added),
        }
    }

    pub fn sorted_set_remove<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        members: &[T],
    ) -> Result<usize, WrongType> {
        if self.remove_expired(key) {
            return Ok(0);
        }

        self.remove_live_sorted_members(key, members)
    }

    /// The first `count` entries of the sorted set at `key` from the low end (the high end
    /// when `max`), in that order: what a pop of `count` takes. One index lookup; nothing
    /// is collected except for a pop of several from the high end of a small set.
    pub fn sorted_set_visit_ends(
        &self,
        key: &[u8],
        count: usize,
        max: bool,
        visit: impl FnMut(&[u8], f64),
    ) -> Result<(), WrongType> {
        if let Some(set) = self.live_sorted_set(key)? {
            set.visit_ends(count, max, visit);
        }

        Ok(())
    }

    /// [`Self::sorted_set_remove`] for members a pop just read with
    /// [`Self::sorted_set_visit_ends`]. A set stored as an object is found with one index
    /// lookup and charged from the entry in hand; emptying it or shrinking it into a slab
    /// record takes the same steps as `sorted_set_remove`. A set in a slab record takes
    /// the general path.
    pub fn sorted_set_remove_popped<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        members: &[T],
    ) -> Result<usize, WrongType> {
        if self.remove_expired(key) {
            return Ok(0);
        }

        let Some(entry) = self.entries.object_mut(key) else {
            return self.remove_live_sorted_members(key, members);
        };
        let before = EntryCharge::for_entry(key, entry);
        let Value::SortedSet(set) = &mut entry.value else {
            return Err(WrongType);
        };
        let removed = members
            .iter()
            .filter(|member| set.remove(member.as_ref()))
            .count();

        if set.card() == 0 {
            self.remove_emptied(key);
            self.apply_entry_charge(before, EntryCharge::default(), key);
        } else if set.slab_bytes().is_some() {
            self.entries.demote_collection(key);
            self.reconcile_entry_charge(before, key);
        } else {
            let after = EntryCharge::for_entry(key, entry);

            self.apply_entry_charge(before, after, key);
        }

        Ok(removed)
    }

    fn remove_live_sorted_members<T: AsRef<[u8]>>(
        &mut self,
        key: &[u8],
        members: &[T],
    ) -> Result<usize, WrongType> {
        let before = self.entry_charge(key);
        let removed = self
            .edit_sorted(key, false, |set| {
                Ok::<usize, WrongType>(
                    members
                        .iter()
                        .filter(|member| set.remove(member.as_ref()))
                        .count(),
                )
            })?
            .unwrap_or(Ok(0))?;

        self.reconcile_entry_charge(before, key);
        Ok(removed)
    }

    pub fn sorted_set_increment(
        &mut self,
        key: &[u8],
        member: &[u8],
        delta: f64,
    ) -> Result<f64, SortedSetError> {
        if delta.is_nan() {
            return Err(SortedSetError::InvalidIncrement);
        }

        self.remove_expired(key);
        let before = self.entry_charge(key);
        let score = self
            .edit_sorted(key, true, |set| {
                set.increment(member, delta).map_err(|error| match error {
                    InsertError::Full => SortedSetError::Full,
                    InsertError::InvalidScore => SortedSetError::InvalidIncrement,
                })
            })
            .map_err(|_| SortedSetError::WrongType)?
            .unwrap_or(Err(SortedSetError::WrongType));

        self.reconcile_entry_charge(before, key);
        score
    }

    pub fn sorted_set_range(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
    ) -> Result<Vec<&[u8]>, WrongType> {
        Ok(self.live_sorted_set(key)?.map_or_else(Vec::new, |set| {
            set.range_entries(start, stop, false)
                .into_iter()
                .map(|(member, _)| member)
                .collect()
        }))
    }

    pub fn sorted_set_range_entries(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
        reverse: bool,
    ) -> Result<Vec<SortedMemberScore<'_>>, WrongType> {
        Ok(self
            .live_sorted_set(key)?
            .map_or_else(Vec::new, |set| set.range_entries(start, stop, reverse)))
    }

    pub fn sorted_set_range_by_score(
        &self,
        key: &[u8],
        min: crate::protocol::ScoreBound,
        max: crate::protocol::ScoreBound,
    ) -> Result<Vec<SortedMemberScore<'_>>, WrongType> {
        Ok(self
            .live_sorted_set(key)?
            .map_or_else(Vec::new, |set| set.range_by_score(min, max)))
    }

    pub fn sorted_set_range_by_lex(
        &self,
        key: &[u8],
        min: &crate::protocol::LexBound<'_>,
        max: &crate::protocol::LexBound<'_>,
    ) -> Result<Vec<SortedMemberScore<'_>>, WrongType> {
        Ok(self
            .live_sorted_set(key)?
            .map_or_else(Vec::new, |set| set.range_by_lex(min, max)))
    }

    pub fn sorted_set_visit_scores(
        &self,
        key: &[u8],
        min: crate::protocol::ScoreBound,
        max: crate::protocol::ScoreBound,
        mut visit: impl FnMut(&[u8], f64) -> bool,
    ) -> Result<(), WrongType> {
        if let Some(set) = self.live_sorted_set(key)? {
            set.visit_scores(min, max, &mut visit);
        }

        Ok(())
    }

    /// One ZSCAN page. An indexed set is walked by lookup bucket, so removing or
    /// rescoring other members between calls cannot make the scan skip one; a flat set
    /// comes back whole with cursor 0, as in Redis.
    pub fn sorted_set_scan(
        &self,
        key: &[u8],
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> Result<(u64, Vec<SortedMemberScore<'_>>), WrongType> {
        let Some(set) = self.live_sorted_set(key)? else {
            return Ok((0, Vec::new()));
        };

        let matches = |member: &[u8]| {
            pattern.is_none_or(|pattern| crate::protocol::glob_match(pattern, member))
        };
        let mut page = Vec::new();

        if let SortedView::Value(value) = &set
            && let SortedBody::Indexed(index) = &value.body
        {
            let next = index.scan_buckets(cursor, count.max(1), |member, score| {
                if matches(member) {
                    page.push((member, score));
                }
            });

            return Ok((next, page));
        }

        for entry in set.range_entries(0, -1, false) {
            if matches(entry.0) {
                page.push(entry);
            }
        }

        Ok((0, page))
    }

    /// `ZRANDMEMBER`: a positive `count` is distinct members, a negative one allows repeats.
    /// Members are drawn by rank, so the cost follows `count`, not the size of the set.
    pub(crate) fn sorted_set_random_entries(
        &self,
        key: &[u8],
        count: Option<i64>,
    ) -> Result<Vec<(Vec<u8>, f64)>, WrongType> {
        let Some(set) = self.live_sorted_set(key)? else {
            return Ok(Vec::new());
        };
        let card = set.card();
        let take = match count {
            None => 1,
            Some(0) => return Ok(Vec::new()),
            Some(count) => usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX),
        };
        let at = |rank: usize| {
            set.range_entries(rank as isize, rank as isize, false)
                .into_iter()
                .next()
                .map(|(member, score)| (member.to_vec(), score))
        };

        if card == 0 {
            return Ok(Vec::new());
        }

        if count.is_some_and(|count| count < 0) || count.is_none() {
            return Ok((0..take).filter_map(|_| at(random_below(card))).collect());
        }

        let ranks = distinct_positions(card, take);

        if ranks.len().saturating_mul(2) >= card {
            let all = set.range_entries(0, -1, false);

            return Ok(ranks
                .into_iter()
                .map(|rank| (all[rank].0.to_vec(), all[rank].1))
                .collect());
        }

        Ok(ranks.into_iter().filter_map(at).collect())
    }

    pub fn sorted_set_card(&self, key: &[u8]) -> Result<usize, WrongType> {
        Ok(self.live_sorted_set(key)?.map_or(0, |set| set.card()))
    }

    pub fn sorted_set_score(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>, WrongType> {
        Ok(self.live_sorted_set(key)?.and_then(|set| set.score(member)))
    }

    pub fn sorted_set_rank(
        &self,
        key: &[u8],
        member: &[u8],
        reverse: bool,
    ) -> Result<Option<usize>, WrongType> {
        Ok(self
            .live_sorted_set(key)?
            .and_then(|set| set.rank(member, reverse)))
    }

    pub(super) fn edit_sorted<R, E>(
        &mut self,
        key: &[u8],
        create: bool,
        edit: impl FnOnce(&mut SortedSet) -> Result<R, E>,
    ) -> Result<Option<Result<R, E>>, WrongType> {
        // A slab record becomes an object for the edit and goes back afterwards when it fits.
        self.entries.promote_collection(key);

        if let Some(IndexRef::Object(_)) = self.entries.lookup(key) {
            let Some(entry) = self.entries.object_mut(key) else {
                return Ok(None);
            };

            let Value::SortedSet(set) = &mut entry.value else {
                return Err(WrongType);
            };

            let result = edit(set);

            if set.card() == 0 {
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

                SortedSet::default()
            }

            Some(_) => return Err(WrongType),
        };

        let result = edit(&mut set);

        if set.card() == 0 {
            self.remove_emptied(key);
        } else {
            self.insert_entry(key, Entry::fresh(Value::SortedSet(Box::new(set))));
        }

        Ok(Some(result))
    }

    pub(super) fn live_sorted_set(&self, key: &[u8]) -> Result<Option<SortedView<'_>>, WrongType> {
        self.live_view(
            key,
            |value| match value {
                Value::SortedSet(set) => Some(SortedView::Value(set)),
                _ => None,
            },
            |record| match record {
                IndexRef::SortedSlab(bytes) => Some(SortedView::Bytes(bytes)),
                _ => None,
            },
        )
    }
}

#[cfg(test)]
mod indexed_tests {
    use super::*;
    use crate::protocol::ScoreBound;

    /// An indexed set against a sorted model through many updates, removals and arena
    /// compactions: ranks, rank ranges both ways, score ranges and demotion.
    #[test]
    fn indexed_set_matches_sorted_model() {
        let mut set = SortedSet::default();
        let mut model: Vec<(f64, Vec<u8>)> = Vec::new();
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut step = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for round in 0..40_000u32 {
            let id = step() % 6_000;
            let member = format!("member-{id:05}-{}", "x".repeat((id % 7) as usize)).into_bytes();
            let at = model.iter().position(|(_, m)| *m == member);

            if round % 3 == 2 {
                assert_eq!(set.remove(&member), at.is_some());

                if let Some(at) = at {
                    model.remove(at);
                }
            } else {
                let score = (step() % 40) as f64 - 20.0;

                assert_eq!(set.insert(score, &member), Ok(at.is_none()));

                if let Some(at) = at {
                    model.remove(at);
                }

                model.push((score, member));
            }
        }

        model.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        assert!(!set.is_flat());
        assert_eq!(set.card(), model.len());

        for (rank, (score, member)) in model.iter().enumerate().step_by(37) {
            assert_eq!(set.rank(member, false), Some(rank));
            assert_eq!(set.rank(member, true), Some(model.len() - 1 - rank));
            assert_eq!(set.score(member), Some(*score));
        }

        let all: Vec<_> = model.iter().map(|(s, m)| (m.as_slice(), *s)).collect();

        assert_eq!(set.range_entries(0, -1, false), all);
        assert_eq!(set.range_entries(100, 140, false), all[100..=140].to_vec());

        let mut reversed = all.clone();

        reversed.reverse();
        assert_eq!(set.range_entries(5, 60, true), reversed[5..=60].to_vec());

        let by_score: Vec<_> = all
            .iter()
            .copied()
            .filter(|(_, s)| *s > -3.0 && *s <= 4.0)
            .collect();

        assert_eq!(
            set.range_by_score(ScoreBound::Exclusive(-3.0), ScoreBound::Inclusive(4.0)),
            by_score
        );

        while set.card() > COLLECTION_FLAT_LIMIT / 2 {
            let member = set.range_entries(0, 0, false)[0].0.to_vec();

            assert!(set.remove(&member));
            model.remove(0);
        }

        assert!(set.is_flat());

        let flat: Vec<_> = model.iter().map(|(s, m)| (m.as_slice(), *s)).collect();

        assert_eq!(set.range_entries(0, -1, false), flat);
    }
}
