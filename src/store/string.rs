use super::*;

/// Path edits up to this value size cannot newly make a large compact document tree-worthy.
pub(crate) const SMALL_JSON_EDIT: usize = 4096;

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Insert or replace a value, returning the previous value when present.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<Box<[u8]>> {
        self.remove_expired(key);
        let before = self.entry_charge(key);

        self.clear_deadline(self.active, key);
        let created = self.alloc_bytes(value);
        let previous = match self.replace_with_bytes(key, created) {
            Some(Value::Bytes(value)) => Some(value.take_boxed(&mut self.value_log)),
            Some(other) => {
                self.discard_value(other, before);
                None
            }
            None => None,
        };

        self.reconcile_entry_charge(before, key);
        self.note_key(key, crate::notify::KeyEvent::Set);
        previous
    }

    /// `set_bytes` for each pair. Each window of pairs is hashed and its index slots
    /// prefetched first, so the cache misses of different keys overlap instead of
    /// following one another, and the hash is reused by the write that follows.
    pub fn set_many_bytes<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, pairs: &[(K, V)]) {
        const WINDOW: usize = 16;

        for window in pairs.chunks(WINDOW) {
            let mut hints = [0u64; WINDOW];

            for ((key, _), hint) in window.iter().zip(&mut hints) {
                *hint = self.entries.prefetch(key.as_ref());
            }

            // These writes only take deadlines away, so with none at the start of the
            // window there is nothing to clear for any key in it.
            let deadlines = self.expiration.keys_with_ttl != 0;

            for ((key, value), hint) in window.iter().zip(hints) {
                let key = key.as_ref();

                if deadlines {
                    self.clear_deadline(self.active, key);
                }

                // The hint stays valid across the writes before it: it is the key's
                // hash under the index's fixed hasher, whatever moves in the table.
                self.write_bytes_hinted::<true>(key, value.as_ref(), hint);
            }
        }
    }

    /// Set bytes without materializing the previous value.
    pub fn set_bytes(&mut self, key: &[u8], value: &[u8]) {
        self.clear_deadline(self.active, key);
        self.write_bytes(key, value);
    }

    /// `set_bytes` followed by a deadline (SETEX, `SET … EX`). A deadline the key already
    /// has is re-timed in place: removing and adding it again would leave a tombstone in
    /// the deadline lookup on every overwrite and force regular rehashes of the table.
    pub fn set_bytes_expiring(&mut self, key: &[u8], value: &[u8], expires_at: UnixMillis) {
        match self.deadline_id(key) {
            Some(id) => {
                self.write_bytes(key, value);
                // The write may have moved the key record.
                self.point_deadline(key, id);
            }
            None => self.set_bytes(key, value),
        }

        self.set_deadline(self.active, key, expires_at);
    }

    /// `set_bytes` after the caller dealt with the key's deadline.
    #[inline]
    fn write_bytes(&mut self, key: &[u8], value: &[u8]) {
        self.write_bytes_hinted::<false>(key, value, 0);
    }

    /// `write_bytes`; with `HINTED`, `hint` is the index's `lookup_hint` of `key` and
    /// saves hashing it again. Without, `hint` is unused and the code is the plain write.
    fn write_bytes_hinted<const HINTED: bool>(&mut self, key: &[u8], value: &[u8], hint: u64) {
        if self.overwrite_log::<HINTED>(key, value, hint) || self.overwrite_heap(key, value) {
            self.finish_set(key);

            return;
        }

        // The deadline is gone or kept on purpose, so one raw lookup is the live view.
        let (before, body, exists) = match self.entries.get_key_value(key) {
            Some((stored_key, hit)) => (charge_of(stored_key, hit), bytes_body_of(hit), true),
            None => (EntryCharge::default(), None, false),
        };

        let previous = if let Some(body) = body {
            let body = self.value_log.rewrite(&body, value);
            let entry = Entry::from_packed_bytes(match body {
                ByteBody::Log { offset, len } => PackedBytes::Log { offset, len },
                ByteBody::Integer(value) => PackedBytes::Integer(value),
                ByteBody::Heap(data) => PackedBytes::Heap(*data),
            });

            let _ = self.entries.insert(key, entry);
            None
        } else if exists {
            let created = self.alloc_bytes(value);

            self.replace_with_bytes(key, created)
        } else {
            let created = self.alloc_bytes(value);

            self.insert_entry(key, Entry::fresh(Value::Bytes(created)));
            None
        };

        if let Some(previous) = previous {
            self.discard_value(previous, before);
        }

        self.reconcile_entry_charge(before, key);
        self.note_key(key, crate::notify::KeyEvent::Set);

        if self.change_log.is_some() {
            self.note_change(format!("SET {}", String::from_utf8_lossy(key)));
        }
    }

    /// Writes `patch` at byte `offset` of the string under `key`, zero-padding a shorter or
    /// missing value first (`SETRANGE` semantics), and keeps its TTL. A heap value, or a log
    /// span that already covers the patch, is changed in place; nothing else is copied.
    /// Returns the new length.
    pub fn set_range(
        &mut self,
        key: &[u8],
        offset: usize,
        patch: &[u8],
    ) -> Result<usize, WrongType> {
        enum Place {
            Log { at: u32, len: usize },
            Heap,
            Other,
        }

        let end = offset + patch.len();
        let place = match self.live(key) {
            Some(IndexRef::Log { offset: at, len }) if !is_integer(len) => Place::Log {
                at,
                len: usize::from(plain_len(len)),
            },
            Some(IndexRef::Heap(_)) => Place::Heap,
            Some(IndexRef::SetSlab(_) | IndexRef::SortedSlab(_) | IndexRef::CollSlab(_)) => {
                return Err(WrongType);
            }
            _ => Place::Other,
        };

        match place {
            Place::Log { at, len } if end <= len => {
                self.value_log.write_at(at + offset as u32, patch);
                self.finish_set(key);

                return Ok(len);
            }
            Place::Heap => {
                let (capacity, new_capacity, len) = {
                    let buf = self.entries.heap_mut(key).expect("live heap entry");
                    let capacity = buf.capacity();

                    if end > buf.len() {
                        buf.resize(end, 0);
                    }

                    buf[offset..end].copy_from_slice(patch);
                    (capacity, buf.capacity(), buf.len())
                };

                // Charged by capacity, from the values in hand: no second lookup.
                if new_capacity != capacity {
                    self.shift_value_bytes(capacity, new_capacity);
                }

                self.finish_set(key);

                return Ok(len);
            }
            _ => {}
        }

        let mut bytes = self
            .value_view(key)?
            .map(ByteView::to_vec)
            .unwrap_or_default();

        if end > bytes.len() {
            bytes.resize(end, 0);
        }

        bytes[offset..end].copy_from_slice(patch);

        let deadline = self.expires_at(key);

        self.set_bytes(key, &bytes);

        if let Some(at) = deadline {
            self.expire_at(key, at);
        }

        Ok(bytes.len())
    }

    /// Where a plain string lives in the value log: `(address, length)`. `None` when the key
    /// is missing, expired, not a string or stored another way. One lookup; the caller
    /// then reads and writes by address with [`Self::write_log_range`].
    pub(crate) fn log_string_loc(&self, key: &[u8]) -> Option<(u32, usize)> {
        match self.live(key) {
            Some(IndexRef::Log { offset, len }) if !is_integer(len) => {
                Some((offset, usize::from(plain_len(len))))
            }
            _ => None,
        }
    }

    pub(crate) fn log_bytes(&self, at: u32, len: usize) -> &[u8] {
        self.value_log.slice(at, len as u16)
    }

    /// With no memory limit, admission checks and snapshot capture never move stored values,
    /// so an address from [`Self::log_string_loc`] stays valid across `before_mutation`.
    pub(crate) fn addresses_survive_admission(&self) -> bool {
        self.memory_limit.is_none()
    }

    /// Overwrites bytes of a log string found by [`Self::log_string_loc`], keeping its TTL,
    /// and does the bookkeeping of a `SET`. The patch must end inside the string.
    pub(crate) fn write_log_range(&mut self, key: &[u8], at: u32, offset: usize, patch: &[u8]) {
        self.value_log.write_at(at + offset as u32, patch);
        self.finish_set(key);
    }

    /// Overwrites bytes inside the existing string without logging, notifying or touching
    /// WATCH: for derived data kept in the value (the HyperLogLog count cache) that changes
    /// nothing a reader can observe. Does nothing when the patch does not fit in place.
    pub(crate) fn patch_in_place_quietly(&mut self, key: &[u8], offset: usize, patch: &[u8]) {
        let end = offset + patch.len();

        match self.live(key) {
            Some(IndexRef::Log { offset: at, len })
                if !is_integer(len) && end <= usize::from(plain_len(len)) =>
            {
                self.value_log.write_at(at + offset as u32, patch);
            }
            Some(IndexRef::Heap(bytes)) if end <= bytes.len() => {
                if let Some(buf) = self.entries.heap_mut(key) {
                    buf[offset..end].copy_from_slice(patch);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn json_view(&self, key: &[u8]) -> Result<Option<&[u8]>, WrongType> {
        match self.live(key) {
            None => Ok(None),
            Some(IndexRef::Object(entry)) => match &entry.value {
                Value::Json(value) => Ok(Some(value.as_slice())),
                _ => Err(WrongType),
            },
            Some(_) => Err(WrongType),
        }
    }

    pub(crate) fn json_tree(&self, key: &[u8]) -> Option<&crate::json::TreeDocument> {
        match self.live(key) {
            Some(IndexRef::Object(entry)) => match &entry.value {
                Value::JsonTree(value) => Some(value),
                _ => None,
            },
            _ => None,
        }
    }

    pub(crate) fn apply_json_tree_edit(&mut self, key: &[u8], edit: crate::json::TreeEdit) {
        let before = self.entry_charge(key);
        let Some(entry) = self.entries.object_mut(key) else {
            unreachable!("validated JSON tree")
        };
        let Value::JsonTree(tree) = &mut entry.value else {
            unreachable!("validated JSON tree")
        };
        tree.apply(edit);
        if tree.len() < crate::json::TREE_THRESHOLD || tree.charge() > tree.len().saturating_mul(2)
        {
            entry.value = Value::Json(Box::new(tree.to_vec()));
        }
        self.reconcile_entry_charge(before, key);
        if self.json_indexes.is_some() {
            self.json_index_refresh(key);
        }
        self.note_key(key, crate::notify::KeyEvent::Set);
        if self.change_log.is_some() {
            self.note_change(format!("JSON.SET {}", String::from_utf8_lossy(key)));
        }
    }

    pub(crate) fn apply_json_delta(
        &mut self,
        key: &[u8],
        path: &[u8],
        value: Option<&[u8]>,
        merge: bool,
    ) -> Result<(), crate::json::JsonError> {
        self.remove_expired(key);
        if let Some(tree) = self.json_tree(key) {
            let edit = tree
                .prepare(path, value, merge)?
                .ok_or(crate::json::JsonError::Path)?;
            if edit.len() > crate::protocol::DEFAULT_MAX_VALUE_BYTES {
                return Err(crate::json::JsonError::Syntax);
            }
            self.apply_json_tree_edit(key, edit);
            return Ok(());
        }
        let current = self
            .json_view(key)
            .map_err(|_| crate::json::JsonError::Syntax)?
            .ok_or(crate::json::JsonError::MissingRoot)?;
        let updated = match value {
            Some(value) if merge => crate::json::merge(Some(current), path, value)?,
            Some(value) => crate::json::set(Some(current), path, value)?,
            None => crate::json::del(current, path)?,
        }
        .ok_or(crate::json::JsonError::Path)?;
        if updated.len() > crate::protocol::DEFAULT_MAX_VALUE_BYTES {
            return Err(crate::json::JsonError::Syntax);
        }
        let small_edit = value.is_none_or(|value| value.len() <= SMALL_JSON_EDIT);
        self.put_json_edit(key, updated, small_edit)
            .map_err(|_| crate::json::JsonError::Syntax)
    }

    /// True when `key` holds a compact document of tree size: it was already judged not worth
    /// a tree, so a small path edit need not repeat that judgement (a full parse).
    pub(crate) fn json_is_large_compact(&self, key: &[u8]) -> bool {
        matches!(self.json_view(key), Ok(Some(document)) if document.len() >= crate::json::TREE_THRESHOLD)
            && self.json_tree(key).is_none()
    }

    pub(crate) fn put_json(
        &mut self,
        key: &[u8],
        value: impl Into<Vec<u8>>,
    ) -> Result<(), WrongType> {
        self.put_json_with(key, value, true, None)
    }

    /// Stores the result of a small path edit. A large compact document stays compact
    /// without re-running the tree eligibility check on every write.
    pub(crate) fn put_json_edit(
        &mut self,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        small_edit: bool,
    ) -> Result<(), WrongType> {
        self.put_json_edit_with(key, value, small_edit, None)
    }

    /// Like `put_json_edit`, reusing the tree eligibility the caller already decided for `value`
    /// (`Some(None)` = not worth a tree) instead of parsing the document a second time.
    pub(crate) fn put_json_edit_with(
        &mut self,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        small_edit: bool,
        prebuilt: Option<Option<crate::json::TreeDocument>>,
    ) -> Result<(), WrongType> {
        let keep_compact = small_edit && self.json_is_large_compact(key);
        self.put_json_with(key, value, !keep_compact, prebuilt)
    }

    fn put_json_with(
        &mut self,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        evaluate_tree: bool,
        prebuilt: Option<Option<crate::json::TreeDocument>>,
    ) -> Result<(), WrongType> {
        self.remove_expired(key);
        if self
            .value_kind(key)
            .is_some_and(|kind| kind != ValueKind::Json)
        {
            return Err(WrongType);
        }

        let before = self.entry_charge(key);
        let mut document: Vec<u8> = value.into();
        let tree = if evaluate_tree {
            match prebuilt {
                Some(evaluated) => evaluated,
                None => crate::json::TreeDocument::from_bytes(&document),
            }
        } else {
            None
        };
        // A document is charged its buffer: the encoder's spare capacity (128 bytes at
        // least) would cost memory and differ from the exact buffer a replica decodes.
        if tree.is_none() && document.capacity() > document.len() {
            document.shrink_to_fit();
        }
        if let Some(entry) = self.entries.object_mut(key) {
            let replaced = if let Some(tree) = tree {
                Some(std::mem::replace(
                    &mut entry.value,
                    Value::JsonTree(Box::new(tree)),
                ))
            } else if let Value::Json(stored) = &mut entry.value {
                // The old document's buffer goes off the shard thread too when large.
                let old = std::mem::replace(&mut **stored, document);
                let bytes = old.capacity();

                crate::lazy_free::defer_drop(old, bytes);
                None
            } else {
                Some(std::mem::replace(
                    &mut entry.value,
                    Value::Json(Box::new(document)),
                ))
            };

            // A replaced tree is freed off the shard thread, like any large value.
            if let Some(replaced) = replaced {
                self.discard_value(replaced, before);
            }
        } else {
            let stored = match tree {
                Some(tree) => Value::JsonTree(Box::new(tree)),
                None => Value::Json(Box::new(document)),
            };
            self.insert_entry(key, Entry::fresh(stored));
        }
        self.reconcile_entry_charge(before, key);
        if self.json_indexes.is_some() {
            self.json_index_refresh(key);
        }
        self.note_key(key, crate::notify::KeyEvent::Set);
        if self.change_log.is_some() {
            self.note_change(format!("JSON.SET {}", String::from_utf8_lossy(key)));
        }
        Ok(())
    }

    /// Same-size and smaller log strings stay in the existing span. A larger
    /// short string takes a new span and the key record keeps its place.
    pub(super) fn overwrite_log<const HINTED: bool>(
        &mut self,
        key: &[u8],
        value: &[u8],
        hint: u64,
    ) -> bool {
        if value.len() > LOG_VALUE_MAX {
            return false;
        }

        let found = if HINTED {
            self.entries.lookup_hinted(key, hint)
        } else {
            self.entries.lookup(key)
        };
        let Some(IndexRef::Log { offset, len }) = found else {
            return false;
        };

        if is_integer(len) {
            return false;
        }

        let old = usize::from(plain_len(len));

        if value.len() <= old {
            self.value_log.write_at(offset, value);

            if value.len() < old {
                self.value_log
                    .free(offset + value.len() as u32, (old - value.len()) as u16);
                self.entries.retarget_log(key, offset, value.len() as u16);
                self.shift_value_bytes(old, value.len());
            }

            return true;
        }

        self.value_log.release_span(offset, len);
        let (new_offset, new_len) = self.value_log.alloc(value);

        self.entries.retarget_log(key, new_offset, new_len);
        self.shift_value_bytes(old, value.len());
        true
    }

    /// A long value reuses the heap buffer when it already has room.
    pub(super) fn overwrite_heap(&mut self, key: &[u8], value: &[u8]) -> bool {
        if value.len() <= LOG_VALUE_MAX {
            return false;
        }

        let Some(buf) = self.entries.heap_mut(key) else {
            return false;
        };

        // A heap string is charged its buffer's capacity: reusing the buffer changes
        // nothing, a new one moves the charge.
        if buf.capacity() >= value.len() {
            buf.clear();
            buf.extend_from_slice(value);

            return true;
        }

        let old = buf.capacity();

        *buf = self.byte_pool.acquire(value, value.len());

        let new = buf.capacity();

        self.shift_value_bytes(old, new);
        true
    }

    pub(super) fn shift_value_bytes(&mut self, before: usize, after: usize) {
        self.memory_usage.value_bytes = self
            .memory_usage
            .value_bytes
            .saturating_sub(before)
            .saturating_add(after);

        if after > before {
            self.memory_stats.allocation_events =
                self.memory_stats.allocation_events.saturating_add(1);
            self.memory_stats.allocation_bytes = self
                .memory_stats
                .allocation_bytes
                .saturating_add((after - before) as u64);
        }
    }

    pub(super) fn finish_set(&mut self, key: &[u8]) {
        self.note_mutation(key);
        self.note_write(key);
        self.refresh_global_memory_usage();
        self.note_key(key, crate::notify::KeyEvent::Set);

        if self.change_log.is_some() {
            self.note_change(format!("SET {}", String::from_utf8_lossy(key)));
        }
    }

    pub(crate) fn lease_plan(
        &mut self,
        key: &[u8],
        ttl_ms: u64,
    ) -> Result<Option<(u64, u64)>, WrongType> {
        self.remove_expired(key);

        let now = self.now_unix_millis().get();
        let expires_at = now.saturating_add(ttl_ms).min(UnixMillis::MAX_DEADLINE);
        let kind = self.value_kind(key);

        if kind.is_some_and(|kind| kind != ValueKind::Lease) {
            return Err(WrongType);
        }

        if kind.is_none() {
            return Ok(Some((self.next_fence_token(0), expires_at)));
        }

        let Some(Value::Lease(lease)) = self.object_value(key).ok().flatten() else {
            return Err(WrongType);
        };

        if now < lease.expires_at {
            return Ok(None);
        }

        Ok(Some((self.next_fence_token(lease.token), expires_at)))
    }

    pub(crate) fn lease_release_plan(
        &mut self,
        key: &[u8],
        token: u64,
    ) -> Result<Option<u64>, WrongType> {
        self.remove_expired(key);

        let now = self.now_unix_millis().get();

        match self.object_value(key).ok().flatten() {
            Some(Value::Lease(lease)) if lease.token == token && now < lease.expires_at => {
                Ok(Some(now))
            }

            Some(Value::Lease(_)) | None => Ok(None),
            Some(_) => Err(WrongType),
        }
    }

    /// `Some((kind, new_deadline))` when `token` is the live lease holder or a live semaphore
    /// holder of `key`; `None` when it is not held.
    pub(crate) fn renew_plan(
        &mut self,
        key: &[u8],
        token: u64,
        ttl_ms: u64,
    ) -> Result<Option<(ValueKind, u64)>, WrongType> {
        self.remove_expired(key);

        let now = self.now_unix_millis().get();
        let expires_at = now.saturating_add(ttl_ms).min(UnixMillis::MAX_DEADLINE);

        match self.object_value(key)? {
            Some(Value::Lease(lease)) if lease.token == token && now < lease.expires_at => {
                Ok(Some((ValueKind::Lease, expires_at)))
            }

            Some(Value::Semaphore(semaphore))
                if semaphore
                    .holders()
                    .any(|(held, at)| held == token && at > now) =>
            {
                Ok(Some((ValueKind::Semaphore, expires_at)))
            }

            Some(Value::Lease(_) | Value::Semaphore(_)) | None => Ok(None),
            Some(_) => Err(WrongType),
        }
    }

    pub(crate) fn lease_put(
        &mut self,
        key: &[u8],
        token: u64,
        expires_at: u64,
        verb: &str,
    ) -> Result<(), WrongType> {
        let kind = self.value_kind(key);

        if kind.is_some_and(|kind| kind != ValueKind::Lease) {
            return Err(WrongType);
        }

        let before = self.entry_charge(key);

        self.note_fence_token(token);

        if kind.is_none() {
            self.insert_entry(
                key,
                Entry::fresh(Value::Lease(Box::new(LeaseValue { token, expires_at }))),
            );
        } else {
            let Some(Value::Lease(lease)) =
                self.entries.object_mut(key).map(|entry| &mut entry.value)
            else {
                return Err(WrongType);
            };

            lease.token = token;
            lease.expires_at = expires_at;
        }

        self.reconcile_entry_charge(before, key);

        if self.change_log.is_some() {
            self.note_change(format!("{verb} {}", String::from_utf8_lossy(key)));
        }

        Ok(())
    }

    /// `Some((token, expires_at, fresh))` when a holder slot is free; `fresh` means the key
    /// does not exist yet. `None` when `max` holders are live.
    pub(crate) fn semaphore_plan(
        &mut self,
        key: &[u8],
        max: u64,
        ttl_ms: u64,
    ) -> Result<Option<(u64, u64, bool)>, WrongType> {
        self.remove_expired(key);

        let now = self.now_unix_millis().get();
        let expires_at = now.saturating_add(ttl_ms).min(UnixMillis::MAX_DEADLINE);

        match self.object_value(key) {
            Ok(None) => Ok(Some((self.next_fence_token(0), expires_at, true))),
            Ok(Some(Value::Semaphore(semaphore))) => {
                let live = semaphore.holders().filter(|(_, at)| *at > now).count() as u64;
                let after = semaphore.next_token().saturating_sub(1);

                Ok((live < max).then(|| (self.next_fence_token(after), expires_at, false)))
            }

            _ => Err(WrongType),
        }
    }

    /// Adds one holder. Expired holders are dropped first, and a holder that already has `token`
    /// is replaced (a renewal). Replay calls this with the recorded token, so `next_token` only
    /// moves forward.
    pub(crate) fn semaphore_put(
        &mut self,
        key: &[u8],
        token: u64,
        expires_at: u64,
    ) -> Result<(), WrongType> {
        let now = self.now_unix_millis().get();
        let before = self.entry_charge(key);

        self.note_fence_token(token);

        match self.value_kind(key) {
            None => {
                self.insert_entry(
                    key,
                    Entry::fresh(Value::Semaphore(Box::new(SemaphoreValue::new(
                        token.saturating_add(1),
                        [(token, expires_at)],
                    )))),
                );
                self.set_deadline(self.active, key, UnixMillis::new(expires_at));
            }

            Some(ValueKind::Semaphore) => {
                let Some(Value::Semaphore(semaphore)) =
                    self.entries.object_mut(key).map(|entry| &mut entry.value)
                else {
                    return Err(WrongType);
                };

                let latest = semaphore.latest();

                semaphore.rewrite(now, Some(token), Some((token, expires_at)));

                if latest.is_none_or(|latest| expires_at > latest) {
                    self.set_deadline(self.active, key, UnixMillis::new(expires_at));
                }
            }

            Some(_) => return Err(WrongType),
        }

        self.reconcile_entry_charge(before, key);

        if self.change_log.is_some() {
            self.note_change(format!("SEMAPHORE {}", String::from_utf8_lossy(key)));
        }

        Ok(())
    }

    /// True when `token` is a live holder of the semaphore at `key`.
    pub(crate) fn semaphore_holds(&mut self, key: &[u8], token: u64) -> bool {
        self.remove_expired(key);

        let now = self.now_unix_millis().get();

        matches!(
            self.unexpired_object_value(key),
            Ok(Some(Value::Semaphore(semaphore)))
                if semaphore.holders().any(|(held, at)| held == token && at > now)
        )
    }

    /// Removes the holder with `token`. The last holder takes the key with it.
    pub(crate) fn semaphore_release(&mut self, key: &[u8], token: u64) -> Result<(), WrongType> {
        let now = self.now_unix_millis().get();
        let before = self.entry_charge(key);
        let Some(Value::Semaphore(semaphore)) =
            self.entries.object_mut(key).map(|entry| &mut entry.value)
        else {
            return Err(WrongType);
        };

        semaphore.rewrite(now, Some(token), None);

        if semaphore.len() == 0 {
            self.remove_emptied(key);
        }

        self.reconcile_entry_charge(before, key);

        if self.change_log.is_some() {
            self.note_change(format!("RELEASE {}", String::from_utf8_lossy(key)));
        }

        Ok(())
    }

    pub(crate) fn lease_ttl_ms(&self, key: &[u8]) -> Option<i64> {
        let Value::Lease(lease) = self.object_value(key).ok().flatten()? else {
            return None;
        };

        let now = self.now_unix_millis().get();

        if now >= lease.expires_at {
            return Some(-1);
        }

        Some(i64::try_from(lease.expires_at - now).unwrap_or(i64::MAX))
    }

    pub(crate) fn plan_limit(
        &mut self,
        key: &[u8],
        max: u64,
        window_ms: u64,
    ) -> Result<LimitDecision, LimitError> {
        self.remove_expired(key);

        let now = self.now_unix_millis().get();
        let kind = self.value_kind(key);

        if kind.is_some_and(|kind| kind != ValueKind::Limit) {
            return Err(LimitError::WrongType);
        }

        if kind.is_none() {
            return Ok(LimitDecision::Allow {
                used: 1,
                window_end: Some(now.saturating_add(window_ms)),
                remaining: max.saturating_sub(1),
                fresh: true,
            });
        }

        let used = self.limit_state(key)?;

        if used >= max {
            return Ok(LimitDecision::Reject);
        }

        let used = used.saturating_add(1);

        Ok(LimitDecision::Allow {
            used,
            window_end: None,
            remaining: max.saturating_sub(used),
            fresh: false,
        })
    }

    /// Store the count. `Ok(true)` when this call created the key. The window end is set by
    /// the caller through `expire_at`, so an in-window increment leaves the deadline index alone.
    pub(crate) fn put_limit(&mut self, key: &[u8], used: u64) -> Result<bool, WrongType> {
        let kind = self.value_kind(key);

        if kind.is_some_and(|kind| kind != ValueKind::Limit) {
            return Err(WrongType);
        }

        if kind.is_none() {
            let before = self.entry_charge(key);

            self.insert_entry(
                key,
                Entry::fresh(Value::Limit(Box::new(LimitValue { used }))),
            );
            self.reconcile_entry_charge(before, key);
        } else {
            let updated = self.edit_object(key, |entry| match &mut entry.value {
                Value::Limit(limit) => {
                    limit.used = used;
                    (true, true)
                }
                _ => (false, false),
            });

            if updated != Some(true) {
                return Err(WrongType);
            }
        }

        if self.change_log.is_some() {
            self.note_change(format!("LIMIT {}", String::from_utf8_lossy(key)));
        }

        Ok(kind.is_none())
    }

    /// `Some(expires_at)` when this call owns the key. `None` when it is already taken.
    pub(crate) fn plan_once(&mut self, key: &[u8], ttl_ms: u64) -> Result<Option<u64>, WrongType> {
        self.remove_expired(key);

        let kind = self.value_kind(key);

        if kind.is_some_and(|kind| kind != ValueKind::Once) {
            return Err(WrongType);
        }

        if kind.is_some() {
            return Ok(None);
        }

        Ok(Some(
            self.now_unix_millis()
                .get()
                .saturating_add(ttl_ms)
                .min(UnixMillis::MAX_DEADLINE),
        ))
    }

    pub(crate) fn put_once(&mut self, key: &[u8], expires_at: u64) -> Result<(), WrongType> {
        let kind = self.value_kind(key);

        if kind.is_some_and(|kind| kind != ValueKind::Once) {
            return Err(WrongType);
        }

        let before = self.entry_charge(key);

        if kind.is_none() {
            self.insert_entry(key, Entry::fresh(Value::Once));
        }

        self.set_deadline(self.active, key, UnixMillis::new(expires_at));
        self.reconcile_entry_charge(before, key);

        if self.change_log.is_some() {
            self.note_change(format!("ONCE {}", String::from_utf8_lossy(key)));
        }

        Ok(())
    }

    pub(crate) fn plan_versioned(
        &mut self,
        key: &[u8],
        expected: Option<u64>,
    ) -> Result<u64, SetVersionError> {
        self.remove_expired(key);

        let kind = self.value_kind(key);
        let current = self.version_of(key);

        match (kind, current, expected) {
            (None, _, None) => Ok(1),
            (Some(ValueKind::Bytes), _, None) => Ok(1),
            (Some(ValueKind::Versioned), Some(version), Some(expected)) if version == expected => {
                Ok(version.saturating_add(1))
            }

            (Some(ValueKind::Versioned), _, _) => Err(SetVersionError::Mismatch),
            (None, _, Some(_)) => Err(SetVersionError::Mismatch),
            (Some(_), _, _) => Err(SetVersionError::WrongType),
        }
    }

    pub(crate) fn put_versioned(
        &mut self,
        key: &[u8],
        value: &[u8],
        version: u64,
    ) -> Result<(), SetVersionError> {
        let kind = self.value_kind(key);

        if kind.is_some_and(|kind| kind != ValueKind::Bytes && kind != ValueKind::Versioned) {
            return Err(SetVersionError::WrongType);
        }

        let before = self.entry_charge(key);
        let body = self.alloc_bytes(value);

        self.insert_versioned(key, version, body);
        self.reconcile_entry_charge(before, key);

        if self.change_log.is_some() {
            self.note_change(format!("SETV {} {version}", String::from_utf8_lossy(key)));
        }

        Ok(())
    }

    pub(super) fn version_of(&self, key: &[u8]) -> Option<u64> {
        match self.object_value(key).ok().flatten() {
            Some(Value::Versioned(value)) => Some(value.version),
            _ => None,
        }
    }

    pub(super) fn insert_versioned(&mut self, key: &[u8], version: u64, body: BytesValue) {
        if let Some(entry) = self.remove_entry(key) {
            self.free_entry(key, entry);
        }

        self.insert_entry(
            key,
            Entry::fresh(Value::Versioned(Box::new(VersionedValue { version, body }))),
        );
    }

    pub(super) fn release_entry(&mut self, entry: Entry) {
        if let Ok(body) = entry.value.into_string_body() {
            body.release(&mut self.value_log, &mut self.byte_pool);
        }
    }

    pub(crate) fn changes_start(&mut self) -> u64 {
        if self.change_log.is_none() {
            self.change_log = Some(std::collections::VecDeque::new());
        }

        self.change_next
    }

    pub(crate) fn changes_after(&self, sequence: u64) -> Vec<ChangeNote> {
        self.change_log
            .as_ref()
            .map(|log| {
                log.iter()
                    .filter(|note| note.sequence > sequence)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(super) fn note_change(&mut self, text: String) {
        let Some(log) = self.change_log.as_mut() else {
            return;
        };

        self.change_next = self.change_next.saturating_add(1);
        log.push_back(ChangeNote {
            sequence: self.change_next,
            text,
        });

        while log.len() > 1024 {
            log.pop_front();
        }
    }

    /// Extend a string without copying the prefix. `Ok(None)` leaves the key unchanged
    /// so the caller can build the value (a new key, a counter, or a paired record).
    pub(crate) fn append_in_place(
        &mut self,
        key: &[u8],
        suffix: &[u8],
    ) -> Result<Option<i64>, WrongType> {
        let hit = match self.live(key) {
            None => return Ok(None),
            Some(IndexRef::SetSlab(_) | IndexRef::SortedSlab(_) | IndexRef::CollSlab(_)) => {
                return Err(WrongType);
            }
            Some(IndexRef::Object(entry)) => {
                if !matches!(entry.value, Value::Bytes(_)) {
                    return Err(WrongType);
                }

                return Ok(None);
            }

            Some(IndexRef::Integer(value)) => AppendHit::Integer(value),
            Some(IndexRef::Heap(_)) => AppendHit::Heap,
            Some(IndexRef::Log { offset, len }) => AppendHit::Log { offset, len },
        };

        let length = match hit {
            AppendHit::Heap => self.append_heap(key, suffix),
            AppendHit::Log { offset, len } => self.append_log(key, offset, len, suffix),
            AppendHit::Integer(value) => self.append_integer(key, value, suffix),
        };

        Ok(Some(length))
    }

    pub(super) fn append_integer(&mut self, key: &[u8], value: i64, suffix: &[u8]) -> i64 {
        let mut digits = [0u8; 20];
        let len = format_integer(value, &mut digits);
        let total = len + suffix.len();
        let deadline = self.expires_at(key);

        if total <= LOG_VALUE_MAX {
            let mut scratch = [0u8; LOG_VALUE_MAX];

            scratch[..len].copy_from_slice(&digits[..len]);
            scratch[len..total].copy_from_slice(suffix);
            self.set_bytes(key, &scratch[..total]);
        } else {
            let mut combined = Vec::with_capacity(total);

            combined.extend_from_slice(&digits[..len]);
            combined.extend_from_slice(suffix);
            self.set_bytes(key, &combined);
        }

        if let Some(expires_at) = deadline {
            self.expire_at(key, expires_at);
        }

        total as i64
    }

    pub(super) fn append_heap(&mut self, key: &[u8], suffix: &[u8]) -> i64 {
        let before = self.entry_charge(key);
        let length = {
            let buf = self.entries.heap_mut(key).expect("heap string");

            buf.reserve(suffix.len());
            buf.extend_from_slice(suffix);
            buf.len() as i64
        };

        self.reconcile_entry_charge(before, key);
        self.note_key(key, crate::notify::KeyEvent::Set);

        length
    }

    pub(super) fn append_log(&mut self, key: &[u8], offset: u32, len: u16, suffix: &[u8]) -> i64 {
        let old = usize::from(plain_len(len));
        let new_len = old + suffix.len();
        let before = self.entry_charge(key);

        if new_len > LOG_VALUE_MAX {
            self.promote_appended_log(key, offset, len, suffix);
        } else if suffix.is_empty() || self.value_log.extend_tail(offset, old, suffix) {
            if !suffix.is_empty() {
                self.entries.retarget_log(key, offset, new_len as u16);
            }
        } else {
            let fresh = self.value_log.alloc_tail(new_len);
            let mut buf = [0u8; LOG_VALUE_MAX];

            buf[..old].copy_from_slice(self.value_log.slice(offset, plain_len(len)));
            buf[old..new_len].copy_from_slice(suffix);
            self.value_log.write_at(fresh, &buf[..new_len]);
            self.value_log.release_span(offset, plain_len(len));
            self.entries.retarget_log(key, fresh, new_len as u16);
        }

        self.reconcile_entry_charge(before, key);
        self.note_key(key, crate::notify::KeyEvent::Set);

        new_len as i64
    }

    pub(super) fn promote_appended_log(
        &mut self,
        key: &[u8],
        offset: u32,
        len: u16,
        suffix: &[u8],
    ) {
        let deadline_id = self.deadline_id(key);
        let old = usize::from(plain_len(len));
        let needed = old + suffix.len();
        let mut buf = Vec::with_capacity((needed + 1).next_power_of_two());

        buf.extend_from_slice(self.value_log.slice(offset, plain_len(len)));
        buf.extend_from_slice(suffix);

        if let Some(previous) = self
            .entries
            .insert(key, Entry::from_packed_bytes(PackedBytes::Heap(buf)))
            && let Value::Bytes(value) = previous.value
        {
            value.release(&mut self.value_log, &mut self.byte_pool);
        }

        if let Some(id) = deadline_id {
            self.point_deadline(key, id);
        }
    }

    pub(super) fn deadline_id(&self, key: &[u8]) -> Option<u32> {
        if self.expiration.keys_with_ttl == 0 || self.deadlines.len() <= self.active as usize {
            return None;
        }

        let active = self.active;
        let entries = &self.entries;
        let databases = &self.databases;

        self.deadlines[active as usize].id_of(key, |address| {
            index_key_bytes(entries, databases, active, active, address)
        })
    }

    pub(super) fn point_deadline(&mut self, key: &[u8], id: u32) {
        let Some(address) = self.entries.key_address(key) else {
            return;
        };

        let active = self.active as usize;

        if self.deadlines.len() <= active {
            return;
        }

        let before = self.deadlines[active].charged_bytes();
        self.deadlines[active].note_address(id, address);
        self.update_deadline_index_bytes(active, before);
    }

    /// Stores a string over whatever `key` held. `SET` comes here only when the type
    /// changes, so the JSON index check costs a plain overwrite nothing.
    pub(super) fn replace_with_bytes(&mut self, key: &[u8], created: BytesValue) -> Option<Value> {
        let previous = self
            .entries
            .insert(key, Entry::fresh(Value::Bytes(created)))
            .map(|entry| entry.value);

        if self.json_indexes.is_some()
            && matches!(previous, Some(Value::Json(_) | Value::JsonTree(_)))
        {
            self.json_index_refresh(key);
        }
        previous
    }

    pub(super) fn alloc_bytes(&mut self, value: &[u8]) -> BytesValue {
        if value.len() <= LOG_VALUE_MAX {
            let (offset, len) = self.value_log.alloc(value);

            return BytesValue::logged(offset, len);
        }

        BytesValue::from_heap(self.byte_pool.acquire(value, value.len()))
    }

    pub(super) fn release_bytes(&mut self, body: ByteBody) {
        match body {
            ByteBody::Log { offset, len } => self.value_log.release_span(offset, len),
            ByteBody::Heap(data) => self.byte_pool.release(*data),
            ByteBody::Integer(_) => {}
        }
    }

    /// Borrow a value directly from shard-owned storage.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.get_bytes(key).ok().flatten()
    }

    /// Decimal text of a string or counter. Counters are formatted, not borrowed.
    pub fn copied_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.value_view(key).ok().flatten().map(ByteView::to_vec)
    }

    pub fn get_bytes(&self, key: &[u8]) -> Result<Option<&[u8]>, WrongType> {
        match self.value_view(key)? {
            Some(ByteView::Borrowed(bytes)) => Ok(Some(bytes)),
            Some(ByteView::Integer(_)) | None => Ok(None),
        }
    }

    pub fn value_view(&self, key: &[u8]) -> Result<Option<ByteView<'_>>, WrongType> {
        // The string read path: a hash with every field due is found in the wrong-type arm,
        // so a string hit pays nothing for it.
        match self.live_unchecked(key) {
            None => Ok(None),
            Some(IndexRef::Log { offset, len }) => Ok(Some(ByteView::Borrowed(
                self.value_log.slice(offset, plain_len(len)),
            ))),
            Some(IndexRef::Integer(value)) => Ok(Some(ByteView::Integer(value))),
            Some(IndexRef::Heap(bytes)) => Ok(Some(ByteView::Borrowed(bytes))),
            Some(IndexRef::SetSlab(_) | IndexRef::SortedSlab(_) | IndexRef::CollSlab(_)) => {
                Err(WrongType)
            }
            Some(IndexRef::Object(entry)) => match &entry.value {
                Value::Bytes(value) => Ok(Some(value.view(&self.value_log))),
                Value::Versioned(value) => Ok(Some(value.body.view(&self.value_log))),
                value @ Value::TimedHash(_) if self.timed_hash_is_dead(value) => Ok(None),
                _ => Err(WrongType),
            },
        }
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.live(key).is_some()
    }

    pub(crate) fn value_kind(&self, key: &[u8]) -> Option<ValueKind> {
        self.live(key).map(hit_kind)
    }
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    pub(crate) fn preview_increment(&self, key: &[u8], delta: i64) -> Result<i64, IncrementError> {
        let Some(current) = self.current_integer(key)? else {
            return Ok(delta);
        };

        current
            .checked_add(delta)
            .ok_or(IncrementError::NotIntegerOrOverflow)
    }

    /// `Ok(None)` leaves the key unchanged: it is missing or smaller than `count`.
    pub(crate) fn preview_take(
        &self,
        key: &[u8],
        count: i64,
    ) -> Result<Option<i64>, IncrementError> {
        let Some(current) = self.current_integer(key)? else {
            return Ok(None);
        };

        if current < count {
            return Ok(None);
        }

        Ok(Some(current - count))
    }

    /// `Ok(None)` leaves both keys unchanged: `source` is missing or smaller than `amount`,
    /// or `destination` would overflow or pass `max`. Otherwise `(source, destination)` after.
    pub(crate) fn preview_transfer(
        &self,
        source: &[u8],
        destination: &[u8],
        amount: i64,
        max: Option<i64>,
    ) -> Result<Option<(i64, i64)>, IncrementError> {
        let Some(from) = self.current_integer(source)? else {
            return Ok(None);
        };

        let to = self.current_integer(destination)?.unwrap_or(0);

        Ok(transfer_result(from, to, amount, max))
    }

    /// `preview_transfer` between two fields of one hash.
    pub(crate) fn preview_hash_transfer(
        &self,
        key: &[u8],
        from: &[u8],
        to: &[u8],
        amount: i64,
        max: Option<i64>,
    ) -> Result<Option<(i64, i64)>, IncrementError> {
        let Some(hash) = self.live_hash(key).map_err(|_| IncrementError::WrongType)? else {
            return Ok(None);
        };

        let integer = |field: &[u8]| -> Result<Option<i64>, IncrementError> {
            hash.get(field)
                .map(|value| {
                    std::str::from_utf8(value)
                        .ok()
                        .and_then(|value| value.parse::<i64>().ok())
                        .ok_or(IncrementError::NotIntegerOrOverflow)
                })
                .transpose()
        };

        let Some(source) = integer(from)? else {
            return Ok(None);
        };

        Ok(transfer_result(
            source,
            integer(to)?.unwrap_or(0),
            amount,
            max,
        ))
    }

    pub(crate) fn copy_string(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongType> {
        let Some(hit) = self
            .entries
            .lookup(key)
            .filter(|_| !self.key_is_expired(key))
        else {
            return Ok(None);
        };

        let bytes = match hit {
            IndexRef::Integer(value) => integer_bytes(value),
            IndexRef::Heap(bytes) => bytes.to_vec(),
            IndexRef::Log { offset, len } if is_integer(len) => {
                integer_bytes(self.value_log.read_integer(offset))
            }

            IndexRef::Log { offset, len } => self.value_log.slice(offset, plain_len(len)).to_vec(),

            IndexRef::SetSlab(_) | IndexRef::SortedSlab(_) | IndexRef::CollSlab(_) => {
                return Err(WrongType);
            }
            IndexRef::Object(entry) => match &entry.value {
                Value::Bytes(value) => match value.view(&self.value_log) {
                    ByteView::Integer(value) => integer_bytes(value),
                    ByteView::Borrowed(bytes) => bytes.to_vec(),
                },
                _ => return Err(WrongType),
            },
        };

        Ok(Some(bytes))
    }

    pub(crate) fn deadline_present(&self, key: &[u8]) -> bool {
        self.deadline(self.active, key).is_some()
    }

    /// Moves the deadline of a key that was just read. Does not read the value again.
    pub(crate) fn keep_for(&mut self, key: &[u8], expires_at: u64) {
        self.set_deadline(self.active, key, UnixMillis::new(expires_at));
        self.note_changed(key);
    }

    pub(super) fn current_integer(&self, key: &[u8]) -> Result<Option<i64>, IncrementError> {
        let Some(hit) = self
            .entries
            .lookup(key)
            .filter(|_| !self.key_is_expired(key))
        else {
            return Ok(None);
        };

        let current = match hit {
            IndexRef::Integer(current) => current,
            IndexRef::Log { offset, len } if is_integer(len) => self.value_log.read_integer(offset),
            IndexRef::Log { offset, len } => {
                parse_counter(self.value_log.slice(offset, plain_len(len)))?
            }

            IndexRef::Heap(bytes) => parse_counter(bytes)?,
            IndexRef::SetSlab(_) | IndexRef::SortedSlab(_) | IndexRef::CollSlab(_) => {
                return Err(IncrementError::WrongType);
            }

            IndexRef::Object(entry) => match &entry.value {
                Value::Bytes(bytes) => match bytes.view(&self.value_log) {
                    ByteView::Integer(current) => current,
                    ByteView::Borrowed(bytes) => parse_counter(bytes)?,
                },
                _ => return Err(IncrementError::WrongType),
            },
        };

        Ok(Some(current))
    }

    pub(crate) fn preview_hash_increment(
        &self,
        key: &[u8],
        field: &[u8],
        delta: i64,
    ) -> Result<i64, IncrementError> {
        let Some(hash) = self.live_hash(key).map_err(|_| IncrementError::WrongType)? else {
            return Ok(delta);
        };

        let Some(current) = hash.get(field) else {
            return Ok(delta);
        };

        let current = std::str::from_utf8(current)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or(IncrementError::NotIntegerOrOverflow)?;

        current
            .checked_add(delta)
            .ok_or(IncrementError::NotIntegerOrOverflow)
    }

    pub(crate) fn set_integer(&mut self, key: &[u8], value: i64) -> Result<(), WrongType> {
        if (self.expiration.keys_with_ttl == 0 || !self.key_is_expired(key))
            && self.entries.write_integer(key, value)
        {
            self.note_mutation(key);
            self.note_write(key);

            return Ok(());
        }

        self.remove_expired(key);
        let before = self.entry_charge(key);
        let existing = match self.entries.lookup(key) {
            Some(IndexRef::Log { .. } | IndexRef::Integer(_) | IndexRef::Heap(_)) => true,
            Some(IndexRef::Object(entry)) => match &entry.value {
                Value::Bytes(_) => true,
                _ => return Err(WrongType),
            },
            Some(IndexRef::SetSlab(_) | IndexRef::SortedSlab(_) | IndexRef::CollSlab(_)) => {
                return Err(WrongType);
            }
            None => false,
        };

        let deadline_id = existing.then(|| self.deadline_id(key)).flatten();

        if existing {
            let old = self.entries.insert(
                key,
                Entry::fresh(Value::Bytes(BytesValue {
                    body: ByteBody::Integer(value),
                })),
            );

            if let Some(Entry {
                value: Value::Bytes(bytes),
            }) = old
            {
                self.release_bytes(bytes.body);
            }
        } else {
            self.insert_entry(
                key,
                Entry::fresh(Value::Bytes(BytesValue {
                    body: ByteBody::Integer(value),
                })),
            );
        }

        self.reconcile_entry_charge(before, key);

        if let Some(id) = deadline_id {
            self.point_deadline(key, id);
        }

        Ok(())
    }

    pub fn increment(&mut self, key: &[u8], delta: i64) -> Result<i64, IncrementError> {
        let updated = self.preview_increment(key, delta)?;

        self.set_integer(key, updated)
            .map_err(|_| IncrementError::WrongType)?;
        Ok(updated)
    }
}

/// The balances after moving `amount`, or `None` when `from` is short or `to` would
/// overflow or pass `max`.
fn transfer_result(from: i64, to: i64, amount: i64, max: Option<i64>) -> Option<(i64, i64)> {
    let from = from.checked_sub(amount).filter(|left| *left >= 0)?;
    let to = to.checked_add(amount)?;

    if max.is_some_and(|max| to > max) {
        return None;
    }

    Some((from, to))
}
