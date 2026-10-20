//! Fenced `LEASE` and `SEMAPHORE` tokens: `generation << 48 | low`.
//!
//! The generation goes up on every promotion and every committed slot move, so a token from a
//! new primary or owner is larger than any token an earlier one handed out, even one that was
//! lost with it. Within a generation, `low` is the larger of one past the last token this shard
//! granted or applied and the clock in milliseconds shifted left by 6. The clock floor keeps
//! tokens moving forward across a restart that lost the counter (memory mode, or a lease that
//! expired before the last snapshot).

use super::*;

const GENERATION_SHIFT: u32 = 48;
const LOW_MASK: u64 = (1 << GENERATION_SHIFT) - 1;
const CLOCK_SHIFT: u32 = 6;

/// Highest generation a token carries, so tokens stay positive signed integers.
pub(crate) const MAX_FENCE_GENERATION: u64 = (1 << (63 - GENERATION_SHIFT)) - 1;

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Raises the generation of new tokens. It never goes down.
    pub fn set_fence_generation(&mut self, generation: u64) {
        let generation = generation.min(MAX_FENCE_GENERATION);

        if generation <= self.fence_generation {
            return;
        }

        self.fence_generation = generation;
        self.fence_last = 0;

        if self.fence_ahead >> GENERATION_SHIFT == generation {
            self.fence_last = self.fence_ahead;
        }
    }

    /// The token the next grant gets, larger than `after` (the key's own last token).
    pub(crate) fn next_fence_token(&self, after: u64) -> u64 {
        let now = self.now_unix_millis().get();
        let clock = (self.fence_generation << GENERATION_SHIFT) | ((now << CLOCK_SHIFT) & LOW_MASK);

        clock
            .max(self.fence_last.saturating_add(1))
            .max(after.saturating_add(1))
    }

    /// Remembers a token this shard granted or applied (live, from its log, or from its
    /// primary). Tokens of an earlier generation are below every new one and are ignored.
    pub(crate) fn note_fence_token(&mut self, token: u64) {
        let generation = token >> GENERATION_SHIFT;

        if generation == self.fence_generation {
            self.fence_last = self.fence_last.max(token);
        } else if generation > self.fence_generation && generation <= MAX_FENCE_GENERATION {
            self.fence_ahead = self.fence_ahead.max(token);
        }
    }
}
