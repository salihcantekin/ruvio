//! Lua 5.1 pattern matching for scripts, with a bound on its work.
//!
//! A port of the matcher in Lua 5.1's `lstrlib.c`, with the same results. The C matcher
//! runs between instruction hooks, so a pattern that backtracks (`a*a*a*...b` against a
//! short string, or `.-x` scanned over a long one) could hold a shard for hours, and its
//! recursion has no depth limit. This one charges every step to a budget and stops at a
//! nesting of [`MAX_DEPTH`], the limit Lua 5.2 added.
//!
//! It allocates nothing: Lua code calls it through C functions whose errors unwind with
//! `longjmp`, which must not skip a destructor.

/// Most captures in one pattern, as `LUA_MAXCAPTURES`.
pub(crate) const MAX_CAPTURES: usize = 32;

/// Deepest recursion of one match: once per quantifier or capture the pattern nests.
pub(crate) const MAX_DEPTH: usize = 200;

const CAP_UNFINISHED: isize = -1;
const CAP_POSITION: isize = -2;

/// Why a match stopped without an answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PatternError {
    /// The pattern is malformed or uses a capture wrongly; Lua's own message.
    Invalid(&'static str),
    /// The pattern nests deeper than [`MAX_DEPTH`].
    TooComplex,
    /// The step budget ran out.
    Quota,
}

/// One capture of a match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Capture {
    /// Bytes `start..end` of the subject.
    Text(usize, usize),
    /// An empty `()` capture: the 1-based position it stood at.
    Position(usize),
}

/// A successful match: bytes `start..end` of the subject and its captures. Filled in
/// place by [`search`]: it is large, and copying it per match was measurable.
#[derive(Debug)]
pub(crate) struct Found {
    pub start: usize,
    pub end: usize,
    level: usize,
    capture: [(usize, isize); MAX_CAPTURES],
}

impl Found {
    pub(crate) const fn new() -> Self {
        Self {
            start: 0,
            end: 0,
            level: 0,
            capture: [(0, 0); MAX_CAPTURES],
        }
    }

    /// Number of captures the pattern made.
    pub(crate) fn count(&self) -> usize {
        self.level
    }

    /// Capture `index`, or the whole match for index 0 of a pattern without captures,
    /// as Lua's `push_onecapture` gives it.
    pub(crate) fn capture(&self, index: usize) -> Result<Capture, PatternError> {
        if index >= self.level {
            return if index == 0 {
                Ok(Capture::Text(self.start, self.end))
            } else {
                Err(PatternError::Invalid("invalid capture index"))
            };
        }

        let (init, len) = self.capture[index];

        match len {
            CAP_UNFINISHED => Err(PatternError::Invalid("unfinished capture")),
            CAP_POSITION => Ok(Capture::Position(init + 1)),
            len => Ok(Capture::Text(init, init + len as usize)),
        }
    }

    /// What `string.match` and `gmatch` return: the captures, or the whole match.
    pub(crate) fn results(&self) -> usize {
        self.level.max(1)
    }
}

/// Where a search may start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Anchor {
    /// A leading `^` anchors the match at the start position (`find`, `match`, `gsub`).
    Leading,
    /// `^` is an ordinary character (`gmatch`).
    Literal,
}

/// Searches `subject` from byte `init` (at most its length) for the first match of
/// `pattern`, as `string.find` does, and fills in `found` if there is one. `steps` is the
/// remaining budget, charged as the search runs.
pub(crate) fn search(
    subject: &[u8],
    pattern: &[u8],
    init: usize,
    anchor: Anchor,
    steps: &mut u64,
    found: &mut Found,
) -> Result<bool, PatternError> {
    // C reads the pattern up to its terminating NUL.
    let pattern = pattern
        .iter()
        .position(|&byte| byte == 0)
        .map_or(pattern, |end| &pattern[..end]);
    let (pattern, anchored) = match (anchor, pattern.first()) {
        (Anchor::Leading, Some(b'^')) => (&pattern[1..], true),
        _ => (pattern, false),
    };
    let mut state = MatchState {
        src: subject,
        pat: pattern,
        level: 0,
        capture: &mut found.capture,
        depth: 0,
        steps: *steps,
    };
    let mut start = init.min(subject.len());
    let outcome = loop {
        state.level = 0;

        match state.do_match(start, 0) {
            Ok(Some(end)) => break Ok(Some(end)),
            Ok(None) if !anchored && start < subject.len() => start += 1,
            Ok(None) => break Ok(None),
            Err(error) => break Err(error),
        }
    };
    let level = state.level;

    *steps = state.steps;
    let end = outcome?;

    if let Some(end) = end {
        found.start = start;
        found.end = end;
        found.level = level;
    }

    Ok(end.is_some())
}

/// Whether `pattern` has a character that makes `string.find` match it as a pattern
/// rather than search for it as text (C's `strpbrk(p, SPECIALS)`, up to the first NUL).
pub(crate) fn has_specials(pattern: &[u8]) -> bool {
    pattern
        .iter()
        .take_while(|&&byte| byte != 0)
        .any(|byte| b"^$*+?.([%-".contains(byte))
}

struct MatchState<'a> {
    src: &'a [u8],
    pat: &'a [u8],
    level: usize,
    capture: &'a mut [(usize, isize); MAX_CAPTURES],
    depth: usize,
    steps: u64,
}

impl MatchState<'_> {
    /// Pattern byte at `p`; 0 past the end, as C's terminator.
    fn pc(&self, p: usize) -> u8 {
        self.pat.get(p).copied().unwrap_or(0)
    }

    /// Subject byte at `s`; 0 past the end, as C's terminator.
    fn sc(&self, s: usize) -> u8 {
        self.src.get(s).copied().unwrap_or(0)
    }

    #[inline]
    fn step(&mut self) -> Result<(), PatternError> {
        self.charge(1)
    }

    /// Charges `bytes` of scanning, so a class of length n costs n steps, not one.
    #[inline]
    fn charge(&mut self, bytes: usize) -> Result<(), PatternError> {
        self.steps = self
            .steps
            .checked_sub(bytes as u64)
            .ok_or(PatternError::Quota)?;
        Ok(())
    }

    fn class_end(&mut self, p: usize) -> Result<usize, PatternError> {
        let end = self.class_end_scan(p)?;

        // A single-byte token costs the step the match loop already charges; a class or
        // an escape is scanned here, so charge the bytes it walks.
        if end - p > 1 {
            self.charge(end - p)?;
        }

        Ok(end)
    }

    fn class_end_scan(&self, mut p: usize) -> Result<usize, PatternError> {
        let first = self.pc(p);

        p += 1;
        match first {
            b'%' => {
                if self.pc(p) == 0 {
                    return Err(PatternError::Invalid("malformed pattern (ends with '%')"));
                }

                Ok(p + 1)
            }

            b'[' => {
                if self.pc(p) == b'^' {
                    p += 1;
                }

                loop {
                    if self.pc(p) == 0 {
                        return Err(PatternError::Invalid("malformed pattern (missing ']')"));
                    }

                    let byte = self.pc(p);

                    p += 1;
                    if byte == b'%' && self.pc(p) != 0 {
                        p += 1;
                    }

                    if self.pc(p) == b']' {
                        break;
                    }
                }

                Ok(p + 1)
            }

            _ => Ok(p),
        }
    }

    /// Whether `c` is in the class `[...]` from `p` (the `[`) to `ec` (the `]`).
    fn match_bracket_class(&mut self, c: u8, mut p: usize, ec: usize) -> Result<bool, PatternError> {
        self.charge(ec - p)?;


        let mut sig = true;

        if self.pc(p + 1) == b'^' {
            sig = false;
            p += 1;
        }

        loop {
            p += 1;
            if p >= ec {
                break;
            }

            if self.pc(p) == b'%' {
                p += 1;
                if match_class(c, self.pc(p)) {
                    return Ok(sig);
                }
            } else if self.pc(p + 1) == b'-' && p + 2 < ec {
                p += 2;
                if self.pc(p - 2) <= c && c <= self.pc(p) {
                    return Ok(sig);
                }
            } else if self.pc(p) == c {
                return Ok(sig);
            }
        }

        Ok(!sig)
    }

    fn single_match(&mut self, c: u8, p: usize, ep: usize) -> Result<bool, PatternError> {
        Ok(match self.pc(p) {
            b'.' => true,
            b'%' => match_class(c, self.pc(p + 1)),
            b'[' => self.match_bracket_class(c, p, ep - 1)?,
            byte => byte == c,
        })
    }

    fn match_balance(&mut self, mut s: usize, p: usize) -> Result<Option<usize>, PatternError> {
        if self.pc(p) == 0 || self.pc(p + 1) == 0 {
            return Err(PatternError::Invalid("unbalanced pattern"));
        }

        if self.sc(s) != self.pc(p) {
            return Ok(None);
        }

        let (open, close) = (self.pc(p), self.pc(p + 1));
        let mut depth = 1usize;

        loop {
            s += 1;
            if s >= self.src.len() {
                return Ok(None);
            }

            self.step()?;
            let byte = self.src[s];

            if byte == close {
                depth -= 1;
                if depth == 0 {
                    return Ok(Some(s + 1));
                }
            } else if byte == open {
                depth += 1;
            }
        }
    }

    fn max_expand(&mut self, s: usize, p: usize, ep: usize) -> Result<Option<usize>, PatternError> {
        let mut count = 0;

        while s + count < self.src.len() && self.single_match(self.src[s + count], p, ep)? {
            self.step()?;
            count += 1;
        }

        loop {
            if let Some(end) = self.do_match(s + count, ep + 1)? {
                return Ok(Some(end));
            }

            if count == 0 {
                return Ok(None);
            }

            count -= 1;
        }
    }

    fn min_expand(
        &mut self,
        mut s: usize,
        p: usize,
        ep: usize,
    ) -> Result<Option<usize>, PatternError> {
        loop {
            if let Some(end) = self.do_match(s, ep + 1)? {
                return Ok(Some(end));
            }

            if s < self.src.len() && self.single_match(self.src[s], p, ep)? {
                s += 1;
            } else {
                return Ok(None);
            }
        }
    }

    fn start_capture(
        &mut self,
        s: usize,
        p: usize,
        what: isize,
    ) -> Result<Option<usize>, PatternError> {
        let level = self.level;

        if level >= MAX_CAPTURES {
            return Err(PatternError::Invalid("too many captures"));
        }

        self.capture[level] = (s, what);
        self.level = level + 1;
        let matched = self.do_match(s, p)?;

        if matched.is_none() {
            self.level -= 1;
        }

        Ok(matched)
    }

    fn end_capture(&mut self, s: usize, p: usize) -> Result<Option<usize>, PatternError> {
        let Some(open) = (0..self.level)
            .rev()
            .find(|&level| self.capture[level].1 == CAP_UNFINISHED)
        else {
            return Err(PatternError::Invalid("invalid pattern capture"));
        };

        self.capture[open].1 = (s - self.capture[open].0) as isize;
        let matched = self.do_match(s, p)?;

        if matched.is_none() {
            self.capture[open].1 = CAP_UNFINISHED;
        }

        Ok(matched)
    }

    fn match_capture(&mut self, s: usize, digit: u8) -> Result<Option<usize>, PatternError> {
        let index = usize::from(digit).wrapping_sub(usize::from(b'1'));

        if index >= self.level || self.capture[index].1 == CAP_UNFINISHED {
            return Err(PatternError::Invalid("invalid capture index"));
        }

        let (init, len) = self.capture[index];

        // A position capture has a negative length, which C compares as huge: no match.
        let Ok(len) = usize::try_from(len) else {
            return Ok(None);
        };

        self.steps = self
            .steps
            .checked_sub(len as u64 / 64)
            .ok_or(PatternError::Quota)?;
        if self.src.len() - s >= len && self.src[init..init + len] == self.src[s..s + len] {
            Ok(Some(s + len))
        } else {
            Ok(None)
        }
    }

    fn do_match(&mut self, s: usize, p: usize) -> Result<Option<usize>, PatternError> {
        if self.depth >= MAX_DEPTH {
            return Err(PatternError::TooComplex);
        }

        self.depth += 1;
        let matched = self.match_here(s, p);

        self.depth -= 1;
        matched
    }

    fn match_here(&mut self, mut s: usize, mut p: usize) -> Result<Option<usize>, PatternError> {
        loop {
            self.step()?;

            match self.pc(p) {
                b'(' => {
                    return if self.pc(p + 1) == b')' {
                        self.start_capture(s, p + 2, CAP_POSITION)
                    } else {
                        self.start_capture(s, p + 1, CAP_UNFINISHED)
                    };
                }

                b')' => return self.end_capture(s, p + 1),
                b'%' if self.pc(p + 1) == b'b' => match self.match_balance(s, p + 2)? {
                    Some(next) => {
                        s = next;
                        p += 4;
                    }
                    None => return Ok(None),
                },
                b'%' if self.pc(p + 1) == b'f' => {
                    p += 2;
                    if self.pc(p) != b'[' {
                        return Err(PatternError::Invalid("missing '[' after '%f' in pattern"));
                    }

                    let ep = self.class_end(p)?;
                    let previous = if s == 0 { 0 } else { self.src[s - 1] };

                    if self.match_bracket_class(previous, p, ep - 1)?
                        || !self.match_bracket_class(self.sc(s), p, ep - 1)?
                    {
                        return Ok(None);
                    }

                    p = ep;
                }

                b'%' if self.pc(p + 1).is_ascii_digit() => {
                    match self.match_capture(s, self.pc(p + 1))? {
                        Some(next) => {
                            s = next;
                            p += 2;
                        }
                        None => return Ok(None),
                    }
                }

                0 => return Ok(Some(s)),
                b'$' if self.pc(p + 1) == 0 => {
                    return Ok((s == self.src.len()).then_some(s));
                }

                _ => {
                    let ep = self.class_end(p)?;
                    let matches = s < self.src.len() && self.single_match(self.src[s], p, ep)?;

                    match self.pc(ep) {
                        b'?' => {
                            if matches && let Some(end) = self.do_match(s + 1, ep + 1)? {
                                return Ok(Some(end));
                            }

                            p = ep + 1;
                        }

                        b'*' => return self.max_expand(s, p, ep),
                        b'+' => {
                            return if matches {
                                self.max_expand(s + 1, p, ep)
                            } else {
                                Ok(None)
                            };
                        }

                        b'-' => return self.min_expand(s, p, ep),
                        _ => {
                            if !matches {
                                return Ok(None);
                            }

                            s += 1;
                            p = ep;
                        }
                    }
                }
            }
        }
    }
}

/// C's `match_class` in the "C" locale.
fn match_class(c: u8, class: u8) -> bool {
    let res = match class.to_ascii_lowercase() {
        b'a' => c.is_ascii_alphabetic(),
        b'c' => c.is_ascii_control(),
        b'd' => c.is_ascii_digit(),
        b'l' => c.is_ascii_lowercase(),
        b'p' => c.is_ascii_punctuation(),
        b's' => matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r'),
        b'u' => c.is_ascii_uppercase(),
        b'w' => c.is_ascii_alphanumeric(),
        b'x' => c.is_ascii_hexdigit(),
        b'z' => c == 0,
        _ => return class == c,
    };

    if class.is_ascii_lowercase() {
        res
    } else {
        !res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Random malformed and edge-case patterns and subjects: the matcher must answer or
    /// return an error, never panic (release builds abort on a panic).
    #[test]
    fn malformed_patterns_never_panic() {
        let tokens: &[&[u8]] = &[
            b"%", b"[", b"[^", b"]", b"^", b"$", b"%b", b"%b(", b"%b()", b"%f", b"%f[", b"%f[a",
            b"%f[%a]", b"(", b")", b"()", b"%1", b"%2", b"%0", b"%9", b"*", b"+", b"-", b"?", b".",
            b"a", b"[a-", b"[%", b"[%]", b"[]", b"[^]", b"%z", b"\0", b"\xff", b"%\xff",
        ];
        let letters = b"a()[]%^$-\0\xff";
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |bound: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % bound as u64) as usize
        };
        let mut pattern = Vec::new();
        let mut subject = Vec::new();

        for case in 0..30_000 {
            pattern.clear();
            subject.clear();
            // Every 50th case nests captures past the limit of 32.
            let captures = if case % 50 == 0 { 40 } else { 0 };

            pattern.resize(captures, b'(');

            for _ in 0..next(12) {
                pattern.extend_from_slice(tokens[next(tokens.len())]);
            }

            pattern.resize(pattern.len() + captures, b')');

            for _ in 0..next(24) {
                subject.push(letters[next(letters.len())]);
            }

            let init = next(subject.len() + 2);

            for anchor in [Anchor::Leading, Anchor::Literal] {
                let mut steps = 20_000;
                let mut found = Found::new();

                if let Ok(true) = search(&subject, &pattern, init, anchor, &mut steps, &mut found) {
                    assert!(found.start <= found.end && found.end <= subject.len());
                    for index in 0..MAX_CAPTURES + 2 {
                        if let Ok(Capture::Text(start, end)) = found.capture(index) {
                            assert!(start <= end && end <= subject.len());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_long_bracket_class_charges_the_bytes_it_scans() {
        // A class of n bytes used to cost one step per subject position, so the work was
        // pattern length x subject length for a budget of eight million steps.
        let class = format!("[{}]", "b".repeat(100_000));
        let pattern = class.clone();
        let subject = "a".repeat(200_000);
        let mut steps = 8_000_000u64;
        let mut found = Found::new();
        let outcome = search(subject.as_bytes(), pattern.as_bytes(), 0, Anchor::Leading, &mut steps, &mut found);

        assert_eq!(outcome, Err(PatternError::Quota));
    }
}
