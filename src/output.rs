use std::{
    io::{self, Write},
    ops::Deref,
};

pub(crate) struct BoundedOutput {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl BoundedOutput {
    pub(crate) fn with_capacity(capacity: usize, maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity.min(maximum)),
            maximum,
            exceeded: false,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.bytes.clear();
        self.exceeded = false;
    }

    /// Drops the first `count` bytes, which were sent; the limit then counts only the rest.
    pub(crate) fn consume_front(&mut self, count: usize) {
        self.bytes.drain(..count);
    }

    pub(crate) fn truncate_to(&mut self, length: usize) {
        self.bytes.truncate(length);
    }

    pub(crate) fn release_idle(&mut self) {
        if self.bytes.is_empty() {
            self.bytes.shrink_to(0);
        }
    }

    /// Gives back a large buffer once it is empty, as Redis's `clientsCron` does.
    pub(crate) fn release_oversized(&mut self, above: usize, keep: usize) {
        if self.bytes.is_empty() && self.bytes.capacity() > above {
            self.bytes.shrink_to(keep);
        }
    }

    pub(crate) fn maximum(&self) -> usize {
        self.maximum
    }

    pub(crate) fn exceeded(&self) -> bool {
        self.exceeded
    }
}

const PAIRS: &[u8; 200] = b"0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

/// The decimal digits of `value`, written two at a time at the end of `buffer`.
pub(crate) fn decimal(mut value: u64, buffer: &mut [u8; 20]) -> &[u8] {
    let mut at = buffer.len();

    while value >= 100 {
        let pair = (value % 100) as usize * 2;

        value /= 100;
        at -= 2;
        buffer[at..at + 2].copy_from_slice(&PAIRS[pair..pair + 2]);
    }

    if value >= 10 {
        let pair = value as usize * 2;

        at -= 2;
        buffer[at..at + 2].copy_from_slice(&PAIRS[pair..pair + 2]);
    } else {
        at -= 1;
        buffer[at] = b'0' + value as u8;
    }

    &buffer[at..]
}

impl Deref for BoundedOutput {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl Write for BoundedOutput {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.exceeded {
            return Ok(buffer.len());
        }

        let Some(new_length) = self.bytes.len().checked_add(buffer.len()) else {
            self.exceeded = true;

            return Ok(buffer.len());
        };

        if new_length > self.maximum {
            self.exceeded = true;

            return Ok(buffer.len());
        }

        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    #[inline]
    fn write_all(&mut self, buffer: &[u8]) -> io::Result<()> {
        self.write(buffer).map(|_| ())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_matches_the_standard_formatting() {
        let mut buffer = [0u8; 20];
        let mut samples = vec![
            0,
            1,
            9,
            10,
            99,
            100,
            101,
            999,
            1000,
            12345,
            u64::MAX,
            u64::MAX - 1,
        ];

        for power in 0..20u32 {
            let base = 10u64.pow(power);

            samples.extend([base - 1, base, base + 1]);
        }

        for value in samples {
            assert_eq!(decimal(value, &mut buffer), value.to_string().as_bytes());
        }
    }

    #[test]
    fn rejects_writes_before_exceeding_the_limit() {
        let mut output = BoundedOutput::with_capacity(4, 5);

        output.write_all(b"1234").unwrap();
        output.write_all(b"67").unwrap();
        assert!(output.exceeded());
        assert_eq!(&*output, b"1234");
    }

    #[test]
    fn sent_bytes_no_longer_count_against_the_limit() {
        let mut output = BoundedOutput::with_capacity(0, 6);

        output.write_all(b"12345").unwrap();
        output.consume_front(4);
        output.write_all(b"6789").unwrap();
        assert!(!output.exceeded());
        assert_eq!(&*output, b"56789");
    }
}
