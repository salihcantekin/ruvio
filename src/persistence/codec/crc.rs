//! CRC-32C of WAL records. Most records are a few dozen bytes, where the `crc32c` crate's
//! setup for its interleaved large-buffer loop costs more than the checksum itself; short
//! inputs take a plain hardware loop instead. Both give the same value.

/// Inputs shorter than this use the short loop when the CPU has the instruction.
const SHORT_INPUT: usize = 128;

#[inline]
pub(super) fn checksum(bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if bytes.len() < SHORT_INPUT && std::arch::is_x86_feature_detected!("sse4.2") {
        // SAFETY: the CPU supports SSE4.2, checked just above.
        #[allow(unsafe_code)]
        return unsafe { short_sse42(bytes) };
    }

    crc32c::crc32c(bytes)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
fn short_sse42(bytes: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};

    let mut crc = u64::from(u32::MAX);
    let mut words = bytes.chunks_exact(8);

    for word in &mut words {
        crc = _mm_crc32_u64(crc, u64::from_le_bytes(word.try_into().unwrap()));
    }

    let mut crc = crc as u32;

    for &byte in words.remainder() {
        crc = _mm_crc32_u8(crc, byte);
    }

    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_crate_for_every_length_around_the_cutoff() {
        // Deterministic pseudo-random bytes (xorshift), so a failure reproduces.
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut input = vec![0u8; 300];

        for _ in 0..20 {
            for byte in &mut input {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }

            for length in 0..=input.len() {
                for start in 0..8.min(input.len() - length + 1) {
                    let slice = &input[start..start + length];

                    assert_eq!(checksum(slice), crc32c::crc32c(slice), "length {length}");
                }
            }
        }
    }

    #[test]
    fn matches_known_check_values() {
        assert_eq!(checksum(b""), 0);
        assert_eq!(checksum(b"123456789"), 0xE306_9283);
    }
}
