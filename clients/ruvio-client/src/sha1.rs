//! SHA-1 of a script body, hex encoded: the digest `EVALSHA` and `SCRIPT LOAD` use.

const HEX: &[u8; 16] = b"0123456789abcdef";

/// The lowercase hex SHA-1 of `data`.
pub(crate) fn hex_digest(data: &[u8]) -> String {
    let mut state: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut tail = [0u8; 128];
    let mut blocks = data.chunks_exact(64);

    for block in &mut blocks {
        compress(&mut state, block);
    }

    let rest = blocks.remainder();
    let padded = if rest.len() < 56 { 64 } else { 128 };

    tail[..rest.len()].copy_from_slice(rest);
    tail[rest.len()] = 0x80;
    tail[padded - 8..padded].copy_from_slice(&(data.len() as u64 * 8).to_be_bytes());

    for block in tail[..padded].chunks_exact(64) {
        compress(&mut state, block);
    }

    let mut text = String::with_capacity(40);

    for byte in state.iter().flat_map(|word| word.to_be_bytes()) {
        text.push(char::from(HEX[usize::from(byte >> 4)]));
        text.push(char::from(HEX[usize::from(byte & 15)]));
    }

    text
}

fn compress(state: &mut [u32; 5], block: &[u8]) {
    let mut words = [0u32; 80];

    for (word, bytes) in words.iter_mut().zip(block.chunks_exact(4)) {
        *word = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    }

    for index in 16..80 {
        words[index] =
            (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                .rotate_left(1);
    }

    let [mut a, mut b, mut c, mut d, mut e] = *state;

    for (index, word) in words.iter().enumerate() {
        let (mix, constant) = match index {
            0..=19 => ((b & c) | (!b & d), 0x5A82_7999),
            20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
            40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
            _ => (b ^ c ^ d, 0xCA62_C1D6),
        };
        let next = a
            .rotate_left(5)
            .wrapping_add(mix)
            .wrapping_add(e)
            .wrapping_add(constant)
            .wrapping_add(*word);

        e = d;
        d = c;
        c = b.rotate_left(30);
        b = a;
        a = next;
    }

    for (slot, value) in state.iter_mut().zip([a, b, c, d, e]) {
        *slot = slot.wrapping_add(value);
    }
}

#[cfg(test)]
mod tests {
    use super::hex_digest;

    #[test]
    fn matches_the_published_vectors() {
        assert_eq!(hex_digest(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            hex_digest(b"abc"),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            hex_digest(&[b'a'; 1000]),
            "291e9a6c66994949b57ba5e650361e98fc36b1ba"
        );
    }
}
