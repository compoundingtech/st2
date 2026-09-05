//! UUIDv7 generation for immutable agent IDs (R24).
//!
//! New agent subjects — and archived legacy subjects whose `<host>.<identity>` bus identity is
//! already claimed when `st2 catalog migrate-ids` freezes IDs — need an identifier that is unique
//! without coordination and sorts chronologically, so a catalog stays readable in time order. That
//! is exactly RFC 9562 §5.7 UUIDv7.
//!
//! This lives in-tree rather than as a `uuid` crate dependency because the whole implementation is
//! the forty lines below: a 48-bit big-endian millisecond timestamp, four version bits, and 74
//! random bits, rendered as canonical lowercase hex. The one non-trivial part — the entropy source
//! — already had to be written here anyway, since a weak fallback would silently break the
//! uniqueness the catalog relies on.

/// A fresh canonical UUIDv7 from the current wall clock.
///
/// Fallible rather than panicking: every caller sits inside an `anyhow` catalog transaction that
/// must refuse cleanly before writing, and a missing entropy source is a real environment failure
/// worth reporting rather than an abort in the middle of a commit.
pub fn uuid_v7() -> anyhow::Result<String> {
    Ok(uuid_v7_from(crate::message::now_ms(), random_bytes()?))
}

/// The pure core: render `unix_ts_ms` plus 74 bits taken from `random` as a canonical UUIDv7.
///
/// Layout per RFC 9562 §5.7 — bits 0..48 are the big-endian millisecond timestamp, bits 48..52 the
/// version `0b0111`, bits 52..64 `rand_a`, bits 64..66 the variant `0b10`, bits 66..128 `rand_b`.
/// Only the low 48 bits of `unix_ts_ms` are representable; anything above is dropped, which is the
/// year-10889 horizon.
pub(crate) fn uuid_v7_from(unix_ts_ms: u64, random: [u8; 10]) -> String {
    let mut bytes = [0u8; 16];
    bytes[0..6].copy_from_slice(&unix_ts_ms.to_be_bytes()[2..8]);
    // version nibble + the top 4 bits of the 12-bit `rand_a`
    bytes[6] = 0x70 | (random[0] & 0x0f);
    bytes[7] = random[1];
    // variant bits + the top 6 bits of the 62-bit `rand_b`
    bytes[8] = 0x80 | (random[2] & 0x3f);
    bytes[9..16].copy_from_slice(&random[3..10]);

    let mut out = String::with_capacity(36);
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push(char::from_digit((byte >> 4) as u32, 16).expect("nibble is a hex digit"));
        out.push(char::from_digit((byte & 0x0f) as u32, 16).expect("nibble is a hex digit"));
    }
    out
}

/// 10 bytes from the OS CSPRNG, or a hard error.
///
/// Linux and Android go through `getrandom(2)`, which needs no file descriptor and cannot be
/// shadowed by a tampered `/dev`. Every other supported target (macOS, the BSDs) has no
/// `libc::getrandom`, so it reads `/dev/urandom` — after confirming the opened descriptor really is
/// a character device, so a planted regular file cannot feed us chosen "randomness". A short read
/// or a failing call is an error: never fall back to a clock- or pid-derived value, because the
/// uniqueness of an immutable agent ID depends on these bits.
fn random_bytes() -> anyhow::Result<[u8; 10]> {
    let mut buf = [0u8; 10];
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let read = unsafe { libc::getrandom(buf.as_mut_ptr().cast(), buf.len(), 0) };
        anyhow::ensure!(
            read == buf.len() as isize,
            "getrandom(2) returned {read} of {} bytes for a UUIDv7: {}",
            buf.len(),
            std::io::Error::last_os_error()
        );
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        use anyhow::Context as _;
        use std::io::Read as _;
        let mut file =
            std::fs::File::open("/dev/urandom").context("opening /dev/urandom for a UUIDv7")?;
        let file_type = std::os::unix::fs::FileTypeExt::is_char_device(
            &file
                .metadata()
                .context("stat /dev/urandom for a UUIDv7")?
                .file_type(),
        );
        anyhow::ensure!(
            file_type,
            "/dev/urandom is not a character device — refusing to seed a UUIDv7 from it"
        );
        file.read_exact(&mut buf)
            .context("reading 10 bytes from /dev/urandom for a UUIDv7")?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The RFC 9562 §5.7 example vector — pinned bytes in, one exact string out, so a change to the
    /// field layout fails here instead of silently producing a plausible-looking UUID.
    #[test]
    fn pinned_vector_renders_exactly() {
        let random = [0x0c, 0xc3, 0x98, 0xc4, 0xdc, 0x0c, 0x0c, 0x07, 0x39, 0x8f];
        assert_eq!(
            uuid_v7_from(0x017f_22e2_79b0, random),
            "017f22e2-79b0-7cc3-98c4-dc0c0c07398f"
        );
    }

    #[test]
    fn version_and_variant_bits_are_fixed() {
        for ts in [0u64, 1, 1_757_000_000_000, (1 << 48) - 1] {
            for fill in [0x00u8, 0xff, 0x5a] {
                let id = uuid_v7_from(ts, [fill; 10]);
                let version = id.as_bytes()[14] as char;
                assert_eq!(version, '7', "version nibble of {id}");
                let variant = id.as_bytes()[19] as char;
                assert!(
                    matches!(variant, '8' | '9' | 'a' | 'b'),
                    "variant bits of {id} must be 0b10"
                );
            }
        }
    }

    #[test]
    fn timestamp_round_trips() {
        for ts in [0u64, 1, 42, 1_757_000_000_000, (1 << 48) - 1] {
            let id = uuid_v7_from(ts, [0xa5; 10]);
            let hex: String = id.chars().filter(|c| *c != '-').take(12).collect();
            let parsed = u64::from_str_radix(&hex, 16).expect("first 48 bits are hex");
            assert_eq!(parsed, ts, "round-trip of {id}");
        }
    }

    #[test]
    fn canonical_form_is_lowercase_hyphenated_36_chars() {
        let id = uuid_v7_from(1_757_000_000_000, [0xde; 10]);
        assert_eq!(id.len(), 36, "{id}");
        for (i, c) in id.char_indices() {
            if matches!(i, 8 | 13 | 18 | 23) {
                assert_eq!(c, '-', "hyphen at {i} of {id}");
            } else {
                assert!(
                    c.is_ascii_digit() || ('a'..='f').contains(&c),
                    "{c} at {i} of {id} must be lowercase hex"
                );
            }
        }
    }

    #[test]
    fn consecutive_live_calls_differ() {
        let first = uuid_v7().expect("entropy source");
        let second = uuid_v7().expect("entropy source");
        assert_ne!(first, second);
    }

    #[test]
    fn output_is_a_valid_agent_id() {
        let id = uuid_v7().expect("entropy source");
        agent_spec::validate_agent_id(&id).expect("a UUIDv7 is a valid agent id");
    }

    #[test]
    fn increasing_timestamps_sort_lexicographically() {
        let ids: Vec<String> = [0u64, 1, 1 << 8, 1 << 16, 1_757_000_000_000, (1 << 48) - 1]
            .into_iter()
            // Constant randomness would make the ordering trivially the timestamp's; vary the
            // random tail in the opposite direction so only the timestamp prefix can carry it.
            .enumerate()
            .map(|(i, ts)| uuid_v7_from(ts, [0xff - i as u8; 10]))
            .collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "v7 ids must sort in timestamp order");
    }
}
