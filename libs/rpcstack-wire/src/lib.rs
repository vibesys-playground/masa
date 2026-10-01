//! Layout of the header value that carries a request's or response's module
//! wire data, and the primitives to read and write it.
//!
//! ```text
//! ctx: <name> : <base64 JSON> [ . <name> : <base64 JSON> ]*
//! ```
//!
//! Each section carries one module's wire data. Neither `.` nor `:` is in the
//! base64 alphabet, and section names may not contain them, so the value is
//! split with plain string searches and one section is found without decoding
//! any other.
//!
//! This crate knows nothing about HTTP or gRPC types, so a crate that cannot
//! depend on them can still read one section selectively. `rpcstack` builds the
//! typed module API (`WireIn`, `WireOut`) on top of these primitives.

/// Name of the header that carries the sections.
pub const HEADER_NAME: &str = "ctx";

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Separates sections.
pub const SECTION_SEPARATOR: char = '.';

/// Separates a section's name from its payload.
pub const NAME_SEPARATOR: char = ':';

/// The `(name, payload)` pairs of a header value, undecoded.
#[inline]
pub fn sections(header: &str) -> impl Iterator<Item = (&str, &str)> {
    header
        .split(SECTION_SEPARATOR)
        .filter(|section| !section.is_empty())
        .map(|section| section.split_once(NAME_SEPARATOR).unwrap_or((section, "")))
}

/// The undecoded payload of the section called `name`.
#[inline]
pub fn find_section<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    sections(header).find_map(|(section, payload)| (section == name).then_some(payload))
}

/// Decode a section payload into `W`. The error says which step failed.
pub fn decode_payload<W: DeserializeOwned>(payload: &str) -> Result<W, String> {
    let bytes = BASE64
        .decode(payload)
        .map_err(|err| format!("base64: {err}"))?;
    serde_json::from_slice(&bytes).map_err(|err| format!("json: {err}"))
}

/// Decode a section payload into `buf`, without allocating, and return the
/// decoded bytes. `None` means this decoder does not handle the payload (it is
/// not valid base64, or does not fit `buf`); [`decode_payload`] is the general
/// decoder and says why.
///
/// For a reader that handles the decoded bytes itself, on a path where a heap
/// allocation and the general decoder's setup per message matter. It accepts
/// exactly what the general decoder accepts, and decodes to the same bytes.
pub fn decode_payload_into<'b>(payload: &str, buf: &'b mut [u8]) -> Option<&'b [u8]> {
    let bytes = payload.as_bytes();
    if bytes.len() % 4 != 0 || bytes.len() / 4 * 3 > buf.len() {
        return None;
    }
    // The last group may be padded; every group before it is not.
    let (body, last) = bytes.split_at(bytes.len().saturating_sub(4));
    let mut invalid = 0;
    let mut written = 0;
    for (group, out) in body.chunks_exact(4).zip(buf.chunks_exact_mut(3)) {
        let (a, b, c, d) = (
            BASE64_VALUE[usize::from(group[0])],
            BASE64_VALUE[usize::from(group[1])],
            BASE64_VALUE[usize::from(group[2])],
            BASE64_VALUE[usize::from(group[3])],
        );
        invalid |= a | b | c | d;
        let bits = u32::from(a) << 18 | u32::from(b) << 12 | u32::from(c) << 6 | u32::from(d);
        out.copy_from_slice(&bits.to_be_bytes()[1..]);
        written += 3;
    }
    if invalid & INVALID != 0 {
        return None;
    }
    if let [c0, c1, c2, c3] = *last {
        let (a, b) = (BASE64_VALUE[usize::from(c0)], BASE64_VALUE[usize::from(c1)]);
        let (c, d) = (BASE64_VALUE[usize::from(c2)], BASE64_VALUE[usize::from(c3)]);
        let tail = &mut buf[written..];
        match (c2 == b'=', c3 == b'=') {
            (false, false) if (a | b | c | d) & INVALID == 0 => {
                let bits =
                    u32::from(a) << 18 | u32::from(b) << 12 | u32::from(c) << 6 | u32::from(d);
                tail[..3].copy_from_slice(&bits.to_be_bytes()[1..]);
                written += 3;
            }
            // Two bytes, whose last character must carry no stray bits.
            (false, true) if (a | b | c) & INVALID == 0 && c & 0b11 == 0 => {
                let bits = u32::from(a) << 18 | u32::from(b) << 12 | u32::from(c) << 6;
                tail[..2].copy_from_slice(&bits.to_be_bytes()[1..3]);
                written += 2;
            }
            (true, true) if (a | b) & INVALID == 0 && b & 0b1111 == 0 => {
                let bits = u32::from(a) << 18 | u32::from(b) << 12;
                tail[0] = bits.to_be_bytes()[1];
                written += 1;
            }
            _ => return None,
        }
    }
    Some(&buf[..written])
}

const INVALID: u8 = 0x80;

/// The value of each character of the standard base64 alphabet, or `INVALID`.
const BASE64_VALUE: [u8; 256] = {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut table = [INVALID; 256];
    let mut at = 0;
    while at < alphabet.len() {
        table[alphabet[at] as usize] = at as u8;
        at += 1;
    }
    table
};

/// Encode `wire` as a section payload.
pub fn encode_payload<W: Serialize>(wire: &W) -> Result<String, String> {
    let json = serde_json::to_vec(wire).map_err(|err| format!("json: {err}"))?;
    Ok(BASE64.encode(json))
}

/// Append `name:payload` to `header`.
#[inline]
pub fn push_section(header: &mut String, name: &str, payload: &str) {
    if !header.is_empty() {
        header.push(SECTION_SEPARATOR);
    }
    header.push_str(name);
    header.push(NAME_SEPARATOR);
    header.push_str(payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_find_sections() {
        let mut header = String::new();
        push_section(&mut header, "a", &encode_payload(&7u32).unwrap());
        push_section(&mut header, "b", &encode_payload(&"x").unwrap());
        assert_eq!(header.matches('.').count(), 1);
        assert_eq!(
            decode_payload::<u32>(find_section(&header, "a").unwrap()),
            Ok(7)
        );
        assert_eq!(
            decode_payload::<String>(find_section(&header, "b").unwrap()),
            Ok("x".to_owned())
        );
        assert_eq!(find_section(&header, "c"), None);
    }

    #[test]
    fn decode_into_agrees_with_the_general_decoder_for_every_padding() {
        for len in 0..80usize {
            let payload = encode_payload(&"x".repeat(len)).unwrap();
            let mut buf = [0u8; 128];
            let decoded = decode_payload_into(&payload, &mut buf).unwrap();
            assert_eq!(decoded, BASE64.decode(&payload).unwrap(), "{len}");
        }
    }

    /// Strings over the base64 alphabet plus `=` and a few other bytes, whatever
    /// their length or padding: the decoder accepts exactly what the general one
    /// does, and decodes it to the same bytes.
    #[test]
    fn decode_into_accepts_exactly_what_the_general_decoder_accepts() {
        const CHARS: &[u8] = b"AAAABQgz09+/=Zz-_ !\xff";
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut accepted = 0;
        for _ in 0..200_000 {
            let len = (next() % 12) as usize * 4;
            // Mostly valid characters, with `=` at the end and the odd stray byte.
            let mut text: Vec<u8> = (0..len).map(|_| CHARS[(next() % 12) as usize]).collect();
            if next() % 4 == 0 && len >= 4 {
                let pad = 1 + (next() % 2) as usize;
                text[len - pad..].fill(b'=');
            }
            if next() % 16 == 0 && len > 0 {
                text[(next() as usize) % len] = CHARS[(next() % CHARS.len() as u64) as usize];
            }
            let Ok(payload) = String::from_utf8(text) else {
                continue;
            };
            let mut buf = [0u8; 64];
            let general = BASE64.decode(&payload);
            match (decode_payload_into(&payload, &mut buf), general) {
                (Some(bytes), Ok(expected)) => {
                    assert_eq!(bytes, expected, "{payload:?}");
                    accepted += 1;
                }
                (None, Err(_)) => {}
                (mine, general) => panic!("{payload:?}: {mine:?} against {general:?}"),
            }
        }
        assert!(
            accepted > 1_000,
            "the test accepted only {accepted} payloads"
        );
    }

    #[test]
    fn decode_into_declines_what_does_not_fit_or_is_not_base64() {
        let mut buf = [0u8; 8];
        assert_eq!(decode_payload_into("!!!!", &mut buf), None);
        assert_eq!(decode_payload_into("AAA", &mut buf), None);
        assert_eq!(decode_payload_into("AA=A", &mut buf), None);
        assert_eq!(decode_payload_into("A===", &mut buf), None);
        assert_eq!(decode_payload_into("=AAA", &mut buf), None);
        assert_eq!(decode_payload_into(&"A".repeat(40), &mut buf), None);
        assert_eq!(decode_payload_into("", &mut buf), Some(&[][..]));
    }

    #[test]
    fn decode_reports_the_failing_step() {
        assert!(decode_payload::<u32>("!!")
            .unwrap_err()
            .starts_with("base64"));
        assert!(decode_payload::<u32>("Ag==")
            .unwrap_err()
            .starts_with("json"));
    }
}
