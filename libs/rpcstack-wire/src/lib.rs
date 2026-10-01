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

/// Decode the last bytes of a section payload into `buf`, without decoding the
/// rest and without allocating: the tail of the decoded bytes, at most
/// `buf.len()` of them (the whole payload if it is that short).
///
/// For a reader that needs only a value at the end of a payload's encoding.
/// It sees a suffix of the encoded bytes, so it cannot tell whether the rest of
/// the payload is valid; a caller that must know decodes the payload.
pub fn decode_payload_tail<'b>(payload: &str, buf: &'b mut [u8]) -> Result<&'b [u8], String> {
    // Padded base64 encodes each 3 bytes as a group of 4 characters, so a
    // suffix that starts on a group boundary decodes by itself.
    if payload.len() % 4 != 0 {
        return Err("base64: invalid length".to_owned());
    }
    let groups = (payload.len() / 4).min(buf.len() / 3);
    let tail = &payload.as_bytes()[payload.len() - groups * 4..];
    let len = BASE64
        .decode_slice(tail, buf)
        .map_err(|err| format!("base64: {err}"))?;
    Ok(&buf[..len])
}

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
    fn tail_decodes_only_the_last_bytes() {
        let payload = encode_payload(&"0123456789abcdefghij").unwrap();
        let mut buf = [0u8; 6];
        assert_eq!(decode_payload_tail(&payload, &mut buf).unwrap(), b"hij\"");
        let mut big = [0u8; 64];
        assert_eq!(
            decode_payload_tail(&payload, &mut big).unwrap(),
            b"\"0123456789abcdefghij\""
        );
        assert!(decode_payload_tail("abc", &mut buf).is_err());
        assert!(decode_payload_tail("!!!!", &mut buf).is_err());
        assert!(decode_payload_tail("AA=A", &mut buf).is_err());
        assert!(decode_payload_tail("A===", &mut buf).is_err());
        assert!(decode_payload_tail("=AAA", &mut buf).is_err());
        assert_eq!(decode_payload_tail("", &mut buf).unwrap(), b"");
    }

    #[test]
    fn tail_agrees_with_the_general_decoder_for_every_padding() {
        for len in 0..40usize {
            let payload = encode_payload(&"x".repeat(len)).unwrap();
            let whole = BASE64.decode(&payload).unwrap();
            let mut buf = [0u8; 12];
            let tail = decode_payload_tail(&payload, &mut buf).unwrap();
            assert!(!tail.is_empty() && whole.ends_with(tail), "{len}");
        }
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
