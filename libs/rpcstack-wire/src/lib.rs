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
    fn decode_reports_the_failing_step() {
        assert!(decode_payload::<u32>("!!")
            .unwrap_err()
            .starts_with("base64"));
        assert!(decode_payload::<u32>("Ag==")
            .unwrap_err()
            .starts_with("json"));
    }
}
