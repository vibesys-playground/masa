//! Framework-owned codec for per-module wire data.
//!
//! Each policy module declares a serde `Wire` type ([`Layer::Wire`]) and a
//! unique `NAME`. This module is the only place that knows how those values
//! are laid out in the `ctx` header; modules and hooks use the typed API
//! ([`WireIn`], [`WireOut`], [`peek`], and the metadata helpers) and never
//! touch header bytes, base64 or JSON.
//!
//! # Layout
//!
//! ```text
//! ctx: <base64 bincode Context> [ . <name> : <base64 JSON> ]*
//! ```
//!
//! The `Context` blob is unchanged. Each module with wire data adds one
//! section, for example `.rajomon:eyJ0b2tlbnMiOjB9` for `{"tokens":0}`.
//! Neither `.` nor `:` is in the base64 alphabet, and module names may not
//! contain them, so the envelope is split with plain string searches.
//!
//! The layout was chosen so that each section is independently addressable:
//! finding one section scans section names only and decodes nothing, and
//! decoding section X never touches sections Y and Z. The alternative, one
//! JSON object keyed by module name, would need a full JSON scan to find any
//! key. The cost of this layout is that each payload is base64-encoded
//! separately (33% overhead on small payloads, versus one base64 pass over a
//! combined object) and the header is not human-readable without decoding.
//!
//! `masa_core::Context::from_header_string` ignores everything after the
//! first `.`, so code that reads only the context is unaffected.
//!
//! # Failure
//!
//! All binaries are assumed to be built from the same module stack, like
//! protobuf stubs. A missing section decodes to `Wire::default()`; a section
//! that does not match its module's type is a [`WireError`], which the hooks
//! turn into a panic, as for a malformed `Context`. Sections for modules not
//! in the stack are ignored.

use std::fmt;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use masa_core::{MASA_CONTEXT_HEADER, WIRE_SEPARATOR};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

use crate::layer::Layer;

/// Separates a section's module name from its payload.
const NAME_SEPARATOR: char = ':';

/// A wire section could not be read or written.
#[derive(Debug)]
pub struct WireError {
    detail: String,
}

impl WireError {
    fn new(detail: impl fmt::Display) -> Self {
        Self {
            detail: detail.to_string(),
        }
    }
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid MASA module wire data in header `{MASA_CONTEXT_HEADER}`; MASA-enabled \
             services require all binaries to be built from the same policy stack: {}",
            self.detail
        )
    }
}

impl std::error::Error for WireError {}

/// Panics unless `name` can be used as a section name.
pub(crate) fn assert_valid_name(name: &str) {
    assert!(
        !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "`Layer::NAME` {name:?} must be non-empty and contain only ASCII letters, digits, `_` and `-`"
    );
}

/// The inbound module wire sections of one message: a borrowed, undecoded
/// view of the `ctx` header.
#[derive(Debug, Clone, Copy, Default)]
pub struct WireIn<'a> {
    sections: &'a str,
}

impl<'a> WireIn<'a> {
    fn from_header_value(value: &'a str) -> Self {
        Self {
            sections: value
                .split_once(WIRE_SEPARATOR)
                .map_or("", |(_, sections)| sections),
        }
    }

    /// Split the `ctx` header of `headers` into sections. Decodes nothing; a
    /// missing header yields no sections.
    pub fn from_headers(headers: &'a http::HeaderMap) -> Result<Self, WireError> {
        match headers.get(MASA_CONTEXT_HEADER) {
            None => Ok(Self::default()),
            Some(value) => Self::from_header_bytes(value.as_bytes()),
        }
    }

    /// Split the `ctx` header of `metadata` into sections. Decodes nothing; a
    /// missing header yields no sections.
    pub fn from_metadata(metadata: &'a MetadataMap) -> Result<Self, WireError> {
        match metadata.get(MASA_CONTEXT_HEADER) {
            None => Ok(Self::default()),
            Some(value) => Self::from_header_bytes(value.as_encoded_bytes()),
        }
    }

    // Bytes rather than `to_str`: the latter re-validates every byte as
    // visible ASCII, which costs more than the split itself.
    fn from_header_bytes(value: &'a [u8]) -> Result<Self, WireError> {
        std::str::from_utf8(value)
            .map(Self::from_header_value)
            .map_err(WireError::new)
    }

    /// Module `M`'s wire data, or `None` if the sender attached none; what
    /// absence means is up to the module. Only `M`'s section is decoded.
    pub fn get<M: Layer>(&self) -> Result<Option<M::Wire>, WireError> {
        self.decode_present(M::NAME)
    }

    fn raw_section(&self, name: &str) -> Option<&'a str> {
        self.raw_sections()
            .find_map(|(section, payload)| (section == name).then_some(payload))
    }

    fn raw_sections(&self) -> impl Iterator<Item = (&'a str, &'a str)> {
        self.sections
            .split(WIRE_SEPARATOR)
            .filter(|section| !section.is_empty())
            .map(|section| section.split_once(NAME_SEPARATOR).unwrap_or((section, "")))
    }

    fn decode_present<W: DeserializeOwned>(&self, name: &str) -> Result<Option<W>, WireError> {
        let Some(payload) = self.raw_section(name) else {
            return Ok(None);
        };
        let bytes = BASE64
            .decode(payload)
            .map_err(|err| WireError::new(format_args!("{name}: base64: {err}")))?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| WireError::new(format_args!("{name}: json: {err}")))
    }
}

/// The outbound module wire sections of one message, encoded.
#[derive(Debug, Clone, Default)]
pub struct WireOut {
    sections: Vec<(String, String)>,
}

impl WireOut {
    /// An empty set of sections.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sections already present in `metadata`'s `ctx` header, kept verbatim.
    fn from_metadata(metadata: &MetadataMap) -> Result<Self, WireError> {
        let input = WireIn::from_metadata(metadata)?;
        Ok(Self {
            sections: input
                .raw_sections()
                .map(|(name, payload)| (name.to_owned(), payload.to_owned()))
                .collect(),
        })
    }

    /// Set module `M`'s wire data, replacing any earlier value.
    pub fn put<M: Layer>(&mut self, wire: &M::Wire) -> Result<(), WireError> {
        self.put_named(M::NAME, wire)
    }

    fn put_named<W: Serialize>(&mut self, name: &str, wire: &W) -> Result<(), WireError> {
        let value = serde_json::to_vec(wire)
            .map_err(|err| WireError::new(format_args!("{name}: json: {err}")))?;
        self.sections.retain(|(existing, _)| existing != name);
        self.sections.push((name.to_owned(), BASE64.encode(value)));
        Ok(())
    }

    fn header_value(&self, context: &str) -> String {
        let mut value = context.to_owned();
        for (name, payload) in &self.sections {
            value.push(WIRE_SEPARATOR);
            value.push_str(name);
            value.push(NAME_SEPARATOR);
            value.push_str(payload);
        }
        value
    }

    /// Replace the sections in `metadata`'s `ctx` header with these, keeping
    /// the context blob. Panics if the header is missing: wire data travels
    /// inside it, so the context must be attached first.
    pub fn install(&self, metadata: &mut MetadataMap) {
        replace_header(metadata, |context| self.header_value(context));
    }
}

/// Rewrite the `ctx` header of `metadata` from its context blob.
fn replace_header(metadata: &mut MetadataMap, build: impl FnOnce(&str) -> String) {
    let current = metadata
        .get(MASA_CONTEXT_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_else(|| panic!("{}", masa_core::MISSING_CONTEXT_HEADER_MESSAGE));
    let context = current
        .split_once(WIRE_SEPARATOR)
        .map_or(current, |(context, _)| context);
    let value: MetadataValue<Ascii> = build(context)
        .parse()
        .expect("base64, `.`, `:` and names are ASCII");
    metadata.insert(MASA_CONTEXT_HEADER, value);
}

/// Set the context blob of `metadata`'s `ctx` header to `context` (the output
/// of `Context::to_header_string`), keeping the wire sections already there.
pub(crate) fn set_context_part(metadata: &mut MetadataMap, context: String) {
    let sections = metadata
        .get(MASA_CONTEXT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(WIRE_SEPARATOR))
        .map(|(_, sections)| sections.to_owned());
    let value = match sections {
        Some(sections) => format!("{context}{WIRE_SEPARATOR}{sections}"),
        None => context,
    };
    let value: MetadataValue<Ascii> = value.parse().expect("base64 and `.` are ASCII");
    metadata.insert(MASA_CONTEXT_HEADER, value);
}

/// Decode only module `M`'s section from `headers`, without decoding the
/// `Context` or any other module's data. `Ok(None)` if the header or the
/// section is absent.
pub fn peek<M: Layer>(headers: &http::HeaderMap) -> Result<Option<M::Wire>, WireError> {
    WireIn::from_headers(headers)?.get::<M>()
}

/// Module `M`'s wire data in `metadata`, if any.
pub fn get_from_metadata<M: Layer>(metadata: &MetadataMap) -> Result<Option<M::Wire>, WireError> {
    WireIn::from_metadata(metadata)?.get::<M>()
}

/// Set module `M`'s wire data in `metadata`, keeping other modules' data.
/// The context must already be attached.
pub fn set_in_metadata<M: Layer>(
    metadata: &mut MetadataMap,
    wire: &M::Wire,
) -> Result<(), WireError> {
    let mut out = WireOut::from_metadata(metadata)?;
    out.put::<M>(wire)?;
    out.install(metadata);
    Ok(())
}

/// A `ctx` header value for `context` (the output of
/// `Context::to_header_string`) carrying module `M`'s wire data, for building
/// inbound requests by hand (tests, tools).
pub fn header_value_with<M: Layer>(context: &str, wire: &M::Wire) -> Result<String, WireError> {
    let mut out = WireOut::new();
    out.put::<M>(wire)?;
    Ok(out.header_value(context))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_with(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut value = "Q1RY".to_owned();
        for (name, payload) in pairs {
            value.push_str(&format!(".{name}:{payload}"));
        }
        let mut headers = http::HeaderMap::new();
        headers.insert(MASA_CONTEXT_HEADER, value.parse().unwrap());
        headers
    }

    #[test]
    fn no_sections_leaves_context_unchanged() {
        assert_eq!(WireOut::new().header_value("Q1RY"), "Q1RY");
    }

    #[test]
    fn named_sections_round_trip_and_replace() {
        let mut out = WireOut::new();
        out.put_named("a", &7u32).unwrap();
        out.put_named("b", &"x".to_string()).unwrap();
        out.put_named("a", &8u32).unwrap();
        let value = out.header_value("Q1RY");
        assert_eq!(value.matches('.').count(), 2);

        let input = WireIn::from_header_value(&value);
        assert_eq!(input.decode_present::<u32>("a").unwrap(), Some(8));
        assert_eq!(
            input.decode_present::<String>("b").unwrap().as_deref(),
            Some("x")
        );
    }

    #[test]
    fn absent_section_is_none() {
        let headers = map_with(&[]);
        let input = WireIn::from_headers(&headers).unwrap();
        assert_eq!(input.decode_present::<u64>("missing").unwrap(), None);
    }

    #[test]
    fn selective_decode_ignores_other_sections() {
        let headers = map_with(&[("good", "NDI="), ("bad", "!!not base64!!")]);
        let input = WireIn::from_headers(&headers).unwrap();
        assert_eq!(input.decode_present::<u32>("good").unwrap(), Some(42));
        assert!(input.decode_present::<u32>("bad").is_err());
    }

    #[test]
    fn names_are_validated() {
        assert_valid_name("rajomon");
        assert_valid_name("pred-admission_2");
        assert!(std::panic::catch_unwind(|| assert_valid_name("a.b")).is_err());
        assert!(std::panic::catch_unwind(|| assert_valid_name("a:b")).is_err());
        assert!(std::panic::catch_unwind(|| assert_valid_name("")).is_err());
    }
}
