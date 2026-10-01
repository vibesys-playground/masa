//! Framework-owned codec for per-module wire data.
//!
//! Each policy module declares a serde `Wire` type ([`Module::Wire`]) and a
//! unique `NAME`. This module is the only place that knows how those values
//! are laid out in the `ctx` header; modules and hooks use the typed API
//! ([`WireIn`], [`WireOut`], [`peek`], and the metadata helpers) and never
//! touch header bytes, base64 or JSON.
//!
//! # Layout
//!
//! ```text
//! ctx: <name> : <base64 JSON> [ . <name> : <base64 JSON> ]*
//! ```
//!
//! Each module with wire data is one section, for example
//! `rajomon:eyJ0b2tlbnMiOjB9` for `{"tokens":0}`. Neither `.` nor `:` is in
//! the base64 alphabet, and module names may not contain them, so the value is
//! split with plain string searches. The primitives live in [`rpcstack_wire`],
//! which has no HTTP or gRPC dependencies, so a crate that cannot depend on
//! this one can still read a single section.
//!
//! The layout was chosen so that each section is independently addressable:
//! finding one section scans section names only and decodes nothing, and
//! decoding section X never touches sections Y and Z. The alternative, one
//! JSON object keyed by module name, would need a full JSON scan to find any
//! key. The cost of this layout is that each payload is base64-encoded
//! separately (33% overhead on small payloads, versus one base64 pass over a
//! combined object) and the header is not human-readable without decoding.
//!
//! There is nothing besides sections: every message carries exactly the
//! sections its sender's modules `put`.
//!
//! # Failure
//!
//! All binaries are assumed to be built from the same module stack, like
//! protobuf stubs. A missing section is `None`, and what that means is up to the module; a section
//! that does not match its module's type is a [`WireError`], which the hooks
//! turn into a panic, as for a malformed header. Sections for modules not
//! in the stack are ignored.

use std::borrow::Cow;
use std::fmt;

use rpcstack_wire as codec;
use rpcstack_wire::HEADER_NAME;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

use crate::module::Module;

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
            "invalid module wire data in header `{HEADER_NAME}`; all binaries of a deployment \
             must be built from the same policy stack: {}",
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
        "`Module::NAME` {name:?} must be non-empty and contain only ASCII letters, digits, `_` and `-`"
    );
}

/// The inbound module wire sections of one message: a borrowed, undecoded
/// view of the `ctx` header.
#[derive(Debug, Clone, Copy, Default)]
pub struct WireIn<'a> {
    sections: &'a str,
}

impl<'a> WireIn<'a> {
    /// Split the `ctx` header of `headers` into sections. Decodes nothing; a
    /// missing header yields no sections.
    pub fn from_headers(headers: &'a http::HeaderMap) -> Result<Self, WireError> {
        match headers.get(HEADER_NAME) {
            None => Ok(Self::default()),
            Some(value) => Self::from_header_bytes(value.as_bytes()),
        }
    }

    /// Split the `ctx` header of `metadata` into sections. Decodes nothing; a
    /// missing header yields no sections.
    pub fn from_metadata(metadata: &'a MetadataMap) -> Result<Self, WireError> {
        match metadata.get(HEADER_NAME) {
            None => Ok(Self::default()),
            Some(value) => Self::from_header_bytes(value.as_encoded_bytes()),
        }
    }

    // Bytes rather than `to_str`: the latter re-validates every byte as
    // visible ASCII, which costs more than the split itself.
    fn from_header_bytes(value: &'a [u8]) -> Result<Self, WireError> {
        std::str::from_utf8(value)
            .map(|sections| Self { sections })
            .map_err(WireError::new)
    }

    /// Module `M`'s wire data, or `None` if the sender attached none; what
    /// absence means is up to the module. Only `M`'s section is decoded.
    pub fn get<M: Module>(&self) -> Result<Option<M::Wire>, WireError> {
        self.decode_present(M::NAME)
    }

    /// Module `M`'s section exactly as the sender encoded it, or `None` if the
    /// sender attached none. Together with [`WireOut::put_encoded`] it
    /// forwards a section without decoding and re-encoding it.
    pub fn get_encoded<M: Module>(&self) -> Option<&'a str> {
        codec::find_section(self.sections, M::NAME)
    }

    fn decode_present<W: DeserializeOwned>(&self, name: &str) -> Result<Option<W>, WireError> {
        let Some(payload) = codec::find_section(self.sections, name) else {
            return Ok(None);
        };
        codec::decode_payload(payload)
            .map(Some)
            .map_err(|err| WireError::new(format_args!("{name}: {err}")))
    }
}

/// The outbound module wire sections of one message, encoded.
#[derive(Debug, Clone, Default)]
pub struct WireOut {
    sections: Vec<(Cow<'static, str>, String)>,
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
            sections: codec::sections(input.sections)
                .map(|(name, payload)| (Cow::Owned(name.to_owned()), payload.to_owned()))
                .collect(),
        })
    }

    /// Set module `M`'s wire data, replacing any earlier value.
    pub fn put<M: Module>(&mut self, wire: &M::Wire) -> Result<(), WireError> {
        self.put_named(M::NAME, wire)
    }

    /// Set module `M`'s section to `payload`, an encoded payload obtained
    /// from [`WireIn::get_encoded`] for a section of the same type. Nothing
    /// is validated: a payload that is not `M::Wire` is a [`WireError`] for
    /// whoever reads it.
    pub fn put_encoded<M: Module>(&mut self, payload: &str) {
        self.set(Cow::Borrowed(M::NAME), payload.to_owned());
    }

    fn put_named<W: Serialize>(&mut self, name: &'static str, wire: &W) -> Result<(), WireError> {
        let payload = codec::encode_payload(wire)
            .map_err(|err| WireError::new(format_args!("{name}: {err}")))?;
        self.set(Cow::Borrowed(name), payload);
        Ok(())
    }

    fn set(&mut self, name: Cow<'static, str>, payload: String) {
        match self
            .sections
            .iter_mut()
            .find(|(existing, _)| *existing == name)
        {
            Some(section) => section.1 = payload,
            None => self.sections.push((name, payload)),
        }
    }

    /// The `ctx` header value these sections make.
    pub fn header_value(&self) -> String {
        let len = self
            .sections
            .iter()
            .map(|(name, payload)| name.len() + payload.len() + 2)
            .sum();
        let mut value = String::with_capacity(len);
        for (name, payload) in &self.sections {
            codec::push_section(&mut value, name, payload);
        }
        value
    }

    /// Make these sections the `ctx` header of `metadata`, replacing whatever
    /// it carried; with no sections the header is removed.
    pub fn install(&self, metadata: &mut MetadataMap) {
        if self.sections.is_empty() {
            metadata.remove(HEADER_NAME);
            return;
        }
        let value: MetadataValue<Ascii> = self
            .header_value()
            .parse()
            .expect("base64, `.`, `:` and names are ASCII");
        metadata.insert(HEADER_NAME, value);
    }
}

/// Decode only module `M`'s section from `headers`, without decoding any
/// other module's data. `Ok(None)` if the header or the section is absent.
pub fn peek<M: Module>(headers: &http::HeaderMap) -> Result<Option<M::Wire>, WireError> {
    WireIn::from_headers(headers)?.get::<M>()
}

/// Module `M`'s wire data in `metadata`, if any.
pub fn get_from_metadata<M: Module>(metadata: &MetadataMap) -> Result<Option<M::Wire>, WireError> {
    WireIn::from_metadata(metadata)?.get::<M>()
}

/// Set module `M`'s wire data in `metadata`, keeping other modules' data.
pub fn set_in_metadata<M: Module>(
    metadata: &mut MetadataMap,
    wire: &M::Wire,
) -> Result<(), WireError> {
    let mut out = WireOut::from_metadata(metadata)?;
    out.put::<M>(wire)?;
    out.install(metadata);
    Ok(())
}

/// A `ctx` header value carrying module `M`'s wire data, for building inbound
/// requests by hand (tests, tools).
pub fn header_value_with<M: Module>(wire: &M::Wire) -> Result<String, WireError> {
    let mut out = WireOut::new();
    out.put::<M>(wire)?;
    Ok(out.header_value())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_with(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut value = String::new();
        for (name, payload) in pairs {
            codec::push_section(&mut value, name, payload);
        }
        let mut headers = http::HeaderMap::new();
        headers.insert(HEADER_NAME, value.parse().unwrap());
        headers
    }

    #[test]
    fn no_sections_is_an_empty_header() {
        assert_eq!(WireOut::new().header_value(), "");
        let mut metadata = MetadataMap::new();
        metadata.insert(HEADER_NAME, "a:NQ==".parse().unwrap());
        WireOut::new().install(&mut metadata);
        assert!(metadata.get(HEADER_NAME).is_none());
    }

    #[test]
    fn named_sections_round_trip_and_replace() {
        let mut out = WireOut::new();
        out.put_named("a", &7u32).unwrap();
        out.put_named("b", &"x".to_string()).unwrap();
        out.put_named("a", &8u32).unwrap();
        let value = out.header_value();
        assert_eq!(value.matches('.').count(), 1);

        let input = WireIn { sections: &value };
        assert_eq!(input.decode_present::<u32>("a").unwrap(), Some(8));
        assert_eq!(
            input.decode_present::<String>("b").unwrap().as_deref(),
            Some("x")
        );
    }

    #[test]
    fn install_replaces_the_previous_header() {
        let mut metadata = MetadataMap::new();
        metadata.insert(HEADER_NAME, "old:NQ==".parse().unwrap());
        let mut out = WireOut::new();
        out.put_named("new", &1u32).unwrap();
        out.install(&mut metadata);
        let value = metadata.get(HEADER_NAME).unwrap().to_str().unwrap();
        assert!(value.starts_with("new:") && !value.contains("old"));
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
