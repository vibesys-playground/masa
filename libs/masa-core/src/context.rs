use std::fmt;

use serde::{Deserialize, Serialize};

use crate::wire;
use crate::{Api, Latency, PriorityHint, RequestId, Timestamp};

/// Name of the budget section in the `ctx` header: the wire data of Masa's
/// budget module, which is the [`Context`].
pub const BUDGET_SECTION: &str = "budget";

pub const MISSING_CONTEXT_HEADER_MESSAGE: &str =
    "missing MASA context header `ctx`; MASA-enabled services require clients to attach context via MASA context helpers";

pub fn invalid_context_header_metadata_message(error: impl fmt::Display) -> String {
    format!(
        "invalid MASA context header `{}`: invalid ASCII/metadata; MASA-enabled services require clients to attach context via MASA context helpers: {}",
        crate::MASA_CONTEXT_HEADER,
        error
    )
}

pub fn missing_budget_section_message() -> String {
    format!(
        "MASA context header `{}` has no `{BUDGET_SECTION}` section; MASA-enabled services require clients to attach context via MASA context helpers",
        crate::MASA_CONTEXT_HEADER
    )
}

pub fn invalid_budget_section_message(error: impl fmt::Display) -> String {
    format!(
        "invalid MASA context header `{}`: invalid `{BUDGET_SECTION}` section; MASA-enabled services require clients to attach context via MASA context helpers: {}",
        crate::MASA_CONTEXT_HEADER,
        error
    )
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "duration")]
pub enum FutureSpan {
    #[serde(rename = "compute")]
    Compute(u64),
    #[serde(rename = "local_block")]
    LocalBlock(u64),
    #[serde(rename = "child_block")]
    ChildBlock(u64),
    #[serde(rename = "queue")]
    Queueing(u64),
}

/// A request's time-budget facts: who asked for what, when it entered the
/// system, how long it may take, and the deadline and priority this hop's
/// scheduler uses.
///
/// This is the wire data of Masa's budget module (the `budget` section of the
/// `ctx` header). It is a tuple on the wire, in field order, because it is
/// built and parsed on every RPC: the API as a length-prefixed string, then the
/// request ID, SLO, gateway entry, deadline and priority as variable-length
/// integers (see `rpcstack-wire`). Clients create one for a root request (see
/// `masa_policy::ContextBuilder`); after that, the budget module derives each
/// child's from its parent's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "ContextCompact", into = "ContextCompact")]
pub struct Context {
    api: Api,
    request_id: RequestId,
    slo: Latency,
    gateway_entry: Timestamp,
    deadline: Timestamp,
    prio_hint: PriorityHint,
}

#[derive(Serialize, Deserialize)]
struct ContextCompact(Api, RequestId, Latency, Timestamp, Timestamp, PriorityHint);

impl From<Context> for ContextCompact {
    fn from(ctx: Context) -> Self {
        Self(
            ctx.api,
            ctx.request_id,
            ctx.slo,
            ctx.gateway_entry,
            ctx.deadline,
            ctx.prio_hint,
        )
    }
}

impl From<ContextCompact> for Context {
    fn from(compact: ContextCompact) -> Self {
        Self {
            api: compact.0,
            request_id: compact.1,
            slo: compact.2,
            gateway_entry: compact.3,
            deadline: compact.4,
            prio_hint: compact.5,
        }
    }
}

/// Longest budget payload [`peek_priority`] scans without the heap: 256 encoded
/// characters, which hold a context whose API name is about 150 bytes.
const SCAN_BUFFER: usize = 192;

/// The priority of a budget section's `payload`, which is an encoded
/// [`Context`]. It runs for every HTTP/2 stream, before any module, so it
/// decodes into a buffer on the stack and reads the priority at the end of the
/// tuple without building the context.
///
/// Anything the scan does not recognize, such as an unusually long or
/// non-ASCII API name, trailing bytes, or a malformed payload, takes the general
/// decoder, which gives the same value and the same error as decoding the whole
/// context would.
pub(crate) fn peek_priority(payload: &str) -> Result<PriorityHint, String> {
    let mut buf = [0u8; SCAN_BUFFER];
    if let Some(bytes) = wire::decode_payload_into(payload, &mut buf) {
        if let Some(priority) = scan_priority(bytes) {
            return Ok(PriorityHint::new(priority));
        }
    }
    wire::decode_payload::<ContextCompact>(payload).map(|compact| compact.5)
}

/// The priority of `bytes`, if `bytes` is exactly a [`ContextCompact`] whose API
/// name is ASCII. `None` for anything else, including what the general decoder
/// accepts but the encoder never writes.
fn scan_priority(bytes: &[u8]) -> Option<u64> {
    let (name_len, rest) = read_varint(bytes)?;
    let (name, mut rest) = rest.split_at_checked(usize::try_from(name_len).ok()?)?;
    // The name is skipped, not read, so what the general decoder rejects (bytes
    // that are not UTF-8) has to be ruled out here.
    if !name.is_ascii() {
        return None;
    }
    for _ in 0..4 {
        rest = read_varint(rest)?.1;
    }
    let (priority, rest) = read_varint(rest)?;
    rest.is_empty().then_some(priority)
}

/// A `u64` as `bincode` writes it with variable-length integers: one byte below
/// 251, else a marker byte (251, 252, 253) followed by the value in 2, 4 or 8
/// little-endian bytes. Returns the value and what follows it.
fn read_varint(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let (&marker, rest) = bytes.split_first()?;
    let width = match marker {
        0..=250 => return Some((u64::from(marker), rest)),
        251 => 2,
        252 => 4,
        253 => 8,
        _ => return None,
    };
    let (value, rest) = rest.split_at_checked(width)?;
    let mut le = [0u8; 8];
    le[..width].copy_from_slice(value);
    Some((u64::from_le_bytes(le), rest))
}

impl Default for Context {
    fn default() -> Self {
        Self::new(Api::default(), 0, 0, 0, 0, PriorityHint::infra())
    }
}

impl Context {
    pub fn new(
        api: impl Into<Api>,
        request_id: RequestId,
        slo: Latency,
        gateway_entry: Timestamp,
        deadline: Timestamp,
        prio_hint: PriorityHint,
    ) -> Self {
        Self {
            api: api.into(),
            request_id,
            slo,
            gateway_entry,
            deadline,
            prio_hint,
        }
    }

    /// Get the API.
    pub fn api(&self) -> &Api {
        &self.api
    }

    /// Get the request ID.
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Get the SLO.
    pub fn slo(&self) -> Latency {
        self.slo
    }

    /// Get the start timestamp.
    pub fn gateway_entry(&self) -> Timestamp {
        self.gateway_entry
    }

    /// Get the deadline.
    pub fn deadline(&self) -> Timestamp {
        self.deadline
    }

    /// Get the e2e deadline.
    pub fn e2e_deadline(&self) -> Timestamp {
        self.gateway_entry + self.slo
    }

    pub fn prio_hint(&self) -> PriorityHint {
        self.prio_hint
    }

    /// Decode a context from a `ctx` header value, reading only its budget
    /// section. Panics if it is missing or malformed: Masa deployments assume
    /// all binaries are built from the same code.
    pub fn from_header_string(s: &str) -> Self {
        let payload = wire::find_section(s, BUDGET_SECTION)
            .unwrap_or_else(|| panic!("{}", missing_budget_section_message()));
        wire::decode_payload(payload)
            .unwrap_or_else(|err| panic!("{}", invalid_budget_section_message(err)))
    }

    /// A `ctx` header value carrying this context as its budget section.
    pub fn to_header_string(&self) -> String {
        wire::encode_payload_with(self, |payload| {
            let mut header = Vec::with_capacity(BUDGET_SECTION.len() + 1 + payload.len());
            header.extend_from_slice(BUDGET_SECTION.as_bytes());
            header.push(wire::NAME_SEPARATOR as u8);
            header.extend_from_slice(payload);
            String::from_utf8(header).expect("a section name and base64 are ASCII")
        })
        .expect("a context always encodes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    fn payload(ctx: &Context) -> String {
        wire::encode_payload(ctx).unwrap()
    }

    fn encode_raw(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// What the general decoder reads, to compare the scan against.
    fn general(payload: &str) -> Result<PriorityHint, String> {
        wire::decode_payload::<ContextCompact>(payload).map(|compact| compact.5)
    }

    /// A deterministic pseudo-random sequence (SplitMix64), so the test needs
    /// no dependency and a failure is reproducible.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        /// An integer with a random number of bits, so every width appears.
        fn integer(&mut self) -> u64 {
            self.next() >> (self.next() % 64)
        }

        fn api(&mut self) -> String {
            const PIECES: [&str; 9] = [
                "a",
                "hotel.Search",
                ".",
                "/",
                ",",
                "\"",
                "]",
                "\\",
                "\u{e9}",
            ];
            (0..self.next() % 12)
                .map(|_| PIECES[(self.next() % 9) as usize])
                .collect()
        }
    }

    #[test]
    fn scan_agrees_with_the_general_decoder_on_the_encoders_output() {
        let mut rng = Rng(1);
        for _ in 0..20_000 {
            let ctx = Context::new(
                rng.api(),
                rng.integer(),
                rng.integer(),
                rng.integer(),
                rng.integer(),
                PriorityHint::new(rng.integer()),
            );
            let payload = payload(&ctx);
            assert_eq!(peek_priority(&payload), Ok(ctx.prio_hint()), "{ctx:?}");
            assert_eq!(peek_priority(&payload), general(&payload));
        }
    }

    #[test]
    fn scan_handles_the_extreme_values() {
        for prio in [
            0,
            1,
            250,
            251,
            65_535,
            65_536,
            u64::from(u32::MAX),
            u64::MAX,
        ] {
            for other in [0, 250, 251, u64::MAX] {
                let ctx = Context::new(
                    "svc/Rpc",
                    other,
                    other,
                    other,
                    other,
                    PriorityHint::new(prio),
                );
                assert_eq!(peek_priority(&payload(&ctx)), Ok(PriorityHint::new(prio)));
            }
        }
    }

    #[test]
    fn scan_takes_the_fast_path_for_what_the_encoder_writes() {
        let ctx = Context::new(
            "hotel.SearchHotels",
            42,
            500_000,
            1,
            2,
            PriorityHint::new(3),
        );
        let mut buf = [0u8; SCAN_BUFFER];
        let bytes = wire::decode_payload_into(&payload(&ctx), &mut buf).unwrap();

        assert_eq!(scan_priority(bytes), Some(3));
    }

    #[test]
    fn what_the_scan_does_not_recognize_takes_the_general_decoder() {
        let long = Context::new("n".repeat(2_000), 1, 2, 3, 4, PriorityHint::new(5));
        let non_ascii = Context::new("caf\u{e9}", 1, 2, 3, 4, PriorityHint::new(5));
        for payload in [payload(&long), payload(&non_ascii)] {
            assert_eq!(peek_priority(&payload), Ok(PriorityHint::new(5)));
        }
        let mut buf = [0u8; SCAN_BUFFER];
        let bytes = wire::decode_payload_into(&payload(&non_ascii), &mut buf).unwrap();
        assert_eq!(scan_priority(bytes), None);
    }

    /// Bytes that the encoder never writes, which the scan must treat exactly as
    /// the general decoder does: the same value where it accepts them, an error
    /// where it does not.
    #[test]
    fn scan_agrees_with_the_general_decoder_on_what_the_encoder_never_writes() {
        let name = [1u8, b'a'];
        let cases: Vec<Vec<u8>> = vec![
            // A value in a wider form than it needs.
            [&name[..], &[251, 5, 0], &[0, 0, 0], &[7]].concat(),
            [&name[..], &[0, 0, 0, 0], &[252, 7, 0, 0, 0]].concat(),
            [&name[..], &[0, 0, 0, 0], &[253, 7, 0, 0, 0, 0, 0, 0, 0]].concat(),
            // Markers that are not a u64.
            [&name[..], &[0, 0, 0, 0], &[254]].concat(),
            [&name[..], &[0, 0, 0, 0], &[255]].concat(),
            // Cut short, or followed by more.
            [&name[..], &[0, 0, 0, 0]].concat(),
            [&name[..], &[0, 0, 0, 0], &[251, 1]].concat(),
            [&name[..], &[0, 0, 0, 0], &[7, 0]].concat(),
            // A name that is not UTF-8, or longer than the payload.
            vec![1, 0xff, 0, 0, 0, 0, 7],
            vec![200, b'a', 0, 0, 0, 0, 7],
            vec![],
        ];
        for bytes in cases {
            let payload = encode_raw(&bytes);
            assert_eq!(peek_priority(&payload), general(&payload), "{bytes:?}");
        }
    }

    #[test]
    fn malformed_payloads_fail_as_the_general_decoder_does() {
        for bytes in [
            &[][..],
            &[0, 0, 0, 0, 0][..],
            &[1, b'a', 7][..],
            &[0, 7][..],
        ] {
            let payload = encode_raw(bytes);
            let peeked = peek_priority(&payload);
            assert!(peeked.is_err(), "{bytes:?}");
            assert_eq!(peeked, general(&payload), "{bytes:?}");
        }
        for payload in ["not-base64", "AAA", "A", "!!!!", "=AAA"] {
            let peeked = peek_priority(payload);
            assert!(peeked.unwrap_err().starts_with("base64"), "{payload}");
        }
    }
}
