use std::fmt;

use serde::de::IgnoredAny;
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
/// `ctx` header). It is encoded as a JSON array, because it is built and
/// parsed on every RPC. Clients create one for a root request (see
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

/// The priority alone, for the slow path of [`peek_priority`]. Mirrors the
/// layout of [`ContextCompact`].
#[derive(Deserialize)]
struct PriorityPeek(
    IgnoredAny,
    IgnoredAny,
    IgnoredAny,
    IgnoredAny,
    IgnoredAny,
    PriorityHint,
);

/// Longest budget payload [`peek_priority`] scans without the heap: 340 encoded
/// characters, which holds a context whose API name is about a hundred bytes.
const SCAN_BUFFER: usize = 256;

/// The priority of a budget section's `payload`, which is an encoded
/// [`Context`]. It runs for every HTTP/2 stream, before any module, so it
/// decodes into a buffer on the stack and scans the JSON array for its last
/// element instead of deserializing it.
///
/// Anything the scan does not recognize, such as an unusually long or escaped
/// API name, whitespace, or a malformed payload, takes the general decoder,
/// which gives the same value and the same error as deserializing the whole
/// context would.
pub(crate) fn peek_priority(payload: &str) -> Result<PriorityHint, String> {
    let mut buf = [0u8; SCAN_BUFFER];
    if let Some(json) = wire::decode_payload_into(payload, &mut buf) {
        if let Some(priority) = scan_priority(json) {
            return Ok(PriorityHint::new(priority));
        }
    }
    wire::decode_payload::<PriorityPeek>(payload).map(|peek| peek.5)
}

/// The sixth element of `json`, if `json` is exactly a compact JSON array of
/// [`ContextCompact`]'s shape: a string, four non-negative integers, and the
/// priority. `None` for anything else, including valid JSON the encoder never
/// writes.
fn scan_priority(json: &[u8]) -> Option<u64> {
    let rest = json.strip_prefix(b"[\"")?;
    // The API name. Escapes and bytes outside ASCII are left to the general
    // decoder.
    let name_len = rest.iter().position(|&b| NAME_STOPS[usize::from(b)])?;
    if rest[name_len] != b'"' {
        return None;
    }
    let mut rest = &rest[name_len + 1..];
    for _ in 0..4 {
        rest = skip_integer(rest.strip_prefix(b",")?)?;
    }
    let digits = rest.strip_prefix(b",")?.strip_suffix(b"]")?;
    parse_integer(digits)
}

/// The bytes that end the scan of an API name: its closing quote, and what the
/// scan leaves to the general decoder (escapes, control characters, non-ASCII).
const NAME_STOPS: [bool; 256] = {
    let mut stops = [false; 256];
    let mut byte = 0;
    while byte < 256 {
        stops[byte] =
            byte == b'"' as usize || byte == b'\\' as usize || byte < 0x20 || byte >= 0x80;
        byte += 1;
    }
    stops
};

/// `rest` without its leading JSON integer.
fn skip_integer(rest: &[u8]) -> Option<&[u8]> {
    let len = digit_run(rest);
    if len == 0 || (len > 1 && rest[0] == b'0') {
        return None;
    }
    Some(&rest[len..])
}

/// The JSON integer that is all of `digits`, if it fits a `u64`.
fn parse_integer(digits: &[u8]) -> Option<u64> {
    if digits.is_empty()
        || (digits.len() > 1 && digits[0] == b'0')
        || digit_run(digits) != digits.len()
    {
        return None;
    }
    if digits.len() > 19 {
        // Only here can the value overflow.
        return digits.iter().try_fold(0u64, |value, &b| {
            value.checked_mul(10)?.checked_add(u64::from(b - b'0'))
        });
    }
    // The leading digits that do not fill a chunk of eight, then whole chunks.
    let (head, chunks) = digits.split_at(digits.len() % 8);
    let mut value = head
        .iter()
        .fold(0u64, |value, &b| value * 10 + u64::from(b - b'0'));
    for chunk in chunks.chunks_exact(8) {
        let chunk = u64::from_le_bytes(chunk.try_into().expect("a chunk of eight"));
        value = value * 100_000_000 + eight_digits(chunk);
    }
    Some(value)
}

/// How many ASCII digits `bytes` starts with, eight at a time.
fn digit_run(bytes: &[u8]) -> usize {
    const ZEROS: u64 = 0x3030_3030_3030_3030;
    const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
    let mut chunks = bytes.chunks_exact(8);
    let mut run = 0;
    for chunk in &mut chunks {
        let word = u64::from_le_bytes(chunk.try_into().expect("a chunk of eight"));
        // A digit's byte is 0..=9 once the `0x30` is gone; adding 0x76 sets the
        // high bit of anything above 9. A carry only leaves a byte that has
        // already set its own bit, so the lowest set bit is the first non-digit.
        let digits = word ^ ZEROS;
        let not_digit = (digits.wrapping_add(0x7676_7676_7676_7676) | digits) & HIGH_BITS;
        if not_digit != 0 {
            return run + (not_digit.trailing_zeros() / 8) as usize;
        }
        run += 8;
    }
    run + chunks
        .remainder()
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count()
}

/// The value of eight ASCII digits, as the little-endian word they were read as.
fn eight_digits(word: u64) -> u64 {
    let mut value = word & 0x0F0F_0F0F_0F0F_0F0F;
    value = (value * 10 + (value >> 8)) & 0x00FF_00FF_00FF_00FF;
    value = (value * 100 + (value >> 16)) & 0x0000_FFFF_0000_FFFF;
    (value * 10_000 + (value >> 32)) & 0xFFFF_FFFF
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
        let payload = wire::encode_payload(self).expect("a context always encodes");
        let mut header = String::new();
        wire::push_section(&mut header, BUDGET_SECTION, &payload);
        header
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(ctx: &Context) -> String {
        wire::encode_payload(ctx).unwrap()
    }

    fn encode_raw(json: &str) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(json)
    }

    /// What the general decoder reads, to compare the scan against.
    fn general(payload: &str) -> Result<PriorityHint, String> {
        wire::decode_payload::<PriorityPeek>(payload).map(|peek| peek.5)
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

        /// An integer with a random number of digits, so every width appears.
        fn integer(&mut self) -> u64 {
            self.next() >> (self.next() % 64)
        }

        fn api(&mut self) -> String {
            const PIECES: [&str; 8] = ["a", "hotel.Search", ".", "/", ",", "\"", "]", "\\"];
            (0..self.next() % 12)
                .map(|_| PIECES[(self.next() % 8) as usize])
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
        for prio in [0, 1, 9, 10, u64::MAX - 1, u64::MAX] {
            for other in [0, u64::MAX] {
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
        let json = wire::decode_payload_into(&payload(&ctx), &mut buf).unwrap();

        assert_eq!(scan_priority(json), Some(3));
    }

    #[test]
    fn what_the_scan_does_not_recognize_takes_the_general_decoder() {
        let long_name = "n".repeat(2_000);
        for json in [
            // Whitespace, escapes, non-ASCII, a name too long for the buffer.
            r#"["a",1,2,3,4, 5]"#.to_owned(),
            r#"["a\"b",1,2,3,4,5]"#.to_owned(),
            r#"["\u00e9",1,2,3,4,5]"#.to_owned(),
            "[\"caf\u{e9}\",1,2,3,4,5]".to_owned(),
            format!(r#"["{long_name}",1,2,3,4,5]"#),
            // Valid for the general decoder though the encoder never writes it.
            r#"["a",-1,2.5,3e2,"x",5]"#.to_owned(),
            r#"["a",1,2,3,4,5] "#.to_owned(),
            "[1,2,3,4,5,6]".to_owned(),
        ] {
            let payload = encode_raw(&json);
            assert_eq!(peek_priority(&payload), general(&payload), "{json}");
        }
    }

    #[test]
    fn malformed_payloads_fail_as_the_general_decoder_does() {
        for json in [
            r#"["a",1,2,3,4]"#,
            r#"["a",1,2,3,4,5,6]"#,
            r#"["a",1,2,3,4,"5"]"#,
            r#"["a",1,2,3,4,-5]"#,
            r#"["a",1,2,3,4,18446744073709551616]"#,
            r#"["a",1,2,3,4,007]"#,
            r#"{"a":1}"#,
            r#""not an array""#,
            "",
            "[",
        ] {
            let payload = encode_raw(json);
            let peeked = peek_priority(&payload);
            assert!(peeked.is_err(), "{json}");
            assert_eq!(peeked, general(&payload), "{json}");
        }
        for payload in ["not-base64", "AAA", "A", "!!!!", "=AAA"] {
            let peeked = peek_priority(payload);
            assert!(peeked.unwrap_err().starts_with("base64"), "{payload}");
        }
    }
}
