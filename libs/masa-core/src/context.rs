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

/// The priority of a budget section's `payload` (an encoded [`Context`]),
/// decoding nothing else and allocating nothing: it runs for every request
/// stream, before any module.
///
/// The priority is the last element of the context's JSON array
/// ([`ContextCompact`]), so only the last bytes of the payload are decoded. A
/// payload whose end is not of that shape takes the slow path, which decodes
/// the first five elements without keeping them and reports why it failed.
pub fn peek_priority(payload: &str) -> Result<PriorityHint, String> {
    // The longest priority is 20 digits, then `]`.
    let mut buf = [0u8; 24];
    if let Ok(tail) = wire::decode_payload_tail(payload, &mut buf) {
        if let Some(priority) = priority_at_end_of_array(tail) {
            return Ok(PriorityHint::new(priority));
        }
    }
    wire::decode_payload::<PriorityPeek>(payload).map(|peek| peek.5)
}

/// The number after the last `,` of `tail`, which must end with `]`.
fn priority_at_end_of_array(tail: &[u8]) -> Option<u64> {
    let digits = tail.strip_suffix(b"]")?;
    let digits = &digits[digits.iter().rposition(|&b| b == b',')? + 1..];
    if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') {
        return None;
    }
    // Nineteen digits cannot overflow a `u64`, which is every priority in
    // practice; only a twentieth digit needs the checked arithmetic.
    let mut value = 0u64;
    for &b in digits {
        let digit = u64::from(b.wrapping_sub(b'0'));
        if digit > 9 {
            return None;
        }
        value = if digits.len() < 20 {
            value * 10 + digit
        } else {
            value.checked_mul(10)?.checked_add(digit)?
        };
    }
    Some(value)
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

    #[test]
    fn peek_priority_reads_the_last_element() {
        for prio in [0, 7, 42, 1_700_000_000_123_456, u64::MAX] {
            let ctx = Context::new("test.Service/Rpc", 9, 100, 10, 110, PriorityHint::new(prio));
            assert_eq!(peek_priority(&payload(&ctx)), Ok(PriorityHint::new(prio)));
        }
    }

    #[test]
    fn peek_priority_ignores_what_the_other_elements_hold() {
        let long_api = "a, \"b\" ,]".repeat(40);
        let ctx = Context::new(
            long_api,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            PriorityHint::new(5),
        );
        assert_eq!(peek_priority(&payload(&ctx)), Ok(PriorityHint::new(5)));
    }

    #[test]
    fn peek_priority_of_a_short_payload() {
        let ctx = Context::new("", 0, 0, 0, 0, PriorityHint::new(1));
        assert_eq!(peek_priority(&payload(&ctx)), Ok(PriorityHint::new(1)));
    }

    #[test]
    fn peek_priority_falls_back_for_a_payload_that_does_not_end_like_a_context() {
        // Whitespace is valid JSON, but the encoder never emits it.
        let spaced = wire::encode_payload(&serde_json::json!(["a", 1, 2, 3, 4, 5])).unwrap();
        assert_eq!(peek_priority(&spaced), Ok(PriorityHint::new(5)));
        let spaced = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(br#"["a",1,2,3,4, 5 ]"#)
        };
        assert_eq!(peek_priority(&spaced), Ok(PriorityHint::new(5)));
    }

    #[test]
    fn peek_priority_reports_malformed_payloads() {
        assert!(peek_priority("not-base64")
            .unwrap_err()
            .starts_with("base64"));
        let short = wire::encode_payload(&"not a context").unwrap();
        assert!(peek_priority(&short).unwrap_err().starts_with("json"));
        let overflow = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .encode(br#"["a",1,2,3,4,18446744073709551616]"#)
        };
        assert!(peek_priority(&overflow).unwrap_err().starts_with("json"));
    }
}
