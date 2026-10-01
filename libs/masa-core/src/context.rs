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

/// The priority alone, for readers that run before any module and must not
/// decode the rest (see [`read_priority_from_headers`](crate::read_priority_from_headers)).
/// Mirrors the layout of [`ContextCompact`].
#[derive(Deserialize)]
pub(crate) struct PriorityPeek(
    IgnoredAny,
    IgnoredAny,
    IgnoredAny,
    IgnoredAny,
    IgnoredAny,
    pub(crate) PriorityHint,
);

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
