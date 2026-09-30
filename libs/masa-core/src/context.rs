use std::fmt;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};

use crate::{Api, Latency, PriorityHint, RequestId, Timestamp};

/// Separates the base64 bincode `Context` from the module-wire suffix in the
/// context header value. Not part of the base64 alphabet.
pub const WIRE_SEPARATOR: char = '.';

pub const MISSING_CONTEXT_HEADER_MESSAGE: &str =
    "missing MASA context header `ctx`; MASA-enabled services require clients to attach context via MASA context helpers";

pub fn invalid_context_header_metadata_message(error: impl fmt::Display) -> String {
    format!(
        "invalid MASA context header `{}`: invalid ASCII/metadata; MASA-enabled services require clients to attach context via MASA context helpers: {}",
        crate::MASA_CONTEXT_HEADER,
        error
    )
}

fn invalid_context_header_base64_message(error: impl fmt::Display) -> String {
    format!(
        "invalid MASA context header `{}`: invalid base64; MASA-enabled services require clients to attach context via MASA context helpers: {}",
        crate::MASA_CONTEXT_HEADER,
        error
    )
}

fn invalid_context_header_bincode_message(error: impl fmt::Display) -> String {
    format!(
        "invalid MASA context header `{}`: invalid bincode payload; MASA-enabled services require clients to attach context via MASA context helpers: {}",
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestContext {
    pub api: Api,
    pub request_id: RequestId,
    pub slo: Latency,
    pub gateway_entry: Timestamp,
    pub deadline: Timestamp,
    pub prio_hint: PriorityHint,
    pub frontend_elapse: Option<u64>,
}

#[cfg(feature = "estimator")]
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct EstimatorResponse {
    pub compute_time_us: u64,
    #[serde(default)]
    pub accumulated_compute_us: u64,
    pub utilization: f32,
    pub max_downstream_util: f32,
    /// Number of early returns in the subtree (this hop + all children).
    #[serde(default)]
    pub early_return_count: u32,
    /// Whether any hop in this request's subtree (this hop or any descendant)
    /// tripped its local deadline under `signal_slack`. Saturated at 1 so a
    /// single user-facing request never counts as multiple events, regardless
    /// of how many hops it traversed. Stored as `u32` for forward-compat with
    /// any future weighted use; today consumers should treat it as a boolean
    /// (`> 0`).
    #[serde(default)]
    pub deadline_signal_count: u32,
}

#[cfg(feature = "estimator")]
pub type ResponseMeta = EstimatorResponse;

#[cfg(feature = "estimator")]
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct EstimatorContext {
    #[serde(default)]
    pub response: Option<EstimatorResponse>,
}

/// Represent a Masa context.
///
/// Header serialization uses bincode, so the positional wire layout changes
/// with these feature-gated fields. Masa deployments assume all binaries are
/// built with the same feature set; invalid decodes panic immediately.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Context {
    // Core request lifecycle data.
    request: RequestContext,
    // Estimation propagation and response metadata.
    #[cfg(feature = "estimator")]
    #[serde(default)]
    pub estimator: EstimatorContext,
}

impl Default for Context {
    fn default() -> Self {
        ContextBuilder::new(Api::default(), 0).build()
    }
}

pub struct ContextBuilder {
    api: Api,
    request_id: RequestId,
    slo: Latency,
    gateway_entry: Timestamp,
    deadline: Timestamp,
    prio_hint: Option<PriorityHint>,
    frontend_elapse: Option<u64>,
    #[cfg(feature = "estimator")]
    response: Option<EstimatorResponse>,
}

impl ContextBuilder {
    pub fn new(api: impl Into<Api>, request_id: RequestId) -> Self {
        Self {
            api: api.into(),
            request_id,
            slo: 0,
            gateway_entry: 0,
            deadline: 0,
            prio_hint: None,
            frontend_elapse: None,
            #[cfg(feature = "estimator")]
            response: None,
        }
    }

    pub fn from(ctx: &Context) -> Self {
        Self {
            api: ctx.request.api.clone(),
            request_id: ctx.request.request_id,
            slo: ctx.request.slo,
            gateway_entry: ctx.request.gateway_entry,
            deadline: ctx.request.deadline,
            prio_hint: Some(ctx.request.prio_hint),
            frontend_elapse: ctx.request.frontend_elapse,
            #[cfg(feature = "estimator")]
            response: ctx.estimator.response.clone(),
        }
    }

    pub fn slo(mut self, slo: Latency) -> Self {
        self.slo = slo;
        self
    }

    pub fn gateway_entry(mut self, gateway_entry: Timestamp) -> Self {
        self.gateway_entry = gateway_entry;
        self
    }

    pub fn deadline(mut self, deadline: Timestamp) -> Self {
        self.deadline = deadline;
        self
    }

    pub fn prio_hint(mut self, prio_hint: PriorityHint) -> Self {
        self.prio_hint = Some(prio_hint);
        self
    }

    pub fn frontend_elapse(mut self, elapse: u64) -> Self {
        self.frontend_elapse = Some(elapse);
        self
    }

    #[cfg(feature = "estimator")]
    pub fn response_meta(mut self, meta: EstimatorResponse) -> Self {
        self.response = Some(meta);
        self
    }

    pub fn build(self) -> Context {
        Context {
            request: RequestContext {
                api: self.api,
                request_id: self.request_id,
                slo: self.slo,
                gateway_entry: self.gateway_entry,
                deadline: self.deadline,
                prio_hint: self.prio_hint.unwrap_or_else(|| {
                    #[cfg(feature = "sched_tailclipper")]
                    {
                        PriorityHint::new(self.gateway_entry)
                    }
                    #[cfg(not(feature = "sched_tailclipper"))]
                    {
                        #[cfg(feature = "sched_pred")]
                        {
                            PriorityHint::new(self.deadline.saturating_sub(crate::time_now()))
                        }
                        #[cfg(not(feature = "sched_pred"))]
                        {
                            PriorityHint::new(self.deadline)
                        }
                    }
                }),
                frontend_elapse: self.frontend_elapse,
            },
            #[cfg(feature = "estimator")]
            estimator: EstimatorContext {
                response: self.response,
            },
        }
    }
}

impl Context {
    /// Get the API.
    pub fn api(&self) -> &Api {
        &self.request.api
    }

    /// Get the request ID.
    pub fn request_id(&self) -> RequestId {
        self.request.request_id
    }

    /// Get the SLO.
    pub fn slo(&self) -> Latency {
        self.request.slo
    }

    /// Get the start timestamp.
    pub fn gateway_entry(&self) -> Timestamp {
        self.request.gateway_entry
    }

    /// Get the deadline.
    pub fn deadline(&self) -> Timestamp {
        self.request.deadline
    }

    /// Get the e2e deadline.
    pub fn e2e_deadline(&self) -> Timestamp {
        self.request.gateway_entry + self.request.slo
    }

    pub fn prio_hint(&self) -> PriorityHint {
        self.request.prio_hint
    }

    /// Get the frontend elapse time.
    pub fn frontend_elapse(&self) -> Option<u64> {
        self.request.frontend_elapse
    }

    /// Set the frontend elapse time.
    pub fn set_frontend_elapse(&mut self, elapse: u64) {
        self.request.frontend_elapse = Some(elapse);
    }

    /// Get request lifecycle data.
    pub fn request(&self) -> &RequestContext {
        &self.request
    }

    /// Get the response metadata.
    #[cfg(feature = "estimator")]
    pub fn response_meta(&self) -> Option<&EstimatorResponse> {
        self.estimator.response.as_ref()
    }

    /// Set the response metadata.
    #[cfg(feature = "estimator")]
    pub fn set_response_meta(&mut self, meta: EstimatorResponse) {
        self.estimator.response = Some(meta);
    }

    /// Create a new Masa context from JSON.
    pub fn from_json(json: &str) -> Self {
        serde_json::from_str(json).unwrap()
    }

    /// Convert a Masa context to JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    /// Create a new Masa context from Base64 encoded bincode.
    ///
    /// The header value may carry a module-wire suffix after a `.` (see
    /// `masa_policy::wire`); the base64 alphabet has no `.`, so the context is
    /// everything before it and the suffix is ignored here.
    pub fn from_header_string(s: &str) -> Self {
        let s = s.split_once(WIRE_SEPARATOR).map_or(s, |(ctx, _)| ctx);
        let bytes = BASE64
            .decode(s)
            .unwrap_or_else(|err| panic!("{}", invalid_context_header_base64_message(err)));
        bincode::deserialize(&bytes)
            .unwrap_or_else(|err| panic!("{}", invalid_context_header_bincode_message(err)))
    }

    /// Convert a Masa context to Base64 encoded bincode.
    pub fn to_header_string(&self) -> String {
        let bytes = bincode::serialize(&self).unwrap();
        BASE64.encode(bytes)
    }
}
