// Masa scheduling policy implementations.
//
// This crate contains the scheduling, estimation, and admission control
// implementations for Masa. It depends on `tonic` for gRPC boundary types and
// hook traits. Tonic does not depend on this crate; `masa::DefaultHooks` selects
// policy hooks when scheduling features are enabled.

/// Agent-owned policy stack selected by the `stack_custom` feature.
#[cfg(feature = "stack_custom")]
pub mod agent;
/// Masa context extension traits and helpers.
pub mod context_ext;
mod hooks;
pub(crate) mod layer;
mod masa_stack;
/// Runtime-configurable policy parameters loaded from policy_param.json.
pub mod policy_params;
/// Method registry for mapping service/method strings to IDs.
pub mod registry;
/// Codec for per-module wire data carried in the context header.
pub mod wire;

#[cfg(feature = "stack_custom")]
pub use agent::AgentStack;
pub use context_ext::{
    get_masa_context_from_metadata, get_method_name_override_from_headers,
    get_method_name_override_from_metadata, get_service_name_override_from_headers,
    get_service_name_override_from_metadata, get_wire_from_metadata, header_string_with_wire,
    read_context, read_context_from_headers, read_priority_from_headers,
    set_masa_context_in_metadata, set_method_name_override_in_headers,
    set_service_name_override_in_headers, set_wire_in_metadata, MasaRequestExt, MasaResponseExt,
    MasaStatusExt, MASA_CONTEXT_HEADER,
};
pub use hooks::{ChildContext, ParentContext, PolicyHooks, ServerContext};
pub use layer::{
    ChildRpcContext, Extensions, Layer, LayerChild, LayerServer, MissingDependency, ServerInit,
    Stack,
};
pub use masa_stack::MasaStack;
pub use wire::{peek, WireError, WireIn, WireOut};

/// Masa's built-in policy modules, for reuse in custom stacks. Each is
/// available only when its feature is enabled.
pub mod modules {
    #[cfg(feature = "abort_slo")]
    pub use crate::layer::E2eDeadlineGuardLayer;
    #[cfg(feature = "estimator")]
    pub use crate::layer::EstimationLayer;
    #[cfg(feature = "sched_oracle")]
    pub use crate::layer::OracleLayer;
    #[cfg(feature = "ac_pred")]
    pub use crate::layer::PredAdmissionLayer;
    #[cfg(feature = "trace_queue_latency")]
    pub use crate::layer::QueueLatencyLayer;
    #[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
    pub use crate::layer::RajomonLayer;
}
#[cfg(feature = "trace_queue_latency")]
pub use layer::QueueLatencyWire;
pub use registry::{MethodId, MethodRegistry};

pub use policy_params::PolicyParams;

// Re-export Rajomon public items when the feature is enabled.
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
pub use layer::admission::rajomon::{
    ClientTokenBucket, RajomonSharedState, RajomonWire, CLIENT_TOKEN_BUCKET, RAJOMON_STATE,
};
