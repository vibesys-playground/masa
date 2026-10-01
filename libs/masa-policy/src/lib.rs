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
mod masa_stack;
pub(crate) mod module;
/// Runtime-configurable policy parameters loaded from policy_param.json.
pub mod policy_params;
/// Method registry for mapping service/method strings to IDs.
pub mod registry;

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
pub use masa_stack::MasaStack;
pub use module::{
    root_priority, BudgetInfo, BudgetModule, ChildDeadline, ChildOutcome, ChildPriority,
    ChildState, ContextBuilder, DecisionClosed, Early, Extensions, MissingDependency, Module,
    ModuleServer, ModuleStack, Outcome, Proposal, Proposals, Rejection, Requires, ServerInit,
    Stack,
};
pub use rpcstack::wire;
pub use rpcstack::{peek, policy_stack, WireError, WireIn, WireOut};

/// Masa's built-in policy modules, for reuse in custom stacks. Each is
/// available only when its feature is enabled.
pub mod modules {
    pub use crate::module::BudgetModule;
    #[cfg(feature = "abort_slo")]
    pub use crate::module::E2eDeadlineGuardModule;
    #[cfg(feature = "estimator")]
    pub use crate::module::EstimationModule;
    #[cfg(feature = "sched_oracle")]
    pub use crate::module::OracleModule;
    #[cfg(feature = "ac_pred")]
    pub use crate::module::PredAdmissionModule;
    #[cfg(feature = "trace_queue_latency")]
    pub use crate::module::QueueLatencyModule;
    #[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
    pub use crate::module::RajomonModule;
}
#[cfg(feature = "trace_queue_latency")]
pub use module::QueueLatencyWire;
#[cfg(feature = "estimator")]
pub use module::{
    EstimationInfo, EstimationRequestWire, EstimationResponseWire, EstimationWire, RootMethod,
    SubtreeHealth,
};
pub use registry::{MethodId, MethodRegistry};

pub use policy_params::PolicyParams;

// Re-export Rajomon public items when the feature is enabled.
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
pub use module::admission::rajomon::{
    ClientTokenBucket, RajomonSharedState, RajomonWire, CLIENT_TOKEN_BUCKET, RAJOMON_STATE,
};
