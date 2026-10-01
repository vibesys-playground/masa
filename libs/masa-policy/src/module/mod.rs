// Masa's policy modules, written against the `rpcstack` module framework.
//
// The framework (the `Module` trait, stack composition, extensions, decision
// points, outcomes and dependency declarations) lives in `rpcstack`; this
// directory holds only Masa's own modules.
//
// Masa's built-in modules:
// - **Budget** (always): `BudgetModule` — the request's facts and time budget,
//   and the child request's budget section.
// - **Guard** (feature `abort_slo`): `E2eDeadlineGuardModule` — rejects
//   past-deadline requests.
// - **Estimation** (feature `estimator`): `EstimationModule` — latency tracking,
//   deadline tightening, reprioritization, feasibility checks.
// - **Oracle** (feature `sched_oracle`): `OracleModule` — perfect-information
//   child deadline and priority assignment for synthetic experiments.
// - **Admission**: `predictive` (feature `ac_pred`) or `rajomon`
//   (feature `ac_rajomon`).
// - **Observer** (feature `trace_queue_latency`): `QueueLatencyModule`.
//
// Which modules make up the default stack is decided in `masa_stack.rs`.
// All dispatch is monomorphic — zero runtime cost.

pub(crate) mod admission;

#[cfg(feature = "estimator")]
pub(crate) mod est;

mod budget;
#[cfg(feature = "abort_slo")]
mod e2e_deadline_guard;
#[cfg(feature = "sched_oracle")]
mod oracle;
#[cfg(feature = "trace_queue_latency")]
mod queue_latency;

pub use rpcstack::{
    ChildOutcome, ChildState, DecisionClosed, Early, Extensions, MissingDependency, Module,
    ModuleServer, ModuleStack, Outcome, Proposal, Proposals, Rejection, Requires, ServerInit,
    Stack,
};

#[cfg(feature = "ac_pred")]
pub use admission::predictive::PredAdmissionModule;
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
pub use admission::rajomon::RajomonModule;
pub use budget::{
    root_priority, BudgetInfo, BudgetModule, ChildDeadline, ChildPriority, ContextBuilder,
};
#[cfg(feature = "abort_slo")]
pub use e2e_deadline_guard::E2eDeadlineGuardModule;
#[cfg(feature = "estimator")]
pub use est::{
    EstimationInfo, EstimationModule, EstimationRequestWire, EstimationResponseWire,
    EstimationWire, RootMethod, SubtreeHealth,
};
#[cfg(feature = "sched_oracle")]
pub use oracle::OracleModule;
#[cfg(feature = "trace_queue_latency")]
pub use queue_latency::{QueueLatencyModule, QueueLatencyWire};
