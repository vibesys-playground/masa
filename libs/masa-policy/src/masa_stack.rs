// Masa's default policy stack.
//
// This is the only place where Cargo features choose which policy modules
// run. Each slot is either a built-in module or `()` (disabled). Modules run
// in slot order in pre-hooks and in the reverse order in post-hooks: budget →
// guard → estimation → oracle → admission → queue_latency. The budget module
// comes first because every other module reads the budget and the ones that
// set a child's deadline and priority overwrite what it opened; the framework
// seals the child's budget section after all of them.
//
// To try a different policy, write a module implementing `Module` and build a
// new stack with `policy_stack!` instead of editing this file.

#[cfg(feature = "abort_slo")]
type Guard = crate::module::E2eDeadlineGuardModule;
#[cfg(not(feature = "abort_slo"))]
type Guard = ();

#[cfg(feature = "estimator")]
type Estimation = crate::module::EstimationModule;
#[cfg(not(feature = "estimator"))]
type Estimation = ();

#[cfg(feature = "sched_oracle")]
type Oracle = crate::module::OracleModule;
#[cfg(not(feature = "sched_oracle"))]
type Oracle = ();

#[cfg(feature = "ac_pred")]
type Admission = crate::module::PredAdmissionModule;
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
type Admission = crate::module::RajomonModule;
#[cfg(not(any(feature = "ac_pred", feature = "ac_rajomon")))]
type Admission = ();

#[cfg(feature = "trace_queue_latency")]
type QueueLatency = crate::module::QueueLatencyModule;
#[cfg(not(feature = "trace_queue_latency"))]
type QueueLatency = ();

/// The policy stack selected by the enabled Masa features.
pub type MasaStack = crate::policy_stack![
    crate::module::BudgetModule,
    Guard,
    Estimation,
    Oracle,
    Admission,
    QueueLatency
];
