// Masa's default policy stack.
//
// This is the only place where Cargo features choose which policy modules
// run. Each slot is either a built-in module or `()` (disabled). Modules run
// in slot order: guard → estimation → oracle → admission → queue_latency.
//
// To try a different policy, write a module implementing `Layer` and build a
// new stack with `policy_stack!` instead of editing this file.

#[cfg(feature = "abort_slo")]
type Guard = crate::layer::E2eDeadlineGuardLayer;
#[cfg(not(feature = "abort_slo"))]
type Guard = ();

#[cfg(feature = "estimator")]
type Estimation = crate::layer::EstimationLayer;
#[cfg(not(feature = "estimator"))]
type Estimation = ();

#[cfg(feature = "sched_oracle")]
type Oracle = crate::layer::OracleLayer;
#[cfg(not(feature = "sched_oracle"))]
type Oracle = ();

#[cfg(feature = "ac_pred")]
type Admission = crate::layer::PredAdmissionLayer;
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
type Admission = crate::layer::RajomonLayer;
#[cfg(not(any(feature = "ac_pred", feature = "ac_rajomon")))]
type Admission = ();

#[cfg(feature = "trace_queue_latency")]
type QueueLatency = crate::layer::QueueLatencyLayer;
#[cfg(not(feature = "trace_queue_latency"))]
type QueueLatency = ();

/// The policy stack selected by the enabled Masa features.
pub type MasaStack = crate::policy_stack![Guard, Estimation, Oracle, Admission, QueueLatency];
