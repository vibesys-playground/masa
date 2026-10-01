// Admission control modules.
//
// - `predictive` (feature `ac_pred`): goodput-tracking token-bucket AC.
// - Rajomon (feature `ac_rajomon`): token-based AC with price signals, in the
//   `rajomon` crate.
//
// `ac_pred` and `ac_rajomon` are mutually exclusive (two admission
// controllers cannot coexist). Which one, if any, joins the default stack is
// decided in `masa_stack.rs`. Estimation (latency tracking, deadline
// tightening, feasibility checks) is handled by the separate
// `EstimationModule` and does not conflict with either AC module.

#[cfg(all(feature = "ac_pred", feature = "ac_rajomon"))]
compile_error!(
    "Features `ac_pred` and `ac_rajomon` are mutually exclusive. Use one admission controller at a time."
);

#[cfg(feature = "ac_pred")]
pub(crate) mod predictive;
