//! Pure-semantics scenarios. Each one states the rule it pins and is written
//! only against the harness API.

/// Declare a scenario: a function over a virtual world, plus a test that runs
/// it against the hooks under test. Doc comments and `cfg` attributes carry
/// over to the test.
macro_rules! scenario {
    ($(#[$meta:meta])* fn $name:ident($w:ident) $body:block) => {
        $(#[$meta])*
        mod $name {
            #[allow(unused_imports)]
            use super::*;

            fn scenario<H: $crate::harness::Hooks>($w: &mut $crate::harness::World<H>) $body

            #[test]
            fn run() {
                $crate::harness::run::<$crate::harness::Under, _>(
                    scenario::<$crate::harness::Under>,
                )
            }
        }
    };
}

mod priority;

#[cfg(feature = "abort_slo")]
mod abort_slo;

mod common;
#[cfg(feature = "estimator")]
mod model;

#[cfg(all(
    feature = "estimator",
    not(any(feature = "est_rms", feature = "est_hist", feature = "sched_oracle"))
))]
mod estimation;

#[cfg(all(
    feature = "estimator",
    any(feature = "est_rms", feature = "est_hist"),
    not(feature = "sched_oracle")
))]
mod estimator_kinds;

#[cfg(feature = "estimator")]
mod response_meta;

#[cfg(feature = "ac_pred")]
mod admission_pred;

#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
mod rajomon;

#[cfg(feature = "sched_oracle")]
mod oracle;

#[cfg(feature = "trace_queue_latency")]
mod queue_latency;

mod ordering;

#[cfg(any(
    feature = "sched_fifo",
    feature = "sched_slo",
    feature = "sched_tailclipper",
    feature = "sched_oracle"
))]
mod finalize;

#[cfg(not(any(
    feature = "sched_fifo",
    feature = "sched_slo",
    feature = "sched_tailclipper",
    feature = "sched_oracle"
)))]
mod noop;

#[cfg(feature = "abort_slack")]
mod abort_slack;

#[cfg(feature = "signal_slack")]
mod signal_slack;
