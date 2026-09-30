//! The histogram (`est_hist`) and RMS (`est_rms`) estimators.
//!
//! Current behavior, pinned here so a refactor cannot change it silently: these
//! estimators do not report when they last observed something, which the
//! staleness decay reads as "infinitely old". Their estimates therefore decay
//! to zero and never influence deadlines, priorities or admission, however
//! much they have learned. (Whether that is intended is a question for the
//! policy's owners; the suite records what is.)

use std::time::Duration;

use super::common::*;
#[cfg(feature = "ac_pred")]
use crate::harness::dur_ms;

/// Enough observations for either estimator to have published a value (both
/// refresh every 512).
const WARMED_UP: usize = 600;

scenario! {
    /// After many requests with 10 ms of post-child work, the child's deadline
    /// is still the parent's: the learned estimate is decayed away.
    fn learned_estimates_do_not_tighten_deadlines(w) {
        let svc = w.service("KindTightSvc");
        let child = w.service("KindTightChild");
        for _ in 0..WARMED_UP {
            train(w, &svc, "Entry", &child, "Next", 4_000, 10_000);
        }
        let p = probe(w, &svc, "Entry", Duration::from_secs(10), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), 0);
        #[cfg(feature = "sched_pred")]
        assert_eq!(p.outbound.priority, 10_000_000);
    }
}

scenario! {
    /// Predictive admission never finds a child call infeasible with these
    /// estimators, even after learning that the child takes 40 ms and 10 ms of
    /// work follow it while only 30 ms remain.
    #[cfg(feature = "ac_pred")]
    fn learned_estimates_never_reject_child_calls(w) {
        let svc = w.service("KindBcfSvc");
        let child = w.service("KindBcfChild");
        for _ in 0..WARMED_UP {
            train(w, &svc, "Entry", &child, "Next", 40_000, 10_000);
        }
        assert!(probe(w, &svc, "Entry", dur_ms(30), &child, "Next").is_ok());
    }
}
