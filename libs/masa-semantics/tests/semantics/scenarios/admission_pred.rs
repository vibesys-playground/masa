//! Predictive admission control (`ac_pred`): an AIMD controller at ingress
//! and a feasibility check before each child RPC.
//!
//! Admission decisions draw random numbers that scenarios cannot seed, so
//! they compare frequencies over many draws. Every such comparison uses a
//! tolerance of at least six standard deviations of the binomial count, which
//! makes a spurious failure vanishingly unlikely (about 2e-9 per check); where
//! the expected probability is exactly 0 or 1 the check is exact.

use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tonic::{Code, Status};

use super::common::*;
use super::model::Aimd;
#[cfg(not(any(feature = "est_rms", feature = "est_hist")))]
use super::model::{MeanVar, Model};
use crate::harness::*;

scenario! {
    /// A service that has seen no overload admits every ingress request.
    fn fresh_service_admits_everything(w) {
        let svc = w.service("AcFresh");
        assert_eq!(count_admitted(w, &svc, INGRESS, 500), 500);
    }
}

scenario! {
    /// Sustained overload closes admission completely: after enough windows in
    /// which every request ended as an early return, every new ingress request
    /// is rejected with DeadlineExceeded and a PredAdmissionRej reason, and its
    /// handler never runs.
    fn sustained_overload_rejects_everything(w) {
        let svc = w.service("AcClosed");
        close_admission(w, &svc, INGRESS);
        let req = w.ingress("AcApi", dur_ms(10_000));
        let ran = std::cell::Cell::new(false);
        let reply = svc.serve(INGRESS, &req, |_h| {
            ran.set(true);
            Ok(())
        });
        assert!(!ran.get());
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(
            reply.message(),
            "/EarlyReturn?src=AcClosed::Ingress&reason=PredAdmissionRej"
        );
        assert_eq!(count_admitted(w, &svc, INGRESS, 500), 0);
    }
}

scenario! {
    /// Admission reopens as traffic goes idle: the rejection probability decays
    /// as `exp(-idle / 2 s)` since the last window closed. After a minute idle
    /// everything is admitted again; after `2 s * ln 2` about half is.
    fn idle_time_reopens_admission(w) {
        let svc = w.service("AcIdle");
        close_admission(w, &svc, INGRESS);
        let closed_at = w.now();

        w.set_now(closed_at + 1_386_294);
        assert_frequency(count_admitted(w, &svc, INGRESS, 4000), 4000, 0.5, "after 2s*ln2 idle");

        w.set_now(closed_at + ms(60_000));
        assert_eq!(count_admitted(w, &svc, INGRESS, 1000), 1000);
    }
}

scenario! {
    /// Healthy windows re-open admission additively: starting from fully
    /// closed, ten windows with no early returns raise the admission
    /// probability to 0.5 and twenty to 1.
    fn healthy_windows_recover_admission(w) {
        let svc = w.service("AcRecover");
        close_admission(w, &svc, INGRESS);
        for _ in 0..10 {
            w.advance_ms(60);
            outcome(w, &svc, INGRESS, false);
        }
        assert_frequency(count_admitted(w, &svc, INGRESS, 4000), 4000, 0.5, "after 10 healthy windows");
        for _ in 0..10 {
            w.advance_ms(60);
            outcome(w, &svc, INGRESS, false);
        }
        assert_eq!(count_admitted(w, &svc, INGRESS, 1000), 1000);
    }
}

scenario! {
    /// A request the controller itself rejected is not an outcome: rejections
    /// do not feed back as overload. After a thousand rejections the next
    /// healthy window still raises the admission probability from zero.
    fn rejections_do_not_feed_the_controller(w) {
        let svc = w.service("AcNoFeedback");
        close_admission(w, &svc, INGRESS);
        for _ in 0..100 {
            let req = w.ingress("AcApi", dur_ms(10_000));
            let reply = svc.serve(INGRESS, &req, |_h| Ok(()));
            assert_eq!(reply.code(), Code::DeadlineExceeded);
        }
        w.advance_ms(60);
        outcome(w, &svc, INGRESS, false);
        assert_frequency(count_admitted(w, &svc, INGRESS, 4000), 4000, 0.05, "after one healthy window");
    }
}

scenario! {
    /// Overload on one root API lifts its rejection probability above the
    /// global one, by at most three quarters of the difference, while a
    /// healthy root on the same service stays fully open.
    fn root_pressure_is_per_root(w) {
        let svc = w.service("AcPerRoot");
        // Globally healthy (5% early returns) but every request of RootA fails.
        for _ in 0..100 {
            for _ in 0..19 {
                outcome(w, &svc, "RootB", false);
            }
            outcome(w, &svc, "RootA", true);
            w.advance_ms(60);
            outcome(w, &svc, "RootB", false);
        }
        assert_eq!(count_admitted(w, &svc, "RootB", 1000), 1000);
        assert_frequency(count_admitted(w, &svc, "RootA", 4000), 4000, 0.25, "overloaded root");
    }
}

scenario! {
    /// Only requests entering the system are subject to admission control: a
    /// request that already travelled (hop count above zero) is admitted even
    /// when ingress admission is fully closed.
    fn only_ingress_requests_are_gated(w) {
        let svc = w.service("AcHops");
        close_admission(w, &svc, INGRESS);
        let req = w.crafted("AcApi", dur_ms(10_000)).hops(1, ("Elsewhere", "Api")).build();
        for _ in 0..200 {
            assert!(svc.accept(INGRESS, &req).poll_begin().is_ok());
        }
    }
}

/// A fresh service after one window of six successful ingress requests whose
/// single child reported `meta`.
fn service_after_window_of_subtree_reports<H: Hooks>(
    w: &World<H>,
    name: &'static str,
    meta: RespMeta,
) -> Service<H> {
    let svc = w.service(name);
    let spec = ReplySpec {
        meta,
        ..ReplySpec::default()
    };
    // Five outcomes in the window, then a sixth after 60 ms closes it.
    for step in 0..6 {
        if step == 5 {
            w.advance_ms(60);
        }
        let req = w.ingress("AcApi", dur_ms(10_000));
        let h = svc.accept(INGRESS, &req);
        assert!(h.poll_begin().is_ok());
        let _ = h
            .call_remote("AcSubtreeChild", "Down")
            .returns(Duration::from_millis(1), Reply::synthetic(spec.clone()));
        h.finalize_now(Ok(()));
    }
    svc
}

scenario! {
    /// An `Ok` request whose subtree reported an early return counts as an
    /// early return for the controller: one window of such requests makes the
    /// service start rejecting.
    fn subtree_early_returns_count_as_overload(w) {
        let meta = RespMeta { early_return_count: 1, ..RespMeta::default() };
        let svc = service_after_window_of_subtree_reports(w, "AcSubtree", meta);
        assert_frequency(count_admitted(w, &svc, INGRESS, 4000), 4000, 0.875, "admitted after one overloaded window");
    }
}

scenario! {
    /// A soft deadline signal from the subtree counts as overload too, although
    /// the request succeeded and nothing was aborted.
    fn subtree_deadline_signals_count_as_overload(w) {
        let meta = RespMeta { deadline_signal_count: 1, ..RespMeta::default() };
        let svc = service_after_window_of_subtree_reports(w, "AcSignals", meta);
        assert_frequency(count_admitted(w, &svc, INGRESS, 4000), 4000, 0.875, "admitted after one signalling window");
    }
}

scenario! {
    /// Control for the previous rule: a window of clean requests leaves
    /// admission fully open.
    fn clean_windows_keep_admission_open(w) {
        let svc = w.service("AcClean");
        for _ in 0..50 {
            w.advance_ms(60);
            outcome(w, &svc, INGRESS, false);
        }
        assert_eq!(count_admitted(w, &svc, INGRESS, 1000), 1000);
    }
}

/// A fresh service after one window of ten ingress outcomes, `er` of which
/// were early returns.
fn service_after_one_window<H: Hooks>(w: &World<H>, name: &'static str, er: usize) -> Service<H> {
    let svc = w.service(name);
    for i in 0..9 {
        outcome(w, &svc, INGRESS, i < er);
    }
    w.advance_ms(60);
    outcome(w, &svc, INGRESS, false);
    svc
}

scenario! {
    /// The window's early-return fraction must exceed 10% to count as
    /// overload: 1 early return in 10 requests is tolerated, 2 in 10 is not.
    fn overload_threshold_is_ten_percent(w) {
        let tolerated = service_after_one_window(w, "AcThreshold1", 1);
        assert_eq!(count_admitted(w, &tolerated, INGRESS, 1000), 1000);
        let over = service_after_one_window(w, "AcThreshold2", 2);
        assert_frequency(count_admitted(w, &over, INGRESS, 4000), 4000, 0.875, "admitted above threshold");
    }
}

scenario! {
    /// Over a seeded random history of outcome windows, the observed rejection
    /// frequency at each checkpoint matches the reference AIMD model.
    fn controller_follows_reference_model(w) {
        let svc = w.service("AcModel");
        let mut rng = StdRng::seed_from_u64(0x5eed_0002);
        let mut model = Aimd::new(w.now());
        for window in 0..40 {
            let er_prob = [0.0, 0.05, 0.3, 1.0][rng.gen_range(0..4)];
            let outcomes = rng.gen_range(1..8);
            for _ in 0..outcomes {
                let er = rng.gen_bool(er_prob);
                outcome(w, &svc, INGRESS, er);
                model.record(w.now(), er);
            }
            let gap_ms = rng.gen_range(20..200);
            w.advance_ms(gap_ms);
            let er = rng.gen_bool(er_prob);
            outcome(w, &svc, INGRESS, er);
            model.record(w.now(), er);
            if window % 4 == 3 {
                let p = model.reject_prob(w.now());
                let draws = 2000;
                let observed = draws - count_admitted(w, &svc, INGRESS, draws);
                if p == 0.0 || p >= 1.0 {
                    assert_eq!(observed, (p * draws as f64) as usize, "window {window}, p={p}");
                } else {
                    assert_frequency(observed, draws, p, &format!("window {window}, p={p}"));
                }
            }
        }
    }
}

scenario! {
    /// With nothing observed yet, a child call is always admitted by the
    /// feasibility check.
    fn cold_estimates_admit_child_calls(w) {
        let svc = w.service("AcBcfCold");
        let child = w.service("AcBcfColdChild");
        let p = probe(w, &svc, "Parent", dur_ms(1), &child, "Child");
        assert!(p.is_ok());
    }
}

scenario! {
    /// A child call issued with no time left is rejected even with no
    /// estimates: DeadlineExceeded naming the parent and child methods and the
    /// BeforeChildFeasibility reason. (With `abort_slo`, the deadline guard
    /// answers first; see the ordering scenarios.)
    #[cfg(not(feature = "abort_slo"))]
    fn child_call_with_no_time_left_is_rejected(w) {
        let svc = w.service("AcBcfLate");
        let child = w.service("AcBcfLateChild");
        let req = w.ingress("AcApi", dur_ms(10));
        let h = svc.accept("Parent", &req);
        w.advance_ms(10);
        let (status, untouched) = expect_rejection(h.call(&child, "Child").outbound_or_untouched());
        assert!(untouched);
        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert_eq!(
            status.message(),
            "/EarlyReturn?src=AcBcfLate::Parent?last_rpc=AcBcfLateChild::Child&reason=BeforeChildFeasibility"
        );
    }
}

fn expect_rejection<T>(r: Result<T, (Status, bool)>) -> (Status, bool) {
    match r {
        Ok(_) => panic!("the child call must be rejected"),
        Err(rejection) => rejection,
    }
}

scenario! {
    /// Once a service has seen a child take 40 ms and 10 ms of work after it,
    /// a child call from a request with only 30 ms left is rejected: even the
    /// optimistic estimate cannot fit. A generous SLO still admits it, and the
    /// rejected request's outbound request is left untouched.
    #[cfg(not(any(feature = "est_rms", feature = "est_hist")))]
    fn learned_latency_rejects_infeasible_child_calls(w) {
        let svc = w.service("AcBcfLearn");
        let child = w.service("AcBcfLearnChild");
        train(w, &svc, "Parent", &child, "Child", 40_000, 10_000);
        let req = w.ingress("AcApi", dur_ms(30));
        let h = svc.accept("Parent", &req);
        let (status, untouched) = expect_rejection(h.call(&child, "Child").outbound_or_untouched());
        assert!(untouched);
        assert_eq!(
            status.message(),
            "/EarlyReturn?src=AcBcfLearn::Parent?last_rpc=AcBcfLearnChild::Child&reason=BeforeChildFeasibility"
        );
        assert!(probe(w, &svc, "Parent", Duration::from_secs(2), &child, "Child").is_ok());
    }
}

scenario! {
    /// Stale estimates stop blocking: a child call the estimates called
    /// infeasible is still rejected a second later (the estimate has barely
    /// decayed), but admitted again after ten idle seconds, when the estimate
    /// has shrunk to a seventh of its value. This breaks the lockout where
    /// rejecting everything prevents the observations that would lift it.
    #[cfg(not(any(feature = "est_rms", feature = "est_hist")))]
    fn stale_estimates_stop_rejecting_child_calls(w) {
        let svc = w.service("AcBcfStale");
        let child = w.service("AcBcfStaleChild");
        train(w, &svc, "Parent", &child, "Child", 40_000, 10_000);
        let observed_at = w.now();
        assert!(probe(w, &svc, "Parent", dur_ms(30), &child, "Child").is_err());
        w.set_now(observed_at + 1_000_000);
        assert!(probe(w, &svc, "Parent", dur_ms(30), &child, "Child").is_err());
        w.set_now(observed_at + 10_000_000);
        assert!(probe(w, &svc, "Parent", dur_ms(30), &child, "Child").is_ok());
    }
}

scenario! {
    /// The optimistic (floor) estimate is a hard backstop, not a coin flip: a
    /// child call whose optimistic completion is even one microsecond past the
    /// deadline is rejected every time, while one that fits exactly is admitted.
    #[cfg(not(any(feature = "est_rms", feature = "est_hist")))]
    fn optimistic_overshoot_is_always_rejected(w) {
        let svc = w.service("AcBcfFloor");
        let child = w.service("AcBcfFloorChild");
        // Constant latencies: every estimate equals the observation.
        for _ in 0..3 {
            train(w, &svc, "Parent", &child, "Child", 20_000, 1_500);
        }
        for _ in 0..200 {
            let p = probe(w, &svc, "Parent", Duration::from_micros(21_499), &child, "Child");
            assert!(p.is_err(), "1 us short of the estimate must be rejected");
        }
        for _ in 0..200 {
            let p = probe(w, &svc, "Parent", Duration::from_micros(21_500), &child, "Child");
            assert!(p.is_ok(), "an exact fit must be admitted");
        }
    }
}

scenario! {
    /// Between "certainly too slow" and "fits on average" lies a probabilistic
    /// band: when the optimistic estimate fits the time left but the mean
    /// estimate overshoots by a fraction `r` of it, the call is shed with
    /// probability `1 - exp(-2r)`.
    #[cfg(not(any(feature = "est_rms", feature = "est_hist")))]
    fn overshoot_is_shed_probabilistically(w) {
        let svc = w.service("AcBcfBand");
        let child = w.service("AcBcfBandChild");
        // Child latency: 20 ms three times, then a 60 ms spike; 1 ms after it.
        let mut child_model = MeanVar::default();
        let mut after_model = MeanVar::default();
        for child_us in [20_000, 20_000, 20_000, 60_000] {
            train(w, &svc, "Parent", &child, "Child", child_us, 1_000);
            child_model.track(child_us);
            after_model.track(1_000);
        }
        assert!(child_model.floor() + after_model.floor() <= 22_000);
        assert!(child_model.full() + after_model.full() > 22_000);
        let overshoot = (child_model.full() + after_model.full() - 22_000) as f64;
        let p_shed = 1.0 - (-2.0 * overshoot / 22_000.0).exp();

        let draws = 4000;
        let shed = (0..draws)
            .filter(|_| probe(w, &svc, "Parent", dur_ms(22), &child, "Child").is_err())
            .count();
        assert_frequency(shed, draws, p_shed, "probabilistic shed");
    }
}
