//! Latency estimation: what a service learns from finished requests and how it
//! uses that to tighten the deadline and priority it hands to children.
//!
//! These scenarios assume the default (`est_mean_var`) estimator; the other
//! kinds are covered in `estimator_kinds`.

use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tonic::{Code, Status};

use super::common::*;
use super::model::{MeanVar, Model};
use crate::harness::*;

/// Microseconds a stale estimate shrinks over: estimates older than 200 ms
/// are scaled by `exp(-age / 5 s)`, and by zero once that exponent reaches 50.
fn decayed(estimate: u64, age_us: u64) -> u64 {
    let factor = if age_us <= 200_000 {
        1.0
    } else {
        let x = age_us as f64 / 5_000_000.0;
        if x >= 50.0 {
            0.0
        } else {
            (-x).exp()
        }
    };
    (estimate as f64 * factor) as u64
}

/// How much earlier than the parent's deadline a child's hard deadline is,
/// given the estimator state: the conservative (floor) estimate, except that
/// `deadline_equals_slack` ties it to the soft estimate used for priority.
fn expected_tightening(model: &MeanVar) -> u64 {
    if cfg!(feature = "deadline_equals_slack") {
        model.full()
    } else {
        model.floor()
    }
}

scenario! {
    /// A service that receives a request straight from the client (hop 0)
    /// records itself as the request's root method; its child receives hop
    /// count 1 and that root.
    fn ingress_service_becomes_root_method(w) {
        let svc = w.service("EstRootSvc");
        let child = w.service("EstRootChild");
        let p = probe(w, &svc, "Entry", dur_ms(100), &child, "Next").unwrap();
        assert_eq!(p.inbound.hop_count, Some(0));
        assert_eq!(p.inbound.root_method, None);
        assert_eq!(p.outbound.hop_count, Some(1));
        assert_eq!(
            p.outbound.root_method,
            Some(("EstRootSvc".to_string(), "Entry".to_string()))
        );
    }
}

scenario! {
    /// A request that already travelled keeps the root method it was given,
    /// and each hop adds exactly one to the hop count.
    fn inherited_root_method_is_never_replaced(w) {
        let svc = w.service("EstInheritSvc");
        let child = w.service("EstInheritChild");
        let req = w
            .crafted("EstApi", dur_ms(100))
            .hops(2, ("OriginSvc", "OriginApi"))
            .build();
        let p = probe_request(&svc, "Mid", &req, &child, "Next").unwrap();
        assert_eq!(p.outbound.hop_count, Some(3));
        assert_eq!(
            p.outbound.root_method,
            Some(("OriginSvc".to_string(), "OriginApi".to_string()))
        );
    }
}

scenario! {
    /// The hop count saturates instead of wrapping around.
    fn hop_count_saturates(w) {
        let svc = w.service("EstSatSvc");
        let child = w.service("EstSatChild");
        let req = w.crafted("EstApi", dur_ms(100)).hops(255, ("O", "A")).build();
        let p = probe_request(&svc, "Mid", &req, &child, "Next").unwrap();
        assert_eq!(p.outbound.hop_count, Some(255));
    }
}

scenario! {
    /// A service that has observed nothing passes the parent's deadline to its
    /// child unchanged, and (without slack-based scheduling) the parent's
    /// priority too.
    fn cold_service_passes_deadline_through(w) {
        let svc = w.service("EstColdSvc");
        let child = w.service("EstColdChild");
        let p = probe(w, &svc, "Entry", dur_ms(100), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), 0);
        #[cfg(not(feature = "sched_pred"))]
        assert_eq!(p.outbound.priority, p.inbound.priority);
        #[cfg(feature = "sched_pred")]
        assert_eq!(p.outbound.priority, ms(100));
    }
}

scenario! {
    /// After one finished request that spent 10 ms after its child returned,
    /// the child's deadline is 10 ms earlier than the parent's: the child must
    /// finish early enough to leave the parent its post-child work.
    fn post_child_work_tightens_child_deadline(w) {
        let svc = w.service("EstTightSvc");
        let child = w.service("EstTightChild");
        train(w, &svc, "Entry", &child, "Next", 4_000, 10_000);
        let p = probe(w, &svc, "Entry", Duration::from_secs(10), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), 10_000);
        #[cfg(feature = "sched_pred")]
        {
            // Priority becomes the slack left at the child: time to the
            // tightened deadline.
            assert_eq!(p.outbound.priority, 10_000_000 - 10_000);
        }
        #[cfg(not(feature = "sched_pred"))]
        assert_eq!(p.outbound.priority, p.inbound.priority);
    }
}

scenario! {
    /// Over a seeded random sequence of requests with varying post-child work,
    /// the tightening a service applies always equals what the reference
    /// estimator model predicts, and the child deadline never exceeds the
    /// parent's.
    fn tightening_follows_reference_model(w) {
        let svc = w.service("EstModelSvc");
        let child = w.service("EstModelChild");
        let mut rng = StdRng::seed_from_u64(0x5eed_0001);
        let mut model = MeanVar::default();
        for step in 0..300 {
            let child_us = rng.gen_range(1_000..30_000);
            let after_us = rng.gen_range(500..60_000);
            train(w, &svc, "Entry", &child, "Next", child_us, after_us);
            model.track(after_us);
            if step % 7 == 0 {
                let p = probe(w, &svc, "Entry", Duration::from_secs(10), &child, "Next").unwrap();
                assert_eq!(
                    p.deadline_tightening(),
                    expected_tightening(&model),
                    "step {step}"
                );
                assert!(p.outbound.deadline <= p.inbound.deadline);
                #[cfg(feature = "sched_pred")]
                assert_eq!(
                    p.outbound.priority,
                    10_000_000 - model.full(),
                    "step {step}"
                );
            }
        }
    }
}

scenario! {
    /// The soft estimate (priority) and the hard estimate (deadline) differ:
    /// after a latency spike the conservative estimate moves less, so hard
    /// deadlines are tightened by less than the priority signal implies.
    #[cfg(not(feature = "deadline_equals_slack"))]
    fn hard_deadline_uses_the_conservative_estimate(w) {
        let svc = w.service("EstFloorSvc");
        let child = w.service("EstFloorChild");
        for after_us in [10_000, 10_000, 30_000] {
            train(w, &svc, "Entry", &child, "Next", 1_000, after_us);
        }
        let mut model = MeanVar::default();
        for after_us in [10_000, 10_000, 30_000] {
            model.track(after_us);
        }
        assert!(model.floor() < model.full());
        let p = probe(w, &svc, "Entry", Duration::from_secs(10), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), model.floor());
        #[cfg(feature = "sched_pred")]
        assert_eq!(p.outbound.priority, 10_000_000 - model.full());
    }
}

scenario! {
    /// With `deadline_equals_slack` the hard deadline is tightened by the same
    /// estimate that drives priority, instead of the conservative one.
    #[cfg(feature = "deadline_equals_slack")]
    fn deadline_equals_slack_uses_the_soft_estimate(w) {
        let svc = w.service("EstTiedSvc");
        let child = w.service("EstTiedChild");
        let mut model = MeanVar::default();
        for after_us in [10_000, 10_000, 30_000] {
            train(w, &svc, "Entry", &child, "Next", 1_000, after_us);
            model.track(after_us);
        }
        assert!(model.floor() < model.full());
        let p = probe(w, &svc, "Entry", Duration::from_secs(10), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), model.full());
        assert_eq!(p.outbound.priority, 10_000_000 - model.full());
    }
}

scenario! {
    /// Tightening never takes more than the time the request has left: with
    /// little of the SLO remaining, the child deadline is at most "now".
    #[cfg(not(feature = "ac_pred"))]
    fn tightening_is_capped_at_time_left(w) {
        let svc = w.service("EstCapSvc");
        let child = w.service("EstCapChild");
        train(w, &svc, "Entry", &child, "Next", 1_000, 50_000);
        let now = w.now();
        let p = probe(w, &svc, "Entry", dur_ms(20), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), 20_000);
        assert_eq!(p.outbound.deadline, now);
    }
}

scenario! {
    /// Estimates go stale: an observation younger than 200 ms counts in full,
    /// older ones shrink by `exp(-age / 5 s)`, and after 250 s nothing is left.
    fn stale_estimates_decay(w) {
        let svc = w.service("EstDecaySvc");
        let child = w.service("EstDecayChild");
        train(w, &svc, "Entry", &child, "Next", 1_000, 10_000);
        let observed_at = w.now();
        for age_us in [0, 199_999, 200_000, 200_001, 1_000_000, 5_000_000, 12_500_000, 250_000_000, 900_000_000] {
            w.set_now(observed_at + age_us);
            let p = probe(w, &svc, "Entry", Duration::from_secs(10_000), &child, "Next").unwrap();
            assert_eq!(
                p.deadline_tightening(),
                decayed(expected_tightening(&{
                    let mut m = MeanVar::default();
                    m.track(10_000);
                    m
                }), age_us),
                "age {age_us}"
            );
        }
        w.set_now(observed_at);
        let p = probe(w, &svc, "Entry", Duration::from_secs(10), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), 10_000);
    }
}

scenario! {
    /// A request that ends in an early return (DeadlineExceeded) teaches the
    /// estimators nothing.
    fn early_return_request_is_not_learned(w) {
        let svc = w.service("EstEarlySvc");
        let child = w.service("EstEarlyChild");
        for _ in 0..3 {
            let req = w.ingress("EstApi", Duration::from_secs(10));
            svc.serve("Entry", &req, |h| {
                h.call(&child, "Next").run_for(dur_ms(4))?;
                h.work_ms(10)?;
                Err(Status::new(Code::DeadlineExceeded, "/EarlyReturn?src=x"))
            });
        }
        let p = probe(w, &svc, "Entry", Duration::from_secs(10), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), 0);
    }
}

scenario! {
    /// Only DeadlineExceeded marks a request as an early return: a request
    /// that fails with any other error status still counts as work the
    /// service observed, and teaches the estimators.
    fn other_errors_are_still_learned(w) {
        let svc = w.service("EstErrSvc");
        let child = w.service("EstErrChild");
        let req = w.ingress("EstApi", Duration::from_secs(10));
        let reply = svc.serve("Entry", &req, |h| {
            h.call(&child, "Next").run_for(dur_ms(4))?;
            h.work_ms(10)?;
            Err(Status::internal("boom"))
        });
        assert_eq!(reply.code(), Code::Internal);
        assert_eq!(reply.message(), "boom");
        let p = probe(w, &svc, "Entry", Duration::from_secs(10), &child, "Next").unwrap();
        assert_eq!(p.deadline_tightening(), 10_000);
    }
}

scenario! {
    /// Siblings issued in parallel are tightened by the work after the whole
    /// group joins, not by waiting for their slower siblings: a fast child
    /// issued alongside a slow one is tightened by the post-join work only.
    fn parallel_children_ignore_sibling_wait(w) {
        let svc = w.service("EstFanSvc");
        let slow = w.service("EstFanSlow");
        let fast = w.service("EstFanFast");
        for _ in 0..4 {
            let req = w.ingress("EstApi", Duration::from_secs(10));
            svc.serve("Entry", &req, |h| {
                let results = h.fanout(vec![
                    h.call(&slow, "Slow").branch(|c| c.work_ms(30)),
                    h.call(&fast, "Fast").branch(|c| c.work_ms(10)),
                ]);
                for r in results {
                    r?;
                }
                h.work_ms(5)
            });
        }
        // Issue the slow child, then the fast one while it is still running.
        let req = w.ingress("EstApi", Duration::from_secs(10));
        let inbound_deadline = req.view().deadline;
        let h = svc.accept("Entry", &req);
        let slow_out = h.call(&slow, "Slow").outbound().unwrap();
        let fast_out = h.call(&fast, "Fast").outbound().unwrap();
        // The slow child was issued alone: it is tightened by the work that
        // follows it, 5 ms of post-join work plus nothing else.
        assert_eq!(inbound_deadline - slow_out.view().deadline, 5_000);
        // Judged on its own the fast child would be tightened by 25 ms (the wait
        // for the slow one plus the post-join work); as a group member, 5 ms.
        // `deadline_equals_slack` ties the hard deadline to the soft estimate,
        // which keeps the per-edge 25 ms.
        let expected = if cfg!(feature = "deadline_equals_slack") { 25_000 } else { 5_000 };
        assert_eq!(inbound_deadline - fast_out.view().deadline, expected);
    }
}
