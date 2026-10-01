//! Interactions between policy modules. Modules run in a fixed order (deadline
//! guard, estimation, oracle, admission control, queue-latency observer) and
//! the first one to reject a request decides the reply; later modules do not
//! see the request at that hook.

#![allow(unused_imports, dead_code)]

use std::time::Duration;

use tonic::Code;

use super::common::*;
use crate::harness::*;

/// A request whose end-to-end SLO expired a second ago.
fn expired<H: Hooks>(w: &World<H>) -> crate::harness::Inbound {
    w.crafted("OrdApi", dur_ms(1))
        .entered_at(w.now() - ms(1000))
        .build()
}

/// A request whose local deadline passed but whose end-to-end SLO has not.
fn locally_late<H: Hooks>(w: &World<H>) -> crate::harness::Inbound {
    w.crafted("OrdApi", dur_ms(10_000))
        .deadline(w.now() - ms(1000))
        .build()
}

scenario! {
    /// The deadline guard runs before Rajomon admission: an expired request
    /// that Rajomon would also reject is answered with the guard's
    /// DeadlineExceeded, for the request itself and for its child calls.
    #[cfg(all(feature = "abort_slo", feature = "ac_rajomon", not(feature = "ac_pred")))]
    fn guard_runs_before_rajomon(w) {
        let svc = w.service("OrdG1");
        learn_price(w, &svc, "First", "OrdG1Down", "Down", 50);
        let req = w.crafted("OrdApi", dur_ms(1)).entered_at(w.now() - ms(1000)).tokens(10).build();
        let h = svc.accept("First", &req);
        let st = h.poll_begin().unwrap_err();
        assert_eq!(st.code(), Code::DeadlineExceeded);
        assert_eq!(st.message(), "/EarlyReturn?src=OrdG1::First");
        let st = h.call_remote("OrdG1Down", "Down").outbound().err().unwrap();
        assert_eq!(st.code(), Code::DeadlineExceeded);
        assert_eq!(st.message(), "/EarlyReturn?src=OrdG1::First");
    }
}

scenario! {
    /// The deadline guard runs before predictive admission: an expired request
    /// arriving while ingress admission is fully closed is answered with the
    /// guard's bare EarlyReturn message, without the admission reason.
    #[cfg(all(feature = "abort_slo", feature = "ac_pred"))]
    fn guard_runs_before_predictive_admission(w) {
        let svc = w.service("OrdG2");
        close_admission(w, &svc, INGRESS);
        let req = expired(w);
        let st = svc.accept(INGRESS, &req).poll_begin().unwrap_err();
        assert_eq!(st.code(), Code::DeadlineExceeded);
        assert_eq!(st.message(), "/EarlyReturn?src=OrdG2::Ingress");
    }
}

scenario! {
    /// Estimation's local-deadline check runs before Rajomon admission: a
    /// locally late request that Rajomon would also reject reports the local
    /// deadline as the reason.
    #[cfg(all(feature = "abort_slack", feature = "ac_rajomon", not(feature = "ac_pred")))]
    fn estimation_runs_before_rajomon(w) {
        let svc = w.service("OrdE1");
        learn_price(w, &svc, "First", "OrdE1Down", "Down", 50);
        let req = w.crafted("OrdApi", dur_ms(10_000)).deadline(w.now() - ms(1000)).tokens(10).build();
        let st = svc.accept("First", &req).poll_begin().unwrap_err();
        assert_eq!(st.code(), Code::DeadlineExceeded);
        assert_eq!(st.message(), "/EarlyReturn?src=OrdE1::First&reason=LocalDeadlineExceeded");
    }
}

scenario! {
    /// Estimation's local-deadline check also runs before predictive admission.
    #[cfg(all(feature = "abort_slack", feature = "ac_pred"))]
    fn estimation_runs_before_predictive_admission(w) {
        let svc = w.service("OrdE2");
        close_admission(w, &svc, INGRESS);
        let req = locally_late(w);
        let st = svc.accept(INGRESS, &req).poll_begin().unwrap_err();
        assert_eq!(st.message(), "/EarlyReturn?src=OrdE2::Ingress&reason=LocalDeadlineExceeded");
    }
}

scenario! {
    /// The oracle runs before Rajomon admission: a child call without oracle
    /// hints fails with the oracle's Internal error even when Rajomon would also
    /// reject it; with hints Rajomon's budget check decides.
    #[cfg(all(feature = "sched_oracle", feature = "ac_rajomon", not(feature = "ac_pred")))]
    fn oracle_runs_before_rajomon(w) {
        let svc = w.service("OrdO1");
        learn_price(w, &svc, "Parent", "OrdO1Down", "Down", 50);
        let req = w.crafted("OrdApi", dur_ms(10_000)).tokens(10).build();
        let h = svc.accept("Other", &req);
        let st = h.call_remote("OrdO1Down", "Down").without_oracle_hints().outbound().err().unwrap();
        assert_eq!(st.code(), Code::Internal);
        let st = h.call_remote("OrdO1Down", "Down").outbound().err().unwrap();
        assert_eq!(st.code(), Code::ResourceExhausted);
    }
}

scenario! {
    /// The oracle also runs before predictive admission's feasibility check.
    #[cfg(all(feature = "sched_oracle", feature = "ac_pred"))]
    fn oracle_runs_before_predictive_admission(w) {
        let svc = w.service("OrdO2");
        let child = w.service("OrdO2Child");
        train(w, &svc, "Parent", &child, "Child", 40_000, 10_000);
        let req = w.ingress("OrdApi", dur_ms(30));
        let h = svc.accept("Parent", &req);
        let st = h.call(&child, "Child").without_oracle_hints().outbound().err().unwrap();
        assert_eq!(st.code(), Code::Internal);
        let st = h.call(&child, "Child").outbound().err().unwrap();
        assert!(st.message().ends_with("reason=BeforeChildFeasibility"), "{}", st.message());
    }
}

scenario! {
    /// When estimation and the oracle both want to set the child's deadline and
    /// priority, the oracle (later in the order) wins; estimation's hop count
    /// still advances.
    #[cfg(all(
        feature = "sched_oracle",
        feature = "estimator",
        not(any(feature = "est_rms", feature = "est_hist"))
    ))]
    fn oracle_overrides_estimation(w) {
        let svc = w.service("OrdO3");
        let child = w.service("OrdO3Child");
        train(w, &svc, "Parent", &child, "Child", 4_000, 10_000);
        let req = w.ingress("OrdApi", Duration::from_secs(10));
        let e2e = req.view().gateway_entry + req.view().slo;
        let h = svc.accept("Parent", &req);
        let v = h.call(&child, "Child").oracle(30_000, 7_000).outbound().unwrap().view();
        assert_eq!(v.deadline, e2e - 7_000);
        assert_eq!(v.priority, e2e - 7_000 - 30_000);
        assert_eq!(v.hop_count, Some(1));
    }
}

scenario! {
    /// A guard rejection is recorded as an early return in the reply's
    /// metadata, contributes no compute time, and teaches the estimators
    /// nothing: a later request on the same service sees cold estimates.
    #[cfg(all(
        feature = "abort_slo",
        feature = "estimator",
        not(any(feature = "est_rms", feature = "est_hist", feature = "sched_oracle"))
    ))]
    fn guard_rejection_is_an_unlearned_early_return(w) {
        let svc = w.service("OrdG3");
        let child = w.service("OrdG3Child");
        let reply = svc.serve("Ping", &expired(w), |_h| Ok(()));
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        let meta = reply.meta();
        assert_eq!(meta.early_return_count, 1);
        assert_eq!(meta.deadline_signal_count, 0);
        assert_eq!(meta.compute_time_us, 0);
        let p = probe(w, &svc, "Ping", Duration::from_secs(10), &child, "Child").unwrap();
        assert_eq!(p.deadline_tightening(), 0);
    }
}

scenario! {
    /// The queue-latency observer runs last, at finalization: even a request
    /// rejected by the guard carries queue telemetry in its reply.
    #[cfg(all(feature = "abort_slo", feature = "trace_queue_latency"))]
    fn rejected_requests_still_report_queue_telemetry(w) {
        let svc = w.service("OrdQ1");
        let reply = svc.serve("Ping", &expired(w), |_h| Ok(()));
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert!(reply.view().unwrap().queue.is_some());
    }
}
