//! `abort_slack`: a request is aborted when it runs past its *local* (per-hop)
//! deadline, which estimation tightens below the end-to-end deadline.

use std::cell::Cell;
use std::time::Duration;

use tonic::Code;

use super::common::*;
use crate::harness::*;

const LATE: &str = "reason=LocalDeadlineExceeded";

scenario! {
    /// A request whose local deadline has passed is rejected before its
    /// handler runs, even though its end-to-end SLO has not expired; the
    /// message names the source and the reason.
    fn locally_late_request_is_rejected_before_handler(w) {
        let svc = w.service("SlackSvc1");
        let req = w
            .crafted("SlackApi", dur_ms(10_000))
            .deadline(w.now() - ms(1000))
            .build();
        let ran = Cell::new(false);
        let reply = svc.serve("Late", &req, |_h| {
            ran.set(true);
            Ok(())
        });
        assert!(!ran.get());
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(
            reply.message(),
            format!("/EarlyReturn?src=SlackSvc1::Late&{LATE}")
        );
        assert_eq!(reply.meta().early_return_count, 1);
    }
}

scenario! {
    /// The local deadline is exclusive: a request is late only strictly after
    /// it, not at it.
    fn local_deadline_boundary_is_exclusive(w) {
        let svc = w.service("SlackSvc2");
        let start = w.now();
        let req = w
            .crafted("SlackApi", dur_ms(10_000))
            .entered_at(start)
            .deadline(start + ms(10))
            .build();
        w.set_now(start + ms(10));
        assert!(svc.serve("At", &req, |_h| Ok(())).is_ok());
        w.set_now(start + ms(10) + 1);
        assert_eq!(svc.serve("After", &req, |_h| Ok(())).code(), Code::DeadlineExceeded);
    }
}

scenario! {
    /// A request with no local deadline (zero) is never aborted.
    fn zero_local_deadline_never_aborts(w) {
        let svc = w.service("SlackSvc3");
        let req = w.crafted("SlackApi", dur_ms(10)).deadline(0).build();
        w.advance_ms(500);
        assert!(svc.serve("Unset", &req, |h| h.work_ms(5)).is_ok());
    }
}

scenario! {
    /// A handler that crosses its local deadline while computing is aborted
    /// when its poll yields, but one that finishes keeps its result.
    fn running_handler_is_aborted_only_at_a_yield(w) {
        let svc = w.service("SlackSvc4");
        let req = w.ingress("SlackApi", dur_ms(10));
        let reached = Cell::new(false);
        let reply = svc.serve("Slow", &req, |h| {
            h.work_ms(15)?;
            h.poll_yield()?;
            reached.set(true);
            Ok(())
        });
        assert!(!reached.get());
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(reply.message(), format!("/EarlyReturn?src=SlackSvc4::Slow&{LATE}"));

        let req = w.ingress("SlackApi", dur_ms(10));
        assert!(svc.serve("Slow", &req, |h| h.work_ms(15)).is_ok());
    }
}

scenario! {
    /// The deadline handed to a child already accounts for the parent's
    /// learned post-child work, so a child that starts after that tightened
    /// deadline aborts although the end-to-end SLO has not expired.
    #[cfg(not(any(feature = "est_rms", feature = "est_hist", feature = "sched_oracle")))]
    fn child_starting_after_tightened_deadline_aborts(w) {
        let svc = w.service("SlackSvc5");
        let child = w.service("SlackChild5");
        train(w, &svc, "Parent", &child, "Child", 5_000, 10_000);
        let req = w.ingress("SlackApi", dur_ms(40));
        let inbound = svc.accept("Parent", &req);
        let out = inbound.call(&child, "Child").outbound().unwrap();
        let v = out.view();
        assert_eq!(req.view().deadline - v.deadline, 10_000);
        assert_eq!(v.slo, 40_000);

        // 35 ms is after the tightened deadline (30 ms) but before the SLO.
        w.advance(Duration::from_millis(35));
        let reply = child.serve("Child", &out.inbound(), |_h| Ok(()));
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(reply.message(), format!("/EarlyReturn?src=SlackChild5::Child&{LATE}"));
    }
}

scenario! {
    /// A request aborted for lateness is not learned from.
    #[cfg(not(any(feature = "est_rms", feature = "est_hist", feature = "sched_oracle")))]
    fn aborted_request_teaches_nothing(w) {
        let svc = w.service("SlackSvc6");
        let child = w.service("SlackChild6");
        for _ in 0..3 {
            let req = w
                .crafted("SlackApi", dur_ms(10_000))
                .deadline(w.now() - ms(1))
                .build();
            let reply = svc.serve("Parent", &req, |h| {
                h.call(&child, "Child").run_for(dur_ms(3))?;
                h.work_ms(9)
            });
            assert_eq!(reply.code(), Code::DeadlineExceeded);
        }
        let p = probe(w, &svc, "Parent", Duration::from_secs(10), &child, "Child").unwrap();
        assert_eq!(p.deadline_tightening(), 0);
    }
}
