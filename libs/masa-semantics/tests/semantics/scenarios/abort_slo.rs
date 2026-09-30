//! `abort_slo`: requests past their end-to-end deadline are rejected instead
//! of consuming work that can no longer meet the SLO.

use std::cell::Cell;

use tonic::Code;

use crate::harness::*;

scenario! {
    /// A request whose end-to-end deadline has already passed is rejected
    /// before the handler runs, with DeadlineExceeded and an EarlyReturn
    /// message naming the source method.
    fn expired_request_is_rejected_before_handler_runs(w) {
        let svc = w.service("GuardSvc1");
        let req = w.crafted("GuardApi", dur_ms(1)).entered_at(w.now() - ms(1000)).build();
        let ran = Cell::new(false);
        let reply = svc.serve("Ping", &req, |_h| {
            ran.set(true);
            Ok(())
        });
        assert!(!ran.get(), "the handler must not run");
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(reply.message(), "/EarlyReturn?src=GuardSvc1::Ping");
    }
}

scenario! {
    /// The end-to-end boundary is inclusive on the late side: at exactly
    /// `entry + slo` the request is rejected, one microsecond earlier it is
    /// served.
    fn deadline_boundary_is_inclusive(w) {
        let svc = w.service("GuardSvc2");
        let entry = w.now();
        let req = w.crafted("GuardApi", dur_ms(50)).entered_at(entry).build();

        w.set_now(entry + ms(50) - 1);
        let on_time = svc.serve("Ping", &req, |_h| Ok(()));
        assert!(on_time.is_ok());

        w.set_now(entry + ms(50));
        let late = svc.serve("Ping", &req, |_h| Ok(()));
        assert_eq!(late.code(), Code::DeadlineExceeded);
    }
}

scenario! {
    /// A request with no deadline information (zero SLO and entry, as sent by
    /// health checks) is never aborted, and neither is one whose per-hop
    /// deadline is zero: zero means "unset", not "already passed".
    fn unset_deadlines_never_abort(w) {
        let svc = w.service("GuardSvc3");
        let no_slo = w.crafted("GuardApi", dur_ms(0)).entered_at(0).deadline(0).build();
        assert!(svc.serve("Ping", &no_slo, |h| h.work_ms(5)).is_ok());

        let no_hop_deadline = w
            .crafted("GuardApi", dur_ms(1))
            .entered_at(w.now() - ms(1000))
            .deadline(0)
            .build();
        assert!(svc.serve("Ping", &no_hop_deadline, |h| h.work_ms(5)).is_ok());
    }
}

scenario! {
    /// The guard looks at the end-to-end deadline (`entry + slo`), not the
    /// per-hop deadline: a request whose tightened hop deadline has passed but
    /// whose end-to-end SLO has not is still served.
    fn hop_deadline_does_not_trigger_the_guard(w) {
        let svc = w.service("GuardSvc4");
        let req = w
            .crafted("GuardApi", dur_ms(10_000))
            .deadline(w.now() - ms(1000))
            .build();
        assert!(svc.serve("Ping", &req, |h| h.work_ms(5)).is_ok());
    }
}

scenario! {
    /// A handler that crosses the deadline while computing is interrupted when
    /// its poll yields (Pending), with the EarlyReturn message.
    fn running_handler_is_interrupted_at_pending_poll(w) {
        let svc = w.service("GuardSvc5");
        let req = w.ingress("GuardApi", dur_ms(20));
        let after_abort = Cell::new(false);
        let reply = svc.serve("Slow", &req, |h| {
            h.work_ms(30)?;
            h.poll_yield()?;
            after_abort.set(true);
            Ok(())
        });
        assert!(!after_abort.get());
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(reply.message(), "/EarlyReturn?src=GuardSvc5::Slow");
    }
}

scenario! {
    /// Only a poll that yields is interrupted: a handler that finishes
    /// (Ready) after the deadline still returns its result.
    fn finishing_handler_is_not_replaced(w) {
        let svc = w.service("GuardSvc6");
        let req = w.ingress("GuardApi", dur_ms(20));
        let reply = svc.serve("Slow", &req, |h| h.work_ms(30));
        assert!(reply.is_ok());
    }
}

scenario! {
    /// A child RPC attempted after the deadline is rejected before anything is
    /// sent: the outbound request carries no Masa context.
    fn child_call_after_deadline_is_rejected_untouched(w) {
        let svc = w.service("GuardSvc7");
        let child = w.service("GuardChild7");
        let req = w.ingress("GuardApi", dur_ms(20));
        let reply = svc.serve("Fan", &req, |h| {
            h.work_ms(25)?;
            match h.call(&child, "Down").outbound_or_untouched() {
                Ok(_) => panic!("child call must be rejected"),
                Err((status, untouched)) => {
                    assert!(untouched);
                    Err(status)
                }
            }
        });
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(reply.message(), "/EarlyReturn?src=GuardSvc7::Fan");
    }
}

scenario! {
    /// After a child RPC completes, later rejections name it as `last_rpc`,
    /// using the child's logical names when the call carries service and
    /// method overrides.
    fn rejection_reports_last_completed_child(w) {
        let svc = w.service("GuardSvc8");
        let child = w.service("GuardChild8");
        let req = w.ingress("GuardApi", dur_ms(20));
        let reply = svc.serve("Fan", &req, |h| {
            h.call(&child, "Plain").run_for(dur_ms(1))?;
            h.work_ms(30)?;
            h.poll_yield()
        });
        assert_eq!(
            reply.message(),
            "/EarlyReturn?src=GuardSvc8::Fan?last_rpc=GuardChild8::Plain"
        );

        let req = w.ingress("GuardApi", dur_ms(20));
        let reply = svc.serve("Fan", &req, |h| {
            h.call(&child, "Plain")
                .service_override("LogicalSvc")
                .method_override("LogicalMethod")
                .run_for(dur_ms(1))?;
            h.work_ms(30)?;
            h.poll_yield()
        });
        assert_eq!(
            reply.message(),
            "/EarlyReturn?src=GuardSvc8::Fan?last_rpc=LogicalSvc::LogicalMethod"
        );
    }
}

scenario! {
    /// An early return cascades up the chain: a callee past the end-to-end
    /// deadline answers DeadlineExceeded, and its caller, equally late, is
    /// rejected in turn when it resumes, naming itself as the source.
    fn early_return_cascades_up_the_chain(w) {
        let a = w.service("GuardChainA");
        let b = w.service("GuardChainB");
        let req = w.ingress("GuardApi", dur_ms(20));
        let reply = a.serve("Entry", &req, |ha| {
            ha.call(&b, "Slow")
                .run(|hb| {
                    hb.work_ms(30)?;
                    hb.poll_yield()
                })
                .map(|_| ())
        });
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(reply.message(), "/EarlyReturn?src=GuardChainA::Entry");
    }
}
