//! `signal_slack`: a request that runs past its local deadline keeps running,
//! but reports a soft "deadline signal" upstream and is not learned from.

use std::time::Duration;

use super::common::*;
use crate::harness::*;

scenario! {
    /// A locally late request is not aborted: its handler runs and the reply is
    /// `Ok`, but the reply reports one deadline signal and no early return.
    fn late_request_continues_and_signals(w) {
        let svc = w.service("SigSvc1");
        let req = w
            .crafted("SigApi", dur_ms(10_000))
            .deadline(w.now() - ms(1000))
            .build();
        let reply = svc.serve("Late", &req, |h| h.work_ms(5));
        assert!(reply.is_ok());
        let meta = reply.meta();
        assert_eq!(meta.deadline_signal_count, 1);
        assert_eq!(meta.early_return_count, 0);
    }
}

scenario! {
    /// An on-time request signals nothing.
    fn on_time_request_does_not_signal(w) {
        let svc = w.service("SigSvc2");
        let req = w.ingress("SigApi", dur_ms(100));
        let reply = svc.serve("Fine", &req, |h| h.work_ms(5));
        assert_eq!(reply.meta().deadline_signal_count, 0);
    }
}

scenario! {
    /// A handler that crosses its local deadline mid-way is noticed at the end
    /// of the poll, even when that poll finishes the request.
    fn crossing_the_deadline_while_computing_signals(w) {
        let svc = w.service("SigSvc3");
        let req = w.ingress("SigApi", dur_ms(10));
        let reply = svc.serve("Slow", &req, |h| h.work_ms(15));
        assert!(reply.is_ok());
        assert_eq!(reply.meta().deadline_signal_count, 1);
    }
}

scenario! {
    /// A signal from a child propagates to the parent's reply, saturated at
    /// one however many the child reported.
    fn child_signal_propagates_saturated(w) {
        let svc = w.service("SigSvc4");
        let req = w.ingress("SigApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| {
            let spec = ReplySpec {
                meta: RespMeta { deadline_signal_count: 5, ..RespMeta::default() },
                ..ReplySpec::default()
            };
            h.call_remote("SigChild4", "Down")
                .returns(Duration::from_millis(1), Reply::synthetic(spec))?;
            Ok(())
        });
        assert_eq!(reply.meta().deadline_signal_count, 1);
        assert_eq!(reply.meta().early_return_count, 0);
    }
}

scenario! {
    /// A request whose subtree signalled is not learned from: its wall-clock
    /// was inflated by running late, so it must not tighten later children.
    fn signalled_request_teaches_nothing(w) {
        let svc = w.service("SigSvc5");
        let child = w.service("SigChild5");
        for _ in 0..3 {
            let req = w.ingress("SigApi", Duration::from_secs(10));
            let reply = svc.serve("Parent", &req, |h| {
                let spec = ReplySpec {
                    meta: RespMeta { deadline_signal_count: 1, ..RespMeta::default() },
                    ..ReplySpec::default()
                };
                h.call_remote("SigChild5", "Child")
                    .returns(Duration::from_millis(5), Reply::synthetic(spec))?;
                h.work_ms(10)
            });
            assert!(reply.is_ok());
        }
        let p = probe(w, &svc, "Parent", Duration::from_secs(10), &child, "Child").unwrap();
        assert_eq!(p.deadline_tightening(), 0);
    }
}

scenario! {
    /// Control: the same requests without a signal do teach the service.
    fn unsignalled_requests_do_teach(w) {
        let svc = w.service("SigSvc6");
        let child = w.service("SigChild6");
        train(w, &svc, "Parent", &child, "Child", 5_000, 10_000);
        let p = probe(w, &svc, "Parent", Duration::from_secs(10), &child, "Child").unwrap();
        assert_eq!(p.deadline_tightening(), 10_000);
    }
}
