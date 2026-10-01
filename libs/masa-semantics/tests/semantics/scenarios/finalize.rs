//! What every reply carries, and how errors travel up the call graph.

use tonic::{Code, Status};

use crate::harness::*;

scenario! {
    /// A successful reply carries the request's Masa context: its API, id and
    /// deadline are echoed back to the caller.
    fn ok_reply_carries_the_request_context(w) {
        let svc = w.service("FinOk");
        let req = w.ingress("FinApi", dur_ms(500));
        let reply = svc.serve("Entry", &req, |h| h.work_ms(1));
        assert!(reply.is_ok());
        let v = reply.view().expect("context on the reply");
        assert_eq!(v.api, "FinApi");
        assert_eq!(v.request_id, req.view().request_id);
        assert_eq!(v.deadline, req.view().deadline);
        assert_eq!(v.slo, req.view().slo);
    }
}

scenario! {
    /// An error reply keeps its code and message and gains the Masa context.
    fn error_reply_keeps_its_status_and_carries_context(w) {
        let svc = w.service("FinErr");
        let req = w.ingress("FinApi", dur_ms(500));
        let reply = svc.serve("Entry", &req, |_h| Err(Status::not_found("no such thing")));
        assert_eq!(reply.code(), Code::NotFound);
        assert_eq!(reply.message(), "no such thing");
        let v = reply.view().expect("context on the error");
        assert_eq!(v.request_id, req.view().request_id);
    }
}

scenario! {
    /// An application error in a leaf travels up a three-hop chain unchanged:
    /// every hop's reply has the leaf's code and message.
    fn leaf_error_propagates_up_the_chain(w) {
        let a = w.service("FinChainA");
        let b = w.service("FinChainB");
        let c = w.service("FinChainC");
        let req = w.ingress("FinApi", dur_ms(500));
        let reply = a.serve("Entry", &req, |ha| {
            let inner = ha.call(&b, "Mid").run(|hb| {
                let leaf = hb.call(&c, "Leaf").run(|_hc| Err(Status::internal("leaf broke")))?;
                Err(leaf.status().clone())
            })?;
            Err(inner.status().clone())
        });
        assert_eq!(reply.code(), Code::Internal);
        assert_eq!(reply.message(), "leaf broke");
    }
}

scenario! {
    /// With slack-based scheduling (`sched_pred`) a failed child call surfaces
    /// to the handler as an error from the call itself; in every other build
    /// the handler receives the child's error reply and decides.
    fn failed_child_is_surfaced_only_with_sched_pred(w) {
        let svc = w.service("FinSurface");
        let child = w.service("FinSurfaceChild");
        let req = w.ingress("FinApi", dur_ms(500));
        svc.serve("Entry", &req, |h| {
            let outcome = h.call(&child, "Down").run(|_c| Err(Status::unavailable("down")));
            match outcome {
                Err(status) => {
                    assert!(cfg!(feature = "sched_pred"), "unexpected call error: {status}");
                    assert_eq!(status.code(), Code::Unavailable);
                }
                Ok(reply) => {
                    assert!(!cfg!(feature = "sched_pred"));
                    assert_eq!(reply.code(), Code::Unavailable);
                }
            }
            Ok(())
        });
    }
}

scenario! {
    /// A failed child does not stop the parent: the parent may handle the error
    /// and still answer successfully.
    fn parent_can_absorb_a_child_error(w) {
        let svc = w.service("FinAbsorb");
        let child = w.service("FinAbsorbChild");
        let req = w.ingress("FinApi", dur_ms(500));
        let reply = svc.serve("Entry", &req, |h| {
            // Some policies surface a failed child as an error from the call,
            // others hand the error reply to the handler; both are absorbed.
            let _ = h.call(&child, "Down").run(|_c| Err(Status::unavailable("down")));
            Ok(())
        });
        assert!(reply.is_ok());
    }
}
