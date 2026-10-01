//! Response metadata that flows back up the call graph: compute time,
//! early-return counts and downstream utilization.

use std::time::Duration;

use tonic::{Code, Status};

use crate::harness::*;

scenario! {
    /// A hop reports the CPU time its handler spent computing: time spent
    /// waiting (for example on I/O) is not counted.
    fn compute_time_excludes_waiting(w) {
        let svc = w.service("MetaComputeSvc");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| {
            h.work_ms(3)?;
            h.wait_ms(7)?;
            h.work_ms(2)
        });
        let meta = reply.meta();
        assert_eq!(meta.compute_time_us, 5_000);
        assert_eq!(meta.accumulated_compute_us, 5_000);
    }
}

scenario! {
    /// A handler that never computed reports zero compute time.
    fn idle_handler_reports_zero_compute(w) {
        let svc = w.service("MetaIdleSvc");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |_h| Ok(()));
        assert_eq!(reply.meta().compute_time_us, 0);
        assert_eq!(reply.meta().accumulated_compute_us, 0);
    }
}

scenario! {
    /// Each hop reports its own compute time and the accumulated compute of
    /// its whole subtree: along client -> A -> B -> C, A's accumulated total is
    /// the sum over all three.
    fn compute_accumulates_up_the_chain(w) {
        let a = w.service("MetaChainA");
        let b = w.service("MetaChainB");
        let c = w.service("MetaChainC");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let b_meta = std::cell::Cell::new(None);
        let reply = a.serve("Entry", &req, |ha| {
            ha.work_ms(1)?;
            let b_reply = ha.call(&b, "Mid").run(|hb| {
                hb.work_ms(2)?;
                hb.call(&c, "Leaf").run(|hc| hc.work_ms(4))?;
                Ok(())
            })?;
            b_meta.set(Some(b_reply.meta()));
            ha.work_ms(8)
        });
        let b_meta = b_meta.get().unwrap();
        assert_eq!(b_meta.compute_time_us, 2_000);
        assert_eq!(b_meta.accumulated_compute_us, 6_000);
        let a_meta = reply.meta();
        assert_eq!(a_meta.compute_time_us, 9_000);
        assert_eq!(a_meta.accumulated_compute_us, 15_000);
    }
}

scenario! {
    /// A child reports its subtree's compute time and downstream utilization
    /// even when the scenario does not model the child: the parent folds them
    /// into its own response.
    fn child_reply_meta_is_folded_into_the_parent(w) {
        let svc = w.service("MetaFoldSvc");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| {
            h.work_ms(2)?;
            let spec = ReplySpec {
                meta: RespMeta {
                    compute_time_us: 4_000,
                    accumulated_compute_us: 5_000,
                    max_downstream_util: 0.5,
                    early_return_count: 2,
                    ..RespMeta::default()
                },
                ..ReplySpec::default()
            };
            h.call_remote("MetaFoldChild", "Down")
                .returns(Duration::from_millis(1), Reply::synthetic(spec))?;
            Ok(())
        });
        let meta = reply.meta();
        assert_eq!(meta.compute_time_us, 2_000);
        assert_eq!(meta.accumulated_compute_us, 7_000);
        assert_eq!(meta.early_return_count, 2);
        assert!(meta.max_downstream_util >= 0.5 && meta.max_downstream_util <= 1.0);
    }
}

scenario! {
    /// Parallel children's metadata is folded into the parent's reply just like
    /// sequential children's: accumulated compute is the sum over all branches,
    /// while the parent's own compute time counts only its own work.
    fn parallel_children_metadata_is_folded(w) {
        let svc = w.service("MetaFanSvc");
        let b = w.service("MetaFanB");
        let c = w.service("MetaFanC");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| {
            h.work_ms(1)?;
            let results = h.fanout(vec![
                h.call(&b, "B").branch(|hb| hb.work_ms(10)),
                h.call(&c, "C").branch(|hc| hc.work_ms(30)),
            ]);
            for r in results {
                r?;
            }
            h.work_ms(2)
        });
        let meta = reply.meta();
        assert_eq!(meta.compute_time_us, 3_000);
        assert_eq!(meta.accumulated_compute_us, 43_000);
    }
}

scenario! {
    /// Early returns are counted across the subtree: the parent adds one for
    /// its own DeadlineExceeded outcome to the count its children reported.
    fn early_returns_are_summed_over_the_subtree(w) {
        let svc = w.service("MetaErSvc");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| {
            let spec = ReplySpec {
                meta: RespMeta { early_return_count: 2, ..RespMeta::default() },
                ..ReplySpec::default()
            };
            h.call_remote("MetaErChild", "Down")
                .returns(Duration::from_millis(1), Reply::synthetic(spec))?;
            Err(Status::new(Code::DeadlineExceeded, "/EarlyReturn?src=MetaErSvc::Entry"))
        });
        assert_eq!(reply.code(), Code::DeadlineExceeded);
        assert_eq!(reply.meta().early_return_count, 3);
    }
}

scenario! {
    /// A child that answers with a DeadlineExceeded error counts as one early
    /// return in the parent's response, even though it reports no metadata.
    fn child_deadline_exceeded_counts_as_an_early_return(w) {
        let svc = w.service("MetaErChildSvc");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| {
            let failed = Reply::bare_err(Status::new(
                Code::DeadlineExceeded,
                "/EarlyReturn?src=Down::Leaf",
            ));
            // Some policies turn the failed child into an error for the
            // handler; either way the count is recorded.
            let _ = h.call_remote("Down", "Leaf").returns(Duration::from_millis(1), failed);
            Ok(())
        });
        assert!(reply.is_ok());
        assert_eq!(reply.meta().early_return_count, 1);
    }
}

scenario! {
    /// Error replies carry the same metadata as successful ones: the status
    /// keeps its code and message and gains the Masa context.
    fn error_reply_carries_metadata(w) {
        let svc = w.service("MetaErrSvc");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| {
            h.work_ms(3)?;
            Err(Status::internal("boom"))
        });
        assert_eq!(reply.code(), Code::Internal);
        assert_eq!(reply.message(), "boom");
        let view = reply.view().expect("context on an error reply");
        assert_eq!(view.request_id, req.view().request_id);
        assert_eq!(view.deadline, req.view().deadline);
        assert_eq!(view.resp().compute_time_us, 3_000);
        assert_eq!(view.resp().early_return_count, 0);
    }
}

scenario! {
    /// The reply of a successful request echoes the request's identity and
    /// deadline.
    fn ok_reply_echoes_request_identity(w) {
        let svc = w.service("MetaEchoSvc");
        let req = w.ingress("MetaApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| h.work_ms(1));
        let view = reply.view().expect("context on the reply");
        assert_eq!(view.api, "MetaApi");
        assert_eq!(view.request_id, req.view().request_id);
        assert_eq!(view.deadline, req.view().deadline);
        assert_eq!(view.resp().deadline_signal_count, 0);
    }
}
