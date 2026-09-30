//! `trace_queue_latency`: queue-latency telemetry flows up the call graph in
//! response metadata.
//!
//! How long a task waited in the runtime queue is measured by the runtime in
//! real time, which a scenario cannot control. These scenarios therefore pin
//! what is deterministic: aggregation of the values children report (as lower
//! bounds on the parent's totals) and the queue-length map, which reports this
//! process's own queue length (zero: the scenario's task is the only one) under
//! its service name.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::harness::*;

fn queue(initial: u64, resume: u64, lengths: &[(&str, u64)]) -> QueueView {
    QueueView {
        initial,
        resume,
        lengths: lengths
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect::<BTreeMap<_, _>>(),
    }
}

fn reply_with_queue(q: QueueView) -> Reply {
    Reply::synthetic(ReplySpec {
        queue: Some(q),
        ..ReplySpec::default()
    })
}

scenario! {
    /// Every reply reports the queue length of the process that produced it,
    /// keyed by the process's service name.
    fn reply_reports_own_queue_length(w) {
        let svc = w.service("QueueOwn");
        let req = w.ingress("QueueApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| h.work_ms(1));
        let q = reply.view().unwrap().queue.expect("queue telemetry on the reply");
        assert_eq!(q.lengths, BTreeMap::from([(QUEUE_SERVICE_NAME.to_string(), 0)]));
    }
}

scenario! {
    /// A parent adds up the queue latencies its children report and merges
    /// their queue lengths, keeping the larger length when two children report
    /// the same service.
    fn parent_aggregates_child_queue_telemetry(w) {
        let svc = w.service("QueueAgg");
        let req = w.ingress("QueueApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |h| {
            h.call_remote("QueueAggA", "Down").returns(
                Duration::from_millis(1),
                reply_with_queue(queue(11, 22, &[("down-a", 5), ("shared", 3)])),
            )?;
            h.call_remote("QueueAggB", "Down").returns(
                Duration::from_millis(1),
                reply_with_queue(queue(100, 200, &[("shared", 9), ("down-b", 1)])),
            )?;
            Ok(())
        });
        let q = reply.view().unwrap().queue.unwrap();
        assert!(q.initial >= 111, "initial {}", q.initial);
        assert!(q.resume >= 222, "resume {}", q.resume);
        assert_eq!(
            q.lengths,
            BTreeMap::from([
                ("down-a".to_string(), 5),
                ("down-b".to_string(), 1),
                ("shared".to_string(), 9),
                (QUEUE_SERVICE_NAME.to_string(), 0),
            ])
        );
    }
}

scenario! {
    /// When the parent itself never ran, its reported queue latencies are
    /// exactly the sums of what its children reported.
    fn queue_totals_are_exact_sums_when_the_parent_never_ran(w) {
        let svc = w.service("QueueExact");
        let req = w.ingress("QueueApi", dur_ms(1000));
        let h = svc.accept_unpolled("Entry", &req);
        h.call_remote("QueueExactChild", "Down")
            .returns(Duration::from_millis(1), reply_with_queue(queue(11, 22, &[("down", 5)])))
            .unwrap();
        let q = h.finalize_now(Ok(())).view().unwrap().queue.unwrap();
        assert_eq!((q.initial, q.resume), (11, 22));
        assert_eq!(
            q.lengths,
            BTreeMap::from([
                ("down".to_string(), 5),
                (QUEUE_SERVICE_NAME.to_string(), 0),
            ])
        );
    }
}

scenario! {
    /// Telemetry travels up a real chain: the top of a three-hop chain reports
    /// the queue lengths of its own process, which all hops share by name here.
    fn telemetry_survives_a_chain(w) {
        let a = w.service("QueueChainA");
        let b = w.service("QueueChainB");
        let req = w.ingress("QueueApi", dur_ms(1000));
        let reply = a.serve("Entry", &req, |ha| {
            ha.call(&b, "Next").run(|hb| hb.work_ms(1))?;
            Ok(())
        });
        let q = reply.view().unwrap().queue.unwrap();
        assert_eq!(q.lengths.len(), 1);
    }
}

scenario! {
    /// Error replies carry queue telemetry as well.
    fn error_replies_report_queue_telemetry(w) {
        let svc = w.service("QueueErr");
        let req = w.ingress("QueueApi", dur_ms(1000));
        let reply = svc.serve("Entry", &req, |_h| Err(tonic::Status::internal("boom")));
        assert!(reply.view().unwrap().queue.is_some());
    }
}
