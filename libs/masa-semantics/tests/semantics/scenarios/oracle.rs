//! `sched_oracle`: perfect-information scheduling for synthetic experiments.
//! The caller tells the framework how long each child will work and how long
//! the caller itself still needs afterwards; the framework turns that into the
//! child's deadline and priority.

use tonic::Code;

use crate::harness::*;

scenario! {
    /// The child's completion deadline is the end-to-end deadline minus the
    /// caller's remaining work after the child; its priority value is the latest
    /// moment it may start, that deadline minus the child's own work.
    fn child_deadline_and_priority_come_from_the_hints(w) {
        let svc = w.service("OraSvc1");
        w.set_now(ms(100));
        let req = w.ingress("OraApi", dur_ms(200));
        let e2e = req.view().gateway_entry + req.view().slo;
        let h = svc.accept("Parent", &req);
        let out = h
            .call_remote("OraChild1", "Child")
            .oracle(30_000, 7_000)
            .outbound()
            .unwrap();
        let v = out.view();
        assert_eq!(v.deadline, e2e - 7_000);
        assert_eq!(v.priority, e2e - 7_000 - 30_000);
    }
}

scenario! {
    /// Zero hints mean "no work": the child gets the end-to-end deadline and a
    /// priority equal to it.
    fn zero_hints_give_the_end_to_end_deadline(w) {
        let svc = w.service("OraSvc2");
        let req = w.ingress("OraApi", dur_ms(200));
        let e2e = req.view().gateway_entry + req.view().slo;
        let h = svc.accept("Parent", &req);
        let v = h
            .call_remote("OraChild2", "Child")
            .oracle(0, 0)
            .outbound()
            .unwrap()
            .view();
        assert_eq!(v.deadline, e2e);
        assert_eq!(v.priority, e2e);
    }
}

scenario! {
    /// The oracle reads the end-to-end deadline, not the parent's per-hop
    /// deadline, and does not inherit the parent's priority.
    fn hints_are_relative_to_the_end_to_end_deadline(w) {
        let svc = w.service("OraSvc3");
        let req = w
            .crafted("OraApi", dur_ms(200))
            .deadline(w.now() + ms(5))
            .priority(1)
            .build();
        let e2e = req.view().gateway_entry + req.view().slo;
        let h = svc.accept("Parent", &req);
        let v = h
            .call_remote("OraChild3", "Child")
            .oracle(1_000, 2_000)
            .outbound()
            .unwrap()
            .view();
        assert_eq!(v.deadline, e2e - 2_000);
        assert_eq!(v.priority, e2e - 3_000);
    }
}

scenario! {
    /// Hints larger than the deadline saturate at zero instead of wrapping.
    fn oversized_hints_saturate_at_zero(w) {
        let svc = w.service("OraSvc4");
        let req = w.ingress("OraApi", dur_ms(200));
        let h = svc.accept("Parent", &req);
        let v = h
            .call_remote("OraChild4", "Child")
            .oracle(u64::MAX, 0)
            .outbound()
            .unwrap()
            .view();
        assert_eq!(v.priority, 0);
        let v = h
            .call_remote("OraChild4", "Child")
            .oracle(1, u64::MAX)
            .outbound()
            .unwrap()
            .view();
        assert_eq!(v.deadline, 0);
        assert_eq!(v.priority, 0);
    }
}

scenario! {
    /// A child call without the oracle hints is an error, not a guess: Internal
    /// naming the missing header and the child, and the request is left
    /// untouched. Either hint missing is reported.
    fn missing_hints_fail_the_child_call(w) {
        let svc = w.service("OraSvc5");
        let req = w.ingress("OraApi", dur_ms(200));
        let h = svc.accept("Parent", &req);
        match h.call_remote("OraChild5", "Child").without_oracle_hints().outbound_or_untouched() {
            Ok(_) => panic!("hints are required"),
            Err((status, untouched)) => {
                assert_eq!(status.code(), Code::Internal);
                assert_eq!(
                    status.message(),
                    "missing oracle header 'x-masa-oracle-child-work-us' for OraChild5::Child"
                );
                assert!(untouched);
            }
        }
        let only_work = h
            .call_remote("OraChild5", "Child")
            .without_oracle_hints()
            .header("x-masa-oracle-child-work-us", "5")
            .outbound();
        assert_eq!(
            only_work.err().unwrap().message(),
            "missing oracle header 'x-masa-oracle-remaining-after-us' for OraChild5::Child"
        );
    }
}

scenario! {
    /// A hint that is not a number is an error naming the header, value and
    /// child.
    fn malformed_hints_fail_the_child_call(w) {
        let svc = w.service("OraSvc6");
        let req = w.ingress("OraApi", dur_ms(200));
        let h = svc.accept("Parent", &req);
        let bad = h
            .call_remote("OraChild6", "Child")
            .without_oracle_hints()
            .header("x-masa-oracle-child-work-us", "soon")
            .header("x-masa-oracle-remaining-after-us", "0")
            .outbound()
            .err()
            .unwrap();
        assert_eq!(bad.code(), Code::Internal);
        assert!(
            bad.message()
                .starts_with("invalid oracle header 'x-masa-oracle-child-work-us' value 'soon' for OraChild6::Child: "),
            "{}",
            bad.message()
        );
    }
}
