//! Root priority assignment and deadline propagation along a call chain.

#[cfg(any(
    feature = "sched_fifo",
    feature = "sched_slo",
    feature = "sched_tailclipper",
    feature = "sched_oracle"
))]
use std::cell::RefCell;

use crate::harness::*;

scenario! {
    /// A client-created request's entry time is its creation time and its
    /// deadline is the creation time plus its SLO.
    fn ingress_deadline_is_creation_plus_slo(w) {
        w.set_now(ms(500));
        let req = w.ingress("IngressApi", dur_ms(80));
        let v = req.view();
        assert_eq!(v.gateway_entry, ms(500));
        assert_eq!(v.slo, ms(80));
        assert_eq!(v.deadline, ms(580));
        assert_eq!(v.api, "IngressApi");
    }
}

scenario! {
    /// FIFO, SLO and oracle scheduling (and builds with no scheduling feature)
    /// give a root request the priority value of its absolute deadline:
    /// earlier deadline means smaller value means served first.
    #[cfg(not(any(feature = "sched_tailclipper", feature = "sched_pred")))]
    fn root_priority_is_absolute_deadline(w) {
        w.set_now(ms(500));
        let tight = w.ingress("PrioApi", dur_ms(20));
        w.advance_ms(1);
        let loose = w.ingress("PrioApi", dur_ms(200));
        assert_eq!(tight.view().priority, ms(520));
        assert_eq!(loose.view().priority, ms(701));
        assert!(tight.view().priority < loose.view().priority);
    }
}

scenario! {
    /// TailClipper orders requests oldest-first: a root request's priority
    /// value is the time it entered the system, regardless of its SLO.
    #[cfg(feature = "sched_tailclipper")]
    fn root_priority_is_entry_time(w) {
        w.set_now(ms(500));
        let old = w.ingress("PrioApi", dur_ms(20));
        w.advance_ms(7);
        let young = w.ingress("PrioApi", dur_ms(2));
        assert_eq!(old.view().priority, ms(500));
        assert_eq!(young.view().priority, ms(507));
    }
}

scenario! {
    /// Slack-based scheduling (`sched_pred`) gives a root request the time
    /// remaining to its deadline at creation, which equals its SLO; requests
    /// created at different moments with the same SLO tie.
    #[cfg(feature = "sched_pred")]
    fn root_priority_is_remaining_time(w) {
        w.set_now(ms(500));
        let first = w.ingress("PrioApi", dur_ms(80));
        w.advance_ms(33);
        let second = w.ingress("PrioApi", dur_ms(80));
        assert_eq!(first.view().priority, ms(80));
        assert_eq!(second.view().priority, ms(80));
    }
}

scenario! {
    /// A crafted priority is kept exactly as given, including zero (the
    /// highest priority): zero is a value, not "absent".
    fn explicit_priority_is_kept_at_ingress(w) {
        let zero = w.crafted("PrioApi", dur_ms(50)).priority(0).build();
        assert_eq!(zero.view().priority, 0);
        let odd = w.crafted("PrioApi", dur_ms(50)).priority(777).build();
        assert_eq!(odd.view().priority, 777);
    }
}

scenario! {
    /// Without slack-based re-prioritization a child inherits the parent's
    /// priority unchanged, and a priority of zero survives the hop.
    #[cfg(all(any(feature = "sched_fifo", feature = "sched_slo", feature = "sched_tailclipper"),
              not(feature = "sched_pred")))]
    fn zero_priority_survives_a_hop(w) {
        let a = w.service("ZeroPrioA");
        let b = w.service("ZeroPrioB");
        for prio in [0u64, 777] {
            let req = w.crafted("PrioApi", dur_ms(500)).priority(prio).build();
            let seen = RefCell::new(None);
            a.serve("Entry", &req, |ha| {
                ha.call(&b, "Next").run(|hb| {
                    *seen.borrow_mut() = Some(hb.request().clone());
                    Ok(())
                })?;
                Ok(())
            });
            assert_eq!(seen.borrow().as_ref().unwrap().priority, prio);
        }
    }
}

scenario! {
    /// Along a client -> A -> B -> C chain the request keeps its identity (id,
    /// SLO, entry time), the deadline never loosens, and each hop that tracks
    /// hops advances the hop count by exactly one while the root method
    /// recorded at ingress stays the same.
    #[cfg(any(
        feature = "sched_fifo",
        feature = "sched_slo",
        feature = "sched_tailclipper",
        feature = "sched_oracle"
    ))]
    fn chain_propagates_identity_and_never_loosens_deadline(w) {
        let a = w.service("ChainA");
        let b = w.service("ChainB");
        let c = w.service("ChainC");
        let req = w.ingress("ChainApi", dur_ms(100));
        let log = RefCell::new(vec![req.view().clone()]);

        let reply = a.serve("Entry", &req, |ha| {
            log.borrow_mut().push(ha.request().clone());
            ha.work_ms(1)?;
            ha.call(&b, "Mid").run(|hb| {
                log.borrow_mut().push(hb.request().clone());
                hb.work_ms(1)?;
                hb.call(&c, "Leaf").run(|hc| {
                    log.borrow_mut().push(hc.request().clone());
                    hc.work_ms(1)
                })?;
                Ok(())
            })?;
            Ok(())
        });
        assert!(reply.is_ok());

        let log = log.into_inner();
        // log[0] is the client's request, log[1..] what A, B and C received.
        for (parent, child) in log.windows(2).skip(1).map(|p| (&p[0], &p[1])) {
            assert_eq!(child.request_id, parent.request_id);
            assert_eq!(child.slo, parent.slo);
            assert_eq!(child.gateway_entry, parent.gateway_entry);
            assert_eq!(child.api, parent.api);
            assert!(
                child.deadline <= parent.deadline,
                "child deadline {} loosened parent's {}",
                child.deadline,
                parent.deadline
            );
            #[cfg(not(feature = "estimator"))]
            assert_eq!(child.deadline, parent.deadline);
            #[cfg(not(any(feature = "sched_pred", feature = "sched_tailclipper")))]
            assert_eq!(child.priority, parent.priority);
            #[cfg(feature = "estimator")]
            assert_eq!(child.hop_count.unwrap(), parent.hop_count.unwrap() + 1);
            if parent.tokens.is_some() {
                assert_eq!(child.tokens, parent.tokens);
            }
            if parent.root_method.is_some() {
                assert_eq!(child.root_method, parent.root_method);
            }
        }
        // What the first hop receives straight from the client.
        assert_eq!(log[1].deadline, req.view().deadline);

        #[cfg(feature = "estimator")]
        {
            assert_eq!(log[1].hop_count, Some(0));
            assert_eq!(log[2].hop_count, Some(1));
            assert_eq!(log[3].hop_count, Some(2));
            assert_eq!(
                log[2].root_method,
                Some(("ChainA".to_string(), "Entry".to_string()))
            );
            assert_eq!(log[3].root_method, log[2].root_method);
        }
    }
}
