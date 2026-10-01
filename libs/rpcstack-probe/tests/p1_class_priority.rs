//! Sample 1 (round 2): per-class weighted priority on the FINAL framework.
//!
//! Class travels in the module's own wire section (set by the root client),
//! children inherit it explicitly (`child_wire.put`), and the child's priority
//! is a `ChildPriority` proposal that `BudgetModule` resolves. The service's
//! OWN task priority at ingress is still not expressible (deferred).

use std::sync::{Arc, Mutex};

use masa_core::PriorityHint;
use masa_policy::{read_priority_from_headers, ServerContext};
use rpcstack_probe::*;
use serde::{Deserialize, Serialize};
use tonic::{CowGrpcMethod, Request, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Class {
    Gold,
    Silver,
    Bronze,
}

impl Class {
    fn bonus_us(self) -> u64 {
        match self {
            Class::Gold => 60 * MS,
            Class::Silver => 25 * MS,
            Class::Bronze => 0,
        }
    }
}

fn weighted(deadline: u64, class: Class) -> PriorityHint {
    PriorityHint::new(deadline.saturating_sub(class.bonus_us()))
}

#[derive(Debug)]
struct ClassPriority {
    class: Class,
    local: PriorityHint,
}

impl Module for ClassPriority {
    type Server = ();
    const NAME: &'static str = "class";
    type Wire = Class;

    fn requires(r: &mut Requires) {
        r.module::<BudgetModule>();
    }

    fn new(_m: &CowGrpcMethod, _s: &(), wire: &WireIn<'_>, ext: &mut Extensions) -> Self {
        let class = wire.get::<Self>().unwrap().unwrap_or(Class::Bronze);
        // `new` can compute the local priority but has nowhere to put it: it
        // returns `Self`, and the task was queued before any module ran.
        let local = weighted(BudgetInfo::of(ext).deadline(), class);
        Self { class, local }
    }

    fn before_poll<Ret>(
        &self,
        _ext: &mut Extensions,
    ) -> Result<(), Result<tonic::Response<Ret>, Status>> {
        tokio::task::reprioritize(tokio::task::TaskPriority::new(self.local.value()));
        Ok(())
    }

    fn before_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        child: &mut ChildState,
        _r: &mut Request<T>,
        child_wire: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        // The deadline the child will get is not known yet (the owner resolves
        // it at seal); use what has been proposed so far, else the parent's.
        let deadline = child
            .proposals::<ChildDeadline>()
            .last()
            .map_or(BudgetInfo::of(ext).deadline(), |p| p.value.0);
        child.propose(ChildPriority(weighted(deadline, self.class)))?;
        child_wire.put::<Self>(&self.class).unwrap();
        Ok(())
    }
}

type Stack = policy_stack![BudgetModule, ClassPriority];

fn class_root(class: Class, slo_ms: u64, encode_in_priority: bool) -> http::Request<()> {
    let n = now();
    let deadline = n + slo_ms * MS;
    let mut b = ContextBuilder::new("probe-api", 1)
        .slo(slo_ms * MS)
        .gateway_entry(n)
        .deadline(deadline);
    if encode_in_priority {
        b = b.prio_hint(weighted(deadline, class));
    }
    root_http(&b.build(), |w| w.put::<ClassPriority>(&class).unwrap())
}

#[test]
fn children_get_class_weighted_priority_via_a_proposal_and_inherit_the_class() {
    reset_clock();
    let a = Svc::<Stack>::new("A");
    let b = Svc::<Stack>::new("B");
    for class in [Class::Gold, Class::Silver, Class::Bronze] {
        let hop = a.accept("Hop", &class_root(class, 100, true));
        let child = hop.child("Next").unwrap();
        let ctx = child.budget();
        assert_eq!(child.wire::<ClassPriority>(), Some(class));
        assert_eq!(ctx.prio_hint(), weighted(ctx.deadline(), class));
        // Second hop: class inherited transitively.
        let hop_b = b.accept("Hop", &child.http());
        let grand = hop_b.child("Leaf").unwrap();
        assert_eq!(grand.wire::<ClassPriority>(), Some(class));
        assert_eq!(grand.budget().prio_hint(), weighted(grand.budget().deadline(), class));
    }
    let prio = |c| {
        a.accept("Hop", &class_root(c, 100, true))
            .child("Next")
            .unwrap()
            .budget()
            .prio_hint()
    };
    // PriorityHint order is flipped: greater = earlier.
    assert!(prio(Class::Gold) > prio(Class::Silver));
    assert!(prio(Class::Silver) > prio(Class::Bronze));
}

#[test]
fn the_class_is_not_inherited_unless_the_module_writes_it() {
    // No framework forwarding: a stack whose class module does not `put`
    // silently drops the class at the next hop (documented behavior).
    #[derive(Debug)]
    struct Forgetful(ClassPriority);
    impl Module for Forgetful {
        type Server = ();
        const NAME: &'static str = "class";
        type Wire = Class;
        fn requires(r: &mut Requires) {
            r.module::<BudgetModule>();
        }
        fn new(m: &CowGrpcMethod, s: &(), w: &WireIn<'_>, e: &mut Extensions) -> Self {
            Self(ClassPriority::new(m, s, w, e))
        }
    }
    reset_clock();
    let svc = Svc::<policy_stack![BudgetModule, Forgetful]>::new("F");
    let hop = svc.accept("Hop", &class_root(Class::Gold, 100, true));
    assert_eq!(hop.child("Next").unwrap().wire::<Forgetful>(), None);
}

// ── adversarial: a derived decision cannot see later proposals ──────────

/// Proposes a tighter child deadline (a stand-in for estimation).
#[derive(Debug)]
struct Tighten;
impl Module for Tighten {
    type Server = ();
    const NAME: &'static str = "tighten";
    type Wire = ();
    fn requires(r: &mut Requires) {
        r.module::<BudgetModule>();
    }
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn before_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        child: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        child.propose(ChildDeadline(BudgetInfo::of(ext).deadline() - 40 * MS))?;
        Ok(())
    }
}

#[test]
fn a_priority_derived_from_the_child_deadline_depends_on_stack_order() {
    // The priority is a function of the child's deadline, but the deadline is
    // only settled by the owner at seal, after every `before_child_rpc`.
    // ClassPriority sees only the proposals that came BEFORE it in stack order.
    reset_clock();
    let req = class_root(Class::Bronze, 100, true);
    let before = Svc::<policy_stack![BudgetModule, ClassPriority, Tighten]>::new("X");
    let after = Svc::<policy_stack![BudgetModule, Tighten, ClassPriority]>::new("Y");
    let c1 = before.accept("Hop", &req).child("n").unwrap().budget();
    let c2 = after.accept("Hop", &req).child("n").unwrap().budget();
    // Same deadline (the tightened one) in both...
    assert_eq!(c1.deadline(), c2.deadline());
    // ...but the priority derived from it differs: in the first stack the
    // class module weighted the parent's deadline, not the child's.
    assert_ne!(c1.prio_hint(), c2.prio_hint());
    assert_eq!(c2.prio_hint(), weighted(c2.deadline(), Class::Bronze));
    // The strain: nothing in the type system tells the author of ClassPriority
    // that Tighten must come first (`requires` could say so, but it is on the
    // author to know that the module reads a decision others write).
}

#[test]
fn proposing_from_seal_before_the_owner_is_a_runtime_error_but_before_child_is_fine() {
    #[derive(Debug)]
    struct SealProposer;
    impl Module for SealProposer {
        type Server = ();
        const NAME: &'static str = "seal_proposer";
        type Wire = ();
        fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
            Self
        }
        fn seal_child_rpc<T>(
            &self,
            _c: &CowGrpcMethod,
            child: &mut ChildState,
            _r: &mut Request<T>,
            _w: &mut WireOut,
            _e: &mut Extensions,
        ) -> Result<(), Status> {
            child.propose(ChildPriority(PriorityHint::new(1)))?;
            Ok(())
        }
    }
    reset_clock();
    // Placed AFTER the owner: seals first, proposal counts.
    let ok = Svc::<policy_stack![BudgetModule, SealProposer]>::new("S1");
    let out = ok.accept("Hop", &root_http(&root_ctx(100), |_| {})).child("n").unwrap();
    assert_eq!(out.budget().prio_hint(), PriorityHint::new(1));
    // Placed BEFORE the owner (cannot read BudgetInfo in `new`, but does not need to):
    // seals after the owner resolved -> DecisionClosed -> child rejected as Internal.
    let bad = Svc::<policy_stack![SealProposer, BudgetModule]>::new("S2");
    let err = match bad.accept("Hop", &root_http(&root_ctx(100), |_| {})).child("n") {
        Err(e) => e,
        Ok(_) => panic!("expected DecisionClosed"),
    };
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("had resolved it"), "{}", err.message());
}

#[test]
fn a_stack_missing_the_required_budget_module_fails_at_construction() {
    let err = ServerContext::<policy_stack![ClassPriority]>::try_new("Bad").unwrap_err();
    assert!(err.to_string().contains("requires module"), "{err}");
    let err = ServerContext::<policy_stack![ClassPriority, BudgetModule]>::try_new("Bad2").unwrap_err();
    assert!(err.to_string().contains("comes later"), "{err}");
}

// ── the own-task priority at ingress: unchanged limitation ──────────────

fn runtime_orders(encode_in_priority: bool) -> (Vec<Class>, Vec<Class>) {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async move {
        reset_clock();
        // Distinct deadlines: plain deadline order is bronze, silver, gold.
        let reqs = [(Class::Gold, 100), (Class::Silver, 90), (Class::Bronze, 80)];
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let mut joins = Vec::new();
        for (class, slo) in reqs {
            let http = class_root(class, slo, encode_in_priority);
            let (first, second) = (first.clone(), second.clone());
            // What hyper does for a stream: priority from the budget section only.
            let prio = read_priority_from_headers(http.headers()).value();
            joins.push(tokio::task::spawn_with_prio(
                async move {
                    let svc = Svc::<Stack>::new("A");
                    let hop = svc.accept("Hop", &http);
                    hop.before_poll().unwrap(); // reprioritize happens here
                    first.lock().unwrap().push(class);
                    tokio::task::yield_now().await;
                    second.lock().unwrap().push(class);
                },
                tokio::task::TaskPriority::new(prio),
            ));
        }
        for j in joins {
            j.await.unwrap();
        }
        let f = first.lock().unwrap().clone();
        let s = second.lock().unwrap().clone();
        (f, s)
    })
}

#[test]
fn own_task_priority_still_cannot_be_set_before_the_first_queue_wait() {
    let (first, second) = runtime_orders(false);
    assert_eq!(first, vec![Class::Bronze, Class::Silver, Class::Gold]);
    assert_eq!(second, vec![Class::Gold, Class::Silver, Class::Bronze]);
    // Only when the sender encodes the class into the root priority is the
    // first poll right, i.e. the sender makes the receiver's decision.
    let (first, _) = runtime_orders(true);
    assert_eq!(first, vec![Class::Gold, Class::Silver, Class::Bronze]);
}
