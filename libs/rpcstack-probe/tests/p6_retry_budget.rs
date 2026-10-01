//! Sample 6 (round 2): retry budget with priority demotion.
//!
//! gRPC-style retry throttling: a first attempt deposits 0.1 token into a
//! per-service bucket, a retry withdraws 1.0 and is rejected when the bucket is
//! empty; retries are demoted (looser priority) and tell the callee the
//! attempt number. Call identity is still a heuristic ("the same child method
//! after an error response of this request"), which is the known-deferred gap.
//! What is new: charges are refunded when a LATER module rejects the child
//! (child-call symmetry) and per-child charge state lives in `ChildState`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use masa_core::PriorityHint;
use rpcstack::{MissingDependency, ModuleServer, ServerInit};
use rpcstack_probe::*;
use serde::{Deserialize, Serialize};
use tonic::{CowGrpcMethod, Request, Response, Status};

const DEMOTION_US: u64 = 500 * MS;

#[derive(Debug)]
struct RetryServer(Arc<Mutex<f64>>);
impl ModuleServer for RetryServer {
    fn new(_i: &mut ServerInit) -> Result<Self, MissingDependency> {
        let bucket = Arc::new(Mutex::new(0.0));
        // No host-to-module channel: the test reaches the bucket through a
        // thread-local that the module's own server constructor fills.
        BUCKET.with(|b| *b.borrow_mut() = Some(bucket.clone()));
        Ok(Self(bucket))
    }
}

thread_local! {
    static BUCKET: std::cell::RefCell<Option<Arc<Mutex<f64>>>> = Default::default();
}
fn bucket() -> f64 {
    BUCKET.with(|b| *b.borrow().as_ref().unwrap().lock().unwrap())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RetryWire {
    attempt: u32,
}

/// Per-child: what this child took from / put into the bucket.
#[derive(Debug, Clone, Copy)]
struct Charge {
    deposited: f64,
    withdrawn: f64,
}

#[derive(Debug, Default)]
struct PerRequest {
    attempts: HashMap<String, u32>,
    failed: HashSet<String>,
}

#[derive(Debug)]
struct RetryBudget {
    bucket: Arc<Mutex<f64>>,
    st: Mutex<PerRequest>,
}

impl Module for RetryBudget {
    type Server = RetryServer;
    const NAME: &'static str = "retry";
    type Wire = RetryWire;

    fn requires(r: &mut Requires) {
        r.module::<BudgetModule>();
    }

    fn new(_m: &CowGrpcMethod, s: &RetryServer, _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self { bucket: s.0.clone(), st: Default::default() }
    }

    fn before_child_rpc<T>(
        &self,
        child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _r: &mut Request<T>,
        child_wire: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        let m = child_method.method().to_string();
        let mut st = self.st.lock().unwrap();
        let is_retry = st.failed.contains(&m); // heuristic identity
        let mut bucket = self.bucket.lock().unwrap();
        let charge = if is_retry {
            if *bucket < 1.0 {
                return Err(Status::unavailable("retry budget exhausted"));
            }
            *bucket -= 1.0;
            Charge { deposited: 0.0, withdrawn: 1.0 }
        } else {
            let before = *bucket;
            *bucket = (*bucket + 0.1).min(10.0);
            Charge { deposited: *bucket - before, withdrawn: 0.0 }
        };
        child.insert(charge);
        let attempt = {
            let a = st.attempts.entry(m).or_insert(0);
            if is_retry {
                *a += 1;
            }
            *a
        };
        if is_retry {
            // The base is the last proposal so far, else the parent's priority:
            // a module AFTER this one that proposes a priority replaces it.
            let base = child
                .proposals::<ChildPriority>()
                .last()
                .map_or(BudgetInfo::of(ext).prio_hint(), |p| p.value.0);
            child.propose(ChildPriority(PriorityHint::new(base.value() + DEMOTION_US)))?;
        }
        child_wire.put::<Self>(&RetryWire { attempt }).unwrap();
        Ok(())
    }

    fn after_child_rpc<T>(
        &self,
        child_method: &CowGrpcMethod,
        outcome: ChildOutcome<'_, T>,
        _w: &WireIn<'_>,
        child: &ChildState,
        _e: &Extensions,
    ) -> Result<(), Status> {
        match outcome {
            ChildOutcome::Sent(response) => {
                if response.is_err() {
                    self.st.lock().unwrap().failed.insert(child_method.method().to_string());
                }
            }
            ChildOutcome::Rejected { .. } => {
                // The child never left: undo this module's own charge, if it
                // made one (it did not when it was the rejecter before charging).
                if let Some(c) = child.get::<Charge>() {
                    let mut b = self.bucket.lock().unwrap();
                    *b = *b + c.withdrawn - c.deposited;
                }
            }
        }
        Ok(())
    }
}

/// Rejects every child (a later module, e.g. admission control).
#[derive(Debug)]
struct RejectAll;
impl Module for RejectAll {
    type Server = ();
    const NAME: &'static str = "reject_all";
    type Wire = ();
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn before_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        _cc: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        _e: &mut Extensions,
    ) -> Result<(), Status> {
        Err(Status::resource_exhausted("admission"))
    }
}

/// Proposes a fixed priority (a later module that decides priority itself).
#[derive(Debug)]
struct FixedPriority;
impl Module for FixedPriority {
    type Server = ();
    const NAME: &'static str = "fixed_priority";
    type Wire = ();
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn before_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        child: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        _e: &mut Extensions,
    ) -> Result<(), Status> {
        child.propose(ChildPriority(PriorityHint::new(777)))?;
        Ok(())
    }
}

type Stack = policy_stack![BudgetModule, RetryBudget];

fn req() -> http::Request<()> {
    root_http(&root_ctx(100), |_| {})
}

fn fail(h: &Hop<Stack>, out: Out<Stack>) {
    let _ = h.answer(out, Err(Status::unavailable("boom"))).unwrap();
}

fn fill(svc: &Svc<Stack>, n: usize) {
    for _ in 0..n {
        let h = svc.accept("Hop", &req());
        let out = h.child("db").unwrap();
        let _ = h.answer(out, Ok(Response::new(()))).unwrap();
    }
}

#[test]
fn retry_is_throttled_by_the_bucket_and_demoted() {
    reset_clock();
    let svc = Svc::<Stack>::new("S");
    fill(&svc, 10); // bucket = 1.0
    let h = svc.accept("Hop", &req());
    let first = h.child("db").unwrap();
    let first_prio = first.budget().prio_hint().value();
    assert_eq!(first.wire::<RetryBudget>(), Some(RetryWire { attempt: 0 }));
    fail(&h, first);
    let retry = h.child("db").unwrap();
    assert_eq!(retry.wire::<RetryBudget>(), Some(RetryWire { attempt: 1 }));
    assert_eq!(retry.budget().prio_hint().value(), first_prio + DEMOTION_US);
    fail(&h, retry);
    let err = h.child("db").map(|_| ()).unwrap_err();
    assert_eq!(err.message(), "retry budget exhausted");
}

#[test]
fn a_charge_is_refunded_when_a_later_module_rejects_the_child() {
    // New: the old probe had no way to undo a charge for a child that never left.
    reset_clock();
    let svc = Svc::<policy_stack![BudgetModule, RetryBudget, RejectAll]>::new("R");
    for _ in 0..30 {
        assert!(svc.accept("Hop", &req()).child("db").is_err());
    }
    assert!(bucket().abs() < 1e-9, "deposits for rejected children were undone: {}", bucket());
    // Control: the same attempts, sent, do fill the bucket.
    let ok = Svc::<Stack>::new("R2");
    fill(&ok, 30);
    assert!((bucket() - 3.0).abs() < 1e-9, "{}", bucket());
}

/// Rejects the second child of each request.
#[derive(Debug)]
struct RejectSecond(Mutex<u32>);
impl Module for RejectSecond {
    type Server = ();
    const NAME: &'static str = "reject_second";
    type Wire = ();
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self(Mutex::new(0))
    }
    fn before_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        _cc: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        _e: &mut Extensions,
    ) -> Result<(), Status> {
        let mut n = self.0.lock().unwrap();
        *n += 1;
        if *n >= 2 {
            return Err(Status::resource_exhausted("second child"));
        }
        Ok(())
    }
}

#[test]
fn a_withdrawal_is_refunded_too() {
    reset_clock();
    type S = policy_stack![BudgetModule, RetryBudget, RejectSecond];
    let svc = Svc::<S>::new("W");
    for _ in 0..10 {
        let h = svc.accept("Hop", &req());
        let out = h.child("db").unwrap();
        let _ = h.answer(out, Ok(Response::new(()))).unwrap();
    }
    assert!((bucket() - 1.0).abs() < 1e-9);
    let h = svc.accept("Hop", &req());
    let first = h.child("db").unwrap();
    let _ = h.answer(first, Err(Status::unavailable("boom"))).unwrap();
    let after_first = bucket();
    // The retry is charged 1.0, then rejected by reject_second: refunded.
    assert!(h.child("db").is_err());
    assert!((bucket() - after_first).abs() < 1e-9, "{} vs {}", bucket(), after_first);
}

#[test]
fn a_rejection_by_the_retry_module_itself_does_not_double_refund() {
    reset_clock();
    let svc = Svc::<Stack>::new("D");
    // Bucket 0: first attempt deposits 0.1; fails; retry rejected by retry
    // itself (bucket < 1.0) without a charge: no refund must be applied.
    let h = svc.accept("Hop", &req());
    let first = h.child("db").unwrap();
    fail(&h, first);
    assert!(h.child("db").is_err());
    // Another request: 0.1 (first) still in the bucket; retries still refused,
    // and the bucket did not go negative or gain from the self-rejection.
    let h2 = svc.accept("Hop", &req());
    let first = h2.child("db").unwrap();
    fail(&h2, first);
    assert!(h2.child("db").is_err());
    assert!((bucket() - 0.2).abs() < 1e-9, "{}", bucket());
}

#[test]
fn a_later_proposal_silently_overrides_the_demotion() {
    reset_clock();
    fill(&Svc::<Stack>::new("unused"), 0);
    // Retry first, FixedPriority later: last proposal wins, so the demotion is lost.
    let a = Svc::<policy_stack![BudgetModule, RetryBudget, FixedPriority]>::new("O1");
    // Retry last: demotion applied on top of the fixed priority.
    let b = Svc::<policy_stack![BudgetModule, FixedPriority, RetryBudget]>::new("O2");
    let (pa, pb) = (retry_prio(&a), retry_prio(&b));
    assert_eq!(pa, 777, "demotion overwritten by the later proposer");
    assert_eq!(pb, 777 + DEMOTION_US, "demotion composes only if the demoter is last");
}

fn retry_prio<S: ModuleStack>(svc: &Svc<S>) -> u64 {
    // Make the bucket allow a retry: ten successful first attempts.
    for _ in 0..10 {
        let h = svc.accept("Hop", &req());
        let out = h.child("db").unwrap();
        let _ = h.answer(out, Ok(Response::new(()))).unwrap();
    }
    let h = svc.accept("Hop", &req());
    let first = h.child("db").unwrap();
    let _ = h.answer(first, Err(Status::unavailable("x")));
    h.child("db").unwrap().budget().prio_hint().value()
}

#[test]
fn identity_is_still_a_heuristic_hedges_and_unrelated_calls_are_misclassified() {
    reset_clock();
    let svc = Svc::<Stack>::new("I");
    fill(&svc, 10);
    // A parallel duplicate (hedge) looks like two first attempts.
    let h = svc.accept("Hop", &req());
    let a = h.child("db").unwrap();
    let b = h.child("db").unwrap();
    assert_eq!(a.wire::<RetryBudget>(), b.wire::<RetryBudget>());
    assert_eq!(a.budget().prio_hint(), b.budget().prio_hint());
    // An UNRELATED later call to the same method (say another key) after an
    // earlier failure is charged and demoted as a retry.
    fail(&h, a);
    let other_key = h.child("db").unwrap();
    assert_eq!(other_key.wire::<RetryBudget>(), Some(RetryWire { attempt: 1 }));
}
