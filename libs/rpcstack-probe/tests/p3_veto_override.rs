//! Sample 3 (round 2): propose / override / veto on the child's deadline.
//!
//! A proposes `parent deadline - 10 ms`; B (requires A) proposes its bound if
//! A's proposal is looser; C vetoes the child in `seal_child_rpc` when what
//! the owner is about to resolve leaves less than 5 ms. `BudgetModule` owns
//! `ChildDeadline` and the last proposal wins.

use masa_policy::ServerContext;
use rpcstack_probe::*;
use tonic::{CowGrpcMethod, Request, Response, Status};

fn proposed(child: &ChildState, parent: u64) -> (u64, Vec<(&'static str, u64)>) {
    let ps = child.proposals::<ChildDeadline>();
    (
        ps.last().map_or(parent, |p| p.value.0),
        ps.iter().map(|p| (p.by, p.value.0)).collect(),
    )
}

#[derive(Debug)]
struct A;
impl Module for A {
    type Server = ();
    const NAME: &'static str = "a";
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
        child.propose(ChildDeadline(BudgetInfo::of(ext).deadline() - 10 * MS))?;
        Ok(())
    }
}

#[derive(Debug)]
struct B;
impl Module for B {
    type Server = ();
    const NAME: &'static str = "b";
    type Wire = ();
    fn requires(r: &mut Requires) {
        // B judges A's proposal, so A must have made it: declared, checked at
        // construction.
        r.module::<A>();
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
        let info = BudgetInfo::of(ext);
        let bound = info.e2e_deadline() - 30 * MS;
        // Unlike the old probe, B can tell a proposal from the default.
        match child.proposals::<ChildDeadline>().last() {
            Some(p) if p.value.0 > bound => child.propose(ChildDeadline(bound))?,
            None if info.deadline() > bound => child.propose(ChildDeadline(bound))?,
            _ => {}
        }
        Ok(())
    }
}

/// Vetoes at seal: sees every module's proposal, with provenance.
#[derive(Debug)]
struct C;
impl Module for C {
    type Server = ();
    const NAME: &'static str = "c";
    type Wire = ();
    fn requires(r: &mut Requires) {
        r.module::<BudgetModule>();
    }
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn seal_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        child: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        let (d, by) = proposed(child, BudgetInfo::of(ext).deadline());
        let left = d.saturating_sub(now());
        if left < 5 * MS {
            return Err(Status::deadline_exceeded(format!(
                "vetoed by c: {left}us left; proposals {by:?}"
            )));
        }
        Ok(())
    }
}

/// The contrasting design: C vetoes in `before_child_rpc`, like the old probe.
#[derive(Debug)]
struct EarlyC;
impl Module for EarlyC {
    type Server = ();
    const NAME: &'static str = "early_c";
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
        let (d, _) = proposed(child, BudgetInfo::of(ext).deadline());
        if d.saturating_sub(now()) < 5 * MS {
            return Err(Status::deadline_exceeded("early veto"));
        }
        Ok(())
    }
}

type ABC = policy_stack![BudgetModule, A, B, C];
type ACB = policy_stack![BudgetModule, A, C, B];
type CAB = policy_stack![BudgetModule, C, A, B];
type BAC = policy_stack![BudgetModule, B, A, C];
type EABC = policy_stack![BudgetModule, A, B, EarlyC];
type EACB = policy_stack![BudgetModule, A, EarlyC, B];

/// Ok(offset of the child deadline from t0, in us) or the rejection code.
fn run<S: ModuleStack>(name: &'static str, slo_ms: u64, hop_ms: u64) -> Result<u64, tonic::Code> {
    reset_clock();
    let t0 = now();
    let svc = Svc::<S>::new(name);
    let ctx = ContextBuilder::from(&root_ctx(slo_ms))
        .deadline(t0 + hop_ms * MS)
        .build();
    let hop = svc.accept("Hop", &root_http(&ctx, |_| {}));
    match hop.child("x") {
        Ok(out) => Ok(out.budget().deadline() - t0),
        Err(s) => Err(s.code()),
    }
}

#[test]
fn b_overrides_a_only_when_a_is_looser() {
    // Hop 100, e2e 100: A proposes 90, B's bound is 70 -> 70.
    assert_eq!(run::<ABC>("S1", 100, 100), Ok(70 * MS));
    // Hop 60: A proposes 50, B's bound 70 is looser -> 50.
    assert_eq!(run::<ABC>("S2", 100, 60), Ok(50 * MS));
}

#[test]
fn b_can_tell_a_proposal_from_the_default() {
    // Provenance: B (and C) see who proposed what.
    reset_clock();
    let svc = Svc::<ABC>::new("S3");
    let ctx = ContextBuilder::from(&root_ctx(33)).deadline(now() + 33 * MS).build();
    let hop = svc.accept("Hop", &root_http(&ctx, |_| {}));
    let err = hop.child("x").map(|_| ()).unwrap_err();
    // A proposed 23 ms, B overrode with 3 ms, C vetoed and names both.
    assert!(err.message().contains("(\"a\", ") && err.message().contains("(\"b\", "), "{}", err.message());
}

#[test]
fn stack_order_abc_and_acb_now_agree_across_a_grid() {
    let mut vetoes = 0;
    for slo in [20, 33, 36, 40, 50, 80, 100] {
        for hop in [10, 15, 25, 33, 36, 40, 60, 100] {
            let (x, y) = (run::<ABC>("G1", slo, hop), run::<ACB>("G2", slo, hop));
            assert_eq!(x, y, "slo {slo} hop {hop}");
            vetoes += x.is_err() as u32;
        }
    }
    assert!(vetoes > 0 && vetoes < 56);
    // The old probe's failing case: C judged before B had spoken.
    assert_eq!(run::<ABC>("G3", 33, 33), Err(tonic::Code::DeadlineExceeded));
    assert_eq!(run::<ACB>("G4", 33, 33), Err(tonic::Code::DeadlineExceeded));
}

#[test]
fn what_closes_the_gap_is_judging_at_seal_not_deadline_proposals() {
    // Same modules, but C vetoes in before_child_rpc (the old design): the
    // order dependence is back.
    assert_eq!(run::<EABC>("E1", 33, 33), Err(tonic::Code::DeadlineExceeded));
    // C ran before B proposed 3 ms: it saw A's 23 ms, passed, and B then
    // tightened the child below C's own threshold.
    assert_eq!(run::<EACB>("E2", 33, 33), Ok(3 * MS));
}

#[test]
fn c_before_a_still_agrees_because_c_acts_at_seal() {
    for (slo, hop) in [(33, 33), (100, 100), (100, 60), (20, 20)] {
        assert_eq!(run::<ABC>("H1", slo, hop), run::<CAB>("H2", slo, hop), "{slo}/{hop}");
    }
}

#[test]
fn b_before_a_is_order_dependent_and_the_stack_refuses_to_build() {
    // B declared `requires::<A>`: the misordered stack is rejected at
    // construction instead of silently letting A's looser value win.
    let err = ServerContext::<BAC>::try_new("Bad").unwrap_err();
    assert!(err.to_string().contains("comes later"), "{err}");
}

/// Reads decision state after the fact.
thread_local! { static SPY_LOG: std::cell::RefCell<Vec<String>> = Default::default(); }
struct SpyLog;
impl SpyLog {
    fn lock(&self) -> SpyGuard { SpyGuard }
}
struct SpyGuard;
impl SpyGuard {
    fn unwrap(self) -> Self { self }
    fn clear(&self) { SPY_LOG.with(|l| l.borrow_mut().clear()) }
    fn push(&self, s: String) { SPY_LOG.with(|l| l.borrow_mut().push(s)) }
    fn clone(&self) -> Vec<String> { SPY_LOG.with(|l| l.borrow().clone()) }
    fn last(&self) -> Option<String> { SPY_LOG.with(|l| l.borrow().last().cloned()) }
}
static SPY: SpyLog = SpyLog;

#[derive(Debug)]
struct Spy;
impl Module for Spy {
    type Server = ();
    const NAME: &'static str = "spy";
    type Wire = ();
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn after_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        outcome: ChildOutcome<'_, T>,
        _w: &WireIn<'_>,
        child: &ChildState,
        _e: &Extensions,
    ) -> Result<(), Status> {
        let n = child.proposals::<ChildDeadline>().len();
        let what = match outcome {
            ChildOutcome::Rejected { by, .. } => format!("rejected by {by}, {n} proposals visible"),
            ChildOutcome::Sent(_) => format!("sent, {n} proposals visible"),
        };
        SPY.lock().unwrap().push(what);
        Ok(())
    }
    fn finalize<Ret>(
        &self,
        _r: &mut Result<Response<Ret>, Status>,
        _o: Outcome<'_>,
        _w: &mut WireOut,
        ext: &Extensions,
    ) {
        SPY.lock()
            .unwrap()
            .push(format!("request-level proposals: {}", ext.proposals::<ChildDeadline>().len()));
    }
}

#[test]
fn a_vetoed_child_leaves_no_decision_state_behind() {
    type S = policy_stack![BudgetModule, A, B, C, Spy];
    reset_clock();
    SPY.lock().unwrap().clear();
    let svc = Svc::<S>::new("Spy1");
    let ctx = ContextBuilder::from(&root_ctx(33)).deadline(now() + 33 * MS).build();
    let hop = svc.accept("Hop", &root_http(&ctx, |_| {}));
    assert!(hop.child("x").is_err()); // vetoed
    // A second child on the same request starts clean.
    let ctx2 = root_ctx(100);
    let _ = ctx2;
    let _ = hop.finalize(Ok(Response::new(())));
    let log = SPY.lock().unwrap().clone();
    // Spy (last in the stack) hears the rejection and can still read the
    // child's proposals (provenance of the veto); the request-level state is
    // empty: nothing stale in `Extensions`.
    assert_eq!(
        log,
        vec![
            "rejected by c, 2 proposals visible".to_string(),
            "request-level proposals: 0".to_string()
        ]
    );
}

/// A module that proposes into the REQUEST's `Extensions` instead of the
/// child's `ChildState`: it compiles (both are `Extensions`), and is ignored.
#[derive(Debug)]
struct WrongMap;
impl Module for WrongMap {
    type Server = ();
    const NAME: &'static str = "wrong_map";
    type Wire = ();
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn before_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        _child: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        ext.propose(ChildDeadline(1))?; // BUG: should be `child.propose`
        Ok(())
    }
}

#[test]
fn proposing_to_the_wrong_map_is_silently_ignored_and_accumulates() {
    type S = policy_stack![BudgetModule, WrongMap, Spy];
    reset_clock();
    SPY.lock().unwrap().clear();
    let svc = Svc::<S>::new("Wrong");
    let t0 = now();
    let hop = svc.accept("Hop", &root_http(&root_ctx(100), |_| {}));
    for _ in 0..3 {
        let out = hop.child("x").unwrap();
        // The owner never sees the proposal: the child carries the parent's deadline.
        assert_eq!(out.budget().deadline(), t0 + 100 * MS);
    }
    let _ = hop.finalize(Ok(Response::new(())));
    // ...and the stray proposals pile up in the request-level map.
    assert_eq!(SPY.lock().unwrap().last().unwrap(), "request-level proposals: 3".to_string());
}

#[allow(dead_code)]
fn unused(_: Request<()>) {}
