//! Sample 2 (round 2): critical-path-first fanout on the FINAL framework.
//!
//! Per-child latency stats live in the module's server state; per-child start
//! time lives in `ChildState`; sibling awareness is a per-request view shared
//! through `Extensions` (no framework call-group view). Looser deadlines for
//! non-critical children are `ChildDeadline` proposals.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use rpcstack::{MissingDependency, ModuleServer, ServerInit};
use rpcstack_probe::*;
use serde::{Deserialize, Serialize};
use tonic::{CowGrpcMethod, Request, Response, Status};

#[derive(Debug, Default, Clone)]
pub struct GroupState {
    pub issued: Vec<String>,
    pub inflight: usize,
    pub finished: Vec<(String, u64)>,
    pub rejected: Vec<(String, &'static str)>,
}

/// Per-request sibling view, published to other modules via `Extensions`.
/// `Arc<Mutex<..>>` because the post-hooks only get `&Extensions`.
#[derive(Debug, Clone, Default)]
pub struct GroupView(pub Arc<Mutex<GroupState>>);

#[derive(Debug, Default)]
struct Learned {
    est: HashMap<(String, String), f64>,
    groups: HashMap<String, Vec<String>>,
}

#[derive(Debug)]
struct CritServer(Arc<Mutex<Learned>>);

impl ModuleServer for CritServer {
    fn new(_i: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(Self(Default::default()))
    }
}

/// Per-child state: when this child was issued.
#[derive(Debug, Clone, Copy)]
struct Started(u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CritWire {
    issued: usize,
    inflight_at_end: usize,
}

#[derive(Debug)]
struct CritPath {
    parent: String,
    learned: Arc<Mutex<Learned>>,
    view: GroupView,
}

impl Module for CritPath {
    type Server = CritServer;
    const NAME: &'static str = "critpath";
    type Wire = CritWire;

    fn requires(r: &mut Requires) {
        r.module::<BudgetModule>();
    }

    fn new(m: &CowGrpcMethod, s: &CritServer, _w: &WireIn<'_>, ext: &mut Extensions) -> Self {
        let view = GroupView::default();
        ext.insert(view.clone());
        LAST_VIEW.with(|v| *v.borrow_mut() = Some(view.clone()));
        Self {
            parent: m.method().to_string(),
            learned: s.0.clone(),
            view,
        }
    }

    fn before_child_rpc<T>(
        &self,
        child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        child.insert(Started(now()));
        let me = child_method.method().to_string();
        {
            let mut g = self.view.0.lock().unwrap();
            g.issued.push(me.clone());
            g.inflight += 1; // undone in after_child_rpc, sent OR rejected
        }
        let learned = self.learned.lock().unwrap();
        let est = |c: &str| learned.est.get(&(self.parent.clone(), c.to_string())).copied();
        // The sibling set is learned: when the first child is issued the
        // framework cannot say that two more are coming.
        let Some(siblings) = learned.groups.get(&self.parent) else {
            return Ok(()); // cold start
        };
        let (Some(mine), Some(crit)) = (
            est(&me),
            siblings
                .iter()
                .filter_map(|s| est(s))
                .fold(None, |a: Option<f64>, x| Some(a.map_or(x, |a| a.max(x)))),
        ) else {
            return Ok(());
        };
        let slack = (crit - mine) as u64;
        let info = BudgetInfo::of(ext);
        let base = child
            .proposals::<ChildDeadline>()
            .last()
            .map_or(info.deadline(), |p| p.value.0);
        let d = (base + slack).min(info.e2e_deadline().max(base));
        child.propose(ChildDeadline(d))?;
        child.propose(ChildPriority(masa_core::PriorityHint::new(d)))?;
        Ok(())
    }

    fn after_child_rpc<T>(
        &self,
        child_method: &CowGrpcMethod,
        outcome: ChildOutcome<'_, T>,
        _w: &WireIn<'_>,
        child: &ChildState,
        _ext: &Extensions,
    ) -> Result<(), Status> {
        let me = child_method.method().to_string();
        let mut g = self.view.0.lock().unwrap();
        g.inflight -= 1;
        match outcome {
            ChildOutcome::Rejected { by, .. } => g.rejected.push((me, by)),
            ChildOutcome::Sent(response) => {
                let lat = now().saturating_sub(child.get::<Started>().unwrap().0);
                g.finished.push((me.clone(), lat));
                if response.is_ok() {
                    let mut l = self.learned.lock().unwrap();
                    let e = l
                        .est
                        .entry((self.parent.clone(), me.clone()))
                        .or_insert(lat as f64);
                    *e = 0.5 * *e + 0.5 * lat as f64;
                    let grp = l.groups.entry(self.parent.clone()).or_default();
                    if !grp.contains(&me) {
                        grp.push(me);
                    }
                }
            }
        }
        Ok(())
    }

    fn finalize<Ret>(
        &self,
        _r: &mut Result<Response<Ret>, Status>,
        _o: Outcome<'_>,
        w: &mut WireOut,
        _e: &Extensions,
    ) {
        let g = self.view.0.lock().unwrap();
        w.put::<Self>(&CritWire {
            issued: g.issued.len(),
            inflight_at_end: g.inflight,
        })
        .unwrap();
    }
}

/// Rejects child "c" in `before_child_rpc`.
#[derive(Debug)]
struct VetoMethod;
impl Module for VetoMethod {
    type Server = ();
    const NAME: &'static str = "veto_method";
    type Wire = ();
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn before_child_rpc<T>(
        &self,
        c: &CowGrpcMethod,
        _cc: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        _e: &mut Extensions,
    ) -> Result<(), Status> {
        if c.method() == "c" {
            Err(Status::resource_exhausted("veto c"))
        } else {
            Ok(())
        }
    }
}

/// Rejects child "c" at seal (after every module accepted it).
#[derive(Debug)]
struct VetoAtSeal;
impl Module for VetoAtSeal {
    type Server = ();
    const NAME: &'static str = "veto_seal";
    type Wire = ();
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn seal_child_rpc<T>(
        &self,
        c: &CowGrpcMethod,
        _cc: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        _e: &mut Extensions,
    ) -> Result<(), Status> {
        if c.method() == "c" {
            Err(Status::resource_exhausted("seal veto c"))
        } else {
            Ok(())
        }
    }
}

/// Limits concurrency using ONLY the sibling view in `Extensions`.
#[derive(Debug)]
struct ConcurrencyCap(usize);
impl Module for ConcurrencyCap {
    type Server = ();
    const NAME: &'static str = "cap";
    type Wire = ();
    fn requires(r: &mut Requires) {
        r.module::<CritPath>();
    }
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self(2)
    }
    fn before_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        _cc: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        // CritPath (earlier) already counted THIS child, hence `>`.
        let inflight = ext.get::<GroupView>().unwrap().0.lock().unwrap().inflight;
        if inflight > self.0 {
            return Err(Status::resource_exhausted("too many siblings in flight"));
        }
        Ok(())
    }
}

/// Panics when asked for child "c".
#[derive(Debug)]
struct PanicsOnC;
impl Module for PanicsOnC {
    type Server = ();
    const NAME: &'static str = "panics";
    type Wire = ();
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
    fn before_child_rpc<T>(
        &self,
        c: &CowGrpcMethod,
        _cc: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        _e: &mut Extensions,
    ) -> Result<(), Status> {
        if c.method() == "c" {
            panic!("module bug");
        }
        Ok(())
    }
}

type Stack = policy_stack![BudgetModule, CritPath];

/// One request that fans out a, b, c in one poll and gets answers at the given
/// virtual latencies (ms). Returns the budgets the children carried.
fn run_request<S: ModuleStack>(
    svc: &Svc<S>,
    lat_ms: [u64; 3],
) -> (Vec<Option<masa_core::Context>>, Reply) {
    let t0 = now();
    // Hop deadline 60 ms is tighter than the e2e deadline (100 ms): there is
    // room to loosen non-critical children.
    let ctx = ContextBuilder::from(&root_ctx(100))
        .deadline(t0 + 60 * MS)
        .build();
    let hop = svc.accept("Hop", &root_http(&ctx, |_| {}));
    hop.before_poll().unwrap();
    let outs: Vec<_> = ["a", "b", "c"].iter().map(|n| hop.child(n)).collect();
    hop.after_poll(false).unwrap();
    let budgets = outs.iter().map(|o| o.as_ref().ok().map(|o| o.budget())).collect();
    let mut pending: Vec<(u64, Out<S>)> = outs
        .into_iter()
        .zip(lat_ms)
        .filter_map(|(o, l)| o.ok().map(|o| (l, o)))
        .collect();
    pending.sort_by_key(|(l, _)| *l);
    for (l, o) in pending {
        set_clock(t0 + l * MS);
        hop.before_poll().unwrap();
        let _ = hop.answer(o, Ok(Response::new(()))).unwrap();
        hop.after_poll(false).unwrap();
    }
    let reply = hop.finalize(Ok(Response::new(())));
    (budgets, reply)
}

fn d(c: &Option<masa_core::Context>) -> u64 {
    c.as_ref().unwrap().deadline()
}

#[test]
fn slowest_child_gets_the_tightest_deadline_once_the_group_is_learned() {
    reset_clock();
    let svc = Svc::<Stack>::new("Svc");
    // Cold start: nothing known about this fanout.
    let (cold, _) = run_request(&svc, [10, 50, 20]);
    assert_eq!(d(&cold[0]), d(&cold[1]));
    assert_eq!(d(&cold[1]), d(&cold[2]));

    reset_clock();
    let (warm, reply) = run_request(&svc, [10, 50, 20]);
    let (da, db, dc) = (d(&warm[0]), d(&warm[1]), d(&warm[2]));
    assert!(db < dc && dc < da);
    assert_eq!(da - db, 40 * MS);
    assert_eq!(dc - db, 30 * MS);
    let p = |c: &Option<masa_core::Context>| c.as_ref().unwrap().prio_hint().value();
    assert_eq!((p(&warm[0]), p(&warm[1]), p(&warm[2])), (da, db, dc));
    assert_eq!(
        wire_of_reply::<CritPath>(&reply),
        Some(CritWire { issued: 3, inflight_at_end: 0 })
    );
}

#[test]
fn a_new_sibling_with_no_history_is_invisible_and_a_drifting_group_needs_warmup_again() {
    reset_clock();
    let svc = Svc::<Stack>::new("Svc");
    run_request(&svc, [10, 50, 20]);
    // The group stats exist, but a request with a changed fanout (say a 4th
    // method "z") cannot be ranked against it: z has no estimate, and the
    // first-issued children were ranked without knowing z was coming.
    reset_clock();
    let t0 = now();
    let ctx = ContextBuilder::from(&root_ctx(100)).deadline(t0 + 60 * MS).build();
    let hop = svc.accept("Hop", &root_http(&ctx, |_| {}));
    let z = hop.child("z").unwrap();
    assert_eq!(z.budget().deadline(), t0 + 60 * MS, "no estimate for z: no proposal");
    // Whereas a known child is ranked against a sibling set that is a guess.
    let b = hop.child("b").unwrap();
    assert_eq!(b.budget().deadline(), t0 + 60 * MS, "b is the learned critical child");
}

#[test]
fn a_child_rejected_by_a_later_module_no_longer_leaks_the_in_flight_count() {
    reset_clock();
    // Old probe: inflight_at_end was 1 here.
    for (name, reply) in [
        ("before_child veto", run_request(&Svc::<policy_stack![BudgetModule, CritPath, VetoMethod]>::new("V1"), [10, 50, 20])),
        ("seal veto", run_request(&Svc::<policy_stack![BudgetModule, CritPath, VetoAtSeal]>::new("V2"), [10, 50, 20])),
    ] {
        let (budgets, reply) = reply;
        assert!(budgets[2].is_none(), "{name}: c was vetoed");
        assert_eq!(
            wire_of_reply::<CritPath>(&reply),
            Some(CritWire { issued: 3, inflight_at_end: 0 }),
            "{name}"
        );
    }
}

#[test]
fn rejected_children_are_reported_with_their_cause_and_stay_out_of_the_stats() {
    reset_clock();
    type S = policy_stack![BudgetModule, CritPath, VetoMethod];
    let svc = Svc::<S>::new("Svc");
    let t0 = now();
    let hop = svc.accept("Hop", &root_http(&root_ctx(100), |_| {}));
    let a = hop.child("a").unwrap();
    assert!(hop.child("c").is_err());
    set_clock(t0 + 5 * MS);
    hop.answer(a, Ok(Response::new(()))).unwrap();
    let reply = hop.finalize(Ok(Response::new(())));
    assert_eq!(
        wire_of_reply::<CritPath>(&reply),
        Some(CritWire { issued: 2, inflight_at_end: 0 })
    );
    // No configuration channel: the test reaches the module's per-request view
    // through a thread-local the module itself fills in `new`.
    let g = LAST_VIEW.with(|v| v.borrow().clone().unwrap()).0.lock().unwrap().clone();
    assert_eq!(g.finished.len(), 1, "only the sent child has a latency sample");
    assert_eq!(g.finished[0], ("a".to_string(), 5 * MS));
    assert_eq!(g.rejected, vec![("c".to_string(), "veto_method")]);
}

thread_local! {
    static LAST_VIEW: std::cell::RefCell<Option<GroupView>> = Default::default();
}

#[test]
fn concurrent_children_of_the_same_method_keep_separate_per_child_state() {
    reset_clock();
    type S = policy_stack![BudgetModule, CritPath];
    let svc = Svc::<S>::new("Svc");
    let t0 = now();
    let hop = svc.accept("Hop", &root_http(&root_ctx(100), |_| {}));
    let first = hop.child("a").unwrap();
    set_clock(t0 + 7 * MS);
    let second = hop.child("a").unwrap();
    // Answer out of issue order: the later child finishes first.
    set_clock(t0 + 9 * MS);
    hop.answer(second, Ok(Response::new(()))).unwrap();
    set_clock(t0 + 30 * MS);
    hop.answer(first, Ok(Response::new(()))).unwrap();
    let reply = hop.finalize(Ok(Response::new(())));
    assert_eq!(
        wire_of_reply::<CritPath>(&reply),
        Some(CritWire { issued: 2, inflight_at_end: 0 })
    );
}

#[test]
fn a_sibling_view_in_extensions_lets_a_later_module_cap_concurrency() {
    reset_clock();
    type S = policy_stack![BudgetModule, CritPath, ConcurrencyCap];
    let svc = Svc::<S>::new("Svc");
    let hop = svc.accept("Hop", &root_http(&root_ctx(100), |_| {}));
    let _a = hop.child("a").unwrap();
    let _b = hop.child("b").unwrap();
    let err = hop.child("c").map(|_| ()).unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    // The cap's own rejection of c is balanced for CritPath (it ran before cap).
    let reply = hop.finalize(Ok(Response::new(())));
    assert_eq!(
        wire_of_reply::<CritPath>(&reply),
        Some(CritWire { issued: 3, inflight_at_end: 2 }) // a and b still in flight, c balanced
    );
}

#[test]
fn the_symmetry_guarantee_does_not_cover_a_cancelled_child_or_a_panic() {
    reset_clock();
    type S = policy_stack![BudgetModule, CritPath];
    let svc = Svc::<S>::new("Svc");
    let hop = svc.accept("Hop", &root_http(&root_ctx(100), |_| {}));
    // Cancellation: the handler future is dropped (timeout, select!) while a
    // child await is pending. The generated client calls `after_child_rpc`
    // only after `.await` returns (tonic-build client.rs:362-370); there is
    // no drop guard, so the pre-hook's count is never undone.
    let a = hop.child("a").unwrap();
    drop(a);
    let reply = hop.finalize(Ok(Response::new(())));
    assert_eq!(
        wire_of_reply::<CritPath>(&reply),
        Some(CritWire { issued: 1, inflight_at_end: 1 })
    );

    // Panic in a LATER module's before_child_rpc: unwinding skips the
    // post-hooks of the earlier modules.
    type P = policy_stack![BudgetModule, CritPath, PanicsOnC];
    let svc = Svc::<P>::new("Svc2");
    let hop = svc.accept("Hop", &root_http(&root_ctx(100), |_| {}));
    let r = catch_unwind(AssertUnwindSafe(|| hop.child("c").map(|_| ())));
    assert!(r.is_err());
    // The request state mutex was poisoned by the panic, but the framework
    // recovers it, so the request can still be finalized.
    let reply = hop.finalize(Ok(Response::new(())));
    assert_eq!(
        wire_of_reply::<CritPath>(&reply),
        Some(CritWire { issued: 1, inflight_at_end: 1 })
    );
}
