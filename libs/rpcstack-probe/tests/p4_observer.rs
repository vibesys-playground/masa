//! Sample 4 (round 2): an observer module placed first and last.
//!
//! What each position sees through `Outcome` (cause) and the `result`
//! argument (final), across a rejected request, a rejected child, a seal
//! rejection, an early reply and a module that rewrites the result in
//! `finalize`.
//!
//! Configuration: still no channel from the host to a module. This sample gives
//! the observer a handle by registering its metrics under the service name in
//! a process-wide registry from `ModuleServer::new` (the one thing `ServerInit`
//! offers the host-facing side is `service_name()`); the per-request chaos knobs
//! travel in a wire section the test "client" sends.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use rpcstack::{MissingDependency, ModuleServer, ServerInit};
use rpcstack_probe::*;
use serde::{Deserialize, Serialize};
use tonic::{Code, CowGrpcMethod, Request, Response, Status};

#[derive(Debug, Default, Clone)]
struct Metrics {
    requests: u64,
    finalized: u64,
    before_polls: u64,
    after_polls: u64,
    child_attempts: u64,
    child_sent: u64,
    /// "rejected by <module>" per child.
    child_rejected: Vec<String>,
    /// One record per finalized request: (cause, final result).
    ended: Vec<(String, String)>,
}

type Registry = Mutex<HashMap<(&'static str, &'static str), Arc<Mutex<Metrics>>>>;
fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(Default::default)
}

/// The host's handle: look the metrics up by service name and position.
fn metrics(service: &'static str, label: &'static str) -> Metrics {
    registry().lock().unwrap()[&(service, label)].lock().unwrap().clone()
}

#[derive(Debug)]
struct ObsServer(Arc<Mutex<Metrics>>);
impl ObsServer {
    fn register<const ID: u8>(init: &mut ServerInit) -> Self {
        let m = Arc::new(Mutex::new(Metrics::default()));
        registry()
            .lock()
            .unwrap()
            .insert((init.service_name(), Obs::<ID>::LABEL), m.clone());
        Self(m)
    }
}
impl ModuleServer for ObsServer {
    fn new(_i: &mut ServerInit) -> Result<Self, MissingDependency> {
        unreachable!("replaced by per-position servers below")
    }
}

// One server type per position so that each registers under its own label.
#[derive(Debug)]
struct ObsServer0(ObsServer);
#[derive(Debug)]
struct ObsServer1(ObsServer);
impl ModuleServer for ObsServer0 {
    fn new(i: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(Self(ObsServer::register::<0>(i)))
    }
}
impl ModuleServer for ObsServer1 {
    fn new(i: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(Self(ObsServer::register::<1>(i)))
    }
}

trait Pos {
    type Srv: ModuleServer;
    fn handle(s: &Self::Srv) -> Arc<Mutex<Metrics>>;
}
#[derive(Debug)]
struct Obs<const ID: u8> {
    m: Arc<Mutex<Metrics>>,
}
impl<const ID: u8> Obs<ID> {
    const LABEL: &'static str = if ID == 0 { "first" } else { "last" };
}
impl Pos for Obs<0> {
    type Srv = ObsServer0;
    fn handle(s: &ObsServer0) -> Arc<Mutex<Metrics>> {
        s.0 .0.clone()
    }
}
impl Pos for Obs<1> {
    type Srv = ObsServer1;
    fn handle(s: &ObsServer1) -> Arc<Mutex<Metrics>> {
        s.0 .0.clone()
    }
}

macro_rules! observer {
    ($id:literal, $name:literal, $srv:ty) => {
        impl Module for Obs<$id> {
            type Server = $srv;
            const NAME: &'static str = $name;
            type Wire = ();
            fn new(_m: &CowGrpcMethod, s: &$srv, _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
                let m = <Self as Pos>::handle(s);
                m.lock().unwrap().requests += 1;
                Self { m }
            }
            fn before_poll<Ret>(
                &self,
                _e: &mut Extensions,
            ) -> Result<(), Result<Response<Ret>, Status>> {
                self.m.lock().unwrap().before_polls += 1;
                Ok(())
            }
            fn after_poll<Ret>(
                &self,
                _p: &std::task::Poll<Result<Response<Ret>, Status>>,
                _e: &Extensions,
            ) -> Result<(), Result<Response<Ret>, Status>> {
                self.m.lock().unwrap().after_polls += 1;
                Ok(())
            }
            fn before_child_rpc<T>(
                &self,
                _c: &CowGrpcMethod,
                _cc: &mut ChildState,
                _r: &mut Request<T>,
                _w: &mut WireOut,
                _e: &mut Extensions,
            ) -> Result<(), Status> {
                self.m.lock().unwrap().child_attempts += 1;
                Ok(())
            }
            fn after_child_rpc<T>(
                &self,
                _c: &CowGrpcMethod,
                outcome: ChildOutcome<'_, T>,
                _w: &WireIn<'_>,
                _cc: &ChildState,
                _e: &Extensions,
            ) -> Result<(), Status> {
                let mut m = self.m.lock().unwrap();
                match outcome {
                    ChildOutcome::Sent(_) => m.child_sent += 1,
                    ChildOutcome::Rejected { by, status } => {
                        m.child_rejected.push(format!("{by}:{:?}", status.code()))
                    }
                }
                Ok(())
            }
            fn finalize<Ret>(
                &self,
                result: &mut Result<Response<Ret>, Status>,
                outcome: Outcome<'_>,
                _w: &mut WireOut,
                _e: &Extensions,
            ) {
                // Cause (structured, from the framework) AND final result (what
                // this position's `result` is at this point of the unwinding).
                let cause = match outcome {
                    Outcome::Handled => "handled".to_string(),
                    Outcome::Rejected { by, status } => format!("rejected by {by} ({:?})", status.code()),
                    Outcome::Replied { by } => format!("replied by {by}"),
                };
                let fin = match result {
                    Ok(_) => "ok".to_string(),
                    Err(s) => format!("{:?}", s.code()),
                };
                let mut m = self.m.lock().unwrap();
                m.finalized += 1;
                m.ended.push((cause, fin));
            }
        }
    };
}
observer!(0, "obs_first", ObsServer0);
observer!(1, "obs_last", ObsServer1);

/// Chaos knobs, sent in the request by the test client.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Mode {
    reject_poll: bool,
    reject_child: bool,
    reject_seal: bool,
    rewrite_in_finalize: bool,
}

#[derive(Debug)]
struct Chaos(Mode);
impl Module for Chaos {
    type Server = ();
    const NAME: &'static str = "chaos";
    type Wire = Mode;
    fn new(_m: &CowGrpcMethod, _s: &(), w: &WireIn<'_>, e: &mut Extensions) -> Self {
        let mode = w.get::<Self>().unwrap().unwrap_or_default();
        e.insert(mode);
        Self(mode)
    }
    fn before_poll<Ret>(
        &self,
        _e: &mut Extensions,
    ) -> Result<(), Result<Response<Ret>, Status>> {
        // An early OK reply (Err(Ok(..)), Outcome::Replied) cannot be written
        // here: Ret is unconstrained, so a module cannot build a Response<Ret>.
        if self.0.reject_poll {
            return Err(Err(Status::resource_exhausted("chaos: poll")));
        }
        Ok(())
    }
    fn before_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        _cc: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        _e: &mut Extensions,
    ) -> Result<(), Status> {
        if self.0.reject_child {
            return Err(Status::resource_exhausted("chaos: child"));
        }
        Ok(())
    }
    fn seal_child_rpc<T>(
        &self,
        _c: &CowGrpcMethod,
        _cc: &mut ChildState,
        _r: &mut Request<T>,
        _w: &mut WireOut,
        _e: &mut Extensions,
    ) -> Result<(), Status> {
        if self.0.reject_seal {
            return Err(Status::failed_precondition("chaos: seal"));
        }
        Ok(())
    }
}

/// Rewrites a rejection into `Unavailable` (like a gateway mapping errors).
#[derive(Debug)]
struct Rewriter(bool);
impl Module for Rewriter {
    type Server = ();
    const NAME: &'static str = "rewriter";
    type Wire = ();
    fn requires(r: &mut Requires) {
        r.module::<Chaos>();
    }
    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, e: &mut Extensions) -> Self {
        Self(e.get::<Mode>().unwrap().rewrite_in_finalize)
    }
    fn finalize<Ret>(
        &self,
        result: &mut Result<Response<Ret>, Status>,
        _o: Outcome<'_>,
        _w: &mut WireOut,
        _e: &Extensions,
    ) {
        if self.0 && result.is_err() {
            *result = Err(Status::unavailable("rewritten"));
        }
    }
}

type Stack = policy_stack![BudgetModule, Obs<0>, Chaos, Rewriter, Obs<1>];

fn client(mode: Mode) -> http::Request<()> {
    root_http(&root_ctx(100), |w| w.put::<Chaos>(&mode).unwrap())
}

/// One request: first poll, optional child (a handler that propagates the
/// child's failure as its own), finalize.
fn serve(svc: &Svc<Stack>, mode: Mode, with_child: bool) -> Reply {
    let hop = svc.accept("Hop", &client(mode));
    let r = (|| {
        hop.before_poll()?;
        advance_ms(4);
        if with_child {
            let out = hop.child("Next")?;
            hop.after_poll(false)?;
            hop.before_poll()?;
            let _ = hop.answer(out, Ok(Response::new(())))?;
        }
        hop.after_poll(true)?;
        Ok(Response::new(()))
    })();
    hop.finalize(r)
}

fn ended(svc: &'static str, label: &'static str) -> Vec<(String, String)> {
    metrics(svc, label).ended
}

#[test]
fn both_positions_count_every_request() {
    reset_clock();
    let svc = Svc::<Stack>::new("o1");
    serve(&svc, Mode::default(), false).unwrap();
    serve(&svc, Mode::default(), true).unwrap();
    for label in ["first", "last"] {
        let m = metrics("o1", label);
        assert_eq!((m.requests, m.finalized), (2, 2), "{label}");
        assert_eq!(m.ended, vec![("handled".to_string(), "ok".to_string()); 2], "{label}");
    }
}

#[test]
fn a_rejected_request_shows_cause_to_both_positions_and_before_poll_only_to_the_first() {
    reset_clock();
    let svc = Svc::<Stack>::new("o2");
    let mode = Mode { reject_poll: true, ..Default::default() };
    assert!(serve(&svc, mode, false).is_err());
    let (f, l) = (metrics("o2", "first"), metrics("o2", "last"));
    let cause = "rejected by chaos (ResourceExhausted)".to_string();
    assert_eq!(f.ended, vec![(cause.clone(), "ResourceExhausted".to_string())]);
    assert_eq!(l.ended, vec![(cause, "ResourceExhausted".to_string())]);
    // Pre-hooks stop at the rejecter, so the observer after it never saw the poll.
    assert_eq!((f.before_polls, l.before_polls), (1, 0));
    assert_eq!((f.after_polls, l.after_polls), (0, 0));
}

#[test]
fn cause_and_final_result_are_both_visible_even_when_a_module_rewrites_the_result() {
    reset_clock();
    let svc = Svc::<Stack>::new("o3");
    let mode = Mode { reject_poll: true, rewrite_in_finalize: true, ..Default::default() };
    let reply = serve(&svc, mode, false);
    assert_eq!(reply.unwrap_err().code(), Code::Unavailable);
    let cause = "rejected by chaos (ResourceExhausted)".to_string();
    // finalize runs in reverse stack order: Obs<1> (after the rewriter in the
    // stack) finalizes BEFORE the rewrite, Obs<0> AFTER it.
    assert_eq!(ended("o3", "last"), vec![(cause.clone(), "ResourceExhausted".to_string())]);
    assert_eq!(ended("o3", "first"), vec![(cause, "Unavailable".to_string())]);
}

#[test]
fn a_child_rejected_before_the_last_observer_ran_is_invisible_to_it_by_design() {
    reset_clock();
    let svc = Svc::<Stack>::new("o4");
    let mode = Mode { reject_child: true, ..Default::default() };
    assert!(serve(&svc, mode, true).is_err());
    let (f, l) = (metrics("o4", "first"), metrics("o4", "last"));
    assert_eq!((f.child_attempts, f.child_sent), (1, 0));
    assert_eq!(f.child_rejected, vec!["chaos:ResourceExhausted".to_string()]);
    assert_eq!((l.child_attempts, l.child_sent), (0, 0));
    assert!(l.child_rejected.is_empty());
    // The handler propagated the child's failure: no module ended the request
    // (Handled), yet the final result is an error.
    assert_eq!(f.ended, vec![("handled".to_string(), "ResourceExhausted".to_string())]);
    assert_eq!(l.ended, vec![("handled".to_string(), "ResourceExhausted".to_string())]);
}

#[test]
fn a_seal_rejection_is_reported_to_every_position_with_its_cause() {
    reset_clock();
    let svc = Svc::<Stack>::new("o5");
    let mode = Mode { reject_seal: true, ..Default::default() };
    assert!(serve(&svc, mode, true).is_err());
    for label in ["first", "last"] {
        let m = metrics("o5", label);
        assert_eq!(m.child_attempts, 1, "{label}");
        assert_eq!(m.child_rejected, vec!["chaos:FailedPrecondition".to_string()], "{label}");
        assert_eq!(m.child_sent, 0, "{label}");
    }
}

#[test]
fn a_request_dropped_before_finalize_leaves_new_without_a_matching_finalize() {
    // Cancellation: tonic calls `finalize_before_serialization` only after the
    // handler future completes (tonic/src/server/grpc.rs:330) and no hook type
    // has a `Drop`. The observer sees `new` and never a terminal event.
    reset_clock();
    let svc = Svc::<Stack>::new("o6");
    let hop = svc.accept("Hop", &client(Mode::default()));
    hop.before_poll().unwrap();
    drop(hop);
    for label in ["first", "last"] {
        let m = metrics("o6", label);
        assert_eq!((m.requests, m.finalized), (1, 0), "{label}");
    }
}
