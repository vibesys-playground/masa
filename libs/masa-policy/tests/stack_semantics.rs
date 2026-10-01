// The framework's composition rules, shown with toy modules that only record
// what the framework tells them: which modules run each hook and in what
// order, who learns about a rejection, how decisions are proposed and
// resolved, and what a stack must declare. None of these modules is a Masa
// policy; the rules hold for any stack.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use masa_core::{time_now, PriorityHint};
use masa_policy::{
    get_masa_context_from_metadata, policy_stack, BudgetModule, ChildOutcome, ChildPriority,
    ChildState, ContextBuilder, DecisionClosed, Extensions, MasaRequestExt, MasaStack, Module,
    ModuleStack, Outcome, PolicyHooks, Requires, ServerContext, WireIn, WireOut,
    MASA_CONTEXT_HEADER,
};
use serde::{Deserialize, Serialize};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{Code, CowGrpcMethod, GrpcMethod, Request, Response, Status};

type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

// ── Recording ───────────────────────────────────────────────────────────

/// What each toy module saw, per test: modules log under the service name of
/// the request, which every test makes unique.
static LOGS: Mutex<Option<HashMap<String, Vec<String>>>> = Mutex::new(None);

fn log(key: &str, line: impl Into<String>) {
    LOGS.lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .entry(key.to_owned())
        .or_default()
        .push(line.into());
}

fn lines(key: &str) -> Vec<String> {
    LOGS.lock()
        .unwrap()
        .as_mut()
        .and_then(|logs| logs.remove(key))
        .unwrap_or_default()
}

fn describe(outcome: Outcome<'_>) -> String {
    match outcome {
        Outcome::Handled => "handled".into(),
        Outcome::Rejected { by, status } => format!("rejected by {by} ({:?})", status.code()),
        Outcome::Replied { by } => format!("replied by {by}"),
    }
}

/// A module that logs every hook it is given and can be told to reject.
macro_rules! recorder {
    ($ty:ident, $name:literal $(, $knob:ident)*) => {
        #[derive(Debug)]
        struct $ty {
            key: String,
        }

        impl Module for $ty {
            type Server = ();
            const NAME: &'static str = $name;
            type Wire = ();

            fn new(
                method: &CowGrpcMethod,
                _server: &(),
                _wire: &WireIn<'_>,
                _ext: &mut Extensions,
            ) -> Self {
                Self { key: method.service().to_string() }
            }

            fn before_poll<Ret>(
                &self,
                _ext: &mut Extensions,
            ) -> Result<(), Result<Response<Ret>, Status>> {
                log(&self.key, concat!($name, ":before_poll"));
                $( if stringify!($knob) == "reject_poll" {
                    return Err(Err(Status::aborted(concat!($name, " ends the request"))));
                } )*
                Ok(())
            }

            fn before_child_rpc<T>(
                &self,
                _child_method: &CowGrpcMethod,
                _child: &mut ChildState,
                _request: &mut Request<T>,
                _child_wire: &mut WireOut,
                _ext: &mut Extensions,
            ) -> Result<(), Status> {
                log(&self.key, concat!($name, ":before_child"));
                $( if stringify!($knob) == "reject_child" {
                    return Err(Status::resource_exhausted(concat!($name, " rejects the child")));
                } )*
                Ok(())
            }

            fn seal_child_rpc<T>(
                &self,
                _child_method: &CowGrpcMethod,
                _child: &mut ChildState,
                _request: &mut Request<T>,
                _child_wire: &mut WireOut,
                _ext: &mut Extensions,
            ) -> Result<(), Status> {
                log(&self.key, concat!($name, ":seal"));
                $( if stringify!($knob) == "reject_seal" {
                    return Err(Status::failed_precondition(concat!($name, " rejects at seal")));
                } )*
                Ok(())
            }

            fn after_child_rpc<T>(
                &self,
                _child_method: &CowGrpcMethod,
                outcome: ChildOutcome<'_, T>,
                _response_wire: &WireIn<'_>,
                _child: &ChildState,
                _ext: &Extensions,
            ) -> Result<(), Status> {
                match outcome {
                    ChildOutcome::Sent(_) => log(&self.key, concat!($name, ":after_child sent")),
                    ChildOutcome::Rejected { by, status } => log(
                        &self.key,
                        format!("{}:after_child rejected by {by} ({:?})", $name, status.code()),
                    ),
                }
                Ok(())
            }

            fn after_poll<Ret>(
                &self,
                _poll: &std::task::Poll<Result<Response<Ret>, Status>>,
                _ext: &Extensions,
            ) -> Result<(), Result<Response<Ret>, Status>> {
                log(&self.key, concat!($name, ":after_poll"));
                Ok(())
            }

            fn finalize<Ret>(
                &self,
                _result: &mut Result<Response<Ret>, Status>,
                outcome: Outcome<'_>,
                _wire: &mut WireOut,
                _ext: &Extensions,
            ) {
                log(&self.key, format!("{}:finalize {}", $name, describe(outcome)));
            }
        }
    };
}

recorder!(A, "a");
recorder!(B, "b");
recorder!(C, "c");
recorder!(D, "d");
recorder!(RejectsChild, "rejects_child", reject_child);
recorder!(RejectsSeal, "rejects_seal", reject_seal);
recorder!(RejectsPoll, "rejects_poll", reject_poll);

// ── Driving a request ───────────────────────────────────────────────────

fn inbound() -> http::Request<()> {
    let now = time_now();
    let ctx = ContextBuilder::new("stack-api", 1)
        .slo(1_000_000)
        .gateway_entry(now)
        .deadline(now + 1_000_000)
        .prio_hint(PriorityHint::new(now + 1_000_000))
        .build();
    http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
        .body(())
        .unwrap()
}

fn begin<S: ModuleStack>(service: &'static str) -> Parent<S> {
    Parent::<S>::begin(
        GrpcMethod::new(service, "Parent"),
        &inbound(),
        Arc::new(Server::<S>::new(service)),
    )
}

fn child_method(service: &'static str) -> GrpcMethod {
    GrpcMethod::new(service, "Child")
}

/// Issue one child RPC: the outbound request, or why a module rejected it.
fn issue<S: ModuleStack>(
    parent: &Parent<S>,
    service: &'static str,
) -> (Result<Request<()>, Status>, Child<S>) {
    let mut request = Request::new(());
    let mut child = Child::<S>::new(child_method(service), &request);
    let result = parent
        .before_child_rpc(child_method(service), &mut request, &mut child)
        .map(|()| request);
    (result, child)
}

fn finish<S: ModuleStack>(parent: &Parent<S>) -> Result<Response<()>, Status> {
    let mut result = Ok(Response::new(()));
    parent.finalize_before_serialization(&mut result);
    result
}

// ── Symmetry and order ──────────────────────────────────────────────────

#[test]
fn post_hooks_run_in_reverse_order_after_pre_hooks_in_stack_order() {
    type S = policy_stack![A, B, C];
    let service = "order";
    let parent = begin::<S>(service);
    parent.before_poll::<()>().unwrap();
    let (request, child) = issue::<S>(&parent, service);
    request.unwrap();
    let mut response = Ok(Response::new(()));
    parent
        .after_child_rpc(child_method(service), &mut response, child)
        .unwrap();
    finish::<S>(&parent).unwrap();

    assert_eq!(
        lines(service),
        [
            "a:before_poll",
            "b:before_poll",
            "c:before_poll",
            "a:before_child",
            "b:before_child",
            "c:before_child",
            // The seal step comes after every module's before_child_rpc, in
            // reverse order.
            "c:seal",
            "b:seal",
            "a:seal",
            "c:after_child sent",
            "b:after_child sent",
            "a:after_child sent",
            "c:finalize handled",
            "b:finalize handled",
            "a:finalize handled",
        ]
    );
}

#[test]
fn after_poll_keeps_stack_order() {
    type S = policy_stack![A, B];
    let service = "after-poll";
    let parent = begin::<S>(service);
    parent.after_poll::<()>(&std::task::Poll::Pending).unwrap();
    assert_eq!(lines(service), ["a:after_poll", "b:after_poll"]);
}

#[test]
fn a_rejected_child_rpc_reaches_only_the_modules_that_ran() {
    type S = policy_stack![A, RejectsChild, C];
    let service = "symmetry";
    let parent = begin::<S>(service);
    let (request, _child) = issue::<S>(&parent, service);

    assert_eq!(request.unwrap_err().code(), Code::ResourceExhausted);
    assert_eq!(
        lines(service),
        [
            "a:before_child",
            "rejects_child:before_child",
            // Reverse order, the rejecting module included, and `c`, whose
            // before_child_rpc never ran, hears nothing: no seal, no rejection.
            "rejects_child:after_child rejected by rejects_child (ResourceExhausted)",
            "a:after_child rejected by rejects_child (ResourceExhausted)",
        ]
    );
}

#[test]
fn a_seal_rejection_reaches_every_module() {
    type S = policy_stack![A, RejectsSeal, C];
    let service = "seal-reject";
    let parent = begin::<S>(service);
    let (request, _child) = issue::<S>(&parent, service);

    assert_eq!(request.unwrap_err().code(), Code::FailedPrecondition);
    assert_eq!(
        lines(service),
        [
            "a:before_child",
            "rejects_seal:before_child",
            "c:before_child",
            // `c` seals first and accepts; the rejecter ends the seal step, so
            // `a` never seals. All three had accepted the child before, so all
            // three hear of the rejection, in reverse order.
            "c:seal",
            "rejects_seal:seal",
            "c:after_child rejected by rejects_seal (FailedPrecondition)",
            "rejects_seal:after_child rejected by rejects_seal (FailedPrecondition)",
            "a:after_child rejected by rejects_seal (FailedPrecondition)",
        ]
    );
}

/// Counts child RPCs in flight: up when one is set up, down when it is over,
/// whether it was sent and answered or a later module rejected it.
#[derive(Debug)]
struct InFlight {
    count: std::sync::atomic::AtomicI64,
    key: String,
}

impl Module for InFlight {
    type Server = ();
    const NAME: &'static str = "in_flight";
    type Wire = ();

    fn new(m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self {
            count: Default::default(),
            key: m.service().to_string(),
        }
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _child: &mut ChildState,
        _request: &mut Request<T>,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn after_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _outcome: ChildOutcome<'_, T>,
        _response_wire: &WireIn<'_>,
        _child: &ChildState,
        _ext: &Extensions,
    ) -> Result<(), Status> {
        self.count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        _wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        let count = self.count.load(std::sync::atomic::Ordering::Relaxed);
        log(&self.key, format!("in flight at finalize: {count}"));
    }
}

#[test]
fn a_module_that_counted_a_child_hears_when_a_later_module_rejects_it() {
    type S = policy_stack![InFlight, RejectsChild];
    let service = "in-flight-rejected";
    let parent = begin::<S>(service);
    for _ in 0..3 {
        assert!(issue::<S>(&parent, service).0.is_err());
    }
    finish::<S>(&parent).unwrap();
    assert_eq!(lines(service).last().unwrap(), "in flight at finalize: 0");
}

#[test]
fn the_same_counter_balances_for_a_seal_rejection_and_for_sent_children() {
    type Sealed = policy_stack![InFlight, RejectsSeal];
    let service = "in-flight-sealed";
    let parent = begin::<Sealed>(service);
    assert!(issue::<Sealed>(&parent, service).0.is_err());
    finish::<Sealed>(&parent).unwrap();
    assert_eq!(lines(service).last().unwrap(), "in flight at finalize: 0");

    type Sent = policy_stack![InFlight, A];
    let service = "in-flight-sent";
    let parent = begin::<Sent>(service);
    let (request, child) = issue::<Sent>(&parent, service);
    request.unwrap();
    let mut response = Ok(Response::new(()));
    parent
        .after_child_rpc(child_method(service), &mut response, child)
        .unwrap();
    finish::<Sent>(&parent).unwrap();
    assert_eq!(lines(service).last().unwrap(), "in flight at finalize: 0");
}

// ── Outcome ─────────────────────────────────────────────────────────────

#[test]
fn an_observer_first_in_the_stack_sees_a_later_modules_rejection() {
    type S = policy_stack![A, RejectsPoll, C];
    let service = "observer";
    let parent = begin::<S>(service);
    let reply = parent.before_poll::<()>().unwrap_err();
    assert_eq!(reply.unwrap_err().code(), Code::Aborted);
    // The framework hands the same reply to the caller, which finalizes it.
    let mut result: Result<Response<()>, Status> =
        Err(Status::aborted("rejects_poll ends the request"));
    parent.finalize_before_serialization(&mut result);

    assert_eq!(
        lines(service),
        [
            "a:before_poll",
            "rejects_poll:before_poll",
            // `c` was never entered, but every module finalizes, and all of
            // them are told who ended the request.
            "c:finalize rejected by rejects_poll (Aborted)",
            "rejects_poll:finalize rejected by rejects_poll (Aborted)",
            "a:finalize rejected by rejects_poll (Aborted)",
        ]
    );
}

#[test]
fn a_child_rpc_rejection_is_not_a_request_outcome() {
    // The handler may recover from a rejected child, or fail with a status of
    // its own; either way no module ended the request.
    type S = policy_stack![A, RejectsChild];
    let service = "child-not-outcome";
    let parent = begin::<S>(service);
    let _ = issue::<S>(&parent, service);
    finish::<S>(&parent).unwrap();

    let log = lines(service);
    assert!(log.contains(&"a:finalize handled".to_owned()), "{log:?}");
}

// ── Declared dependencies ───────────────────────────────────────────────

#[derive(Debug)]
struct Provider;

impl Module for Provider {
    type Server = ();
    const NAME: &'static str = "provider";
    type Wire = ();

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
}

#[derive(Debug)]
struct Needs;

impl Module for Needs {
    type Server = ();
    const NAME: &'static str = "needs";
    type Wire = ();

    fn requires(requires: &mut Requires) {
        requires.module::<Provider>();
    }

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
}

#[test]
fn a_module_without_its_required_module_fails_at_construction_naming_both() {
    let err = ServerContext::<policy_stack![Needs]>::try_new("svc").unwrap_err();
    assert!(err.module().ends_with("Needs"), "{err}");
    assert!(err.resource().ends_with("Provider"), "{err}");
    let message = err.to_string();
    assert!(
        message.contains("Needs") && message.contains("Provider"),
        "{message}"
    );
    assert!(message.contains("the stack has none"), "{message}");
}

#[test]
fn a_required_module_placed_later_is_reported_as_misordered() {
    let err = ServerContext::<policy_stack![Needs, Provider]>::try_new("svc").unwrap_err();
    assert!(err.module().ends_with("Needs"), "{err}");
    assert!(err.resource().ends_with("Provider"), "{err}");
    assert!(err.to_string().contains("it comes later"), "{err}");
}

#[test]
fn a_required_module_placed_earlier_is_accepted_whatever_lies_between() {
    assert!(ServerContext::<policy_stack![Provider, Needs]>::try_new("svc").is_ok());
    assert!(ServerContext::<policy_stack![Provider, A, B, Needs]>::try_new("svc").is_ok());
}

#[test]
#[should_panic(expected = "requires module")]
fn server_hooks_new_panics_with_the_construction_error() {
    let _ = Server::<policy_stack![Needs]>::new("svc");
}

// ── Per-child state keyed by type ───────────────────────────────────────

/// Counts the child RPCs issued so far on the request, and keeps the count for
/// each child in that child's own state.
#[derive(Debug)]
struct Numbers {
    issued: Mutex<u32>,
}

#[derive(Debug, PartialEq)]
struct ChildNumber(u32);

impl Module for Numbers {
    type Server = ();
    const NAME: &'static str = "numbers";
    type Wire = ();

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self {
            issued: Mutex::new(0),
        }
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _request: &mut Request<T>,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        let mut issued = self.issued.lock().unwrap();
        *issued += 1;
        child.insert(ChildNumber(*issued));
        Ok(())
    }
}

/// Reads another module's per-child state by naming its type.
#[derive(Debug)]
struct ReadsNumbers {
    key: String,
}

impl Module for ReadsNumbers {
    type Server = ();
    const NAME: &'static str = "reads_numbers";
    type Wire = ();

    fn new(m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self {
            key: m.service().to_string(),
        }
    }

    fn after_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _outcome: ChildOutcome<'_, T>,
        _response_wire: &WireIn<'_>,
        child: &ChildState,
        _ext: &Extensions,
    ) -> Result<(), Status> {
        log(
            &self.key,
            format!("child number {:?}", child.get::<ChildNumber>()),
        );
        Ok(())
    }
}

fn numbers_seen<S: ModuleStack>(service: &'static str) -> Vec<String> {
    let parent = begin::<S>(service);
    for _ in 0..2 {
        let (request, child) = issue::<S>(&parent, service);
        request.unwrap();
        let mut response = Ok(Response::new(()));
        parent
            .after_child_rpc(child_method(service), &mut response, child)
            .unwrap();
    }
    lines(service)
}

#[test]
fn per_child_state_does_not_depend_on_where_modules_sit() {
    let plain = numbers_seen::<policy_stack![Numbers, ReadsNumbers]>("child-state-plain");
    let padded =
        numbers_seen::<policy_stack![A, Numbers, B, C, ReadsNumbers, D]>("child-state-padded");
    let numbers = |log: &[String]| -> Vec<String> {
        log.iter()
            .filter(|line| line.starts_with("child number"))
            .cloned()
            .collect()
    };

    // Each child sees its own number, and neither the order of the modules nor
    // the modules between them changes that.
    assert_eq!(
        numbers(&plain),
        [
            "child number Some(ChildNumber(1))",
            "child number Some(ChildNumber(2))"
        ]
    );
    assert_eq!(numbers(&padded), numbers(&plain));
}

#[test]
fn a_reader_placed_before_the_writer_still_finds_the_state_in_post_hooks() {
    let service = "child-state-reader-first";
    let seen = numbers_seen::<policy_stack![ReadsNumbers, Numbers]>(service);
    assert!(
        seen.contains(&"child number Some(ChildNumber(2))".to_owned()),
        "{seen:?}"
    );
}

// ── Decisions proposed by many modules, resolved by one ─────────────────

/// A decision type for the toy owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cost(u64);

/// A module that proposes a cost for every child.
macro_rules! proposer {
    ($ty:ident, $name:literal, $cost:expr) => {
        #[derive(Debug)]
        struct $ty;

        impl Module for $ty {
            type Server = ();
            const NAME: &'static str = $name;
            type Wire = ();

            fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
                Self
            }

            fn before_child_rpc<T>(
                &self,
                _child_method: &CowGrpcMethod,
                child: &mut ChildState,
                _request: &mut Request<T>,
                _child_wire: &mut WireOut,
                _ext: &mut Extensions,
            ) -> Result<(), Status> {
                child.propose(Cost($cost))?;
                Ok(())
            }
        }
    };
}

proposer!(Ten, "ten", 10);
proposer!(Twenty, "twenty", 20);
proposer!(Five, "five", 5);

/// What an owner settled on, and who proposed what, as the child sees it.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Settled {
    cost: Option<u64>,
    proposed_by: Vec<String>,
}

macro_rules! owner {
    ($ty:ident, $name:literal, $rule:expr) => {
        #[derive(Debug)]
        struct $ty;

        impl Module for $ty {
            type Server = ();
            const NAME: &'static str = $name;
            type Wire = Settled;

            fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
                Self
            }

            fn seal_child_rpc<T>(
                &self,
                _child_method: &CowGrpcMethod,
                child: &mut ChildState,
                _request: &mut Request<T>,
                child_wire: &mut WireOut,
                _ext: &mut Extensions,
            ) -> Result<(), Status> {
                let proposals = child.resolve::<Cost>();
                let rule: fn(&[masa_policy::Proposal<Cost>]) -> Option<u64> = $rule;
                child_wire
                    .put::<Self>(&Settled {
                        cost: rule(&proposals),
                        proposed_by: proposals.iter().map(|p| p.by.to_owned()).collect(),
                    })
                    .unwrap();
                Ok(())
            }
        }
    };
}

// The framework supplies no rule: each owner brings its own.
owner!(LastWins, "last_wins", |p| p.last().map(|p| p.value.0));
owner!(Cheapest, "cheapest", |p| p.iter().map(|p| p.value.0).min());

fn settled<O: Module<Wire = Settled>, S: ModuleStack>(service: &'static str) -> Settled {
    let parent = begin::<S>(service);
    let (request, _child) = issue::<S>(&parent, service);
    request.unwrap().get_wire::<O>().expect("owner sealed")
}

#[test]
fn an_owner_that_takes_the_last_proposal() {
    let settled = settled::<LastWins, policy_stack![LastWins, Ten, Five, Twenty]>("last-wins");
    assert_eq!(settled.cost, Some(20));
}

#[test]
fn another_owner_takes_the_cheapest_proposal() {
    let settled = settled::<Cheapest, policy_stack![Cheapest, Ten, Twenty, Five]>("cheapest");
    assert_eq!(settled.cost, Some(5));
}

#[test]
fn the_same_proposals_resolve_differently_under_different_owners() {
    let last = settled::<LastWins, policy_stack![LastWins, Ten, Twenty]>("rule-last");
    let min = settled::<Cheapest, policy_stack![Cheapest, Ten, Twenty]>("rule-min");
    assert_eq!(last.cost, Some(20));
    assert_eq!(min.cost, Some(10));
}

#[test]
fn the_owner_sees_who_proposed_what_in_order() {
    let settled = settled::<LastWins, policy_stack![LastWins, Twenty, Ten, Five]>("provenance");
    assert_eq!(settled.proposed_by, ["twenty", "ten", "five"]);
}

#[test]
fn no_proposal_is_an_empty_resolution() {
    let settled = settled::<LastWins, policy_stack![LastWins, A]>("no-proposal");
    assert_eq!(settled.cost, None);
    assert!(settled.proposed_by.is_empty());
}

#[test]
fn a_later_module_can_read_what_earlier_ones_proposed() {
    #[derive(Debug)]
    struct Sees {
        key: String,
    }

    impl Module for Sees {
        type Server = ();
        const NAME: &'static str = "sees";
        type Wire = ();

        fn new(m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
            Self {
                key: m.service().to_string(),
            }
        }

        fn before_child_rpc<T>(
            &self,
            _child_method: &CowGrpcMethod,
            child: &mut ChildState,
            _request: &mut Request<T>,
            _child_wire: &mut WireOut,
            _ext: &mut Extensions,
        ) -> Result<(), Status> {
            let seen: Vec<_> = child
                .proposals::<Cost>()
                .iter()
                .map(|p| (p.by, p.value.0))
                .collect();
            log(&self.key, format!("{seen:?}"));
            Ok(())
        }
    }

    let service = "reads-proposals";
    let parent = begin::<policy_stack![LastWins, Ten, Five, Sees]>(service);
    issue::<policy_stack![LastWins, Ten, Five, Sees]>(&parent, service)
        .0
        .unwrap();
    assert_eq!(lines(service), [r#"[("ten", 10), ("five", 5)]"#]);
}

/// Proposes that the child call be refused.
#[derive(Debug, Clone, Copy)]
struct Veto;

#[derive(Debug)]
struct Vetoes;

impl Module for Vetoes {
    type Server = ();
    const NAME: &'static str = "vetoes";
    type Wire = ();

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _request: &mut Request<T>,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        child.propose(Veto)?;
        Ok(())
    }
}

/// Owns the veto: refuses the child call if any module proposed one.
#[derive(Debug)]
struct Refuses;

impl Module for Refuses {
    type Server = ();
    const NAME: &'static str = "refuses";
    type Wire = ();

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }

    fn seal_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _request: &mut Request<T>,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        let vetoes = child.resolve::<Veto>();
        match vetoes.first() {
            None => Ok(()),
            Some(first) => Err(Status::permission_denied(format!(
                "child call vetoed by `{}`",
                first.by
            ))),
        }
    }
}

#[test]
fn a_veto_proposal_makes_the_owner_reject_the_child_call() {
    type S = policy_stack![A, Refuses, B, Vetoes, C];
    let service = "veto";
    let parent = begin::<S>(service);
    let (request, _child) = issue::<S>(&parent, service);

    let status = request.unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "child call vetoed by `vetoes`");
    // Every module had accepted the child, so every module is told who
    // rejected it.
    let log = lines(service);
    for module in ["a", "b", "c"] {
        let told = format!("{module}:after_child rejected by refuses (PermissionDenied)");
        assert!(log.contains(&told), "{module} not told: {log:?}");
    }
}

#[test]
fn without_a_veto_the_child_call_goes_out() {
    type S = policy_stack![Refuses, A];
    let parent = begin::<S>("no-veto");
    assert!(issue::<S>(&parent, "no-veto").0.is_ok());
}

/// Proposes in `seal_child_rpc`, which runs after the owner's when the owner is
/// later in the stack.
#[derive(Debug)]
struct ProposesTooLate;

impl Module for ProposesTooLate {
    type Server = ();
    const NAME: &'static str = "proposes_too_late";
    type Wire = ();

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }

    fn seal_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _request: &mut Request<T>,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        child.propose(Cost(1))?;
        Ok(())
    }
}

#[test]
fn a_proposal_after_the_owner_has_sealed_fails_with_a_clear_error() {
    // Seal runs in reverse order, so the owner, last in the stack, seals
    // first and the early module's proposal arrives after.
    type S = policy_stack![ProposesTooLate, LastWins];
    let service = "late";
    let parent = begin::<S>(service);
    let status = issue::<S>(&parent, service).0.unwrap_err();

    assert_eq!(status.code(), Code::Internal);
    let message = status.message();
    assert!(message.contains("proposes_too_late"), "{message}");
    assert!(message.contains("last_wins"), "{message}");
    assert!(message.contains("Cost"), "{message}");
    assert!(message.contains("seal_child_rpc"), "{message}");
}

#[test]
fn a_closed_decision_names_proposer_resolver_and_type() {
    let mut ext = Extensions::new();
    ext.propose(Cost(1)).unwrap();
    assert_eq!(ext.proposals::<Cost>().len(), 1);
    assert_eq!(ext.resolve::<Cost>().len(), 1);
    assert!(ext.proposals::<Cost>().is_empty());

    let closed: DecisionClosed = ext.propose(Cost(2)).unwrap_err();
    assert!(closed.decision().ends_with("Cost"), "{closed}");
    // Outside a stack no module is running, so no name is recorded.
    assert_eq!(closed.proposer(), "");
    assert_eq!(closed.resolver(), "");
}

#[test]
fn decisions_of_different_types_are_independent() {
    let mut ext = Extensions::new();
    ext.propose(Cost(1)).unwrap();
    ext.propose(Veto).unwrap();
    assert_eq!(ext.resolve::<Cost>().len(), 1);
    // Resolving one type leaves the other open.
    ext.propose(Veto).unwrap();
    assert_eq!(ext.proposals::<Veto>().len(), 2);
}

// ── Atomicity across concurrent children ────────────────────────────────

/// Proposes the priority carried in the request's `x-priority` header.
#[derive(Debug)]
struct PriorityFromHeader;

impl Module for PriorityFromHeader {
    type Server = ();
    const NAME: &'static str = "priority_from_header";
    type Wire = ();

    fn requires(requires: &mut Requires) {
        requires.module::<BudgetModule>();
    }

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        child: &mut ChildState,
        request: &mut Request<T>,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        let priority: u64 = request
            .metadata()
            .get("x-priority")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .expect("the test sets x-priority");
        child.propose(ChildPriority(PriorityHint::new(priority)))?;
        Ok(())
    }
}

/// Gives other threads a chance to run between a proposal and the seal.
#[derive(Debug)]
struct Dawdles;

impl Module for Dawdles {
    type Server = ();
    const NAME: &'static str = "dawdles";
    type Wire = ();

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _child: &mut ChildState,
        _request: &mut Request<T>,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        std::thread::sleep(std::time::Duration::from_micros(200));
        Ok(())
    }
}

#[test]
fn concurrent_child_rpcs_each_get_their_own_decision() {
    type S = policy_stack![BudgetModule, PriorityFromHeader, Dawdles];
    let service = "atomic";
    let parent = Arc::new(begin::<S>(service));

    let threads: Vec<_> = (1..=16u64)
        .map(|n| {
            let parent = parent.clone();
            std::thread::spawn(move || {
                for round in 0..20u64 {
                    let wanted = n * 1_000 + round;
                    let mut request = Request::new(());
                    request
                        .metadata_mut()
                        .insert("x-priority", wanted.to_string().parse().unwrap());
                    let mut child = Child::<S>::new(child_method(service), &request);
                    parent
                        .before_child_rpc(child_method(service), &mut request, &mut child)
                        .unwrap();
                    let sent = get_masa_context_from_metadata(request.metadata())
                        .expect("child carries a budget");
                    assert_eq!(sent.prio_hint(), PriorityHint::new(wanted));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

// ── Masa's default stack needs no writer module ─────────────────────────

#[test]
fn the_default_stack_sends_the_child_a_budget() {
    let service = "masa-default";
    let now = time_now();
    let ctx = ContextBuilder::new("stack-api", 1)
        .slo(1_000_000)
        .gateway_entry(now)
        .deadline(now + 1_000_000)
        .build();
    let req = http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
        .body(())
        .unwrap();
    let parent = Parent::<MasaStack>::begin(
        GrpcMethod::new(service, "Parent"),
        &req,
        Arc::new(Server::<MasaStack>::new(service)),
    );
    let mut request = Request::new(());
    let mut child = Child::<MasaStack>::new(child_method(service), &request);
    parent
        .before_child_rpc(child_method(service), &mut request, &mut child)
        .unwrap();

    let sent = request.get_masa_context().expect("child carries a budget");
    assert_eq!(sent.request_id(), 1);
    assert_eq!(sent.slo(), 1_000_000);
}
