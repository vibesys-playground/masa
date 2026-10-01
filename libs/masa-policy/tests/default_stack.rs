// Masa's budget module under the framework's rules: the decision the budget
// module owns is settled atomically per child RPC even when many children are
// in flight, and the default stack sends every child a budget without any
// writer module of its own.

use std::sync::Arc;

use masa_core::{time_now, PriorityHint};
use masa_policy::{
    get_masa_context_from_metadata, policy_stack, BudgetModule, ChildPriority, ChildState,
    ContextBuilder, Extensions, MasaRequestExt, MasaStack, Module, PolicyHooks, Requires, WireIn,
    WireOut, MASA_CONTEXT_HEADER,
};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{CowGrpcMethod, GrpcMethod, Request, Status};

type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

fn budget_request() -> http::Request<()> {
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

fn begin<S: masa_policy::ModuleStack>(service: &'static str) -> Parent<S> {
    Parent::<S>::begin(
        GrpcMethod::new(service, "Parent"),
        &budget_request(),
        Arc::new(Server::<S>::new(service)),
    )
}

fn child_method(service: &'static str) -> GrpcMethod {
    GrpcMethod::new(service, "Child")
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
