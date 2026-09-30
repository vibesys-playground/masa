// Custom policy stacks built only from masa-policy's public API.
//
// These tests stand in for a policy written in a new module outside this
// crate: they compose modules with `policy_stack!`, plug the stack into
// `PolicyHooks`, and drive it through tonic's hook traits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use masa_core::{time_now, Context, ContextBuilder, PriorityHint};
use masa_policy::{
    get_masa_context_from_metadata, policy_stack, ChildRpcContext, Layer, LayerServer, PolicyHooks,
    ServerInit, WireIn, WireOut, MASA_CONTEXT_HEADER,
};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{Code, CowGrpcMethod, GrpcMethod, Request, Status};

type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

fn parent_method() -> GrpcMethod {
    GrpcMethod::new("custom.Service", "Parent")
}

fn child_method() -> GrpcMethod {
    GrpcMethod::new("custom.Service", "Child")
}

fn inbound(deadline: u64) -> http::Request<()> {
    let now = time_now();
    let ctx = ContextBuilder::new("custom-api", 1)
        .slo(deadline.saturating_sub(now))
        .gateway_entry(now)
        .deadline(deadline)
        .prio_hint(PriorityHint::new(deadline))
        .build();
    http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
        .body(())
        .unwrap()
}

fn begin<S: Layer + 'static>(service: &'static str, deadline: u64) -> Parent<S> {
    let server = Arc::new(Server::<S>::new(service));
    Parent::<S>::begin(parent_method(), &inbound(deadline), server)
}

/// Issue one child RPC and return the context the child would receive.
fn send_child<S: Layer + 'static>(parent: &Parent<S>) -> Result<Context, Status> {
    let mut request = Request::new(());
    let mut child = Child::<S>::new(child_method(), &request);
    parent.before_child_rpc(child_method(), &mut request, &mut child)?;
    Ok(get_masa_context_from_metadata(request.metadata()).expect("child context is attached"))
}

// ── Modules ─────────────────────────────────────────────────────────────

/// Assigns every child RPC a fixed priority.
#[derive(Debug)]
struct FixedChildPriority<const P: u64>;

impl<const P: u64> Layer for FixedChildPriority<P> {
    type Server = ();
    type Child = ();
    const NAME: &'static str = "FixedChildPriority";
    type Wire = ();

    fn new(_method: &CowGrpcMethod, _server: &(), _ctx: &mut Context, _wire: &WireIn<'_>) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child_method: &CowGrpcMethod,
        _child_ctx: &mut (),
        _request: &mut Request<T>,
        child_rpc: &mut ChildRpcContext,
        _child_wire: &mut WireOut,
    ) -> Result<(), Status> {
        child_rpc.prio_hint = PriorityHint::new(P);
        Ok(())
    }
}

/// Rejects every child RPC.
#[derive(Debug)]
struct RejectChildren;

impl Layer for RejectChildren {
    type Server = ();
    type Child = ();
    const NAME: &'static str = "RejectChildren";
    type Wire = ();

    fn new(_method: &CowGrpcMethod, _server: &(), _ctx: &mut Context, _wire: &WireIn<'_>) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child_method: &CowGrpcMethod,
        _child_ctx: &mut (),
        _request: &mut Request<T>,
        _child_rpc: &mut ChildRpcContext,
        _child_wire: &mut WireOut,
    ) -> Result<(), Status> {
        Err(Status::resource_exhausted("rejected by custom module"))
    }
}

/// Fails the test if a child RPC reaches it.
#[derive(Debug)]
struct UnreachableOnChild;

impl Layer for UnreachableOnChild {
    type Server = ();
    type Child = ();
    const NAME: &'static str = "UnreachableOnChild";
    type Wire = ();

    fn new(_method: &CowGrpcMethod, _server: &(), _ctx: &mut Context, _wire: &WireIn<'_>) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child_method: &CowGrpcMethod,
        _child_ctx: &mut (),
        _request: &mut Request<T>,
        _child_rpc: &mut ChildRpcContext,
        _child_wire: &mut WireOut,
    ) -> Result<(), Status> {
        panic!("module after a rejecting module must not run");
    }
}

/// Server-level counter shared between two modules through `ServerInit`.
#[derive(Debug, Clone, Default)]
struct ChildCounter(Arc<AtomicU64>);

/// Counts child RPCs and publishes the counter for later modules.
#[derive(Debug)]
struct CountChildren(ChildCounter);

#[derive(Debug)]
struct CountChildrenServer(ChildCounter);

impl LayerServer for CountChildrenServer {
    fn new(init: &mut ServerInit) -> Self {
        let counter = ChildCounter::default();
        init.provide(counter.clone());
        Self(counter)
    }
}

impl Layer for CountChildren {
    type Server = CountChildrenServer;
    type Child = ();
    const NAME: &'static str = "CountChildren";
    type Wire = ();

    fn new(
        _method: &CowGrpcMethod,
        server: &CountChildrenServer,
        _ctx: &mut Context,
        _wire: &WireIn<'_>,
    ) -> Self {
        Self(server.0.clone())
    }

    fn before_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child_method: &CowGrpcMethod,
        _child_ctx: &mut (),
        _request: &mut Request<T>,
        _child_rpc: &mut ChildRpcContext,
        _child_wire: &mut WireOut,
    ) -> Result<(), Status> {
        self.0 .0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Encodes the counter published by `CountChildren` into the child priority,
/// so the test can observe cross-module state through the wire context.
#[derive(Debug)]
struct PriorityFromCount(ChildCounter);

#[derive(Debug)]
struct PriorityFromCountServer(ChildCounter);

impl LayerServer for PriorityFromCountServer {
    fn new(init: &mut ServerInit) -> Self {
        Self(
            init.get::<ChildCounter>()
                .expect("CountChildren runs earlier in the stack"),
        )
    }
}

impl Layer for PriorityFromCount {
    type Server = PriorityFromCountServer;
    type Child = ();
    const NAME: &'static str = "PriorityFromCount";
    type Wire = ();

    fn new(
        _method: &CowGrpcMethod,
        server: &PriorityFromCountServer,
        _ctx: &mut Context,
        _wire: &WireIn<'_>,
    ) -> Self {
        Self(server.0.clone())
    }

    fn before_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child_method: &CowGrpcMethod,
        _child_ctx: &mut (),
        _request: &mut Request<T>,
        child_rpc: &mut ChildRpcContext,
        _child_wire: &mut WireOut,
    ) -> Result<(), Status> {
        child_rpc.prio_hint = PriorityHint::new(self.0 .0.load(Ordering::Relaxed));
        Ok(())
    }
}

/// Records the service name each server was constructed for.
#[derive(Debug)]
struct RecordServiceName;

#[derive(Debug)]
struct RecordServiceNameServer;

static SEEN_SERVICES: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

impl LayerServer for RecordServiceNameServer {
    fn new(init: &mut ServerInit) -> Self {
        SEEN_SERVICES.lock().unwrap().push(init.service_name());
        Self
    }
}

impl Layer for RecordServiceName {
    type Server = RecordServiceNameServer;
    type Child = ();
    const NAME: &'static str = "RecordServiceName";
    type Wire = ();

    fn new(
        _method: &CowGrpcMethod,
        _server: &RecordServiceNameServer,
        _ctx: &mut Context,
        _wire: &WireIn<'_>,
    ) -> Self {
        Self
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn empty_stack_forwards_parent_deadline_and_priority() {
    let deadline = time_now() + 1_000_000;
    let parent = begin::<policy_stack![]>("custom-empty", deadline);

    let child = send_child(&parent).unwrap();

    assert_eq!(child.deadline(), deadline);
    assert_eq!(child.prio_hint(), PriorityHint::new(deadline));
}

#[test]
fn custom_module_sets_child_priority() {
    let parent = begin::<policy_stack![FixedChildPriority<7>]>("custom-prio", time_now() + 1_000);

    let child = send_child(&parent).unwrap();

    assert_eq!(child.prio_hint(), PriorityHint::new(7));
}

#[test]
fn later_module_overrides_earlier_module() {
    type S = policy_stack![FixedChildPriority<7>, FixedChildPriority<9>];
    let parent = begin::<S>("custom-order", time_now() + 1_000);

    let child = send_child(&parent).unwrap();

    assert_eq!(child.prio_hint(), PriorityHint::new(9));
}

#[test]
fn rejection_short_circuits_later_modules() {
    type S = policy_stack![RejectChildren, UnreachableOnChild];
    let parent = begin::<S>("custom-reject", time_now() + 1_000);

    let err = send_child(&parent).unwrap_err();

    assert_eq!(err.code(), Code::ResourceExhausted);
}

#[test]
fn modules_share_server_state_through_server_init() {
    type S = policy_stack![CountChildren, PriorityFromCount];
    let parent = begin::<S>("custom-shared", time_now() + 1_000);

    assert_eq!(
        send_child(&parent).unwrap().prio_hint(),
        PriorityHint::new(1)
    );
    assert_eq!(
        send_child(&parent).unwrap().prio_hint(),
        PriorityHint::new(2)
    );
}

#[test]
fn server_init_carries_service_name() {
    let _ = Server::<policy_stack![RecordServiceName]>::new("custom-named");

    assert!(SEEN_SERVICES.lock().unwrap().contains(&"custom-named"));
}

#[test]
#[should_panic(expected = "CountChildren runs earlier in the stack")]
fn misordered_dependency_fails_at_server_construction() {
    let _ = Server::<policy_stack![PriorityFromCount, CountChildren]>::new("custom-misordered");
}
