// Per-request typed extensions shared between modules, and server-level
// dependencies between modules that fail at construction when misordered.

use std::sync::Arc;

use masa_core::time_now;
use masa_policy::ContextBuilder;
use masa_policy::{
    policy_stack, ChildState, Extensions, Layer, LayerServer, LayerStack, MasaResponseExt,
    MissingDependency, Outcome, PolicyHooks, ServerContext, ServerInit, WireIn, WireOut,
    MASA_CONTEXT_HEADER,
};
use serde::{Deserialize, Serialize};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

fn root_request() -> http::Request<()> {
    let now = time_now();
    let ctx = ContextBuilder::new("ext-api", 1)
        .slo(1_000_000)
        .gateway_entry(now)
        .deadline(now + 1_000_000)
        .build();
    http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
        .body(())
        .unwrap()
}

// ── Per-request extensions ──────────────────────────────────────────────

/// Data one module shares with later modules of the same request.
#[derive(Debug, PartialEq)]
struct Shared {
    child_calls: u64,
}

/// Creates `Shared` and counts child calls in it.
#[derive(Debug)]
struct Producer;

impl Layer for Producer {
    type Server = ();
    const NAME: &'static str = "producer";
    type Wire = ();

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, ext: &mut Extensions) -> Self {
        ext.insert(Shared { child_calls: 0 });
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _child: &mut ChildState,
        _request: &mut Request<T>,
        _child_wire: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        ext.get_mut::<Shared>()
            .expect("inserted in new")
            .child_calls += 1;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SeenWire {
    in_new: Option<u64>,
    in_before_child: Option<u64>,
    in_after_child: Option<u64>,
}

/// Reports what it found in `Shared` at each hook.
#[derive(Debug)]
struct Consumer {
    in_new: Option<u64>,
    in_before_child: std::sync::Mutex<Option<u64>>,
    in_after_child: std::sync::Mutex<Option<u64>>,
}

impl Layer for Consumer {
    type Server = ();
    const NAME: &'static str = "consumer";
    type Wire = SeenWire;

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, ext: &mut Extensions) -> Self {
        Self {
            in_new: ext.get::<Shared>().map(|shared| shared.child_calls),
            in_before_child: Default::default(),
            in_after_child: Default::default(),
        }
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _child: &mut ChildState,
        _request: &mut Request<T>,
        _child_wire: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        *self.in_before_child.lock().unwrap() = ext.get::<Shared>().map(|s| s.child_calls);
        Ok(())
    }

    fn after_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _response: &Result<Response<T>, Status>,
        _response_wire: &WireIn<'_>,
        _child: &ChildState,
        ext: &Extensions,
    ) -> Result<(), Status> {
        *self.in_after_child.lock().unwrap() = ext.get::<Shared>().map(|s| s.child_calls);
        Ok(())
    }

    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        wire.put::<Self>(&SeenWire {
            in_new: self.in_new,
            in_before_child: *self.in_before_child.lock().unwrap(),
            in_after_child: *self.in_after_child.lock().unwrap(),
        })
        .unwrap();
    }
}

/// One request through one child call; returns what `Consumer` reported.
fn run_request<S: LayerStack>() -> SeenWire {
    let server = Arc::new(Server::<S>::new("ext.Service"));
    let parent = Parent::<S>::begin(
        GrpcMethod::new("ext.Service", "Hop"),
        &root_request(),
        server,
    );
    let method = GrpcMethod::new("ext.Service", "Child");
    let mut request = Request::new(());
    let mut child = Child::<S>::new(method, &request);
    parent
        .before_child_rpc(method, &mut request, &mut child)
        .unwrap();
    let mut reply = Ok(Response::new(()));
    parent.after_child_rpc(method, &mut reply, child).unwrap();
    let mut result = Ok(Response::new(()));
    parent.finalize_before_serialization(&mut result);
    result
        .unwrap()
        .get_wire::<Consumer>()
        .expect("consumer wrote its wire")
}

#[test]
fn two_modules_share_per_request_data_through_extensions() {
    type Stack = policy_stack![Producer, Consumer];
    assert_eq!(
        run_request::<Stack>(),
        SeenWire {
            in_new: Some(0),
            // The producer runs first in the hook, so its update is visible.
            in_before_child: Some(1),
            in_after_child: Some(1),
        }
    );
}

#[test]
fn a_module_earlier_in_the_stack_does_not_see_values_inserted_later_in_new() {
    type Stack = policy_stack![Consumer, Producer];
    let seen = run_request::<Stack>();
    assert_eq!(seen.in_new, None);
    // By the later hooks the value exists, and the consumer now runs first.
    assert_eq!(seen.in_before_child, Some(0));
    assert_eq!(seen.in_after_child, Some(1));
}

#[test]
fn extensions_do_not_leak_between_requests() {
    type Stack = policy_stack![Producer, Consumer];
    let first = run_request::<Stack>();
    let second = run_request::<Stack>();
    assert_eq!(first, second);
}

// ── Server-level dependencies ───────────────────────────────────────────

#[derive(Clone)]
struct Published;

#[derive(Debug)]
struct ProvidesServer;

impl LayerServer for ProvidesServer {
    fn new(init: &mut ServerInit) -> Result<Self, MissingDependency> {
        init.provide(Published);
        Ok(Self)
    }
}

#[derive(Debug)]
struct RequiresServer;

impl LayerServer for RequiresServer {
    fn new(init: &mut ServerInit) -> Result<Self, MissingDependency> {
        init.require::<Published>()?;
        Ok(Self)
    }
}

macro_rules! dependency_module {
    ($name:ident, $server:ty, $wire_name:literal) => {
        #[derive(Debug)]
        struct $name;

        impl Layer for $name {
            type Server = $server;
            const NAME: &'static str = $wire_name;
            type Wire = ();

            fn new(_m: &CowGrpcMethod, _s: &$server, _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
                Self
            }
        }
    };
}

dependency_module!(Provides, ProvidesServer, "provides");
dependency_module!(Requires, RequiresServer, "requires");

#[test]
fn a_required_value_published_earlier_is_found() {
    assert!(ServerContext::<policy_stack![Provides, Requires]>::try_new("svc").is_ok());
}

#[test]
fn a_missing_dependency_is_an_error_naming_both_sides() {
    let err = ServerContext::<policy_stack![Requires]>::try_new("svc").unwrap_err();
    assert!(err.resource().ends_with("Published"), "{err}");
    assert!(err.module().ends_with("RequiresServer"), "{err}");
    let message = err.to_string();
    assert!(message.contains("Published"), "{message}");
    assert!(message.contains("RequiresServer"), "{message}");
}

#[test]
fn a_dependency_published_later_in_the_stack_is_missing() {
    let err = ServerContext::<policy_stack![Requires, Provides]>::try_new("svc").unwrap_err();
    assert!(err.resource().ends_with("Published"), "{err}");
}

#[test]
#[should_panic(expected = "requires a `extensions::Published`")]
fn server_construction_panics_with_the_same_message() {
    let _ = Server::<policy_stack![Requires]>::new("svc");
}
