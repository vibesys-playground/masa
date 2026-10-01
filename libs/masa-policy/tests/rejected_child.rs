// Masa's modules treat a child RPC that a later module rejected as what it is,
// a child that was never sent: every module whose `before_child_rpc` ran hears
// of it in `after_child_rpc`, and the built-in ones ignore it.

#![cfg(feature = "abort_slo")]

use std::sync::Arc;
use std::time::Duration;

use masa_core::time_now;
use masa_policy::modules::E2eDeadlineGuardModule;
use masa_policy::{
    policy_stack, BudgetModule, ChildState, ContextBuilder, Extensions, Module, PolicyHooks,
    WireIn, WireOut, MASA_CONTEXT_HEADER,
};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

/// Rejects every child RPC.
#[derive(Debug)]
struct RejectsChildren;

impl Module for RejectsChildren {
    type Server = ();
    const NAME: &'static str = "rejects_children";
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
        Err(Status::resource_exhausted("no children"))
    }
}

type Rejecting = policy_stack![BudgetModule, E2eDeadlineGuardModule, RejectsChildren];
type Accepting = policy_stack![BudgetModule, E2eDeadlineGuardModule];

/// The message of the guard's rejection after a child call, once the request's
/// SLO has run out.
fn guard_message_after_child<S: masa_policy::ModuleStack>(send: bool) -> String {
    const SERVICE: &str = "rejected-child";
    let now = time_now();
    let slo = 20_000; // 20 ms
    let ctx = ContextBuilder::new("guard-api", 1)
        .slo(slo)
        .gateway_entry(now)
        .deadline(now + slo)
        .build();
    let req = http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
        .body(())
        .unwrap();
    type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
    type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;
    let method = GrpcMethod::new(SERVICE, "Parent");
    let child_method = GrpcMethod::new(SERVICE, "Child");
    let parent = Parent::<S>::begin(
        method,
        &req,
        Arc::new(<PolicyHooks<S> as Hooks>::ServerContext::new(SERVICE)),
    );

    let mut request = Request::new(());
    let mut child = Child::<S>::new(child_method, &request);
    let issued = parent.before_child_rpc(child_method, &mut request, &mut child);
    assert_eq!(issued.is_ok(), send);
    if issued.is_ok() {
        let mut response: Result<Response<()>, Status> = Ok(Response::new(()));
        parent
            .after_child_rpc(child_method, &mut response, child)
            .unwrap();
    }

    std::thread::sleep(Duration::from_millis(30));
    match parent.before_poll::<()>() {
        Err(Err(status)) => status.message().to_owned(),
        other => panic!("the guard should reject an expired request, got {other:?}"),
    }
}

#[test]
fn the_guard_reports_the_last_child_that_was_sent() {
    let message = guard_message_after_child::<Accepting>(true);
    assert!(
        message.contains("last_rpc=rejected-child::Child"),
        "{message}"
    );
}

#[test]
fn the_guard_ignores_a_child_that_a_later_module_rejected() {
    let message = guard_message_after_child::<Rejecting>(false);
    assert!(!message.contains("last_rpc"), "{message}");
}
