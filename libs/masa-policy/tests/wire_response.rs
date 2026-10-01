// Response-side wire data: a module reads what a child's modules wrote into
// the child's response in `after_child_rpc`, and reports upward only what it
// writes itself in `finalize`. Everything here runs in-process through the
// public hook API: parent `begin` -> `before_child_rpc` -> child `begin` ->
// child `finalize` -> parent `after_child_rpc` -> parent `finalize`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use masa_core::time_now;
use masa_policy::ContextBuilder;
use masa_policy::{
    policy_stack, BudgetLayer, ChildOutcome, ChildState, Extensions, Layer, MasaResponseExt,
    MasaStatusExt, Outcome, PolicyHooks, WireIn, WireOut, MASA_CONTEXT_HEADER,
};
use serde::{Deserialize, Serialize};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SumWire {
    total: u64,
    /// How many child response sections this hop read.
    seen: u64,
}

/// Reports `own + sum of children` upward. `own` depends on the hop's method:
/// `Leaf` contributes 0, `Mid` 10, `Root` 100.
#[derive(Debug)]
struct Sum {
    own: u64,
    children: AtomicU64,
    seen_child_sections: AtomicU64,
}

impl Layer for Sum {
    type Server = ();
    const NAME: &'static str = "sum";
    type Wire = SumWire;

    fn new(method: &CowGrpcMethod, _s: &(), _wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        let own = match method.method() {
            "Root" => 100,
            "Mid" => 10,
            _ => 0,
        };
        Self {
            own,
            children: AtomicU64::new(0),
            seen_child_sections: AtomicU64::new(0),
        }
    }

    fn after_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _outcome: ChildOutcome<'_, T>,
        response_wire: &WireIn<'_>,
        _child: &ChildState,
        _ext: &Extensions,
    ) -> Result<(), Status> {
        if let Some(wire) = response_wire.get::<Self>().unwrap() {
            self.children.fetch_add(wire.total, Ordering::Relaxed);
            self.seen_child_sections.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        wire.put::<Self>(&SumWire {
            total: self.own + self.children.load(Ordering::Relaxed),
            seen: self.seen_child_sections.load(Ordering::Relaxed),
        })
        .unwrap();
    }
}

/// Never writes a response section of its own.
#[derive(Debug)]
struct Mute;

impl Layer for Mute {
    type Server = ();
    const NAME: &'static str = "mute";
    type Wire = u8;

    fn new(_m: &CowGrpcMethod, _s: &(), _wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self
    }
}

type Stack = policy_stack![BudgetLayer, Sum, Mute];

fn root_request() -> http::Request<()> {
    let now = time_now();
    let ctx = ContextBuilder::new("wire-api", 1)
        .slo(1_000_000)
        .gateway_entry(now)
        .deadline(now + 1_000_000)
        .build();
    http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
        .body(())
        .unwrap()
}

fn begin(
    server: &Arc<Server<Stack>>,
    method: &'static str,
    req: &http::Request<()>,
) -> Parent<Stack> {
    Parent::<Stack>::begin(GrpcMethod::new("wire.Service", method), req, server.clone())
}

/// Sends a child request from `parent` and returns it as the child hop's
/// transport would hand it to `begin`, with the child context to pass back to
/// `after_child_rpc`.
fn send_to_child(
    parent: &Parent<Stack>,
    child_method: &'static str,
) -> (http::Request<()>, Child<Stack>) {
    let method = GrpcMethod::new("wire.Service", child_method);
    let mut request = Request::new(());
    let mut child = Child::<Stack>::new(method, &request);
    parent
        .before_child_rpc(method, &mut request, &mut child)
        .unwrap();
    let value = request.metadata().get(MASA_CONTEXT_HEADER).unwrap();
    let arrived = http::Request::builder()
        .header(MASA_CONTEXT_HEADER, value.to_str().unwrap())
        .body(())
        .unwrap();
    (arrived, child)
}

fn finish(parent: &Parent<Stack>) -> Result<Response<()>, Status> {
    let mut result = Ok(Response::new(()));
    parent.finalize_before_serialization(&mut result);
    result
}

fn receive(
    parent: &Parent<Stack>,
    child_method: &'static str,
    mut response: Result<Response<()>, Status>,
    child: Child<Stack>,
) {
    parent
        .after_child_rpc(
            GrpcMethod::new("wire.Service", child_method),
            &mut response,
            child,
        )
        .unwrap();
}

fn total(response: &Response<()>) -> Option<u64> {
    response.get_wire::<Sum>().map(|wire| wire.total)
}

#[test]
fn parent_reads_the_child_response_section_across_two_hops() {
    let server = Arc::new(Server::<Stack>::new("wire.Service"));
    let root = begin(&server, "Root", &root_request());

    let (to_mid, mid_child_ctx) = send_to_child(&root, "Mid");
    let mid = begin(&server, "Mid", &to_mid);

    let (to_leaf, leaf_child_ctx) = send_to_child(&mid, "Leaf");
    let leaf = begin(&server, "Leaf", &to_leaf);

    let leaf_reply = finish(&leaf);
    assert_eq!(total(leaf_reply.as_ref().unwrap()), Some(0));

    receive(&mid, "Leaf", leaf_reply, leaf_child_ctx);
    let mid_reply = finish(&mid);
    assert_eq!(total(mid_reply.as_ref().unwrap()), Some(10));

    receive(&root, "Mid", mid_reply, mid_child_ctx);
    let root_reply = finish(&root);
    assert_eq!(total(root_reply.as_ref().unwrap()), Some(110));
}

#[test]
fn a_fan_out_aggregates_every_child_response() {
    let server = Arc::new(Server::<Stack>::new("wire.Service"));
    let root = begin(&server, "Root", &root_request());

    for _ in 0..3 {
        let (to_mid, child_ctx) = send_to_child(&root, "Mid");
        let mid = begin(&server, "Mid", &to_mid);
        receive(&root, "Mid", finish(&mid), child_ctx);
    }
    assert_eq!(total(finish(&root).as_ref().unwrap()), Some(130));
}

#[test]
fn a_zero_section_is_seen_as_present() {
    let server = Arc::new(Server::<Stack>::new("wire.Service"));
    let root = begin(&server, "Root", &root_request());
    let (to_leaf, child_ctx) = send_to_child(&root, "Leaf");
    let leaf = begin(&server, "Leaf", &to_leaf);
    receive(&root, "Leaf", finish(&leaf), child_ctx);

    let reply = finish(&root).unwrap();
    assert_eq!(
        reply.get_wire::<Sum>(),
        Some(SumWire {
            total: 100,
            seen: 1
        })
    );
}

#[test]
fn an_error_status_carries_its_wire_section_to_the_parent() {
    let server = Arc::new(Server::<Stack>::new("wire.Service"));
    let root = begin(&server, "Root", &root_request());
    let (to_mid, child_ctx) = send_to_child(&root, "Mid");
    let mid = begin(&server, "Mid", &to_mid);

    let mut failed = Err(Status::internal("handler failed"));
    mid.finalize_before_serialization(&mut failed);
    let status = failed.as_ref().unwrap_err();
    assert_eq!(
        status.get_wire::<Sum>(),
        Some(SumWire { total: 10, seen: 0 })
    );

    receive(&root, "Mid", failed, child_ctx);
    assert_eq!(total(finish(&root).as_ref().unwrap()), Some(110));
}

#[test]
fn nothing_from_the_child_response_is_carried_to_the_parent_response() {
    let server = Arc::new(Server::<Stack>::new("wire.Service"));
    let root = begin(&server, "Root", &root_request());
    let (to_leaf, child_ctx) = send_to_child(&root, "Leaf");
    let leaf = begin(&server, "Leaf", &to_leaf);

    let mut leaf_reply = finish(&leaf);
    // A section of a module that does not write a response of its own.
    leaf_reply
        .as_mut()
        .unwrap()
        .metadata_mut()
        .insert("unrelated", "1".parse().unwrap());
    receive(&root, "Leaf", leaf_reply, child_ctx);

    let reply = finish(&root).unwrap();
    assert_eq!(reply.get_wire::<Mute>(), None);
    assert!(reply.metadata().get("unrelated").is_none());
}

#[test]
fn a_child_response_without_sections_reads_as_absent() {
    let server = Arc::new(Server::<Stack>::new("wire.Service"));
    let root = begin(&server, "Root", &root_request());
    let (_to_leaf, child_ctx) = send_to_child(&root, "Leaf");
    receive(&root, "Leaf", Ok(Response::new(())), child_ctx);
    let reply = finish(&root).unwrap();
    assert_eq!(
        reply.get_wire::<Sum>(),
        Some(SumWire {
            total: 100,
            seen: 0
        })
    );
}
