// Queue latencies aggregate up the call tree through the `queue_latency`
// response section: each hop sums its children's totals (queue lengths take
// the maximum per service) and adds its own queue length.

#![cfg(feature = "trace_queue_latency")]

use std::collections::HashMap;
use std::sync::{Arc, Once};

use masa_core::time_now;
use masa_policy::modules::QueueLatencyModule;
use masa_policy::ContextBuilder;
use masa_policy::{
    policy_stack, MasaRequestExt, MasaResponseExt, PolicyHooks, QueueLatencyWire,
    MASA_CONTEXT_HEADER,
};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{GrpcMethod, Request, Response, Status};

type Stack = policy_stack![QueueLatencyModule];
type Server = <PolicyHooks<Stack> as Hooks>::ServerContext;
type Parent = <PolicyHooks<Stack> as Hooks>::ParentContext;
type Child = <PolicyHooks<Stack> as Hooks>::ChildContext;

fn init() {
    static ONCE: Once = Once::new();
    // The module reads its service name from the environment once per process.
    ONCE.call_once(|| std::env::set_var("SERVICE_NAME", "q-svc"));
}

fn begin(server: &Arc<Server>, method: &'static str) -> Parent {
    let now = time_now();
    let ctx = ContextBuilder::new("q-api", 1)
        .slo(1_000_000)
        .gateway_entry(now)
        .deadline(now + 1_000_000)
        .build();
    let req = http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
        .body(())
        .unwrap();
    Parent::begin(GrpcMethod::new("q.Service", method), &req, server.clone())
}

fn call(parent: &Parent, method: &'static str, reply: Response<()>) {
    let m = GrpcMethod::new("q.Service", method);
    let mut request = Request::new(());
    let mut child = Child::new(m, &request);
    parent
        .before_child_rpc(m, &mut request, &mut child)
        .unwrap();
    parent.after_child_rpc(m, &mut Ok(reply), child).unwrap();
}

fn reply_with(wire: Option<QueueLatencyWire>) -> Response<()> {
    let now = time_now();
    let ctx = ContextBuilder::new("q-api", 1)
        .slo(1_000_000)
        .gateway_entry(now)
        .deadline(now + 1_000_000)
        .build();
    let mut reply = Response::new(()).with_masa_context(&ctx);
    if let Some(wire) = wire {
        reply.set_wire::<QueueLatencyModule>(&wire);
    }
    reply
}

fn lengths(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

fn finish(parent: &Parent) -> Response<()> {
    let mut result: Result<Response<()>, Status> = Ok(Response::new(()));
    parent.finalize_before_serialization(&mut result);
    result.unwrap()
}

fn run<F: FnOnce() + Send + 'static>(f: F) {
    init();
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async move { tokio::spawn(async move { f() }).await.unwrap() });
}

#[test]
fn totals_aggregate_across_two_hops() {
    run(|| {
        let server = Arc::new(Server::new("q.Service"));

        // Middle hop: one child (the leaf) reported these totals.
        let mid = begin(&server, "Mid");
        call(
            &mid,
            "Leaf",
            reply_with(Some(QueueLatencyWire {
                initial: 11,
                resume: 22,
                queue_lengths: lengths(&[("leaf", 5)]),
            })),
        );
        let mid_reply = finish(&mid);
        let mid_wire = mid_reply.get_wire::<QueueLatencyModule>().unwrap();
        assert_eq!(mid_wire.initial, 11);
        assert_eq!(mid_wire.resume, 22);
        assert_eq!(
            mid_wire.queue_lengths,
            lengths(&[("leaf", 5), ("q-svc", 0)])
        );

        // Root hop: the middle hop's real response plus a sibling's.
        let root = begin(&server, "Root");
        call(&root, "Mid", mid_reply);
        call(
            &root,
            "Sibling",
            reply_with(Some(QueueLatencyWire {
                initial: 1,
                resume: 2,
                queue_lengths: lengths(&[("leaf", 9)]),
            })),
        );
        let root_wire = finish(&root).get_wire::<QueueLatencyModule>().unwrap();
        assert_eq!(root_wire.initial, 12);
        assert_eq!(root_wire.resume, 24);
        assert_eq!(
            root_wire.queue_lengths,
            lengths(&[("leaf", 9), ("q-svc", 0)])
        );
    });
}

#[test]
fn an_all_zero_child_section_round_trips_as_present() {
    run(|| {
        let server = Arc::new(Server::new("q.Service"));
        let parent = begin(&server, "Root");
        call(
            &parent,
            "Leaf",
            reply_with(Some(QueueLatencyWire::default())),
        );
        let wire = finish(&parent).get_wire::<QueueLatencyModule>().unwrap();
        assert_eq!((wire.initial, wire.resume), (0, 0));
        assert_eq!(wire.queue_lengths, lengths(&[("q-svc", 0)]));
    });
}

#[test]
fn a_child_without_a_section_contributes_nothing() {
    run(|| {
        let server = Arc::new(Server::new("q.Service"));
        let parent = begin(&server, "Root");
        call(&parent, "Leaf", reply_with(None));
        let wire = finish(&parent).get_wire::<QueueLatencyModule>().unwrap();
        assert_eq!((wire.initial, wire.resume), (0, 0));
        assert_eq!(wire.queue_lengths, lengths(&[("q-svc", 0)]));
    });
}

#[test]
fn the_response_section_is_the_only_carrier() {
    run(|| {
        let server = Arc::new(Server::new("q.Service"));
        let parent = begin(&server, "Root");
        let (request, _child) = {
            let m = GrpcMethod::new("q.Service", "Leaf");
            let mut request = Request::new(());
            let mut child = Child::new(m, &request);
            parent
                .before_child_rpc(m, &mut request, &mut child)
                .unwrap();
            (request, child)
        };
        // Nothing queue-related is sent down with the child request.
        assert_eq!(request.get_wire::<QueueLatencyModule>(), None);
    });
}
