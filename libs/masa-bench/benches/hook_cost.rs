//! Cost of the Masa hooks for one RPC that makes one child call.
//!
//! One iteration plays the hooks of a service ("Front") handling a request:
//! the client creates the root request, the server hook `begin`s it, the
//! handler future is polled three times (`before_poll`/`after_poll` around
//! each, the first two returning `Pending`), during the first poll it calls a
//! child service ("Back", simulated by a second in-process set of server
//! hooks that begins, polls once and finalizes), then the last poll returns
//! `Ready`, the response is finalized and the hooks are dropped. Nothing
//! crosses a network and no handler logic runs, so the number is the hook
//! overhead alone.
//!
//! It goes through the stable `tonic::masa` hook traits on
//! `masa::DefaultHooks`, which the Cargo features of this crate select.
//!
//! Usage: `hook_cost [iterations]` (default 150000).

use std::hint::black_box;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use masa::DefaultHooks;
use masa_bench::alloc::{self, CountingAlloc};
use masa_bench::compat::root_request;
use masa_bench::report::{metric, note};
use masa_bench::{feature_label, header_bytes, http_request};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{GrpcMethod, Request, Response, Status};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

type Server = <DefaultHooks as Hooks>::ServerContext;
type Child = <DefaultHooks as Hooks>::ChildContext;
type Parent = <DefaultHooks as Hooks>::ParentContext;
type Outcome = Result<Response<()>, Status>;

/// Polls of the handler future per RPC: two `Pending`, one `Ready`.
const POLLS: usize = 3;

/// Time spent in each phase of one RPC, in nanoseconds, summed over all
/// iterations of a pass.
#[derive(Default, Clone, Copy)]
struct Phases {
    client_request: u64,
    begin: u64,
    before_poll: u64,
    after_poll: u64,
    before_child_rpc: u64,
    child_before_send: u64,
    callee_hop: u64,
    after_child_rpc: u64,
    finalize: u64,
    drop: u64,
}

impl Phases {
    fn min(self, other: Self) -> Self {
        Self {
            client_request: self.client_request.min(other.client_request),
            begin: self.begin.min(other.begin),
            before_poll: self.before_poll.min(other.before_poll),
            after_poll: self.after_poll.min(other.after_poll),
            before_child_rpc: self.before_child_rpc.min(other.before_child_rpc),
            child_before_send: self.child_before_send.min(other.child_before_send),
            callee_hop: self.callee_hop.min(other.callee_hop),
            after_child_rpc: self.after_child_rpc.min(other.after_child_rpc),
            finalize: self.finalize.min(other.finalize),
            drop: self.drop.min(other.drop),
        }
    }
}

/// The callee's hooks for one child request: begin, one poll that returns
/// `Ready`, finalize. Returns the response the caller would receive.
fn callee_hop(child_server: &Arc<Server>, request: &Request<()>) -> Outcome {
    let method = GrpcMethod::new("Back", "Work");
    let http = http_request(request.metadata());
    let parent = <Parent as ParentHooks<Child, Server>>::begin(method, &http, child_server.clone());
    let _ = parent.before_poll::<()>();
    let mut response: Outcome = Ok(Response::new(()));
    let _ = parent.after_poll(&Poll::Ready(Ok(Response::new(()))));
    parent.finalize_before_serialization(&mut response);
    response
}

/// One RPC. If `phases` is given, adds the time of each phase to it.
fn one_rpc(
    server: &Arc<Server>,
    child_server: &Arc<Server>,
    id: u64,
    mut phases: Option<&mut Phases>,
) {
    macro_rules! timed {
        ($field:ident, $body:expr) => {{
            if let Some(p) = phases.as_deref_mut() {
                let start = Instant::now();
                let result = $body;
                p.$field += start.elapsed().as_nanos() as u64;
                result
            } else {
                $body
            }
        }};
    }
    let pending: Poll<Outcome> = Poll::Pending;

    let (request, http) = timed!(client_request, {
        let request = root_request(id);
        let http = http_request(request.metadata());
        (request, http)
    });
    let method = GrpcMethod::new("Front", "Handle");
    let parent = timed!(
        begin,
        <Parent as ParentHooks<Child, Server>>::begin(method, &http, server.clone())
    );
    let _ = timed!(before_poll, parent.before_poll::<()>());
    let _ = timed!(after_poll, parent.after_poll(&pending));

    let child_method = GrpcMethod::new("Back", "Work");
    let mut child_request = Request::new(());
    let mut child = Child::new(child_method, &child_request);
    let _ = timed!(
        before_child_rpc,
        parent.before_child_rpc(child_method, &mut child_request, &mut child)
    );
    timed!(child_before_send, child.before_send(&mut child_request));
    let mut child_response = timed!(callee_hop, callee_hop(child_server, &child_request));
    child.after_recv(&mut child_response);
    let _ = timed!(
        after_child_rpc,
        parent.after_child_rpc(child_method, &mut child_response, child)
    );

    for _ in 0..POLLS - 2 {
        let _ = timed!(before_poll, parent.before_poll::<()>());
        let _ = timed!(after_poll, parent.after_poll(&pending));
    }
    let _ = timed!(before_poll, parent.before_poll::<()>());
    let mut response: Outcome = Ok(Response::new(()));
    let _ = timed!(
        after_poll,
        parent.after_poll(&Poll::Ready(Ok(Response::new(()))))
    );
    timed!(
        finalize,
        parent.finalize_before_serialization(&mut response)
    );
    black_box((&response, &request));
    timed!(drop, drop(parent));
}

/// Header bytes of the root request, the child request and the response of
/// one RPC, as `(root request, child request, response)`.
fn wire_sizes(server: &Arc<Server>, child_server: &Arc<Server>) -> (usize, usize, usize) {
    let request = root_request(1);
    let http = http_request(request.metadata());
    let method = GrpcMethod::new("Front", "Handle");
    let parent = <Parent as ParentHooks<Child, Server>>::begin(method, &http, server.clone());
    let child_method = GrpcMethod::new("Back", "Work");
    let mut child_request = Request::new(());
    let mut child = Child::new(child_method, &child_request);
    let _ = parent.before_child_rpc(child_method, &mut child_request, &mut child);
    child.before_send(&mut child_request);
    let response = callee_hop(child_server, &child_request);
    let response_bytes = response
        .as_ref()
        .map(|r| header_bytes(r.metadata()))
        .unwrap_or(0);
    (
        header_bytes(request.metadata()),
        header_bytes(child_request.metadata()),
        response_bytes,
    )
}

fn run(server: &Arc<Server>, child_server: &Arc<Server>, iters: u64) {
    let label = feature_label();

    let (request_bytes, child_request_bytes, response_bytes) = wire_sizes(&server, &child_server);
    metric(
        "hook_cost",
        &label,
        "ctx_request_bytes",
        request_bytes as f64,
    );
    metric(
        "hook_cost",
        &label,
        "ctx_child_request_bytes",
        child_request_bytes as f64,
    );
    metric(
        "hook_cost",
        &label,
        "ctx_response_bytes",
        response_bytes as f64,
    );

    for i in 0..20_000 {
        one_rpc(&server, &child_server, i, None);
    }

    let rpcs = 10_000u64;
    let (allocs_before, bytes_before) = alloc::snapshot();
    for i in 0..rpcs {
        one_rpc(&server, &child_server, i, None);
    }
    let (allocs_after, bytes_after) = alloc::snapshot();
    let allocs_per_rpc = (allocs_after - allocs_before) as f64 / rpcs as f64;
    let bytes_per_rpc = (bytes_after - bytes_before) as f64 / rpcs as f64;
    metric("hook_cost", &label, "allocs_per_rpc", allocs_per_rpc);
    metric("hook_cost", &label, "alloc_bytes_per_rpc", bytes_per_rpc);

    // Whole RPCs without per-phase clock reads, then per-phase passes; each
    // metric is the best of several passes.
    const PASSES: usize = 3;
    let mut total_ns = f64::MAX;
    let mut phases: Option<Phases> = None;
    for _ in 0..PASSES {
        let start = Instant::now();
        for i in 0..iters {
            one_rpc(&server, &child_server, i, None);
        }
        total_ns = total_ns.min(start.elapsed().as_nanos() as f64 / iters as f64);
        let mut pass = Phases::default();
        for i in 0..iters {
            one_rpc(&server, &child_server, i, Some(&mut pass));
        }
        phases = Some(phases.map_or(pass, |best| best.min(pass)));
    }
    let phases = phases.unwrap_or_default();
    let per_rpc = |ns: u64| ns as f64 / iters as f64;
    let named = [
        ("client_request_ns", phases.client_request),
        ("begin_ns", phases.begin),
        ("before_poll_x3_ns", phases.before_poll),
        ("after_poll_x3_ns", phases.after_poll),
        ("before_child_rpc_ns", phases.before_child_rpc),
        ("child_before_send_ns", phases.child_before_send),
        ("callee_hop_ns", phases.callee_hop),
        ("after_child_rpc_ns", phases.after_child_rpc),
        ("finalize_ns", phases.finalize),
        ("drop_ns", phases.drop),
    ];
    metric("hook_cost", &label, "total_ns", total_ns);
    for (name, ns) in named {
        metric("hook_cost", &label, &format!("phase.{name}"), per_rpc(ns));
    }

    // One before_poll plus one after_poll on an already begun parent, the
    // per-poll tax every handler poll pays.
    let request = root_request(7);
    let http = http_request(request.metadata());
    let parent = <Parent as ParentHooks<Child, Server>>::begin(
        GrpcMethod::new("Front", "Handle"),
        &http,
        server.clone(),
    );
    let pending: Poll<Outcome> = Poll::Pending;
    let pair_ns = masa_bench::min_ns_per_call(5, 2_000_000, || {
        let _ = black_box(parent.before_poll::<()>());
        let _ = black_box(parent.after_poll(black_box(&pending)));
    });
    metric("hook_cost", &label, "poll_pair_ns", pair_ns);

    note(&format!(
        "hook_cost [{label}] ({iters} RPCs per pass, best of {PASSES})"
    ));
    note(&format!("  per RPC            {total_ns:>9.0} ns"));
    for (name, ns) in named {
        note(&format!("  {name:<19}{:>9.0} ns", per_rpc(ns)));
    }
    note(&format!("  poll pair          {pair_ns:>9.1} ns"));
    note(&format!(
        "  allocations/RPC    {allocs_per_rpc:>9.1} ({bytes_per_rpc:.0} bytes)"
    ));
    note(&format!(
        "  ctx header bytes   request {request_bytes}, child request {child_request_bytes}, response {response_bytes}"
    ));
}

fn main() {
    let iters = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(150_000);
    std::env::set_var("SERVICE_NAME", "hook_cost");
    let server = Arc::new(<Server as ServerHooks>::new("Front"));
    let child_server = Arc::new(<Server as ServerHooks>::new("Back"));
    // Some hooks start a one-shot background worker on first use. Make that
    // first use here, outside any runtime, as the applications do at startup.
    let init = root_request(0);
    let init_http = http_request(init.metadata());
    drop(<Parent as ParentHooks<Child, Server>>::begin(
        GrpcMethod::new("Init", "Init"),
        &init_http,
        server.clone(),
    ));
    // The hooks run inside a task of a current-thread runtime, like handlers.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(|err| panic!("cannot build the tokio runtime: {err}"));
    runtime.block_on(async move {
        tokio::spawn(async move { run(&server, &child_server, iters) })
            .await
            .unwrap_or_else(|err| panic!("benchmark task failed: {err}"));
    });
}
