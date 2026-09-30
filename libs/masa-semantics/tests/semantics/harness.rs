//! The one file that knows how Masa's hooks are driven.
//!
//! Scenarios describe *what happens* to a request as it flows through a
//! virtual call graph (deadlines, priorities, admission, aborts, metadata).
//! This adapter turns that description into calls on the public hook traits of
//! `tonic::masa` and reads results back through `masa`'s public request,
//! response and status extensions. If the policy machinery is re-expressed,
//! only this file may need to change.
//!
//! No servers or sockets are involved. A call graph `client -> A -> B` is
//! simulated in-process: the outbound child request produced by A's hooks
//! becomes B's inbound request, and B's response becomes the input of A's
//! `after_child_rpc`. Time is virtual (`masa_core::test_clock`), so nothing
//! sleeps and no assertion depends on the wall clock.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::panic::resume_unwind;
use std::sync::{Arc, Once};
use std::task::Poll;
use std::time::Duration;

use masa::{MasaRequestExt, MasaResponseExt, RootContext, MASA_CONTEXT_HEADER};
use masa_core::{test_clock, Context, ContextBuilder, PriorityHint};
use tonic::masa::{ClientHooks, ParentHooks, ServerHooks};
use tonic::{Code, GrpcMethod, Request, Response, Status};

/// The hooks implementation the suite runs against. Scenarios never name it.
pub type Under = masa::DefaultHooks;

pub use tonic::masa::Hooks;

/// Name under which the queue-latency module reports this process.
pub const QUEUE_SERVICE_NAME: &str = "sem-svc";

/// Policy parameters the process runs with (`MASA_POLICY_PARAMS_PATH`).
///
/// Defaults are Masa's shipped defaults except where a scenario binary opts
/// into a deterministic setting.
#[derive(Clone, Copy, Debug)]
pub struct Params {
    /// Rajomon: attach the price to one response in `price_freq`.
    pub rajomon_price_freq: u64,
}

impl Default for Params {
    fn default() -> Self {
        // Always attaching makes price propagation deterministic.
        Self {
            rajomon_price_freq: 1,
        }
    }
}

// ── Process setup ───────────────────────────────────────────────────────

/// One-time process setup. `suppress_background_workers` consumes the
/// one-shot start of background price/replenish workers outside any runtime
/// so they never tick during a scenario.
pub fn init<H: Hooks>(params: Params, suppress_background_workers: bool) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("SERVICE_NAME", QUEUE_SERVICE_NAME);
        let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("policy_params_{}.json", std::process::id()));
        let json = format!(
            "{{\"rajomon\": {{\"price_freq\": {}}}}}",
            params.rajomon_price_freq
        );
        std::fs::write(&path, json).expect("write policy params");
        std::env::set_var("MASA_POLICY_PARAMS_PATH", &path);
        if suppress_background_workers {
            let server = Arc::new(<H::ServerContext as ServerHooks>::new("SemInit"));
            let ctx = masa::create_context("SemInit", Duration::from_secs(1));
            let _ = <H::ParentContext as ParentHooks<H::ChildContext, H::ServerContext>>::begin(
                GrpcMethod::new("SemInit", "Init"),
                &inbound_http(&ctx),
                server,
            );
        }
    });
}

fn inbound_http(ctx: &Context) -> http::Request<()> {
    http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
        .body(())
        .unwrap()
}

/// Run `f` inside a task of a current-thread runtime, with a fresh virtual
/// clock. Some modules read per-task runtime state that only exists in a task.
pub fn run<H: Hooks, R: Send + 'static>(f: impl FnOnce(&mut World<H>) -> R + Send + 'static) -> R {
    run_with::<H, R>(Params::default(), true, f)
}

/// Like [`run`], with explicit process parameters and worker behavior.
pub fn run_with<H: Hooks, R: Send + 'static>(
    params: Params,
    suppress_background_workers: bool,
    f: impl FnOnce(&mut World<H>) -> R + Send + 'static,
) -> R {
    init::<H>(params, suppress_background_workers);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let joined = rt.block_on(async move {
        tokio::spawn(async move {
            test_clock::set_now_us(test_clock::START_US);
            f(&mut World::new())
        })
        .await
    });
    match joined {
        Ok(r) => r,
        Err(e) => resume_unwind(e.into_panic()),
    }
}

// ── The world: clock and services ───────────────────────────────────────

/// The virtual environment of one scenario.
pub struct World<H: Hooks> {
    _hooks: PhantomData<fn() -> H>,
}

impl<H: Hooks> World<H> {
    fn new() -> Self {
        Self {
            _hooks: PhantomData,
        }
    }

    /// Current virtual time, microseconds since the virtual epoch.
    pub fn now(&self) -> u64 {
        test_clock::now_us()
    }

    pub fn set_now(&self, us: u64) {
        test_clock::set_now_us(us);
    }

    pub fn advance(&self, d: Duration) {
        test_clock::advance_us(d.as_micros() as u64);
    }

    pub fn advance_ms(&self, ms: u64) {
        self.advance(Duration::from_millis(ms));
    }

    /// A service node with its own server-side hook state. Keep the handle to
    /// reuse what the service has learned across requests.
    pub fn service(&self, name: &'static str) -> Service<H> {
        Service {
            name,
            server: Arc::new(<H::ServerContext as ServerHooks>::new(name)),
        }
    }

    /// A client-created request for `api` with an end-to-end SLO, created the
    /// way a real client gateway does (`masa::create_context`).
    pub fn ingress(&self, api: &str, slo: Duration) -> Inbound {
        Inbound::from_root(RootContext::from(masa::create_context(api, slo)))
    }

    /// Start building a hand-crafted client request.
    pub fn crafted(&self, api: &str, slo: Duration) -> Crafted {
        Crafted {
            api: api.to_string(),
            slo_us: slo.as_micros() as u64,
            entry: None,
            deadline: None,
            priority: None,
            hop_count: None,
            root: None,
            tokens: None,
            now: self.now(),
        }
    }
}

/// One service's server-side hook state.
pub struct Service<H: Hooks> {
    pub name: &'static str,
    server: Arc<H::ServerContext>,
}

impl<H: Hooks> Clone for Service<H> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            server: self.server.clone(),
        }
    }
}

impl<H: Hooks> Service<H> {
    /// Begin serving `method` for `inbound`, without running a handler.
    pub fn accept(&self, method: &'static str, inbound: &Inbound) -> Handler<H> {
        let req = inbound.http();
        let parent = <H::ParentContext as ParentHooks<H::ChildContext, H::ServerContext>>::begin(
            GrpcMethod::new(self.name, method),
            &req,
            self.server.clone(),
        );
        Handler {
            parent,
            service: self.name,
            method,
            request: inbound.view.clone(),
            in_poll: std::cell::Cell::new(false),
        }
    }

    /// Serve `method` for `inbound`: run the handler `body` the way the
    /// server would (first poll gated by the hooks, final poll, finalize) and
    /// return the reply that would go back to the caller.
    pub fn serve(
        &self,
        method: &'static str,
        inbound: &Inbound,
        body: impl FnOnce(&Handler<H>) -> Result<(), Status>,
    ) -> Reply {
        let handler = self.accept(method, inbound);
        handler.run(body)
    }
}

// ── Requests ────────────────────────────────────────────────────────────

/// What the framework reads from a request's propagated Masa context.
///
/// Fields a build does not track are `None`.
#[derive(Clone, Debug, PartialEq)]
pub struct CtxView {
    pub api: String,
    pub request_id: u64,
    pub slo: u64,
    pub gateway_entry: u64,
    pub deadline: u64,
    pub priority: u64,
    pub hop_count: Option<u8>,
    pub root_method: Option<(String, String)>,
    pub tokens: Option<u64>,
    pub meta: Option<RespMeta>,
    pub queue: Option<QueueView>,
}

/// Per-hop response metadata flowing back up the call graph.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RespMeta {
    pub compute_time_us: u64,
    pub accumulated_compute_us: u64,
    pub utilization: f32,
    pub max_downstream_util: f32,
    pub early_return_count: u32,
    pub deadline_signal_count: u32,
}

/// Queue-latency telemetry (only with `trace_queue_latency`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueueView {
    pub initial: u64,
    pub resume: u64,
    pub lengths: BTreeMap<String, u64>,
}

/// Tokens of the Rajomon wire section in `metadata`, if any.
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
fn rajomon_tokens(metadata: &tonic::metadata::MetadataMap) -> Option<u64> {
    masa::WireIn::from_metadata(metadata)
        .expect("well-formed wire sections")
        .get::<masa_policy::modules::RajomonLayer>()
        .expect("decodable rajomon section")
        .map(|wire| wire.tokens)
}

#[cfg(not(all(feature = "ac_rajomon", not(feature = "ac_pred"))))]
fn rajomon_tokens(_metadata: &tonic::metadata::MetadataMap) -> Option<u64> {
    None
}

impl CtxView {
    /// The view of whatever Masa data `metadata` carries.
    fn of_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<Self> {
        masa::get_masa_context_from_metadata(metadata)
            .map(|ctx| Self::from_parts(&ctx, rajomon_tokens(metadata)))
    }

    fn from_parts(ctx: &Context, tokens: Option<u64>) -> Self {
        Self {
            api: ctx.api().clone(),
            request_id: ctx.request_id(),
            slo: ctx.slo(),
            gateway_entry: ctx.gateway_entry(),
            deadline: ctx.deadline(),
            priority: ctx.prio_hint().value(),
            #[cfg(feature = "estimator")]
            hop_count: Some(ctx.hop_count()),
            #[cfg(not(feature = "estimator"))]
            hop_count: None,
            #[cfg(feature = "estimator")]
            root_method: ctx
                .root_method()
                .map(|r| (r.service.clone(), r.method.clone())),
            #[cfg(not(feature = "estimator"))]
            root_method: None,
            tokens,
            #[cfg(feature = "estimator")]
            meta: ctx.response_meta().map(|m| RespMeta {
                compute_time_us: m.compute_time_us,
                accumulated_compute_us: m.accumulated_compute_us,
                utilization: m.utilization,
                max_downstream_util: m.max_downstream_util,
                early_return_count: m.early_return_count,
                deadline_signal_count: m.deadline_signal_count,
            }),
            #[cfg(not(feature = "estimator"))]
            meta: None,
            #[cfg(feature = "trace_queue_latency")]
            queue: ctx.queue_latencies().map(|q| QueueView {
                initial: q.initial,
                resume: q.resume,
                lengths: q
                    .queue_lengths
                    .iter()
                    .map(|(k, v)| (k.clone(), *v))
                    .collect(),
            }),
            #[cfg(not(feature = "trace_queue_latency"))]
            queue: None,
        }
    }

    /// Response metadata; panics when the build does not track it.
    pub fn resp(&self) -> RespMeta {
        self.meta.expect("response metadata present")
    }
}

/// A request as it arrives at a service: transport headers only.
#[derive(Clone)]
pub struct Inbound {
    headers: http::HeaderMap,
    view: CtxView,
}

impl Inbound {
    fn from_root(root: RootContext) -> Self {
        let request = root.attach(Request::new(()));
        Self {
            headers: request.metadata().clone().into_headers(),
            view: CtxView::of_metadata(request.metadata()).expect("attached context"),
        }
    }

    fn http(&self) -> http::Request<()> {
        let mut req = http::Request::new(());
        *req.headers_mut() = self.headers.clone();
        req
    }

    /// The propagated context this request carries.
    pub fn view(&self) -> &CtxView {
        &self.view
    }
}

/// A hand-crafted client request, for scenarios that need exact field values.
pub struct Crafted {
    api: String,
    slo_us: u64,
    entry: Option<u64>,
    deadline: Option<u64>,
    priority: Option<u64>,
    hop_count: Option<u8>,
    root: Option<(String, String)>,
    tokens: Option<u64>,
    now: u64,
}

impl Crafted {
    /// The request entered the system at virtual time `t` (deadline = `t` + SLO
    /// unless set explicitly).
    pub fn entered_at(mut self, t: u64) -> Self {
        self.entry = Some(t);
        self
    }

    /// Override the per-hop deadline without changing the end-to-end SLO.
    pub fn deadline(mut self, t: u64) -> Self {
        self.deadline = Some(t);
        self
    }

    pub fn priority(mut self, p: u64) -> Self {
        self.priority = Some(p);
        self
    }

    /// Pretend the request already travelled `n` hops from `root`.
    pub fn hops(mut self, n: u8, root: (&str, &str)) -> Self {
        self.hop_count = Some(n);
        self.root = Some((root.0.to_string(), root.1.to_string()));
        self
    }

    pub fn tokens(mut self, n: u64) -> Self {
        self.tokens = Some(n);
        self
    }

    pub fn build(self) -> Inbound {
        let entry = self.entry.unwrap_or(self.now);
        let mut b = ContextBuilder::new(self.api.clone(), 1)
            .slo(self.slo_us)
            .gateway_entry(entry)
            .deadline(self.deadline.unwrap_or(entry + self.slo_us));
        if let Some(p) = self.priority {
            b = b.prio_hint(PriorityHint::new(p));
        }
        #[cfg(feature = "estimator")]
        {
            if let Some(n) = self.hop_count {
                b = b.hop_count(n);
            }
            if let Some((svc, method)) = self.root {
                b = b.root_method(masa_core::RootMethod {
                    service: svc,
                    method,
                });
            }
        }
        #[allow(unused_mut)]
        let mut root = RootContext::from(b.build());
        #[cfg(feature = "ac_rajomon")]
        if let Some(t) = self.tokens {
            root = root.with_rajomon_tokens(t);
        }
        #[cfg(not(feature = "ac_rajomon"))]
        let _ = self.tokens;
        Inbound::from_root(root)
    }
}

// ── Replies ─────────────────────────────────────────────────────────────

/// A response travelling back to the caller: `Ok` or an error status, with
/// whatever metadata the callee's hooks attached.
pub struct Reply {
    result: Result<Response<()>, Status>,
}

/// Metadata for a synthetic callee reply (a callee whose internals the
/// scenario does not model).
#[derive(Clone, Debug, Default)]
pub struct ReplySpec {
    pub meta: RespMeta,
    pub queue: Option<QueueView>,
    pub price: Option<u64>,
}

impl Reply {
    /// A plain `Ok` reply with no Masa metadata.
    pub fn bare_ok() -> Self {
        Self {
            result: Ok(Response::new(())),
        }
    }

    /// An error reply with no Masa metadata.
    pub fn bare_err(status: Status) -> Self {
        Self {
            result: Err(status),
        }
    }

    /// An `Ok` reply carrying the given metadata.
    pub fn synthetic(spec: ReplySpec) -> Self {
        let mut resp = Response::new(());
        #[allow(unused_mut)]
        let mut b = ContextBuilder::new("synthetic", 1);
        #[cfg(feature = "estimator")]
        {
            b = b.response_meta(masa_core::EstimatorResponse {
                compute_time_us: spec.meta.compute_time_us,
                accumulated_compute_us: spec.meta.accumulated_compute_us,
                utilization: spec.meta.utilization,
                max_downstream_util: spec.meta.max_downstream_util,
                early_return_count: spec.meta.early_return_count,
                deadline_signal_count: spec.meta.deadline_signal_count,
            });
        }
        #[cfg(feature = "trace_queue_latency")]
        if let Some(q) = &spec.queue {
            b = b.queue_latencies(masa_core::QueueLatencies {
                initial: q.initial,
                resume: q.resume,
                queue_lengths: q.lengths.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            });
        }
        resp.set_masa_context(&b.build());
        if let Some(price) = spec.price {
            resp.metadata_mut()
                .insert("x-masa-rajomon-price", price.to_string().parse().unwrap());
        }
        Self { result: Ok(resp) }
    }

    pub fn is_ok(&self) -> bool {
        self.result.is_ok()
    }

    /// The error status; panics if the reply is `Ok`.
    pub fn status(&self) -> &Status {
        self.result.as_ref().err().expect("reply is an error")
    }

    pub fn code(&self) -> Code {
        self.status().code()
    }

    pub fn message(&self) -> &str {
        self.status().message()
    }

    /// The Masa context the callee's hooks attached, if any.
    pub fn view(&self) -> Option<CtxView> {
        match &self.result {
            Ok(resp) => CtxView::of_metadata(resp.metadata()),
            Err(status) => CtxView::of_metadata(status.metadata()),
        }
    }

    /// The Rajomon price the callee advertised, if any.
    pub fn price(&self) -> Option<u64> {
        let md = match &self.result {
            Ok(resp) => resp.metadata(),
            Err(status) => status.metadata(),
        };
        md.get("x-masa-rajomon-price")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    /// Response metadata of the callee; panics when absent.
    pub fn meta(&self) -> RespMeta {
        self.view().expect("reply carries context").resp()
    }
}

// ── Handlers ────────────────────────────────────────────────────────────

/// How a handler's poll ends.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PollEnd {
    Pending,
    Ready,
}

/// One in-flight request at a service: the server-side hook context plus the
/// poll state of a simulated handler future.
pub struct Handler<H: Hooks> {
    parent: H::ParentContext,
    pub service: &'static str,
    pub method: &'static str,
    request: CtxView,
    in_poll: std::cell::Cell<bool>,
}

fn early_status<Ret>(r: Result<(), Result<Response<Ret>, Status>>) -> Result<(), Status> {
    match r {
        Ok(()) => Ok(()),
        Err(Err(status)) => Err(status),
        Err(Ok(_)) => panic!("a policy returned an Ok early response; unsupported by the suite"),
    }
}

impl<H: Hooks> Handler<H> {
    /// The propagated context this handler's request arrived with.
    pub fn request(&self) -> &CtxView {
        &self.request
    }

    fn enter_poll(&self) -> Result<(), Status> {
        if self.in_poll.replace(true) {
            return Ok(());
        }
        early_status(self.parent.before_poll::<()>())
    }

    fn leave_poll(&self, end: PollEnd) -> Result<(), Status> {
        if !self.in_poll.replace(false) {
            return Ok(());
        }
        let poll: Poll<Result<Response<()>, Status>> = match end {
            PollEnd::Pending => Poll::Pending,
            PollEnd::Ready => Poll::Ready(Ok(Response::new(()))),
        };
        early_status(self.parent.after_poll::<()>(&poll))
    }

    /// Start a poll (if none is open) without computing. The handler future
    /// is polled for the first time, for instance.
    pub fn poll_begin(&self) -> Result<(), Status> {
        self.enter_poll()
    }

    /// Close the open poll as `Pending`.
    pub fn poll_yield(&self) -> Result<(), Status> {
        self.leave_poll(PollEnd::Pending)
    }

    /// Spend `d` of CPU inside the handler's current poll (opening one if
    /// needed). Virtual time advances by `d`.
    pub fn work(&self, d: Duration) -> Result<(), Status> {
        self.enter_poll()?;
        test_clock::advance_us(d.as_micros() as u64);
        Ok(())
    }

    pub fn work_ms(&self, ms: u64) -> Result<(), Status> {
        self.work(Duration::from_millis(ms))
    }

    /// Block without computing for `d` (for example waiting on I/O): the
    /// open poll yields first.
    pub fn wait(&self, d: Duration) -> Result<(), Status> {
        self.leave_poll(PollEnd::Pending)?;
        test_clock::advance_us(d.as_micros() as u64);
        Ok(())
    }

    pub fn wait_ms(&self, ms: u64) -> Result<(), Status> {
        self.wait(Duration::from_millis(ms))
    }

    /// Finish the request with `result` and return the reply for the caller.
    /// An open poll is closed as `Ready` first, as the last poll of the
    /// handler future would be.
    pub fn finish(&self, result: Result<(), Status>) -> Reply {
        let result = match result {
            Ok(()) => self
                .enter_poll()
                .and_then(|()| self.leave_poll(PollEnd::Ready)),
            Err(status) => Err(status),
        };
        self.in_poll.set(false);
        let mut r: Result<Response<()>, Status> = match result {
            Ok(()) => Ok(Response::new(())),
            Err(status) => Err(status),
        };
        self.parent.finalize_before_serialization(&mut r);
        Reply { result: r }
    }

    /// Run a handler body and finish: the body's `Err` (an aborted poll or a
    /// rejected child RPC propagated with `?`) becomes the reply.
    pub fn run(&self, body: impl FnOnce(&Handler<H>) -> Result<(), Status>) -> Reply {
        let result = self.enter_poll().and_then(|()| body(self));
        self.finish(result)
    }

    /// Start a child RPC to `service::method` with its response handled by a
    /// real virtual callee.
    pub fn call<'a>(&'a self, callee: &'a Service<H>, method: &'static str) -> Call<'a, H> {
        Call {
            parent: self,
            callee: Some(callee),
            service: callee.name,
            method,
            oracle: None,
            method_override: None,
            service_override: None,
        }
    }

    /// Start a child RPC to a callee the scenario does not model; give its
    /// reply with [`Call::returns`].
    pub fn call_remote<'a>(&'a self, service: &'static str, method: &'static str) -> Call<'a, H> {
        Call {
            parent: self,
            callee: None,
            service,
            method,
            oracle: None,
            method_override: None,
            service_override: None,
        }
    }

    /// Issue `calls` in parallel: all child RPCs are issued from the current
    /// poll, callees run concurrently from the same virtual instant, and the
    /// parent resumes at the latest completion. Results are in issue order.
    pub fn fanout(&self, calls: Vec<Branch<'_, H>>) -> Vec<Result<Reply, Status>> {
        let mut issued = Vec::new();
        for branch in calls {
            issued.push((branch.call.issue(), branch));
        }
        let _ = self.leave_poll(PollEnd::Pending);
        let t0 = test_clock::now_us();
        let mut outcomes: Vec<(usize, u64, Result<Reply, Status>)> = Vec::new();
        let mut pending: Vec<(usize, Outbound<H>, Branch<'_, H>)> = Vec::new();
        for (i, (out, branch)) in issued.into_iter().enumerate() {
            match out {
                Ok(out) => pending.push((i, out, branch)),
                Err(status) => outcomes.push((i, t0, Err(status))),
            }
        }
        let mut replies: Vec<(usize, u64, Outbound<H>, Reply)> = Vec::new();
        for (i, out, branch) in pending {
            test_clock::set_now_us(t0);
            let reply = branch.call.execute(&out, branch.body);
            replies.push((i, test_clock::now_us(), out, reply));
        }
        replies.sort_by_key(|(_, end, _, _)| *end);
        let latest = replies.last().map(|r| r.1).unwrap_or(t0);
        test_clock::set_now_us(latest.max(t0));
        let _ = self.enter_poll();
        for (i, end, out, reply) in replies {
            let done = self.complete(out, reply);
            outcomes.push((i, end, done));
        }
        outcomes.sort_by_key(|(i, _, _)| *i);
        outcomes.into_iter().map(|(_, _, r)| r).collect()
    }

    /// Deliver a child's reply to the hooks (`after_child_rpc`). The hooks may
    /// turn the reply into an error, which is returned.
    pub fn complete(&self, mut out: Outbound<H>, reply: Reply) -> Result<Reply, Status> {
        let mut resp = reply.result;
        out.child.after_recv(&mut resp);
        self.parent.after_child_rpc(
            GrpcMethod::new(out.service, out.method),
            &mut resp,
            out.child,
        )?;
        Ok(Reply { result: resp })
    }

    /// Discard the hook state without finalizing (an abandoned request).
    pub fn abandon(self) {}
}

// ── Child calls ─────────────────────────────────────────────────────────

/// A child RPC being set up by a handler.
pub struct Call<'a, H: Hooks> {
    parent: &'a Handler<H>,
    callee: Option<&'a Service<H>>,
    service: &'static str,
    method: &'static str,
    oracle: Option<(u64, u64)>,
    method_override: Option<&'static str>,
    service_override: Option<&'static str>,
}

/// A child RPC plus the callee body, for [`Handler::fanout`].
pub struct Branch<'a, H: Hooks> {
    call: Call<'a, H>,
    body: Box<dyn FnOnce(&Handler<H>) -> Result<(), Status> + 'a>,
}

/// The outbound side of a child RPC after the caller's hooks ran.
pub struct Outbound<H: Hooks> {
    request: Request<()>,
    child: H::ChildContext,
    pub service: &'static str,
    pub method: &'static str,
}

impl<H: Hooks> Outbound<H> {
    /// What the callee receives.
    pub fn inbound(&self) -> Inbound {
        let headers = self.request.metadata().clone().into_headers();
        let view = CtxView::of_metadata(self.request.metadata())
            .expect("outbound request carries a context");
        Inbound { headers, view }
    }

    /// Whether the hooks attached a context to the outbound request.
    pub fn carries_context(&self) -> bool {
        self.request.get_masa_context().is_some()
    }

    /// The propagated context on the outbound request.
    pub fn view(&self) -> CtxView {
        self.inbound().view
    }
}

impl<'a, H: Hooks> Call<'a, H> {
    /// Oracle headers: the child's perfect-information work and the work the
    /// caller still has after it.
    pub fn oracle(mut self, child_work_us: u64, remaining_after_us: u64) -> Self {
        self.oracle = Some((child_work_us, remaining_after_us));
        self
    }

    pub fn method_override(mut self, name: &'static str) -> Self {
        self.method_override = Some(name);
        self
    }

    pub fn service_override(mut self, name: &'static str) -> Self {
        self.service_override = Some(name);
        self
    }

    fn request(&self) -> Request<()> {
        let mut req = Request::new(());
        // Oracle builds need hints on every child RPC; a caller that does not
        // care about the oracle supplies neutral ones.
        #[cfg(feature = "sched_oracle")]
        let oracle = self.oracle.or(Some((0, 0)));
        #[cfg(not(feature = "sched_oracle"))]
        let oracle = self.oracle;
        if let Some((work, rest)) = oracle {
            req.metadata_mut().insert(
                masa::ORACLE_CHILD_WORK_US_HEADER,
                work.to_string().parse().unwrap(),
            );
            req.metadata_mut().insert(
                masa::ORACLE_REMAINING_AFTER_US_HEADER,
                rest.to_string().parse().unwrap(),
            );
        }
        if let Some(m) = self.method_override {
            req.set_method_name_override(m).unwrap();
        }
        if let Some(s) = self.service_override {
            req.set_service_name_override(s).unwrap();
        }
        req
    }

    /// Run the caller's `before_child_rpc`. `Err` is the hook's rejection; the
    /// request is then dropped untouched.
    fn issue(&self) -> Result<Outbound<H>, Status> {
        self.issue_keep().map_err(|(status, _)| status)
    }

    fn issue_keep(&self) -> Result<Outbound<H>, (Status, Request<()>)> {
        let m = GrpcMethod::new(self.service, self.method);
        let mut request = self.request();
        let mut child = <H::ChildContext as ClientHooks>::new(m, &request);
        self.parent
            .enter_poll()
            .map_err(|s| (s, Request::new(())))?;
        match self
            .parent
            .parent
            .before_child_rpc(m, &mut request, &mut child)
        {
            Ok(()) => {
                child.before_send(&mut request);
                Ok(Outbound {
                    request,
                    child,
                    service: self.service,
                    method: self.method,
                })
            }
            Err(status) => Err((status, request)),
        }
    }

    /// Issue the RPC and stop: returns what the hooks produced for the wire
    /// without running a callee. Use [`Handler::complete`] to finish it.
    pub fn outbound(&self) -> Result<Outbound<H>, Status> {
        self.issue()
    }

    /// Like [`Call::outbound`], but on rejection also reports whether the
    /// outbound request was left without a Masa context.
    pub fn outbound_or_untouched(&self) -> Result<Outbound<H>, (Status, bool)> {
        self.issue_keep()
            .map_err(|(status, req)| (status, req.get_masa_context().is_none()))
    }

    fn execute(
        &self,
        out: &Outbound<H>,
        body: impl FnOnce(&Handler<H>) -> Result<(), Status>,
    ) -> Reply {
        let callee = self
            .callee
            .expect("a modeled callee is required; use `returns` for remote calls");
        let inbound = out.inbound();
        callee.serve(self.method, &inbound, body)
    }

    /// Run the RPC against the modeled callee, whose handler is `body`. The
    /// caller yields its poll while the callee runs. `Err` means the caller's
    /// hooks rejected the child RPC or turned its response into an error;
    /// otherwise the callee's reply is returned (it may itself be an error).
    pub fn run(
        self,
        body: impl FnOnce(&Handler<H>) -> Result<(), Status>,
    ) -> Result<Reply, Status> {
        let out = self.issue()?;
        self.parent.leave_poll(PollEnd::Pending)?;
        let reply = self.execute(&out, body);
        self.parent.enter_poll()?;
        self.parent.complete(out, reply)
    }

    /// Run the RPC with a callee that does nothing but answer `Ok` after `d`.
    pub fn run_for(self, d: Duration) -> Result<Reply, Status> {
        self.run(move |c| c.work(d))
    }

    /// Complete the RPC with a synthetic reply after `elapsed` of virtual
    /// time, without modeling the callee.
    pub fn returns(self, elapsed: Duration, reply: Reply) -> Result<Reply, Status> {
        let out = self.issue()?;
        self.parent.leave_poll(PollEnd::Pending)?;
        test_clock::advance_us(elapsed.as_micros() as u64);
        self.parent.enter_poll()?;
        self.parent.complete(out, reply)
    }

    /// Turn this call into a parallel branch with a modeled callee body.
    pub fn branch(
        self,
        body: impl FnOnce(&Handler<H>) -> Result<(), Status> + 'a,
    ) -> Branch<'a, H> {
        Branch {
            call: self,
            body: Box::new(body),
        }
    }
}

// ── Helpers shared by scenarios ─────────────────────────────────────────

/// Milliseconds to microseconds.
pub const fn ms(n: u64) -> u64 {
    n * 1_000
}

pub fn dur_ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// `Err(status)` of an early-return check on `result`.
pub fn expect_err<T>(result: Result<T, Status>) -> Status {
    match result {
        Ok(_) => panic!("expected an error status"),
        Err(status) => status,
    }
}
