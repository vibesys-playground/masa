//! Characterization tests for the policy hook stack.
//!
//! These drive `PolicyHooks` in-process through the public tonic hook traits
//! (`ServerHooks`, `ParentHooks`, `ClientHooks`) on the associated types of
//! `<PolicyHooks as Hooks>`, without any server or network. The expected values
//! were observed from the pre-refactor implementation; they pin the pipeline
//! semantics: modules run in the order guard -> estimation -> oracle ->
//! admission -> queue_latency, and the first `Err` short-circuits every hook.
//!
//! Only public API is used, so the file must compile and pass unchanged on any
//! implementation of the stack. Each test is gated by the feature flags it
//! needs, so the file builds under every feature combination.
//!
//! Timing: tests that need estimator observations use short `thread::sleep`s.
//! Sleeps only give lower bounds, so the assertions on learned latencies are
//! inequalities that hold however slow the machine is.

#![allow(dead_code, unused_imports, unused_variables)]

use std::sync::{Arc, Once};
use std::task::Poll;
use std::time::Duration;

use masa_core::{time_now, Context, ContextBuilder, PriorityHint};
#[cfg(feature = "ac_rajomon")]
use masa_policy::{header_string_with_wire, modules::RajomonLayer, RajomonWire};
#[cfg(feature = "trace_queue_latency")]
use masa_policy::{modules::QueueLatencyLayer, QueueLatencyWire};
use masa_policy::{
    MasaRequestExt, MasaResponseExt, MasaStatusExt, PolicyHooks, MASA_CONTEXT_HEADER,
};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{Code, GrpcMethod, Request, Response, Status};

type Srv = <PolicyHooks as Hooks>::ServerContext;
type Par = <PolicyHooks as Hooks>::ParentContext;
type Chi = <PolicyHooks as Hooks>::ChildContext;

/// 2 seconds: far larger than any test's runtime.
const SLO: u64 = 2_000_000;

// ── Helpers ─────────────────────────────────────────────────────────────

fn init() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("SERVICE_NAME", "char-svc");
        // Rajomon spawns a price-updating background worker the first time a
        // request begins if it is inside a tokio runtime. Its ticks would make
        // prices nondeterministic, so consume the one-shot start outside a
        // runtime, where no worker is spawned.
        let server = Arc::new(Srv::new("CharInit"));
        let ctx = fresh_ctx("CharInit", SLO);
        let _ = begin(&server, "CharInit", "Init", &ctx);
    });
}

/// Run `f` inside a task of a current-thread runtime. Some layers (rajomon,
/// queue latency) read per-task state that only exists inside a task.
fn run<F: FnOnce() + Send + 'static>(f: F) {
    init();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async move { tokio::spawn(async move { f() }).await.unwrap() });
}

fn sleep_ms(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}

fn fresh_builder(svc: &'static str, slo: u64) -> ContextBuilder {
    let now = time_now();
    ContextBuilder::new(svc, 1)
        .slo(slo)
        .gateway_entry(now)
        .deadline(now + slo)
}

fn fresh_ctx(svc: &'static str, slo: u64) -> Context {
    fresh_builder(svc, slo).build()
}

/// A context whose end-to-end SLO expired one second ago.
fn expired_ctx(svc: &'static str) -> Context {
    let slo = 1_000;
    let entry = time_now() - 1_000_000;
    ContextBuilder::new(svc, 1)
        .slo(slo)
        .gateway_entry(entry)
        .deadline(entry + slo)
        .build()
}

/// A context whose per-hop (local) deadline passed but whose end-to-end SLO is
/// still far away, so only local-deadline checks fire.
fn locally_late_ctx(svc: &'static str) -> Context {
    let now = time_now();
    ContextBuilder::new(svc, 1)
        .slo(10 * SLO)
        .gateway_entry(now)
        .deadline(now - 1_000_000)
        .build()
}

/// An inbound context and the Rajomon tokens its sender attached. Tokens are
/// Rajomon's wire data rather than part of `Context`, so they travel beside it.
struct Inbound {
    ctx: Context,
    tokens: u64,
}

impl std::ops::Deref for Inbound {
    type Target = Context;

    fn deref(&self) -> &Context {
        &self.ctx
    }
}

struct InboundBuilder {
    builder: ContextBuilder,
    tokens: u64,
}

impl InboundBuilder {
    fn build(self) -> Inbound {
        Inbound {
            ctx: self.builder.build(),
            tokens: self.tokens,
        }
    }
}

fn with_tokens(builder: ContextBuilder, tokens: u64) -> InboundBuilder {
    InboundBuilder { builder, tokens }
}

/// What `begin` needs to build the inbound `ctx` header.
trait InboundCtx {
    fn header_value(&self) -> String;
}

impl InboundCtx for Context {
    fn header_value(&self) -> String {
        self.to_header_string()
    }
}

impl InboundCtx for Inbound {
    #[cfg(feature = "ac_rajomon")]
    fn header_value(&self) -> String {
        header_string_with_wire::<RajomonLayer>(&self.ctx, &RajomonWire::request(self.tokens))
    }

    #[cfg(not(feature = "ac_rajomon"))]
    fn header_value(&self) -> String {
        self.ctx.to_header_string()
    }
}

fn begin(server: &Arc<Srv>, svc: &'static str, method: &'static str, ctx: &impl InboundCtx) -> Par {
    let req = http::Request::builder()
        .header(MASA_CONTEXT_HEADER, ctx.header_value())
        .body(())
        .unwrap();
    Par::begin(GrpcMethod::new(svc, method), &req, server.clone())
}

/// Issue a child RPC; returns the hook result, the (possibly annotated)
/// outbound request, and the child context to hand to `after_child_rpc`.
fn issue_child_req(
    parent: &Par,
    svc: &'static str,
    method: &'static str,
    mut req: Request<()>,
) -> (Result<(), Status>, Request<()>, Chi) {
    let m = GrpcMethod::new(svc, method);
    let mut child = Chi::new(m, &req);
    let r = parent.before_child_rpc(m, &mut req, &mut child);
    (r, req, child)
}

fn issue_child(
    parent: &Par,
    svc: &'static str,
    method: &'static str,
) -> (Result<(), Status>, Request<()>, Chi) {
    issue_child_req(parent, svc, method, Request::new(()))
}

fn child_done(
    parent: &Par,
    svc: &'static str,
    method: &'static str,
    child: Chi,
    mut resp: Result<Response<()>, Status>,
) -> Result<(), Status> {
    parent.after_child_rpc(GrpcMethod::new(svc, method), &mut resp, child)
}

fn finalize_ok(parent: &Par) -> Response<()> {
    let mut r: Result<Response<()>, Status> = Ok(Response::new(()));
    parent.finalize_before_serialization(&mut r);
    r.unwrap()
}

fn finalize_err(parent: &Par, status: Status) -> Status {
    let mut r: Result<Response<()>, Status> = Err(status);
    parent.finalize_before_serialization(&mut r);
    r.unwrap_err()
}

fn early_return_err(r: Result<(), Result<Response<()>, Status>>) -> Status {
    match r {
        Err(Err(status)) => status,
        Err(Ok(_)) => panic!("expected Err(Err(status)), got an Ok response"),
        Ok(()) => panic!("expected an early return, got Ok"),
    }
}

/// One complete parent request with a single child call that takes at least
/// `child_ms`, followed by at least `after_ms` of post-child work, finalized
/// with `result_status` (`None` = Ok).
#[cfg(all(feature = "estimator", not(feature = "sched_oracle")))]
fn train_cycle(
    server: &Arc<Srv>,
    svc: &'static str,
    parent: &'static str,
    child: &'static str,
    child_ms: u64,
    after_ms: u64,
    result_status: Option<Status>,
) {
    let ctx = with_tokens(fresh_builder(svc, SLO), 100).build();
    let p = begin(server, svc, parent, &ctx);
    let (r, _req, c) = issue_child(&p, svc, child);
    r.expect("training child RPC must be admitted");
    sleep_ms(child_ms);
    child_done(&p, svc, child, c, Ok(Response::new(()))).unwrap();
    sleep_ms(after_ms);
    match result_status {
        None => {
            finalize_ok(&p);
        }
        Some(s) => {
            finalize_err(&p, s);
        }
    }
}

#[cfg(feature = "estimator")]
fn response_meta(ctx: &Context) -> masa_core::EstimatorResponse {
    ctx.response_meta().expect("response_meta present").clone()
}

// ── Tests that issue child RPCs without the oracle ──────────────────────

#[cfg(not(feature = "sched_oracle"))]
mod generic {
    use super::*;

    // ── 2. Child request context ────────────────────────────────────────

    /// With no latency observations, the child context carries the parent's
    /// identity and deadline unchanged; priority, hop count, root method and
    /// tokens follow the active features.
    #[test]
    fn child_context_without_observations() {
        run(|| {
            let server = Arc::new(Srv::new("CharB"));
            let now = time_now();
            let ctx = with_tokens(
                ContextBuilder::new("CharB", 4242)
                    .slo(SLO)
                    .gateway_entry(now)
                    .deadline(now + SLO)
                    .prio_hint(PriorityHint::new(777)),
                40,
            )
            .build();
            let p = begin(&server, "CharB", "Parent", &ctx);
            let (r, req, _child) = issue_child(&p, "CharB", "Child");
            r.unwrap();
            let cc = req.get_masa_context().expect("child ctx set on request");

            assert_eq!(cc.api(), ctx.api());
            assert_eq!(cc.request_id(), 4242);
            assert_eq!(cc.slo(), SLO);
            assert_eq!(cc.gateway_entry(), now);
            assert_eq!(cc.deadline(), now + SLO);

            #[cfg(feature = "sched_pred")]
            {
                // Priority becomes the remaining time to the deadline.
                let prio = cc.prio_hint().value();
                assert!(prio <= SLO && prio > SLO - 1_000_000, "prio {prio}");
            }
            #[cfg(not(feature = "sched_pred"))]
            assert_eq!(cc.prio_hint().value(), 777);

            #[cfg(feature = "estimator")]
            {
                assert_eq!(cc.hop_count(), 1);
                let root = cc.root_method().expect("root method set at ingress");
                assert_eq!(root.service, "CharB");
                assert_eq!(root.method, "Parent");
            }
            #[cfg(feature = "ac_rajomon")]
            assert_eq!(
                req.get_wire::<RajomonLayer>(),
                Some(RajomonWire::request(40))
            );
        });
    }

    /// Non-ingress hops increment the hop count and keep the inherited root.
    #[cfg(feature = "estimator")]
    #[test]
    fn child_context_keeps_root_method_past_ingress() {
        run(|| {
            let server = Arc::new(Srv::new("CharB2"));
            let ctx = with_tokens(
                fresh_builder("CharB2", SLO)
                    .hop_count(2)
                    .root_method(masa_core::RootMethod {
                        service: "CharRootSvc".into(),
                        method: "CharRootMethod".into(),
                    }),
                100,
            )
            .build();
            let p = begin(&server, "CharB2", "Parent", &ctx);
            let (r, req, _child) = issue_child(&p, "CharB2", "Child");
            r.unwrap();
            let cc = req.get_masa_context().unwrap();
            assert_eq!(cc.hop_count(), 3);
            let root = cc.root_method().unwrap();
            assert_eq!(root.service, "CharRootSvc");
            assert_eq!(root.method, "CharRootMethod");
        });
    }

    /// Estimation tightens the child deadline by the learned post-child time
    /// (and, under sched_pred, the priority by the full estimate).
    #[cfg(feature = "estimator")]
    #[test]
    fn estimation_tightens_child_deadline_after_training() {
        run(|| {
            let server = Arc::new(Srv::new("CharC"));
            for _ in 0..2 {
                train_cycle(&server, "CharC", "Parent", "Child", 5, 10, None);
            }
            let ctx = with_tokens(fresh_builder("CharC", SLO), 100).build();
            let p = begin(&server, "CharC", "Parent", &ctx);
            let (r, req, _child) = issue_child(&p, "CharC", "Child");
            r.unwrap();
            let cc = req.get_masa_context().unwrap();

            // Post-child work took >= 10 ms, so the deadline moves earlier by
            // at least that much, but never by an absurd amount.
            let delta = ctx.deadline() - cc.deadline();
            assert!(
                (10_000..500_000).contains(&delta),
                "deadline tightened by {delta} us"
            );
            assert_eq!(cc.hop_count(), 1);

            #[cfg(feature = "sched_pred")]
            {
                let prio = cc.prio_hint().value();
                assert!(prio <= SLO - 10_000, "prio {prio}");
            }
            #[cfg(not(feature = "sched_pred"))]
            assert_eq!(cc.prio_hint().value(), ctx.prio_hint().value());
        });
    }

    /// The estimator learns only from on-time responses: an early return at
    /// the parent discards that request's observations.
    #[cfg(feature = "estimator")]
    #[test]
    fn estimator_skips_early_return_observations() {
        run(|| {
            let server = Arc::new(Srv::new("CharD"));
            let er = || Some(Status::new(Code::DeadlineExceeded, "/EarlyReturn?src=x"));
            for _ in 0..2 {
                train_cycle(&server, "CharD", "Parent", "Child", 5, 10, er());
            }
            let ctx = with_tokens(fresh_builder("CharD", SLO), 100).build();
            let p = begin(&server, "CharD", "Parent", &ctx);
            let (r, req, _child) = issue_child(&p, "CharD", "Child");
            r.unwrap();
            let cc = req.get_masa_context().unwrap();
            assert_eq!(cc.deadline(), ctx.deadline(), "nothing was learned");
        });
    }

    // ── 5. finalize_before_serialization ────────────────────────────────

    #[test]
    fn finalize_ok_attaches_context() {
        run(|| {
            let server = Arc::new(Srv::new("CharE1"));
            let ctx = with_tokens(fresh_builder("CharE1", SLO), 33).build();
            let p = begin(&server, "CharE1", "Parent", &ctx);
            p.before_poll::<()>().map_err(|_| ()).unwrap();
            let resp = finalize_ok(&p);
            let rc = resp.get_masa_context().expect("context on response");
            assert_eq!(rc.api(), ctx.api());
            assert_eq!(rc.request_id(), ctx.request_id());
            assert_eq!(rc.deadline(), ctx.deadline());
            #[cfg(feature = "estimator")]
            {
                let meta = response_meta(&rc);
                assert_eq!(meta.early_return_count, 0);
                assert_eq!(meta.deadline_signal_count, 0);
                assert_eq!(rc.hop_count(), 0);
                let root = rc.root_method().unwrap();
                assert_eq!(
                    (root.service.as_str(), root.method.as_str()),
                    ("CharE1", "Parent")
                );
            }
            #[cfg(feature = "trace_queue_latency")]
            {
                let ql = resp
                    .get_wire::<QueueLatencyLayer>()
                    .expect("queue latencies on response");
                assert_eq!(ql.queue_lengths.len(), 1);
                assert!(ql.queue_lengths.contains_key("char-svc"));
            }
            #[cfg(feature = "ac_rajomon")]
            assert_eq!(
                resp.get_wire::<RajomonLayer>().map(|wire| wire.tokens),
                Some(33)
            );
        });
    }

    #[test]
    fn finalize_err_attaches_context_to_status() {
        run(|| {
            let server = Arc::new(Srv::new("CharE2"));
            let ctx = fresh_ctx("CharE2", SLO);
            let p = begin(&server, "CharE2", "Parent", &ctx);
            let status = finalize_err(&p, Status::internal("boom"));
            assert_eq!(status.code(), Code::Internal);
            assert_eq!(status.message(), "boom");
            let rc = status.get_masa_context().expect("context on status");
            assert_eq!(rc.request_id(), ctx.request_id());
            #[cfg(feature = "estimator")]
            assert_eq!(response_meta(&rc).early_return_count, 0);

            // A DeadlineExceeded status counts as a local early return.
            let p = begin(&server, "CharE2", "Parent", &ctx);
            let er = Status::new(Code::DeadlineExceeded, "/EarlyReturn?src=CharE2::Parent");
            let status = finalize_err(&p, er);
            assert_eq!(status.code(), Code::DeadlineExceeded);
            let rc = status.get_masa_context().unwrap();
            #[cfg(feature = "estimator")]
            assert_eq!(response_meta(&rc).early_return_count, 1);
        });
    }

    /// Child response metadata is folded into the parent's response metadata.
    #[cfg(feature = "estimator")]
    #[test]
    fn finalize_aggregates_child_response_meta() {
        run(|| {
            let server = Arc::new(Srv::new("CharE3"));
            let ctx = with_tokens(fresh_builder("CharE3", SLO), 100).build();
            let p = begin(&server, "CharE3", "Parent", &ctx);
            let (r, _req, c) = issue_child(&p, "CharE3", "Child");
            r.unwrap();

            let child_ctx = fresh_builder("CharE3", SLO)
                .response_meta(masa_core::EstimatorResponse {
                    compute_time_us: 4_000,
                    accumulated_compute_us: 5_000,
                    utilization: 0.5,
                    max_downstream_util: 0.5,
                    early_return_count: 2,
                    deadline_signal_count: 0,
                })
                .build();
            let resp = Response::new(()).with_masa_context(&child_ctx);
            child_done(&p, "CharE3", "Child", c, Ok(resp)).unwrap();

            let rc = finalize_ok(&p).get_masa_context().unwrap();
            let meta = response_meta(&rc);
            // No polls ran, so local compute is zero.
            assert_eq!(meta.compute_time_us, 0);
            assert_eq!(meta.accumulated_compute_us, 5_000);
            assert_eq!(meta.early_return_count, 2);
            assert!(meta.max_downstream_util >= 0.5);
        });
    }

    /// Queue latencies reported by children are summed into the parent's
    /// response, with the parent's own queue length added under its service.
    #[cfg(feature = "trace_queue_latency")]
    #[test]
    fn finalize_aggregates_child_queue_latencies() {
        run(|| {
            let server = Arc::new(Srv::new("CharE4"));
            let ctx = with_tokens(fresh_builder("CharE4", SLO), 100).build();
            let p = begin(&server, "CharE4", "Parent", &ctx);
            let (r, _req, c) = issue_child(&p, "CharE4", "Child");
            r.unwrap();

            let mut lens = std::collections::HashMap::new();
            lens.insert("down".to_string(), 5);
            let child_ctx = fresh_builder("CharE4", SLO).build();
            let mut resp = Response::new(()).with_masa_context(&child_ctx);
            resp.set_wire::<QueueLatencyLayer>(&QueueLatencyWire {
                initial: 11,
                resume: 22,
                queue_lengths: lens,
            });
            child_done(&p, "CharE4", "Child", c, Ok(resp)).unwrap();

            let ql = finalize_ok(&p).get_wire::<QueueLatencyLayer>().unwrap();
            assert_eq!(ql.initial, 11);
            assert_eq!(ql.resume, 22);
            assert_eq!(ql.queue_lengths.get("down"), Some(&5));
            assert_eq!(ql.queue_lengths.get("char-svc"), Some(&0));
            assert_eq!(ql.queue_lengths.len(), 2);
        });
    }

    // ── 1 and 4. Guard first, first Err short-circuits ──────────────────

    /// The guard rejects an expired request before any other module: the
    /// message is the guard's bare `/EarlyReturn?src=...`, with none of the
    /// `reason=` suffixes the other modules would add.
    #[cfg(feature = "abort_slo")]
    #[test]
    fn guard_rejects_expired_request_before_other_modules() {
        run(|| {
            let server = Arc::new(Srv::new("CharG1"));
            let p = begin(&server, "CharG1", "Ping", &expired_ctx("CharG1"));

            let st = early_return_err(p.before_poll::<()>());
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(st.message(), "/EarlyReturn?src=CharG1::Ping");

            let st = early_return_err(p.after_poll::<()>(&Poll::Pending));
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(st.message(), "/EarlyReturn?src=CharG1::Ping");

            // Only Pending is replaced; a finished handler is left alone.
            assert!(p
                .after_poll::<()>(&Poll::Ready(Ok(Response::new(()))))
                .is_ok());
        });
    }

    /// A guard rejection in before_child_rpc stops the pipeline: the outbound
    /// request is left without a masa context.
    #[cfg(feature = "abort_slo")]
    #[test]
    fn guard_child_rejection_leaves_request_untouched() {
        run(|| {
            let server = Arc::new(Srv::new("CharG2"));
            let p = begin(&server, "CharG2", "Ping", &expired_ctx("CharG2"));
            let (r, req, _child) = issue_child(&p, "CharG2", "Child");
            let st = r.unwrap_err();
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(st.message(), "/EarlyReturn?src=CharG2::Ping");
            assert!(req.get_masa_context().is_none());
        });
    }

    /// The guard remembers the last completed child (resolved through method
    /// overrides) and reports it in later rejections.
    #[cfg(feature = "abort_slo")]
    #[test]
    fn guard_reports_last_child_using_method_override() {
        run(|| {
            let server = Arc::new(Srv::new("CharG3"));
            let ctx = with_tokens(fresh_builder("CharG3", 80_000), 100).build();
            let p = begin(&server, "CharG3", "Ping", &ctx);
            let mut req = Request::new(());
            req.set_service_name_override("OvSvc").unwrap();
            req.set_method_name_override("OvMethod").unwrap();
            let (r, _req, c) = issue_child_req(&p, "CharG3", "Child", req);
            r.unwrap();
            child_done(&p, "CharG3", "Child", c, Ok(Response::new(()))).unwrap();

            sleep_ms(90);
            let st = early_return_err(p.before_poll::<()>());
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(
                st.message(),
                "/EarlyReturn?src=CharG3::Ping?last_rpc=OvSvc::OvMethod"
            );
        });
    }

    /// A request rejected by the guard is recorded as a local early return and
    /// contributes no latency observations.
    #[cfg(all(feature = "abort_slo", feature = "estimator"))]
    #[test]
    fn guard_rejection_reports_early_return_and_is_not_learned() {
        run(|| {
            let server = Arc::new(Srv::new("CharG4"));
            let p = begin(&server, "CharG4", "Ping", &expired_ctx("CharG4"));
            let st = early_return_err(p.before_poll::<()>());
            let st = finalize_err(&p, st);
            let meta = response_meta(&st.get_masa_context().unwrap());
            assert_eq!(meta.early_return_count, 1);
            assert_eq!(meta.deadline_signal_count, 0);
            // Not polled, so no compute was recorded either.
            assert_eq!(meta.compute_time_us, 0);

            // A later request on the same server sees cold estimates.
            let ctx = with_tokens(fresh_builder("CharG4", SLO), 100).build();
            let p = begin(&server, "CharG4", "Ping", &ctx);
            let (r, req, _c) = issue_child(&p, "CharG4", "Child");
            r.unwrap();
            assert_eq!(req.get_masa_context().unwrap().deadline(), ctx.deadline());
        });
    }

    // ── Rajomon admission ───────────────────────────────────────────────

    #[cfg(feature = "ac_rajomon")]
    fn seed_price(
        server: &Arc<Srv>,
        svc: &'static str,
        parent: &'static str,
        child: &'static str,
        price: &str,
    ) {
        let ctx = with_tokens(fresh_builder(svc, SLO), 100).build();
        let p = begin(server, svc, parent, &ctx);
        let (r, _req, c) = issue_child(&p, svc, child);
        r.unwrap();
        let mut resp = Response::new(()).with_masa_context(&ctx.ctx);
        resp.set_wire::<RajomonLayer>(&RajomonWire::response(100, price.parse().unwrap()));
        child_done(&p, svc, child, c, Ok(resp)).unwrap();
    }

    #[cfg(feature = "ac_rajomon")]
    fn rajomon_ctx(svc: &'static str, tokens: u64) -> Inbound {
        with_tokens(fresh_builder(svc, SLO), tokens).build()
    }

    /// A request is admitted iff its tokens cover the accumulated price
    /// (own price plus the max downstream price); an admitted request may
    /// only call children whose price fits its remaining tokens. Prices are
    /// learned from child response metadata.
    #[cfg(feature = "ac_rajomon")]
    #[test]
    fn rajomon_inbound_gate_and_child_budget() {
        run(|| {
            let server = Arc::new(Srv::new("RajSvc"));
            seed_price(&server, "RajSvc", "Gate", "RajDown", "50");

            // tokens 10 < price 50: rejected at every hook, before the
            // outbound request is touched.
            let p = begin(&server, "RajSvc", "Gate", &rajomon_ctx("RajSvc", 10));
            let msg = "/EarlyReturn?src=RajSvc::Gate&reason=RajomonAdmissionRej";
            let st = early_return_err(p.before_poll::<()>());
            assert_eq!(st.code(), Code::ResourceExhausted);
            assert_eq!(st.message(), msg);
            let (r, req, _c) = issue_child(&p, "RajSvc", "RajDown");
            let st = r.unwrap_err();
            assert_eq!(st.code(), Code::ResourceExhausted);
            assert_eq!(st.message(), msg);
            assert!(req.get_masa_context().is_none());
            let st = early_return_err(p.after_poll::<()>(&Poll::Pending));
            assert_eq!(st.message(), msg);
            assert!(p
                .after_poll::<()>(&Poll::Ready(Ok(Response::new(()))))
                .is_ok());

            // tokens 100 and exactly 50 are admitted and forward all
            // remaining tokens (own price is 0) to the child.
            for tokens in [100, 50] {
                let p = begin(&server, "RajSvc", "Gate", &rajomon_ctx("RajSvc", tokens));
                assert!(p.before_poll::<()>().is_ok());
                let (r, req, _c) = issue_child(&p, "RajSvc", "RajDown");
                r.unwrap();
                assert_eq!(
                    req.get_wire::<RajomonLayer>(),
                    Some(RajomonWire::request(tokens))
                );
            }

            // A different parent has no downstream price, so it is admitted
            // with 10 tokens, but the child's cached price (50) exceeds its
            // budget.
            let p = begin(&server, "RajSvc", "Other", &rajomon_ctx("RajSvc", 10));
            assert!(p.before_poll::<()>().is_ok());
            let (r, req, _c) = issue_child(&p, "RajSvc", "RajDown");
            let st = r.unwrap_err();
            assert_eq!(st.code(), Code::ResourceExhausted);
            assert_eq!(
                st.message(),
                "/EarlyReturn?src=RajSvc::Other?last_rpc=RajSvc::RajDown&reason=RajomonChildBudgetRej"
            );
            assert!(req.get_masa_context().is_none());
        });
    }

    /// Guard runs before admission: an expired request that admission would
    /// also reject reports the guard's DeadlineExceeded, not Rajomon's.
    #[cfg(all(feature = "ac_rajomon", feature = "abort_slo"))]
    #[test]
    fn guard_runs_before_rajomon_admission() {
        run(|| {
            let server = Arc::new(Srv::new("RajGuard"));
            seed_price(&server, "RajGuard", "First", "GuardDown", "50");

            let entry = time_now() - 1_000_000;
            let ctx = with_tokens(
                ContextBuilder::new("RajGuard", 1)
                    .slo(1_000)
                    .gateway_entry(entry)
                    .deadline(entry + 1_000),
                10,
            )
            .build();
            let p = begin(&server, "RajGuard", "First", &ctx);
            let st = early_return_err(p.before_poll::<()>());
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(st.message(), "/EarlyReturn?src=RajGuard::First");
            let (r, _req, _c) = issue_child(&p, "RajGuard", "GuardDown");
            let st = r.unwrap_err();
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(st.message(), "/EarlyReturn?src=RajGuard::First");
        });
    }

    /// Estimation runs before admission: a locally late request that admission
    /// would also reject reports estimation's LocalDeadlineExceeded.
    #[cfg(all(feature = "ac_rajomon", feature = "abort_slack"))]
    #[test]
    fn estimation_runs_before_rajomon_admission() {
        run(|| {
            let server = Arc::new(Srv::new("RajEst"));
            seed_price(&server, "RajEst", "First", "EstDown", "50");

            let now = time_now();
            let ctx = with_tokens(
                ContextBuilder::new("RajEst", 1)
                    .slo(10 * SLO)
                    .gateway_entry(now)
                    .deadline(now - 1_000_000),
                10,
            )
            .build();
            let p = begin(&server, "RajEst", "First", &ctx);
            let st = early_return_err(p.before_poll::<()>());
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(
                st.message(),
                "/EarlyReturn?src=RajEst::First&reason=LocalDeadlineExceeded"
            );
        });
    }

    // ── 3. Estimates flow to predictive admission ───────────────────────

    /// Latencies observed by estimation are what predictive admission's
    /// feasibility check consults: after the child has been observed to take
    /// >= 40 ms, a child call with only 30 ms left is rejected, while a
    /// generous SLO or a server with no observations admits it.
    #[cfg(feature = "ac_pred")]
    #[test]
    fn estimates_feed_predictive_admission() {
        run(|| {
            let feas_msg = "/EarlyReturn?src=CharP::Parent?last_rpc=CharP::Child&reason=BeforeChildFeasibility";

            let cold = Arc::new(Srv::new("CharP"));
            let ctx = fresh_ctx("CharP", 30_000);
            let p = begin(&cold, "CharP", "Parent", &ctx);
            let (r, req, _c) = issue_child(&p, "CharP", "Child");
            r.expect("cold estimators admit unconditionally");
            assert!(req.get_masa_context().is_some());

            let server = Arc::new(Srv::new("CharP"));
            for _ in 0..2 {
                train_cycle(&server, "CharP", "Parent", "Child", 40, 10, None);
            }

            let ctx = fresh_ctx("CharP", 30_000);
            let p = begin(&server, "CharP", "Parent", &ctx);
            let (r, req, _c) = issue_child(&p, "CharP", "Child");
            let st = r.unwrap_err();
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(st.message(), feas_msg);
            assert!(req.get_masa_context().is_none());

            let ctx = fresh_ctx("CharP", SLO);
            let p = begin(&server, "CharP", "Parent", &ctx);
            let (r, req, _c) = issue_child(&p, "CharP", "Child");
            r.expect("a generous SLO fits the learned latency");
            assert!(req.get_masa_context().is_some());
        });
    }

    /// Early-return outcomes at ingress drive the AIMD controller: after
    /// several 50 ms windows of all-early-return outcomes, new ingress
    /// requests are rejected with PredAdmissionRej, while a fresh server never
    /// rejects.
    #[cfg(feature = "ac_pred")]
    #[test]
    fn early_return_outcomes_close_predictive_admission() {
        run(|| {
            let fresh = Arc::new(Srv::new("CharP2"));
            for _ in 0..100 {
                let p = begin(&fresh, "CharP2", "Ingress", &fresh_ctx("CharP2", SLO));
                assert!(p.before_poll::<()>().is_ok());
            }

            let server = Arc::new(Srv::new("CharP2"));
            for _ in 0..10 {
                sleep_ms(55);
                let p = begin(&server, "CharP2", "Ingress", &fresh_ctx("CharP2", SLO));
                let er = Status::new(Code::DeadlineExceeded, "/EarlyReturn?src=CharP2::Ingress");
                finalize_err(&p, er);
            }
            let mut rejected = 0;
            for _ in 0..50 {
                let p = begin(&server, "CharP2", "Ingress", &fresh_ctx("CharP2", SLO));
                if let Err(Err(st)) = p.before_poll::<()>() {
                    assert_eq!(st.code(), Code::DeadlineExceeded);
                    assert_eq!(
                        st.message(),
                        "/EarlyReturn?src=CharP2::Ingress&reason=PredAdmissionRej"
                    );
                    rejected += 1;
                }
            }
            assert!(rejected > 0, "admission never rejected after overload");
        });
    }

    // ── 6. abort_slack / signal_slack ───────────────────────────────────

    /// abort_slack: a past local deadline aborts before the poll and replaces
    /// a Pending poll, but not a finished one.
    #[cfg(feature = "abort_slack")]
    #[test]
    fn abort_slack_aborts_locally_late_request() {
        run(|| {
            let server = Arc::new(Srv::new("CharS1"));
            let msg = "/EarlyReturn?src=CharS1::Late&reason=LocalDeadlineExceeded";

            let p = begin(&server, "CharS1", "Late", &locally_late_ctx("CharS1"));
            let st = early_return_err(p.before_poll::<()>());
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(st.message(), msg);
            let st = early_return_err(p.after_poll::<()>(&Poll::Pending));
            assert_eq!(st.message(), msg);
            assert!(p
                .after_poll::<()>(&Poll::Ready(Ok(Response::new(()))))
                .is_ok());
            let st = finalize_err(&p, st);
            assert_eq!(
                response_meta(&st.get_masa_context().unwrap()).early_return_count,
                1
            );

            // On-time control.
            let p = begin(&server, "CharS1", "OnTime", &fresh_ctx("CharS1", SLO));
            assert!(p.before_poll::<()>().is_ok());
            assert!(p.after_poll::<()>(&Poll::Pending).is_ok());
        });
    }

    /// abort_slack end to end: the deadline handed to a child already
    /// accounts for the learned post-child work, so a child that starts after
    /// the tightened deadline aborts even though the end-to-end SLO has not
    /// expired.
    #[cfg(all(feature = "abort_slack", not(feature = "ac_pred")))]
    #[test]
    fn abort_slack_child_hits_tightened_deadline() {
        run(|| {
            let server = Arc::new(Srv::new("CharS2"));
            for _ in 0..2 {
                train_cycle(&server, "CharS2", "Parent", "Child", 5, 10, None);
            }
            let ctx = with_tokens(fresh_builder("CharS2", 40_000), 100).build();
            let p = begin(&server, "CharS2", "Parent", &ctx);
            let (r, req, _c) = issue_child(&p, "CharS2", "Child");
            r.unwrap();
            let cc = req.get_masa_context().unwrap();
            assert!(ctx.deadline() - cc.deadline() >= 10_000);
            assert_eq!(cc.slo(), 40_000);

            // 35 ms > 40 ms SLO - 10 ms tightening, yet < the 40 ms SLO.
            sleep_ms(35);
            let child_server = Arc::new(Srv::new("CharS2"));
            let cp = begin(&child_server, "CharS2", "Child", &cc);
            let st = early_return_err(cp.before_poll::<()>());
            assert_eq!(st.code(), Code::DeadlineExceeded);
            assert_eq!(
                st.message(),
                "/EarlyReturn?src=CharS2::Child&reason=LocalDeadlineExceeded"
            );
        });
    }

    /// signal_slack: a locally late request keeps running and returns Ok, but
    /// its response reports one deadline signal and no early return.
    #[cfg(feature = "signal_slack")]
    #[test]
    fn signal_slack_continues_and_counts_signal() {
        run(|| {
            let server = Arc::new(Srv::new("CharS3"));

            let p = begin(&server, "CharS3", "Late", &locally_late_ctx("CharS3"));
            assert!(p.before_poll::<()>().is_ok());
            assert!(p.after_poll::<()>(&Poll::Pending).is_ok());
            assert!(p
                .after_poll::<()>(&Poll::Ready(Ok(Response::new(()))))
                .is_ok());
            let meta = response_meta(&finalize_ok(&p).get_masa_context().unwrap());
            assert_eq!(meta.deadline_signal_count, 1);
            assert_eq!(meta.early_return_count, 0);

            // On-time control.
            let p = begin(&server, "CharS3", "OnTime", &fresh_ctx("CharS3", SLO));
            assert!(p.before_poll::<()>().is_ok());
            assert!(p.after_poll::<()>(&Poll::Pending).is_ok());
            let meta = response_meta(&finalize_ok(&p).get_masa_context().unwrap());
            assert_eq!(meta.deadline_signal_count, 0);
        });
    }

    /// signal_slack: a signal from a child propagates to the parent's response
    /// (saturated at 1) and the signaled request is not learned from.
    #[cfg(feature = "signal_slack")]
    #[test]
    fn signal_slack_child_signal_propagates_and_skips_learning() {
        run(|| {
            let server = Arc::new(Srv::new("CharS4"));

            for _ in 0..2 {
                let p = begin(&server, "CharS4", "Parent", &fresh_ctx("CharS4", SLO));
                let (r, _req, c) = issue_child(&p, "CharS4", "Child");
                r.unwrap();
                let child_ctx = fresh_builder("CharS4", SLO)
                    .response_meta(masa_core::EstimatorResponse {
                        deadline_signal_count: 1,
                        ..Default::default()
                    })
                    .build();
                let resp = Response::new(()).with_masa_context(&child_ctx);
                sleep_ms(5);
                child_done(&p, "CharS4", "Child", c, Ok(resp)).unwrap();
                sleep_ms(10);
                let meta = response_meta(&finalize_ok(&p).get_masa_context().unwrap());
                assert_eq!(meta.deadline_signal_count, 1);
                assert_eq!(meta.early_return_count, 0);
            }

            let ctx = fresh_ctx("CharS4", SLO);
            let p = begin(&server, "CharS4", "Parent", &ctx);
            let (r, req, _c) = issue_child(&p, "CharS4", "Child");
            r.unwrap();
            assert_eq!(req.get_masa_context().unwrap().deadline(), ctx.deadline());
        });
    }
}

// ── Oracle (sched_oracle): last writer wins, ordered before admission ───

#[cfg(feature = "sched_oracle")]
mod oracle {
    use super::*;
    use masa_core::{ORACLE_CHILD_WORK_US_HEADER, ORACLE_REMAINING_AFTER_US_HEADER};

    fn oracle_req(child_work_us: u64, remaining_after_us: u64) -> Request<()> {
        let mut req = Request::new(());
        req.metadata_mut().insert(
            ORACLE_CHILD_WORK_US_HEADER,
            child_work_us.to_string().parse().unwrap(),
        );
        req.metadata_mut().insert(
            ORACLE_REMAINING_AFTER_US_HEADER,
            remaining_after_us.to_string().parse().unwrap(),
        );
        req
    }

    /// The child completion deadline is e2e_deadline - remaining_after and the
    /// priority is that minus the child's work. When estimation has learned
    /// something too, the oracle (later in the pipeline) still wins.
    #[test]
    fn oracle_sets_child_deadline_and_priority() {
        run(|| {
            let server = Arc::new(Srv::new("CharO1"));
            #[cfg(feature = "estimator")]
            for _ in 0..2 {
                let ctx = with_tokens(fresh_builder("CharO1", SLO), 100).build();
                let p = begin(&server, "CharO1", "Parent", &ctx);
                let (r, _req, c) = issue_child_req(&p, "CharO1", "Child", oracle_req(1, 1));
                r.unwrap();
                sleep_ms(5);
                child_done(&p, "CharO1", "Child", c, Ok(Response::new(()))).unwrap();
                sleep_ms(10);
                finalize_ok(&p);
            }

            let ctx = with_tokens(fresh_builder("CharO1", SLO), 100).build();
            let p = begin(&server, "CharO1", "Parent", &ctx);
            let (r, req, _c) = issue_child_req(&p, "CharO1", "Child", oracle_req(30_000, 7_000));
            r.unwrap();
            let cc = req.get_masa_context().unwrap();
            let completion = ctx.e2e_deadline() - 7_000;
            assert_eq!(cc.deadline(), completion);
            assert_eq!(cc.prio_hint().value(), completion - 30_000);
            #[cfg(feature = "estimator")]
            assert_eq!(cc.hop_count(), 1);
        });
    }

    /// Missing oracle headers fail the child RPC with Internal and leave the
    /// request untouched. With predictive admission, which would otherwise
    /// reject this request, the oracle error wins because the oracle runs
    /// first.
    #[test]
    fn oracle_error_short_circuits_before_admission() {
        run(|| {
            let server = Arc::new(Srv::new("CharO2"));
            let p = begin(&server, "CharO2", "Parent", &fresh_ctx("CharO2", SLO));
            let (r, req, _c) = issue_child(&p, "CharO2", "Child");
            let st = r.unwrap_err();
            assert_eq!(st.code(), Code::Internal);
            assert_eq!(
                st.message(),
                format!(
                    "missing oracle header '{}' for CharO2::Child",
                    ORACLE_CHILD_WORK_US_HEADER
                )
            );
            assert!(req.get_masa_context().is_none());

            #[cfg(feature = "ac_pred")]
            {
                let server = Arc::new(Srv::new("CharO3"));
                for _ in 0..2 {
                    let p = begin(&server, "CharO3", "Parent", &fresh_ctx("CharO3", SLO));
                    let (r, _req, c) = issue_child_req(&p, "CharO3", "Child", oracle_req(1, 1));
                    r.unwrap();
                    sleep_ms(40);
                    child_done(&p, "CharO3", "Child", c, Ok(Response::new(()))).unwrap();
                    sleep_ms(10);
                    finalize_ok(&p);
                }
                let p = begin(&server, "CharO3", "Parent", &fresh_ctx("CharO3", 30_000));
                // With headers, admission rejects on learned latency ...
                let (r, _req, _c) = issue_child_req(&p, "CharO3", "Child", oracle_req(1, 1));
                let st = r.unwrap_err();
                assert_eq!(st.code(), Code::DeadlineExceeded);
                assert!(st.message().ends_with("reason=BeforeChildFeasibility"));
                // ... but without headers the oracle's error comes first.
                let (r, _req, _c) = issue_child(&p, "CharO3", "Child");
                assert_eq!(r.unwrap_err().code(), Code::Internal);
            }
        });
    }
}
