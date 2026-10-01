// What estimation puts on the wire and shares through per-request extensions:
// hop count and root method travel down in the request section, estimation
// alone decides when the hop count grows, each hop's cost report travels up in
// the response section and is aggregated by its parent, and later modules read
// the parsed facts from `Extensions` instead of from `Context`.

#![cfg(feature = "estimator")]

use std::sync::Arc;

use masa_core::{time_now, Context};
use masa_policy::modules::EstimationLayer;
use masa_policy::ContextBuilder;
use masa_policy::{
    get_wire_from_metadata, policy_stack, set_masa_context_in_metadata, set_wire_in_metadata,
    BudgetLayer, EstimationInfo, EstimationRequestWire, EstimationResponseWire, EstimationWire,
    Extensions, Layer, LayerStack, MasaRequestExt, MasaResponseExt, MasaStatusExt, Outcome,
    PolicyHooks, RootMethod, SubtreeHealth, WireIn, WireOut, MASA_CONTEXT_HEADER,
};
use serde::{Deserialize, Serialize};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::metadata::MetadataMap;
use tonic::{Code, CowGrpcMethod, GrpcMethod, Request, Response, Status};

type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

fn context() -> Context {
    let now = time_now();
    ContextBuilder::new("est-api", 1)
        .slo(10_000_000_000)
        .gateway_entry(now)
        .deadline(now + 10_000_000_000)
        .build()
}

/// An inbound request with estimation's request section, or without one.
fn inbound(estimation: Option<EstimationWire>) -> http::Request<()> {
    let mut metadata = MetadataMap::new();
    set_masa_context_in_metadata(&mut metadata, &context());
    if let Some(wire) = estimation {
        set_wire_in_metadata::<EstimationLayer>(&mut metadata, &wire);
    }
    request_with(&metadata)
}

fn request_with(metadata: &MetadataMap) -> http::Request<()> {
    http::Request::builder()
        .header(
            MASA_CONTEXT_HEADER,
            metadata.get(MASA_CONTEXT_HEADER).unwrap().to_str().unwrap(),
        )
        .body(())
        .unwrap()
}

/// Begin `method` on a fresh server, call one child, and return the outbound
/// child request.
fn call_child<S: LayerStack>(
    service: &'static str,
    method: &'static str,
    req: &http::Request<()>,
) -> Request<()> {
    let parent = begin::<S>(service, method, req);
    issue_child(&parent).0
}

fn begin<S: LayerStack>(
    service: &'static str,
    method: &'static str,
    req: &http::Request<()>,
) -> Parent<S> {
    let server = Arc::new(Server::<S>::new(service));
    Parent::<S>::begin(GrpcMethod::new(service, method), req, server)
}

fn child_method() -> GrpcMethod {
    GrpcMethod::new("down", "Child")
}

/// Issue a child RPC from `parent`; returns the outbound request and the child
/// context to hand back with the response.
fn issue_child<S: LayerStack>(parent: &Parent<S>) -> (Request<()>, Child<S>) {
    let mut request = Request::new(());
    // The oracle refuses child RPCs that lack its headers.
    #[cfg(feature = "sched_oracle")]
    for header in [
        masa_core::ORACLE_CHILD_WORK_US_HEADER,
        masa_core::ORACLE_REMAINING_AFTER_US_HEADER,
    ] {
        request.metadata_mut().insert(header, "1".parse().unwrap());
    }
    let mut child_ctx = Child::<S>::new(child_method(), &request);
    parent
        .before_child_rpc(child_method(), &mut request, &mut child_ctx)
        .unwrap();
    (request, child_ctx)
}

type DefaultStack = masa_policy::MasaStack;

fn sent(request: &Request<()>) -> EstimationRequestWire {
    request
        .get_wire::<EstimationLayer>()
        .expect("estimation section on the child request")
        .request
        .expect("request part")
}

fn root(service: &str, method: &str) -> RootMethod {
    RootMethod {
        service: service.into(),
        method: method.into(),
    }
}

// ── Wire round trips ────────────────────────────────────────────────────

#[test]
fn request_section_round_trips_including_zero_hop_count() {
    for wire in [
        EstimationWire::request(0, None),
        EstimationWire::request(0, Some(root("a", "b"))),
        EstimationWire::request(7, Some(root("svc.Name", "Method"))),
        EstimationWire::request(u8::MAX, None),
    ] {
        let mut request = Request::new(());
        request.set_masa_context(&context());
        request.set_wire::<EstimationLayer>(&wire);
        assert_eq!(request.get_wire::<EstimationLayer>(), Some(wire));
    }
}

#[test]
fn a_zero_hop_count_is_a_present_value() {
    let mut metadata = MetadataMap::new();
    set_masa_context_in_metadata(&mut metadata, &context());
    assert_eq!(get_wire_from_metadata::<EstimationLayer>(&metadata), None);
    set_wire_in_metadata::<EstimationLayer>(&mut metadata, &EstimationWire::request(0, None));
    let wire = get_wire_from_metadata::<EstimationLayer>(&metadata).expect("present");
    assert_eq!(wire.request.expect("request part").hop_count, 0);
}

// ── Ingress: absent and zero mean the same to estimation ────────────────

#[test]
fn no_section_is_ingress_and_makes_this_method_the_root() {
    let request = call_child::<DefaultStack>("edge", "Entry", &inbound(None));
    assert_eq!(
        sent(&request),
        EstimationRequestWire {
            hop_count: 1,
            root_method: Some(root("edge", "Entry")),
        }
    );
}

#[test]
fn a_zero_hop_count_section_is_ingress_and_replaces_any_root() {
    let req = inbound(Some(EstimationWire::request(0, Some(root("old", "Root")))));
    let request = call_child::<DefaultStack>("edge", "Entry", &req);
    assert_eq!(
        sent(&request),
        EstimationRequestWire {
            hop_count: 1,
            root_method: Some(root("edge", "Entry")),
        }
    );
}

// ── Propagation ─────────────────────────────────────────────────────────

#[test]
fn hop_count_grows_by_one_per_hop_and_the_root_is_kept() {
    let mut req = inbound(None);
    for (hop, service) in ["a", "b", "c", "d"].into_iter().enumerate() {
        let request = call_child::<DefaultStack>(service, "Method", &req);
        let wire = sent(&request);
        assert_eq!(usize::from(wire.hop_count), hop + 1, "service {service}");
        assert_eq!(wire.root_method, Some(root("a", "Method")));
        req = request_with(request.metadata());
    }
}

#[test]
fn hop_count_saturates() {
    let req = inbound(Some(EstimationWire::request(
        u8::MAX,
        Some(root("a", "Method")),
    )));
    let request = call_child::<DefaultStack>("z", "Method", &req);
    assert_eq!(sent(&request).hop_count, u8::MAX);
}

#[test]
fn a_section_without_a_root_passes_none_on() {
    let req = inbound(Some(EstimationWire::request(3, None)));
    let request = call_child::<DefaultStack>("z", "Method", &req);
    assert_eq!(
        sent(&request),
        EstimationRequestWire {
            hop_count: 4,
            root_method: None,
        }
    );
}

#[test]
fn an_inbound_section_is_not_forwarded_by_the_framework() {
    // A stack without estimation sends no estimation section at all.
    type NoEstimation = policy_stack![];
    let req = inbound(Some(EstimationWire::request(5, Some(root("a", "b")))));
    let request = call_child::<NoEstimation>("z", "Method", &req);
    assert_eq!(request.get_wire::<EstimationLayer>(), None);
}

// ── Sharing through extensions ──────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Seen {
    hop_count: u8,
    ingress: bool,
    root: Option<(String, String)>,
    root_id_known: bool,
}

/// Reports what estimation published in the extensions.
#[derive(Debug)]
struct Probe(Seen);

impl Layer for Probe {
    type Server = ();
    const NAME: &'static str = "probe";
    type Wire = Seen;

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, ext: &mut Extensions) -> Self {
        let info = ext
            .get::<EstimationInfo>()
            .expect("estimation runs before the probe");
        Self(Seen {
            hop_count: info.hop_count(),
            ingress: info.is_ingress(),
            root: info
                .root_method()
                .map(|root| (root.service.clone(), root.method.clone())),
            root_id_known: info.root_method_id().is_some(),
        })
    }

    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        wire.put::<Self>(&self.0).unwrap();
    }
}

type Probed = policy_stack![BudgetLayer, EstimationLayer, Probe];

fn probe(req: &http::Request<()>) -> Seen {
    let server = Arc::new(Server::<Probed>::new("probed"));
    let parent = Parent::<Probed>::begin(GrpcMethod::new("probed", "Method"), req, server);
    let mut result: Result<Response<()>, Status> = Ok(Response::new(()));
    parent.finalize_before_serialization(&mut result);
    result.unwrap().get_wire::<Probe>().unwrap()
}

#[test]
fn later_modules_read_ingress_facts_from_extensions() {
    assert_eq!(
        probe(&inbound(None)),
        Seen {
            hop_count: 0,
            ingress: true,
            root: Some(("probed".into(), "Method".into())),
            root_id_known: true,
        }
    );
}

#[test]
fn later_modules_read_inherited_facts_from_extensions() {
    let req = inbound(Some(EstimationWire::request(2, Some(root("a", "Root")))));
    assert_eq!(
        probe(&req),
        Seen {
            hop_count: 2,
            ingress: false,
            root: Some(("a".into(), "Root".into())),
            root_id_known: true,
        }
    );
}

#[test]
fn a_non_ingress_request_without_a_root_has_no_root_id() {
    let req = inbound(Some(EstimationWire::request(2, None)));
    let seen = probe(&req);
    assert_eq!(seen.root, None);
    assert!(!seen.root_id_known);
}

// ── Ordering ────────────────────────────────────────────────────────────

#[cfg(feature = "ac_pred")]
mod ordering {
    use super::*;
    use masa_policy::modules::PredAdmissionLayer;

    #[test]
    fn admission_before_estimation_is_reported_at_construction() {
        type Misordered = policy_stack![BudgetLayer, PredAdmissionLayer, EstimationLayer];
        let err = masa_policy::ServerContext::<Misordered>::try_new("misordered")
            .expect_err("admission needs estimation earlier in the stack");
        let message = err.to_string();
        assert!(
            message.contains("PredAdmissionLayer") && message.contains("EstimationLayer"),
            "message names both modules: {message}"
        );
        assert!(message.contains("comes later"), "{message}");
    }

    #[test]
    fn admission_without_estimation_is_reported_at_construction() {
        type Missing = policy_stack![PredAdmissionLayer];
        assert!(masa_policy::ServerContext::<Missing>::try_new("missing").is_err());
    }

    #[test]
    fn admission_after_estimation_constructs() {
        type Ordered = policy_stack![BudgetLayer, EstimationLayer, PredAdmissionLayer];
        assert!(masa_policy::ServerContext::<Ordered>::try_new("ordered").is_ok());
    }
}

// ── Response section ────────────────────────────────────────────────────

fn report(
    accumulated: u64,
    max_util: f32,
    early_returns: u32,
    signals: u32,
) -> EstimationResponseWire {
    EstimationResponseWire {
        compute_time_us: accumulated / 2,
        accumulated_compute_us: accumulated,
        utilization: max_util / 2.0,
        max_downstream_util: max_util,
        early_return_count: early_returns,
        deadline_signal_count: signals,
    }
}

fn child_ok(report: Option<EstimationResponseWire>) -> Result<Response<()>, Status> {
    let mut response = Response::new(()).with_masa_context(&context());
    if let Some(report) = report {
        response.set_wire::<EstimationLayer>(&EstimationWire::response(report));
    }
    Ok(response)
}

fn child_err(
    code: Code,
    message: &str,
    report: Option<EstimationResponseWire>,
) -> Result<Response<()>, Status> {
    let mut status = Status::new(code, message).with_masa_context(&context());
    if let Some(report) = report {
        status.set_wire::<EstimationLayer>(&EstimationWire::response(report));
    }
    Err(status)
}

/// Begin a request at ingress, run one child RPC per entry of `children`, and
/// return the estimation report of the parent's (successful) response.
fn report_after(children: Vec<Result<Response<()>, Status>>) -> EstimationResponseWire {
    let parent = begin::<DefaultStack>("agg", "Method", &inbound(None));
    for mut response in children {
        let (_request, child_ctx) = issue_child(&parent);
        // `sched_pred` passes a failed child's status on; what it does with
        // it is not under test, only what the hooks recorded.
        let _ = parent.after_child_rpc(child_method(), &mut response, child_ctx);
    }
    let mut result: Result<Response<()>, Status> = Ok(Response::new(()));
    parent.finalize_before_serialization(&mut result);
    let wire = result
        .unwrap()
        .get_wire::<EstimationLayer>()
        .expect("estimation section");
    assert_eq!(
        wire.request, None,
        "a response carries only the response half"
    );
    wire.response.expect("response half")
}

#[test]
fn response_section_round_trips_including_zero_values() {
    for wire in [
        report(0, 0.0, 0, 0),
        report(5_000, 0.5, 2, 1),
        EstimationResponseWire {
            compute_time_us: u64::MAX,
            accumulated_compute_us: u64::MAX,
            utilization: 1.0,
            max_downstream_util: 1.0,
            early_return_count: u32::MAX,
            deadline_signal_count: u32::MAX,
        },
    ] {
        let mut response = Response::new(()).with_masa_context(&context());
        response.set_wire::<EstimationLayer>(&EstimationWire::response(wire.clone()));
        assert_eq!(
            response.get_wire::<EstimationLayer>(),
            Some(EstimationWire::response(wire))
        );
    }
}

#[test]
fn a_quiet_request_reports_zeros() {
    let quiet = report_after(vec![]);
    assert_eq!(quiet.compute_time_us, 0, "no polls ran");
    assert_eq!(quiet.accumulated_compute_us, 0);
    assert_eq!(quiet.early_return_count, 0);
    assert_eq!(quiet.deadline_signal_count, 0);
    assert!(quiet.max_downstream_util >= quiet.utilization);
}

#[test]
fn a_parent_aggregates_its_children_reports() {
    let total = report_after(vec![
        child_ok(Some(report(3_000, 0.2, 1, 0))),
        child_ok(Some(report(2_000, 0.9, 2, 1))),
        child_ok(Some(report(1_000, 0.4, 0, 1))),
    ]);
    assert_eq!(total.compute_time_us, 0, "local compute only");
    assert_eq!(total.accumulated_compute_us, 6_000);
    assert_eq!(total.early_return_count, 3);
    assert_eq!(total.deadline_signal_count, 1, "signals saturate at one");
    assert!(total.max_downstream_util >= 0.9);
}

#[test]
fn a_child_without_a_report_contributes_nothing() {
    let total = report_after(vec![
        child_ok(None),
        child_ok(Some(report(4_000, 0.3, 0, 0))),
    ]);
    assert_eq!(total.accumulated_compute_us, 4_000);
    assert_eq!(total.early_return_count, 0);
}

#[test]
fn an_early_return_child_counts_once_whatever_its_status_carries() {
    let total = report_after(vec![
        child_err(
            Code::DeadlineExceeded,
            "/EarlyReturn?src=x",
            Some(report(9_000, 0.99, 5, 1)),
        ),
        child_err(Code::DeadlineExceeded, "/EarlyReturn?src=y", None),
    ]);
    assert_eq!(total.early_return_count, 2);
    assert_eq!(total.accumulated_compute_us, 0);
    assert_eq!(total.deadline_signal_count, 0);
}

#[test]
fn a_failed_child_that_is_not_an_early_return_contributes_nothing() {
    let total = report_after(vec![child_err(
        Code::Internal,
        "boom",
        Some(report(9_000, 0.99, 5, 1)),
    )]);
    assert_eq!(total.accumulated_compute_us, 0);
    assert_eq!(total.early_return_count, 0);
    assert_eq!(total.deadline_signal_count, 0);
}

#[test]
fn an_error_status_carries_the_response_half_and_counts_its_own_early_return() {
    let parent = begin::<DefaultStack>("agg", "Method", &inbound(None));
    let mut result: Result<Response<()>, Status> =
        Err(Status::new(Code::DeadlineExceeded, "/EarlyReturn?src=agg"));
    parent.finalize_before_serialization(&mut result);
    let wire = result
        .unwrap_err()
        .get_wire::<EstimationLayer>()
        .expect("estimation section on the status");
    assert_eq!(wire.response.expect("response half").early_return_count, 1);
}

#[test]
fn a_report_makes_a_grandparent_see_the_whole_subtree() {
    // The middle hop aggregates its child's report into its own response,
    // which is what the ingress hop reads.
    let middle = begin::<DefaultStack>(
        "mid",
        "Method",
        &inbound(Some(EstimationWire::request(1, None))),
    );
    let (_request, child_ctx) = issue_child(&middle);
    let mut response = child_ok(Some(report(8_000, 0.7, 1, 0)));
    middle
        .after_child_rpc(child_method(), &mut response, child_ctx)
        .unwrap();
    let mut result: Result<Response<()>, Status> = Ok(Response::new(()));
    middle.finalize_before_serialization(&mut result);
    let sent_up = result
        .unwrap()
        .get_wire::<EstimationLayer>()
        .and_then(|wire| wire.response)
        .expect("response half");

    let total = report_after(vec![child_ok(Some(sent_up))]);
    assert_eq!(total.accumulated_compute_us, 8_000);
    assert_eq!(total.early_return_count, 1);
    assert!(total.max_downstream_util >= 0.7);
}

// ── The subtree outcome through extensions ──────────────────────────────

/// Reports, at finalize, whether estimation recorded an early return or
/// signal in the subtree. Estimation finalizes after this module (post-hooks
/// run in reverse stack order), so the tally must already be complete.
#[derive(Debug)]
struct OutcomeProbe;

impl Layer for OutcomeProbe {
    type Server = ();
    const NAME: &'static str = "outcome_probe";
    type Wire = bool;

    fn requires(requires: &mut masa_policy::Requires) {
        requires.module::<EstimationLayer>();
    }

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self
    }

    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        ext: &Extensions,
    ) {
        wire.put::<Self>(&SubtreeHealth::of(ext).any()).unwrap();
    }
}

type Subtree = policy_stack![BudgetLayer, EstimationLayer, OutcomeProbe];

fn subtree_outcome(child: Option<Result<Response<()>, Status>>) -> bool {
    let parent = begin::<Subtree>("outcome", "Method", &inbound(None));
    if let Some(mut response) = child {
        let (_request, child_ctx) = issue_child(&parent);
        // `sched_pred` passes a failed child's status on; what it does with
        // it is not under test, only what the hooks recorded.
        let _ = parent.after_child_rpc(child_method(), &mut response, child_ctx);
    }
    let mut result: Result<Response<()>, Status> = Ok(Response::new(()));
    parent.finalize_before_serialization(&mut result);
    result.unwrap().get_wire::<OutcomeProbe>().unwrap()
}

#[test]
fn later_modules_see_early_returns_and_signals_below_them() {
    assert!(!subtree_outcome(None));
    assert!(!subtree_outcome(Some(child_ok(Some(report(1, 0.1, 0, 0))))));
    assert!(subtree_outcome(Some(child_ok(Some(report(1, 0.1, 1, 0))))));
    assert!(subtree_outcome(Some(child_ok(Some(report(1, 0.1, 0, 1))))));
    assert!(subtree_outcome(Some(child_err(
        Code::DeadlineExceeded,
        "/EarlyReturn",
        None
    ))));
}
