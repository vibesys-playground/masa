// What estimation puts on the wire and shares through per-request extensions:
// hop count and root method travel down in the request section, estimation
// alone decides when the hop count grows, and later modules read the parsed
// facts from `Extensions` instead of from `Context`.

#![cfg(feature = "estimator")]

use std::sync::Arc;

use masa_core::{time_now, Context, ContextBuilder};
use masa_policy::modules::EstimationLayer;
use masa_policy::{
    get_wire_from_metadata, policy_stack, set_masa_context_in_metadata, set_wire_in_metadata,
    EstimationInfo, EstimationRequestWire, EstimationWire, Extensions, Layer, MasaRequestExt,
    MasaResponseExt, PolicyHooks, RootMethod, WireIn, WireOut, MASA_CONTEXT_HEADER,
};
use serde::{Deserialize, Serialize};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::metadata::MetadataMap;
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

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
fn call_child<S: Layer + 'static>(
    service: &'static str,
    method: &'static str,
    req: &http::Request<()>,
) -> Request<()> {
    let server = Arc::new(Server::<S>::new(service));
    let parent = Parent::<S>::begin(GrpcMethod::new(service, method), req, server);
    let child = GrpcMethod::new("down", "Child");
    let mut request = Request::new(());
    // The oracle refuses child RPCs that lack its headers.
    #[cfg(feature = "sched_oracle")]
    for header in [
        masa_core::ORACLE_CHILD_WORK_US_HEADER,
        masa_core::ORACLE_REMAINING_AFTER_US_HEADER,
    ] {
        request.metadata_mut().insert(header, "1".parse().unwrap());
    }
    let mut child_ctx = Child::<S>::new(child, &request);
    parent
        .before_child_rpc(child, &mut request, &mut child_ctx)
        .unwrap();
    request
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
    type Child = ();
    const NAME: &'static str = "probe";
    type Wire = Seen;

    fn new(
        _m: &CowGrpcMethod,
        _s: &(),
        _c: &mut Context,
        _w: &WireIn<'_>,
        ext: &mut Extensions,
    ) -> Self {
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
        _ctx: &mut Context,
        _result: &mut Result<Response<Ret>, Status>,
        wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        wire.put::<Self>(&self.0).unwrap();
    }
}

type Probed = policy_stack![EstimationLayer, Probe];

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
        type Misordered = policy_stack![PredAdmissionLayer, EstimationLayer];
        let err = masa_policy::ServerContext::<Misordered>::try_new("misordered")
            .expect_err("admission needs estimation earlier in the stack");
        assert!(
            err.to_string().contains("PredAdmissionServer"),
            "message names the module: {err}"
        );
    }

    #[test]
    fn admission_without_estimation_is_reported_at_construction() {
        type Missing = policy_stack![PredAdmissionLayer];
        assert!(masa_policy::ServerContext::<Missing>::try_new("missing").is_err());
    }

    #[test]
    fn admission_after_estimation_constructs() {
        type Ordered = policy_stack![EstimationLayer, PredAdmissionLayer];
        assert!(masa_policy::ServerContext::<Ordered>::try_new("ordered").is_ok());
    }
}
