// The ingress decision: before a request's task is queued, the stack's modules
// see the inbound wire and their server state. They propose a `Meta`, and the
// one module that owns the decision settles it; or one of them turns the
// request away. Shown with toy modules; the framework gives no meaning to the
// numbers and has no combination rule: each owner below applies its own.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use rpcstack::{
    policy_stack, Extensions, Ingress as StackIngress, MissingDependency, Module, ModuleServer,
    Proposal, ServerInit, WireIn, WireOut,
};
use rpcstack_sched::Meta;
use rpcstack_tonic::ServerContext;
use serde::{Deserialize, Serialize};
use tonic::masa::{Ingress, ServerHooks};
use tonic::{Code, CowGrpcMethod, Status};

const PATH: &str = "/pkg.Svc/Method";

#[derive(Serialize, Deserialize)]
struct Want(u64);

/// A `ctx` header carrying `sections`, each a toy module's wire data, after
/// `junk`, a raw prefix that is not valid wire data.
fn headers_with(junk: &str, sections: &[(&str, u64)]) -> http::HeaderMap {
    let mut out = WireOut::new();
    for (name, value) in sections {
        match *name {
            "first" => out.put::<First>(&Want(*value)).unwrap(),
            "second" => out.put::<Second>(&Want(*value)).unwrap(),
            other => panic!("no module `{other}`"),
        }
    }
    let value = if junk.is_empty() {
        out.header_value()
    } else {
        format!("{junk}.{}", out.header_value())
    };
    let mut headers = http::HeaderMap::new();
    headers.insert(rpcstack::HEADER_NAME, value.parse().unwrap());
    headers
}

/// Proposes the number in its own section, if the sender attached one, and
/// rejects a section that does not decode.
macro_rules! proposer {
    ($ty:ident, $name:literal) => {
        #[derive(Debug)]
        struct $ty;

        impl Module for $ty {
            type Server = ();
            const NAME: &'static str = $name;
            type Wire = Want;

            fn ingress(_server: &(), wire: &WireIn<'_>, ingress: &mut StackIngress) {
                match wire.get::<Self>() {
                    Ok(Some(Want(value))) => ingress.propose(Meta::new(value)),
                    Ok(None) => {}
                    Err(err) => ingress.reject(Status::invalid_argument(err.to_string())),
                }
            }

            fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
                Self
            }
        }
    };
}

proposer!(First, "first");
proposer!(Second, "second");

macro_rules! owner {
    ($ty:ident, $name:literal, $rule:expr) => {
        #[derive(Debug)]
        struct $ty;

        impl Module for $ty {
            type Server = ();
            const NAME: &'static str = $name;
            type Wire = ();
            const OWNS_INGRESS: bool = true;

            fn resolve_ingress(proposals: &[Proposal<Meta>]) -> Option<Meta> {
                let rule: fn(&[Proposal<Meta>]) -> Option<Meta> = $rule;
                rule(proposals)
            }

            fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
                Self
            }
        }
    };
}

// `Meta` orders the smaller value as greater, so `max` is the smallest value.
owner!(Smallest, "smallest", |proposals| proposals
    .iter()
    .map(|proposal| proposal.value)
    .max());
owner!(Earliest, "earliest", |proposals| proposals
    .first()
    .map(|proposal| proposal.value));
// Reports the proposers' name lengths in the order the framework recorded them.
owner!(Names, "names", |proposals| {
    let lengths: String = proposals
        .iter()
        .map(|proposal| proposal.by.len().to_string())
        .collect();
    Some(Meta::new(lengths.parse().unwrap()))
});

/// The decision of a stack with a fresh server state.
fn decide<S: rpcstack::ModuleStack>(headers: &http::HeaderMap) -> Option<Ingress> {
    ServerContext::<S>::try_new("svc")
        .unwrap()
        .ingress(PATH, headers)
}

/// The `Meta` of an admitted request; panics on a rejection.
fn admitted<S: rpcstack::ModuleStack>(headers: &http::HeaderMap) -> Option<Meta> {
    match decide::<S>(headers) {
        Some(Ingress::Admit(meta)) => Some(meta),
        None => None,
        Some(Ingress::Reject(status)) => panic!("rejected: {status}"),
    }
}

fn rejected(decision: Option<Ingress>) -> Status {
    match decision {
        Some(Ingress::Reject(status)) => status,
        other => panic!("not rejected: {other:?}"),
    }
}

#[test]
fn the_owner_settles_the_proposals_by_its_own_rule() {
    let headers = headers_with("", &[("first", 30), ("second", 10)]);

    assert_eq!(
        admitted::<policy_stack![First, Second, Smallest]>(&headers),
        Some(Meta::new(10))
    );
    assert_eq!(
        admitted::<policy_stack![First, Second, Earliest]>(&headers),
        Some(Meta::new(30))
    );
    assert_eq!(
        admitted::<policy_stack![Second, First, Earliest]>(&headers),
        Some(Meta::new(10)),
        "proposals are in stack order"
    );
}

#[test]
fn the_owner_may_sit_anywhere_in_the_stack() {
    let headers = headers_with("", &[("first", 5)]);

    assert_eq!(
        admitted::<policy_stack![Earliest, First]>(&headers),
        Some(Meta::new(5))
    );
    assert_eq!(
        admitted::<policy_stack![First, Earliest]>(&headers),
        Some(Meta::new(5))
    );
}

#[test]
fn proposals_carry_the_proposing_modules_name() {
    let headers = headers_with("", &[("first", 1), ("second", 1)]);

    // "first" has 5 letters and "second" 6.
    assert_eq!(
        admitted::<policy_stack![First, Second, Names]>(&headers),
        Some(Meta::new(56))
    );
    assert_eq!(
        admitted::<policy_stack![Second, First, Names]>(&headers),
        Some(Meta::new(65))
    );
}

#[test]
fn without_proposals_or_an_owner_there_is_no_decision() {
    let headers = headers_with("", &[("first", 5)]);

    assert_eq!(admitted::<policy_stack![Second, Smallest]>(&headers), None);
    assert_eq!(admitted::<policy_stack![First, Second]>(&headers), None);
    assert_eq!(admitted::<policy_stack![]>(&headers), None);
}

#[test]
fn a_request_without_the_header_has_no_sections() {
    assert_eq!(
        admitted::<policy_stack![First, Smallest]>(&http::HeaderMap::new()),
        None
    );
}

#[test]
fn ingress_decodes_only_the_sections_modules_ask_for() {
    // A section that no module in the stack asks for is never decoded, even
    // though it is not valid wire data.
    let headers = headers_with("unrelated:!!not base64!!", &[("first", 7)]);

    assert_eq!(
        admitted::<policy_stack![First, Earliest]>(&headers),
        Some(Meta::new(7))
    );
}

#[test]
fn a_header_that_is_not_text_is_rejected_not_a_panic() {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        rpcstack::HEADER_NAME,
        http::HeaderValue::from_bytes(b"\xff\xfe").unwrap(),
    );

    let status = rejected(decide::<policy_stack![First, Earliest]>(&headers));

    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("invalid module wire data"));
}

#[test]
fn a_module_rejects_a_section_that_does_not_decode() {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        rpcstack::HEADER_NAME,
        "first:!!not base64!!".parse().unwrap(),
    );

    let status = rejected(decide::<policy_stack![First, Earliest]>(&headers));

    assert_eq!(status.code(), Code::InvalidArgument);
}

// ── Server state and rejection ──────────────────────────────────────────

/// Server-level state shared by every request of a service: how many requests
/// reached `ingress`.
#[derive(Debug, Default)]
struct Arrivals(AtomicU64);

impl ModuleServer for Arrivals {
    fn new(_init: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(Self::default())
    }
}

/// What a rejecting module reports to the sender.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Retry {
    after_ms: u64,
}

/// Turns away every third request, from nothing but its server state, and tells
/// the sender when to retry in its own wire section.
#[derive(Debug)]
struct EveryThird;

impl Module for EveryThird {
    type Server = Arrivals;
    const NAME: &'static str = "every-third";
    type Wire = Retry;

    fn ingress(server: &Arrivals, _wire: &WireIn<'_>, ingress: &mut StackIngress) {
        if (server.0.fetch_add(1, Ordering::Relaxed) + 1) % 3 == 0 {
            ingress.reject(Status::resource_exhausted("every third request"));
            ingress
                .wire_mut()
                .put::<Self>(&Retry { after_ms: 25 })
                .unwrap();
        }
    }

    fn new(_m: &CowGrpcMethod, _s: &Arrivals, _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
}

static RAN_AFTER: AtomicUsize = AtomicUsize::new(0);

/// Counts how often its `ingress` ran.
#[derive(Debug)]
struct Counting;

impl Module for Counting {
    type Server = ();
    const NAME: &'static str = "counting";
    type Wire = ();

    fn ingress(_server: &(), _wire: &WireIn<'_>, _ingress: &mut StackIngress) {
        RAN_AFTER.fetch_add(1, Ordering::Relaxed);
    }

    fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
        Self
    }
}

#[test]
fn a_module_rejects_from_its_server_state() {
    let server =
        ServerContext::<policy_stack![First, EveryThird, Earliest]>::try_new("svc").unwrap();
    let headers = headers_with("", &[("first", 4)]);

    for at in 0..6 {
        match (at % 3 == 2, server.ingress(PATH, &headers)) {
            (false, Some(Ingress::Admit(meta))) => assert_eq!(meta, Meta::new(4)),
            (true, Some(Ingress::Reject(status))) => {
                assert_eq!(status.code(), Code::ResourceExhausted);
                assert_eq!(status.message(), "every third request");
            }
            (_, other) => panic!("request {at}: {other:?}"),
        }
    }
}

#[test]
fn a_rejection_carries_the_wire_sections_the_module_put() {
    let server = ServerContext::<policy_stack![EveryThird]>::try_new("svc").unwrap();
    let headers = http::HeaderMap::new();
    server.ingress(PATH, &headers);
    server.ingress(PATH, &headers);

    let status = rejected(server.ingress(PATH, &headers));

    let wire = WireIn::from_metadata(status.metadata()).unwrap();
    assert_eq!(
        wire.get::<EveryThird>().unwrap(),
        Some(Retry { after_ms: 25 })
    );
}

#[test]
fn server_state_is_per_server_not_per_stack() {
    type Stack = policy_stack![EveryThird];
    let a = ServerContext::<Stack>::try_new("a").unwrap();
    let b = ServerContext::<Stack>::try_new("b").unwrap();
    let headers = http::HeaderMap::new();

    for (server, rejects) in [
        (&a, false),
        (&a, false),
        (&b, false),
        (&a, true),
        (&b, false),
    ] {
        let rejected = matches!(server.ingress(PATH, &headers), Some(Ingress::Reject(_)));
        assert_eq!(rejected, rejects);
    }
}

#[test]
fn a_rejection_ends_the_ingress_phase() {
    RAN_AFTER.store(0, Ordering::Relaxed);
    let server = ServerContext::<policy_stack![EveryThird, Counting]>::try_new("svc").unwrap();
    let headers = http::HeaderMap::new();

    server.ingress(PATH, &headers);
    server.ingress(PATH, &headers);
    rejected(server.ingress(PATH, &headers));

    assert_eq!(
        RAN_AFTER.load(Ordering::Relaxed),
        2,
        "the module after the rejecting one did not run for the rejected request"
    );
}

#[test]
fn a_rejection_wins_over_every_proposal() {
    let server =
        ServerContext::<policy_stack![First, EveryThird, Second, Earliest]>::try_new("svc")
            .unwrap();
    let headers = headers_with("", &[("first", 1), ("second", 2)]);
    server.ingress(PATH, &headers);
    server.ingress(PATH, &headers);

    let status = rejected(server.ingress(PATH, &headers));

    assert_eq!(status.code(), Code::ResourceExhausted);
}
