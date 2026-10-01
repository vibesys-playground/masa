// The ingress decision: before a request's task is queued, the stack's modules
// propose a `Meta` from the inbound wire, and the one module that owns the
// decision settles it. Shown with toy modules; the framework gives no meaning
// to the numbers and has no combination rule: each owner below applies its own.

use rpcstack::{policy_stack, Extensions, Ingress, Module, Proposal, WireIn, WireOut};
use rpcstack_tonic::PolicyHooks;
use serde::{Deserialize, Serialize};
use tonic::masa::{Hooks, Meta};
use tonic::CowGrpcMethod;

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

/// Proposes the number in its own section, if the sender attached one.
macro_rules! proposer {
    ($ty:ident, $name:literal) => {
        #[derive(Debug)]
        struct $ty;

        impl Module for $ty {
            type Server = ();
            const NAME: &'static str = $name;
            type Wire = Want;

            fn ingress(wire: &WireIn<'_>, ingress: &mut Ingress) {
                if let Some(Want(value)) = wire.get::<Self>().unwrap_or_else(|err| panic!("{err}"))
                {
                    ingress.propose(Meta::new(value));
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

fn ingress<S: rpcstack::ModuleStack>(headers: &http::HeaderMap) -> Option<Meta> {
    <PolicyHooks<S> as Hooks>::ingress(headers)
}

#[test]
fn the_owner_settles_the_proposals_by_its_own_rule() {
    let headers = headers_with("", &[("first", 30), ("second", 10)]);

    assert_eq!(
        ingress::<policy_stack![First, Second, Smallest]>(&headers),
        Some(Meta::new(10))
    );
    assert_eq!(
        ingress::<policy_stack![First, Second, Earliest]>(&headers),
        Some(Meta::new(30))
    );
    assert_eq!(
        ingress::<policy_stack![Second, First, Earliest]>(&headers),
        Some(Meta::new(10)),
        "proposals are in stack order"
    );
}

#[test]
fn the_owner_may_sit_anywhere_in_the_stack() {
    let headers = headers_with("", &[("first", 5)]);

    assert_eq!(
        ingress::<policy_stack![Earliest, First]>(&headers),
        Some(Meta::new(5))
    );
    assert_eq!(
        ingress::<policy_stack![First, Earliest]>(&headers),
        Some(Meta::new(5))
    );
}

#[test]
fn proposals_carry_the_proposing_modules_name() {
    let headers = headers_with("", &[("first", 1), ("second", 1)]);

    // "first" has 5 letters and "second" 6.
    assert_eq!(
        ingress::<policy_stack![First, Second, Names]>(&headers),
        Some(Meta::new(56))
    );
    assert_eq!(
        ingress::<policy_stack![Second, First, Names]>(&headers),
        Some(Meta::new(65))
    );
}

#[test]
fn without_proposals_or_an_owner_there_is_no_decision() {
    let headers = headers_with("", &[("first", 5)]);

    assert_eq!(ingress::<policy_stack![Second, Smallest]>(&headers), None);
    assert_eq!(ingress::<policy_stack![First, Second]>(&headers), None);
    assert_eq!(ingress::<policy_stack![]>(&headers), None);
}

#[test]
fn a_request_without_the_header_has_no_sections() {
    assert_eq!(
        ingress::<policy_stack![First, Smallest]>(&http::HeaderMap::new()),
        None
    );
}

#[test]
fn ingress_decodes_only_the_sections_modules_ask_for() {
    // A section that no module in the stack asks for is never decoded, even
    // though it is not valid wire data.
    let headers = headers_with("unrelated:!!not base64!!", &[("first", 7)]);

    assert_eq!(
        ingress::<policy_stack![First, Earliest]>(&headers),
        Some(Meta::new(7))
    );
}

#[test]
#[should_panic(expected = "invalid module wire data")]
fn malformed_wire_data_of_a_module_in_the_stack_panics_as_in_new() {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        rpcstack::HEADER_NAME,
        "first:!!not base64!!".parse().unwrap(),
    );

    let _ = ingress::<policy_stack![First, Earliest]>(&headers);
}
