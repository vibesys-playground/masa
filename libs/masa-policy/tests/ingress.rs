// What Masa's default stack decides at ingress, before a request's task is
// queued: the budget module owns the decision and proposes the priority the
// sender wrote, so the task is queued exactly as it was before ingress hooks
// existed. A request without a valid budget section is rejected.

use masa_core::{Context, PriorityHint};
use masa_policy::ServerContext;
use tonic::masa::{Ingress, Meta, ServerHooks};
use tonic::{Code, Status};

const PATH: &str = "/svc.Service/Rpc";

fn headers_with(ctx: &Context) -> http::HeaderMap {
    let mut headers = http::HeaderMap::new();
    headers.insert("ctx", ctx.to_header_string().parse().unwrap());
    headers
}

fn decide(headers: &http::HeaderMap) -> Option<Ingress> {
    <ServerContext as ServerHooks>::new("svc").ingress(PATH, headers)
}

fn queued_with(headers: &http::HeaderMap) -> Meta {
    match decide(headers) {
        Some(Ingress::Admit(meta)) => meta,
        other => panic!("not admitted: {other:?}"),
    }
}

fn rejected(headers: &http::HeaderMap) -> Status {
    match decide(headers) {
        Some(Ingress::Reject(status)) => status,
        other => panic!("not rejected: {other:?}"),
    }
}

#[test]
fn the_task_is_queued_with_the_priority_the_sender_wrote() {
    for priority in [1, 42, 1_700_000_000_000_000, u64::MAX] {
        let ctx = Context::new("svc/Rpc", 9, 100, 10, 110, PriorityHint::new(priority));

        assert_eq!(queued_with(&headers_with(&ctx)), Meta::new(priority));
    }
}

#[test]
fn the_priority_is_independent_of_the_deadline_and_the_other_fields() {
    let ctx = Context::new("svc/Rpc", 9, 100, 10, 5_000, PriorityHint::new(7));

    assert_eq!(queued_with(&headers_with(&ctx)), Meta::new(7));
}

#[test]
fn priority_zero_is_passed_through_unchanged() {
    // What zero means (infrastructure work, which runs first) is decided by the
    // scheduler's default policy, not here.
    let ctx = Context::new("svc/Rpc", 9, 0, 0, 0, PriorityHint::infra());

    assert_eq!(queued_with(&headers_with(&ctx)), Meta::new(0));
}

#[test]
fn other_modules_sections_are_not_decoded() {
    let ctx = Context::new("svc/Rpc", 9, 100, 10, 110, PriorityHint::new(42));
    let mut headers = http::HeaderMap::new();
    headers.insert(
        "ctx",
        format!("junk:!!not base64!!.{}", ctx.to_header_string())
            .parse()
            .unwrap(),
    );

    assert_eq!(queued_with(&headers), Meta::new(42));
}

#[test]
fn a_request_without_a_budget_section_is_rejected_as_a_misconfigured_sender() {
    let mut other_section = http::HeaderMap::new();
    other_section.insert("ctx", "other:AA==".parse().unwrap());

    for headers in [http::HeaderMap::new(), other_section] {
        let status = rejected(&headers);

        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("has no `budget` section"));
    }
}

#[test]
fn a_malformed_budget_section_is_rejected_as_a_misconfigured_sender() {
    for section in ["budget:not-base64", "budget:AAAA", "budget:IjEi"] {
        let mut headers = http::HeaderMap::new();
        headers.insert("ctx", section.parse().unwrap());

        let status = rejected(&headers);

        assert_eq!(status.code(), Code::InvalidArgument, "{section}");
        assert!(
            status.message().contains("invalid"),
            "{section}: {}",
            status.message()
        );
    }
}
