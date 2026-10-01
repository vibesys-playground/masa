// What Masa's default stack decides at ingress, before a request's task is
// queued: the budget module owns the decision and proposes the priority the
// sender wrote, so the task is queued exactly as it was before ingress hooks
// existed.

use masa_core::{Context, PriorityHint};
use masa_policy::PolicyHooks;
use tonic::masa::{Hooks, Meta};

fn headers_with(ctx: &Context) -> http::HeaderMap {
    let mut headers = http::HeaderMap::new();
    headers.insert("ctx", ctx.to_header_string().parse().unwrap());
    headers
}

fn ingress(headers: &http::HeaderMap) -> Option<Meta> {
    <PolicyHooks as Hooks>::ingress(headers)
}

#[test]
fn the_task_is_queued_with_the_priority_the_sender_wrote() {
    for priority in [1, 42, 1_700_000_000_000_000, u64::MAX] {
        let ctx = Context::new("svc/Rpc", 9, 100, 10, 110, PriorityHint::new(priority));

        assert_eq!(ingress(&headers_with(&ctx)), Some(Meta::new(priority)));
    }
}

#[test]
fn the_priority_is_independent_of_the_deadline_and_the_other_fields() {
    let ctx = Context::new("svc/Rpc", 9, 100, 10, 5_000, PriorityHint::new(7));

    assert_eq!(ingress(&headers_with(&ctx)), Some(Meta::new(7)));
}

#[test]
fn priority_zero_is_passed_through_unchanged() {
    // What zero means (infrastructure work, which runs first) is decided by the
    // scheduler's default policy, not here.
    let ctx = Context::new("svc/Rpc", 9, 0, 0, 0, PriorityHint::infra());

    assert_eq!(ingress(&headers_with(&ctx)), Some(Meta::new(0)));
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

    assert_eq!(ingress(&headers), Some(Meta::new(42)));
}

#[test]
#[should_panic(expected = "has no `budget` section")]
fn a_request_without_a_budget_section_is_a_misconfigured_sender() {
    let _ = ingress(&http::HeaderMap::new());
}

#[test]
#[should_panic(expected = "invalid `budget` section")]
fn a_malformed_budget_section_is_a_misconfigured_sender() {
    let mut headers = http::HeaderMap::new();
    headers.insert("ctx", "budget:not-base64".parse().unwrap());

    let _ = ingress(&headers);
}
