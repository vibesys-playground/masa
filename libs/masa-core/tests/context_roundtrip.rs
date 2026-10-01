use masa_core::{Context, PriorityHint};

fn sample_context() -> Context {
    Context::new(
        "hotel.SearchHotels",
        42,
        500_000,
        1_000_000,
        1_500_000,
        PriorityHint::new(1_250_000),
    )
}

#[test]
fn header_round_trip_preserves_all_fields() {
    let ctx = sample_context();

    let encoded = ctx.to_header_string();
    assert!(!encoded.is_empty());

    let decoded = Context::from_header_string(&encoded);

    assert_eq!(decoded.api(), ctx.api());
    assert_eq!(decoded.request_id(), ctx.request_id());
    assert_eq!(decoded.slo(), ctx.slo());
    assert_eq!(decoded.gateway_entry(), ctx.gateway_entry());
    assert_eq!(decoded.deadline(), ctx.deadline());
    assert_eq!(decoded.prio_hint(), ctx.prio_hint());
}

#[test]
fn header_carries_only_the_budget_section() {
    let encoded = sample_context().to_header_string();

    assert!(encoded.starts_with("budget:"));
    assert_eq!(encoded.matches('.').count(), 0);
}

#[test]
fn reading_the_context_ignores_other_sections() {
    let ctx = sample_context();
    let encoded = format!("other:!!not base64!!.{}", ctx.to_header_string());

    assert_eq!(Context::from_header_string(&encoded), ctx);
}

#[test]
#[should_panic(expected = "invalid `budget` section")]
fn malformed_section_reports_invalid_base64() {
    let _ = Context::from_header_string("budget:not-base64");
}

#[test]
#[should_panic(expected = "invalid `budget` section")]
fn malformed_section_reports_invalid_payload() {
    let _ = Context::from_header_string("budget:AA==");
}
