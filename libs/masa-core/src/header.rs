use crate::context::{PriorityPeek, BUDGET_SECTION};
use crate::{wire, Context, PriorityHint, MASA_CONTEXT_HEADER};

fn header_value(headers: &http::HeaderMap) -> &str {
    let ctx = headers
        .get(MASA_CONTEXT_HEADER)
        .unwrap_or_else(|| panic!("{}", crate::MISSING_CONTEXT_HEADER_MESSAGE));
    ctx.to_str()
        .unwrap_or_else(|err| panic!("{}", crate::invalid_context_header_metadata_message(err)))
}

/// Read the MASA context from HTTP headers.
pub fn read_context_from_headers(headers: &http::HeaderMap) -> Context {
    Context::from_header_string(header_value(headers))
}

/// Read the MASA context from HTTP request headers.
pub fn read_context<B>(req: &http::Request<B>) -> Context {
    read_context_from_headers(req.headers())
}

/// Read the priority hint from MASA context HTTP headers.
///
/// Decodes the priority of the budget section only; Hyper calls this for every
/// stream, before any policy module has run.
pub fn read_priority_from_headers(headers: &http::HeaderMap) -> PriorityHint {
    let payload = wire::find_section(header_value(headers), BUDGET_SECTION)
        .unwrap_or_else(|| panic!("{}", crate::missing_budget_section_message()));
    wire::decode_payload::<PriorityPeek>(payload)
        .unwrap_or_else(|err| panic!("{}", crate::invalid_budget_section_message(err)))
        .5
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    #[should_panic(expected = "missing MASA context header `ctx`")]
    fn read_context_panics_with_explicit_message_when_missing() {
        let req = http::Request::new(());

        let _ = read_context(&req);
    }

    #[test]
    #[should_panic(expected = "invalid MASA context header `ctx`: invalid ASCII/metadata")]
    fn read_context_panics_with_explicit_message_for_invalid_ascii() {
        let mut req = http::Request::new(());
        req.headers_mut().insert(
            MASA_CONTEXT_HEADER,
            HeaderValue::from_bytes(b"\xff").unwrap(),
        );

        let _ = read_context(&req);
    }

    #[test]
    #[should_panic(expected = "invalid `budget` section")]
    fn read_context_panics_with_explicit_message_for_invalid_section() {
        let mut req = http::Request::new(());
        req.headers_mut().insert(
            MASA_CONTEXT_HEADER,
            HeaderValue::from_static("budget:not-base64"),
        );

        let _ = read_context(&req);
    }

    #[test]
    #[should_panic(expected = "has no `budget` section")]
    fn read_context_panics_with_explicit_message_when_budget_section_is_missing() {
        let mut req = http::Request::new(());
        req.headers_mut()
            .insert(MASA_CONTEXT_HEADER, HeaderValue::from_static("other:AA=="));

        let _ = read_context(&req);
    }

    #[test]
    fn read_context_preserves_valid_context() {
        let ctx = Context::new("test.Service", 7, 100, 10, 110, PriorityHint::new(110));
        let req = http::Request::builder()
            .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
            .body(())
            .unwrap();

        let decoded = read_context(&req);

        assert_eq!(decoded.api(), ctx.api());
        assert_eq!(decoded.request_id(), ctx.request_id());
        assert_eq!(decoded.deadline(), ctx.deadline());
    }

    #[test]
    #[should_panic(expected = "missing MASA context header `ctx`")]
    fn read_context_from_headers_panics_with_explicit_message_when_missing() {
        let headers = http::HeaderMap::new();

        let _ = read_context_from_headers(&headers);
    }

    #[test]
    #[should_panic(expected = "invalid MASA context header `ctx`: invalid ASCII/metadata")]
    fn read_context_from_headers_panics_with_explicit_message_for_invalid_ascii() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            MASA_CONTEXT_HEADER,
            HeaderValue::from_bytes(b"\xff").unwrap(),
        );

        let _ = read_context_from_headers(&headers);
    }

    #[test]
    fn read_priority_from_headers_preserves_priority_hint() {
        let ctx = Context::new("test.Service", 9, 100, 10, 110, PriorityHint::new(42));
        let mut headers = http::HeaderMap::new();
        headers.insert(
            MASA_CONTEXT_HEADER,
            HeaderValue::from_str(&ctx.to_header_string()).unwrap(),
        );

        assert_eq!(read_priority_from_headers(&headers), PriorityHint::new(42));
    }
}
