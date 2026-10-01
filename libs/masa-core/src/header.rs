use crate::context::{peek_priority, BUDGET_SECTION};
use crate::{wire, Context, PriorityHint, MASA_CONTEXT_HEADER};

fn header_value(headers: &http::HeaderMap) -> &str {
    let ctx = headers
        .get(MASA_CONTEXT_HEADER)
        .unwrap_or_else(|| panic!("{}", crate::MISSING_CONTEXT_HEADER_MESSAGE));
    // `HeaderValue::to_str` checks each byte for visible ASCII, which costs more
    // than everything else this does with the header. A header value holds no
    // control characters other than tab, so ASCII means `to_str` would succeed;
    // anything else takes it, for the same error as before.
    let bytes = ctx.as_bytes();
    if bytes.is_ascii() {
        if let Ok(text) = std::str::from_utf8(bytes) {
            return text;
        }
    }
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
/// Reads only the priority of the budget section, without decoding the rest or
/// allocating (see [`peek_priority`]); Hyper calls this for every stream,
/// before any policy module has run.
pub fn read_priority_from_headers(headers: &http::HeaderMap) -> PriorityHint {
    let payload = wire::find_section(header_value(headers), BUDGET_SECTION)
        .unwrap_or_else(|| panic!("{}", crate::missing_budget_section_message()));
    peek_priority(payload)
        .unwrap_or_else(|err| panic!("{}", crate::invalid_budget_section_message(err)))
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

    fn headers_with(value: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert(MASA_CONTEXT_HEADER, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn read_priority_agrees_with_the_full_context_for_many_values() {
        let mut state = 7u64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> (state % 40)
        };
        for _ in 0..2_000 {
            let ctx = Context::new(
                "hotel.Search/Rpc",
                next(),
                next(),
                next(),
                next(),
                PriorityHint::new(next()),
            );
            // Another module's section may come before the budget section.
            for value in [
                ctx.to_header_string(),
                format!("other:AAAA.{}", ctx.to_header_string()),
            ] {
                let headers = headers_with(&value);
                assert_eq!(read_priority_from_headers(&headers), ctx.prio_hint());
                assert_eq!(read_context_from_headers(&headers), ctx);
            }
        }
    }

    #[test]
    #[should_panic(expected = "missing MASA context header `ctx`")]
    fn read_priority_panics_with_explicit_message_when_missing() {
        let _ = read_priority_from_headers(&http::HeaderMap::new());
    }

    #[test]
    #[should_panic(expected = "invalid MASA context header `ctx`: invalid ASCII/metadata")]
    fn read_priority_panics_with_explicit_message_for_invalid_ascii() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            MASA_CONTEXT_HEADER,
            HeaderValue::from_bytes(b"budget:\xff").unwrap(),
        );

        let _ = read_priority_from_headers(&headers);
    }

    #[test]
    #[should_panic(expected = "has no `budget` section")]
    fn read_priority_panics_with_explicit_message_when_the_budget_section_is_missing() {
        let _ = read_priority_from_headers(&headers_with("other:AA=="));
    }

    fn section_of(bytes: &[u8]) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn read_priority_panics_with_the_message_the_general_decoder_gives_for_a_malformed_section() {
        let name = [3u8, b'a', b'p', b'i'];
        let sections = [
            "not-base64".to_owned(),
            "AAAA".to_owned(),
            "AAA".to_owned(),
            section_of(&[]),
            section_of(&[1, 2, 3]),
            // Cut short.
            section_of(&[&name[..], &[1, 2, 3, 4]].concat()),
            // A marker that is not a u64, a name that is not UTF-8, trailing bytes.
            section_of(&[&name[..], &[1, 2, 3, 4, 254]].concat()),
            section_of(&[3, 0xff, 0xfe, 0xfd, 1, 2, 3, 4, 5]),
            section_of(&[&name[..], &[1, 2, 3, 4, 5, 6]].concat()),
        ];
        for section in sections {
            let before = wire::decode_payload::<Context>(&section).map(|ctx| ctx.prio_hint());
            let headers = headers_with(&format!("budget:{section}"));

            let result = std::panic::catch_unwind(|| read_priority_from_headers(&headers));

            match (before, result) {
                (Ok(priority), Ok(read)) => assert_eq!(read, priority, "{section}"),
                (Err(err), Err(panic)) => assert_eq!(
                    panic.downcast_ref::<String>().unwrap(),
                    &crate::invalid_budget_section_message(err),
                    "{section}"
                ),
                (before, result) => {
                    panic!(
                        "{section}: general decoder ok {:?}, scan ok {:?}",
                        before.is_ok(),
                        result.is_ok()
                    )
                }
            }
        }
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
