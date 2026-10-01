//! Encode and decode of the `ctx` header, the Masa wire format.
//!
//! Every hop encodes a header when the caller sends a request and decodes it
//! when the callee receives it. This benchmark times the pieces on the header
//! a root request carries under the selected features:
//!
//! - `encode`: create a context and write it into a request's headers;
//! - `to_headers`: copy the request's metadata into an HTTP header map, which
//!   is what transport does on send;
//! - `decode_full`: decode the whole context from the header map;
//! - `decode_one_section`: read only the priority, which sits in one section
//!   of the header and must not pay for decoding the others;
//! - `hop`: `encode` plus `to_headers` plus `decode_full`.
//!
//! Usage: `wire_codec [calls per run]` (default 1000000; best of 7 runs).

use std::hint::black_box;

use masa_bench::compat::root_request;
use masa_bench::report::{metric, note};
use masa_bench::{feature_label, min_ns_per_call};

fn main() {
    let calls: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let label = feature_label();
    let mut id = 0u64;

    let encode_ns = min_ns_per_call(7, calls, || {
        id += 1;
        black_box(root_request(black_box(id)));
    });

    let request = root_request(42);
    let to_headers_ns = min_ns_per_call(7, calls, || {
        black_box(black_box(request.metadata()).clone().into_headers());
    });

    let headers = request.metadata().clone().into_headers();
    let decode_full_ns = min_ns_per_call(7, calls, || {
        black_box(masa::read_context_from_headers(black_box(&headers)));
    });
    let decode_one_section_ns = min_ns_per_call(7, calls, || {
        black_box(masa::read_priority_from_headers(black_box(&headers)));
    });

    let hop_ns = min_ns_per_call(7, calls, || {
        id += 1;
        let request = root_request(id);
        let headers = request.metadata().clone().into_headers();
        black_box(masa::read_context_from_headers(&headers));
    });

    for (name, ns) in [
        ("encode_ns", encode_ns),
        ("to_headers_ns", to_headers_ns),
        ("decode_full_ns", decode_full_ns),
        ("decode_one_section_ns", decode_one_section_ns),
        ("hop_ns", hop_ns),
    ] {
        metric("wire_codec", &label, name, ns);
    }
    note(&format!(
        "wire_codec [{label}] encode {encode_ns:.0} ns, to_headers {to_headers_ns:.0} ns, decode_full {decode_full_ns:.0} ns, decode_one_section {decode_one_section_ns:.0} ns, hop {hop_ns:.0} ns"
    ));
}
