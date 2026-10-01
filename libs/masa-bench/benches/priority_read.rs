//! Hyper's per-stream priority read.
//!
//! For every incoming HTTP/2 stream the vendored Hyper reads the request's
//! priority from the `ctx` header before it spawns the handler task
//! (`libs/hyper/src/common/exec.rs`). The function it calls is
//! `masa::read_priority_from_headers`. This benchmark times that call, and for
//! comparison the full context decode, on the header a root request carries
//! under the selected features.
//!
//! Usage: `priority_read [calls per run]` (default 2000000; best of 7 runs).

use std::hint::black_box;

use masa_bench::compat::root_request;
use masa_bench::report::{metric, note};
use masa_bench::{feature_label, header_bytes, min_ns_per_call};

fn main() {
    let calls: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let label = feature_label();
    let request = root_request(42);
    let headers = request.metadata().clone().into_headers();
    let header_len = header_bytes(request.metadata());

    let priority_ns = min_ns_per_call(7, calls, || {
        black_box(masa::read_priority_from_headers(black_box(&headers)));
    });
    let context_ns = min_ns_per_call(7, calls / 5, || {
        black_box(masa::read_context_from_headers(black_box(&headers)));
    });

    metric("priority_read", &label, "header_bytes", header_len as f64);
    metric("priority_read", &label, "read_priority_ns", priority_ns);
    metric("priority_read", &label, "read_context_ns", context_ns);
    note(&format!(
        "priority_read [{label}] header {header_len} bytes: priority {priority_ns:.1} ns, full context {context_ns:.1} ns"
    ));
}
