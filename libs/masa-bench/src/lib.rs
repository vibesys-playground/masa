//! Shared pieces of the Masa benchmark suite.
//!
//! Every benchmark is a `harness = false` binary under `benches/`. It prints
//! `key value` lines on stdout (see [`report`]), which `scripts/bench_compare.py`
//! parses, and a human-readable summary on stderr.
//!
//! The benchmarks touch the stack only through items that exist both before
//! and after the rpcstack refactor: the `tonic::masa` hook traits and the
//! `masa` facade. The one thing that differs between refs, how a root request
//! gets its context, lives in [`compat`].

pub mod alloc;
pub mod compat;
pub mod report;

use std::time::Instant;

/// The enabled benchmark features in a fixed order, joined by `,`. It names
/// the configuration in output keys, so two builds of different refs with the
/// same features produce the same keys.
pub fn feature_label() -> String {
    let enabled: &[(&str, bool)] = &[
        ("sched_slo", cfg!(feature = "sched_slo")),
        ("sched_pred", cfg!(feature = "sched_pred")),
        ("abort_slo", cfg!(feature = "abort_slo")),
        ("abort_slack", cfg!(feature = "abort_slack")),
        ("ac_pred", cfg!(feature = "ac_pred")),
        ("ac_rajomon", cfg!(feature = "ac_rajomon")),
        ("est_mean_var", cfg!(feature = "est_mean_var")),
        ("trace_queue_latency", cfg!(feature = "trace_queue_latency")),
    ];
    let names: Vec<&str> = enabled
        .iter()
        .filter(|(_, on)| *on)
        .map(|(name, _)| *name)
        .collect();
    if names.is_empty() {
        "none".to_owned()
    } else {
        names.join(",")
    }
}

/// The HTTP request hyper would hand to the server hooks for a call carrying
/// `metadata`: only its headers matter to the hooks.
pub fn http_request(metadata: &tonic::metadata::MetadataMap) -> http::Request<()> {
    let mut request = http::Request::new(());
    *request.headers_mut() = metadata.clone().into_headers();
    request
}

/// Bytes the headers of `metadata` take on the wire (names plus values).
pub fn header_bytes(metadata: &tonic::metadata::MetadataMap) -> usize {
    metadata
        .clone()
        .into_headers()
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len())
        .sum()
}

/// The fastest of `runs` measurements of `iters` calls of `f`, in nanoseconds
/// per call. The minimum is the estimate least disturbed by other load on the
/// machine. `f` runs `iters / runs` times unmeasured first to warm caches.
pub fn min_ns_per_call(runs: u32, iters: u64, mut f: impl FnMut()) -> f64 {
    for _ in 0..iters / u64::from(runs) {
        f();
    }
    let mut best = f64::MAX;
    for _ in 0..runs {
        let start = Instant::now();
        for _ in 0..iters {
            f();
        }
        best = best.min(start.elapsed().as_nanos() as f64 / iters as f64);
    }
    best
}
