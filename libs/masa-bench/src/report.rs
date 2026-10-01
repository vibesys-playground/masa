//! Output of the benchmarks.
//!
//! Metrics go to stdout as one `key value` pair per line, with the key
//! `<benchmark>.<feature label>.<metric>` and a decimal value. Every metric is
//! "lower is better" (nanoseconds, bytes, counts). Human-readable text goes to
//! stderr so it never mixes with the parseable stream.

/// Print the metric `<bench>.<label>.<name>` with `value`.
pub fn metric(bench: &str, label: &str, name: &str, value: f64) {
    println!("{bench}.{label}.{name} {value:.2}");
}

/// Print a line of the human-readable summary.
pub fn note(text: &str) {
    eprintln!("{text}");
}
