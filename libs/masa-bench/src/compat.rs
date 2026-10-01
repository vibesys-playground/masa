//! The only place where benchmark code differs between refs.
//!
//! Everything else in the suite uses the `tonic::masa` hook traits and the
//! `masa` facade items that exist on every supported ref. What differs is how
//! a root request (one that enters the system from a client, with no parent
//! hook) gets its context:
//!
//! - current refs build a `masa::RootContext` from the context, add module
//!   wire data such as Rajomon tokens, and attach it to the request;
//! - `legacy-main` refs (before the rpcstack refactor, for example
//!   `origin/main` at 7ad21552b) put the tokens in the context and call
//!   `Request::set_masa_context`.
//!
//! To support a new ref whose API differs again, add a module below with the
//! same items (the `pub use` line selects it), gate it on a Cargo feature
//! such as `legacy-foo`, add the feature to `Cargo.toml`, and teach
//! `scripts/bench.sh` when to pass it (`detect_compat`). Keep the module to
//! the functions listed here; if a benchmark needs another API difference
//! isolated, grow this module rather than adding `cfg`s to a benchmark.

use tonic::Request;

/// End-to-end latency budget of the benchmark's requests, in microseconds.
pub const SLO_US: u64 = 1_000_000;

/// Rajomon token budget a root request carries under `ac_rajomon`.
#[cfg(feature = "ac_rajomon")]
const RAJOMON_TOKENS: u64 = 1000;

/// A root request for method `Front` with request id `id`: the client-side
/// work of creating a context and writing it into the request's headers.
pub fn root_request(id: u64) -> Request<()> {
    imp::root_request(id)
}

#[cfg(not(feature = "legacy-main"))]
mod imp {
    #[cfg(feature = "ac_rajomon")]
    use super::RAJOMON_TOKENS;
    use super::SLO_US;
    use tonic::Request;

    pub fn root_request(id: u64) -> Request<()> {
        let now = masa::time_now();
        let context = masa::ContextBuilder::new("Front", id)
            .slo(SLO_US)
            .gateway_entry(now)
            .deadline(now + SLO_US)
            .build();
        let root = masa::RootContext::from(context);
        #[cfg(feature = "ac_rajomon")]
        let root = root.with_rajomon_tokens(RAJOMON_TOKENS);
        root.attach(Request::new(()))
    }
}

#[cfg(feature = "legacy-main")]
mod imp {
    #[cfg(feature = "ac_rajomon")]
    use super::RAJOMON_TOKENS;
    use super::SLO_US;
    use masa::MasaRequestExt;
    use tonic::Request;

    pub fn root_request(id: u64) -> Request<()> {
        let now = masa::time_now();
        let builder = masa::ContextBuilder::new("Front", id)
            .slo(SLO_US)
            .gateway_entry(now)
            .deadline(now + SLO_US);
        #[cfg(feature = "ac_rajomon")]
        let builder = builder.tokens(RAJOMON_TOKENS);
        let mut request = Request::new(());
        request.set_masa_context(&builder.build());
        request
    }
}
