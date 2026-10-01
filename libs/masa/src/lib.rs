pub use masa_core::{
    time_now, Context, FutureSpan, LatencyDistribution, MethodId, PriorityHint,
    ORACLE_CHILD_WORK_US_HEADER, ORACLE_REMAINING_AFTER_US_HEADER,
};
pub use masa_policy::{
    get_masa_context_from_metadata, get_method_name_override_from_headers,
    get_method_name_override_from_metadata, get_service_name_override_from_headers,
    get_service_name_override_from_metadata, read_context, read_context_from_headers,
    set_masa_context_in_metadata, set_method_name_override_in_headers,
    set_service_name_override_in_headers, ContextBuilder, MasaRequestExt, MasaResponseExt,
    MasaStatusExt, WireError, WireIn, WireOut, MASA_CONTEXT_HEADER,
};

#[cfg(feature = "trace_queue_latency")]
pub use masa_policy::QueueLatencyWire;

#[cfg(feature = "estimator")]
pub use masa_policy::{EstimationRequestWire, EstimationResponseWire, EstimationWire, RootMethod};

#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
pub use masa_policy::RajomonWire;

/// The estimation module's wire data (hop count, root method, response
/// metadata) in `metadata`, if any.
#[cfg(feature = "estimator")]
pub fn estimation_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<EstimationWire> {
    masa_policy::get_wire_from_metadata::<masa_policy::modules::EstimationModule>(metadata)
}

/// Set the estimation module's wire data in `metadata`, keeping other modules' data.
#[cfg(feature = "estimator")]
pub fn set_estimation_in_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    wire: &EstimationWire,
) {
    masa_policy::set_wire_in_metadata::<masa_policy::modules::EstimationModule>(metadata, wire);
}

/// Set the queue-latency module's wire data in `metadata`, keeping other modules' data.
#[cfg(feature = "trace_queue_latency")]
pub fn set_queue_latencies_in_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    wire: &QueueLatencyWire,
) {
    masa_policy::set_wire_in_metadata::<masa_policy::modules::QueueLatencyModule>(metadata, wire);
}

/// Rajomon's wire data (request tokens or response price) in `metadata`, if any.
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
pub fn rajomon_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<RajomonWire> {
    masa_policy::get_wire_from_metadata::<masa_policy::modules::RajomonModule>(metadata)
}

/// Set Rajomon's wire data in `metadata`, keeping other modules' data.
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
pub fn set_rajomon_in_metadata(metadata: &mut tonic::metadata::MetadataMap, wire: &RajomonWire) {
    masa_policy::set_wire_in_metadata::<masa_policy::modules::RajomonModule>(metadata, wire);
}

/// The queue latencies the call tree below a response reported, read from the
/// response's (or error status's) metadata. `None` if the sender attached none.
#[cfg(feature = "trace_queue_latency")]
pub fn queue_latencies_from_metadata(
    metadata: &tonic::metadata::MetadataMap,
) -> Option<QueueLatencyWire> {
    masa_policy::get_wire_from_metadata::<masa_policy::modules::QueueLatencyModule>(metadata)
}

use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(0);

/// The default Hooks implementation, selected at compile time by Masa features.
///
/// No scheduling features select `NoopHooks` (zero overhead). Any scheduling
/// feature selects `masa_policy::PolicyHooks` with full scheduling hooks, and
/// `stack_custom` swaps its stack for `masa_policy::AgentStack`. `sched_custom`
/// does not change the hooks; it swaps Tokio's run queue for
/// `rpcstack_sched::custom::Queue`.
#[cfg(not(any(
    feature = "sched_fifo",
    feature = "sched_slo",
    feature = "sched_tailclipper",
    feature = "sched_oracle"
)))]
pub type DefaultHooks = tonic::masa::noop::NoopHooks;

/// The default Hooks implementation, selected at compile time by Masa features.
#[cfg(all(
    any(
        feature = "sched_fifo",
        feature = "sched_slo",
        feature = "sched_tailclipper",
        feature = "sched_oracle"
    ),
    not(feature = "stack_custom")
))]
pub type DefaultHooks = masa_policy::PolicyHooks;

/// The default Hooks implementation, selected at compile time by Masa features.
#[cfg(all(
    any(
        feature = "sched_fifo",
        feature = "sched_slo",
        feature = "sched_tailclipper",
        feature = "sched_oracle"
    ),
    feature = "stack_custom"
))]
pub type DefaultHooks = masa_policy::PolicyHooks<masa_policy::AgentStack>;

// Without a scheduling feature, Hyper spawns handlers without priorities and
// Tokio ignores them, so the agent stack would never run.
#[cfg(all(
    feature = "stack_custom",
    not(any(
        feature = "sched_fifo",
        feature = "sched_slo",
        feature = "sched_tailclipper",
        feature = "sched_oracle"
    ))
))]
compile_error!("`stack_custom` requires a scheduling feature (e.g. `sched_slo`)");

// `sched_custom` only swaps the run queue. Hyper passes priorities to Tokio
// only under a scheduling feature, so without one the custom queue would
// receive tasks that carry no priority.
#[cfg(all(
    feature = "sched_custom",
    not(any(
        feature = "sched_fifo",
        feature = "sched_slo",
        feature = "sched_tailclipper",
        feature = "sched_oracle"
    ))
))]
compile_error!("`sched_custom` requires a scheduling feature (e.g. `sched_slo`)");

pub mod transport;

/// Utility function to create a Masa Context.
///
/// This handles:
/// 1. Generating a unique request ID (process-local)
/// 2. Capturing the current time as start time
/// 3. Calculating deadline based on SLO
/// 4. Deriving priority hint from the compile-time scheduling policy
pub fn create_context(api: &str, slo: Duration) -> Context {
    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let slo_us = slo.as_micros() as u64;
    let start_at = time_now();
    let deadline = start_at + slo_us;

    ContextBuilder::new(api, request_id)
        .slo(slo_us)
        .gateway_entry(start_at)
        .deadline(deadline)
        .build()
}

/// A root request's context together with the module wire data its sender
/// attaches (for example Rajomon's tokens). The context is the budget module's
/// wire data; both travel in the `ctx` header.
#[derive(Debug, Clone)]
pub struct RootContext {
    context: Context,
    wire: WireOut,
}

impl From<Context> for RootContext {
    fn from(context: Context) -> Self {
        Self {
            context,
            wire: WireOut::new(),
        }
    }
}

impl Deref for RootContext {
    type Target = Context;

    fn deref(&self) -> &Context {
        &self.context
    }
}

impl RootContext {
    /// The context without the wire data.
    pub fn into_context(self) -> Context {
        self.context
    }

    /// The wire data to attach to the request.
    pub fn wire_mut(&mut self) -> &mut WireOut {
        &mut self.wire
    }

    /// Set the Rajomon token budget this request carries.
    #[cfg(feature = "ac_rajomon")]
    pub fn with_rajomon_tokens(mut self, tokens: u64) -> Self {
        self.wire
            .put::<masa_policy::modules::RajomonModule>(&masa_policy::RajomonWire::request(tokens))
            .unwrap_or_else(|err| panic!("{err}"));
        self
    }

    /// Mark the request as sent `hop_count` hops below ingress, entering at `root_method`.
    #[cfg(feature = "estimator")]
    pub fn with_estimation_request(
        mut self,
        hop_count: u8,
        root_method: Option<RootMethod>,
    ) -> Self {
        self.wire
            .put::<masa_policy::modules::EstimationModule>(&EstimationWire::request(
                hop_count,
                root_method,
            ))
            .unwrap_or_else(|err| panic!("{err}"));
        self
    }

    /// Attach the context and wire data to `request`.
    pub fn attach<T>(&self, mut request: tonic::Request<T>) -> tonic::Request<T> {
        let mut wire = self.wire.clone();
        wire.put::<masa_policy::BudgetModule>(&self.context)
            .unwrap_or_else(|err| panic!("{err}"));
        wire.install(request.metadata_mut());
        request
    }
}

/// Try to create a Masa context, checking the client-side Rajomon token bucket first.
/// Returns None if the client-side rate limiter rejects the request.
#[cfg(feature = "ac_rajomon")]
pub fn try_create_context(api: &str, slo: std::time::Duration) -> Option<RootContext> {
    use masa_policy::CLIENT_TOKEN_BUCKET;

    let method = tonic::CowGrpcMethod::new("", api.to_string());
    masa_policy::ClientTokenBucket::ensure_worker_started();
    let tokens = CLIENT_TOKEN_BUCKET.try_acquire(&method)?;

    Some(RootContext::from(create_context(api, slo)).with_rajomon_tokens(tokens))
}

/// Utility function to create and attach a Masa Context to a Request.
pub fn attach_context<T>(req: &mut tonic::Request<T>, api: &str, slo: Duration) {
    let ctx = create_context(api, slo);
    req.set_masa_context(&ctx);
}

/// The price a response (or error status) propagated, read from its metadata.
/// `None` if the responder did not propagate one.
#[cfg(feature = "ac_rajomon")]
pub fn rajomon_price_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<u64> {
    masa_policy::get_wire_from_metadata::<masa_policy::modules::RajomonModule>(metadata)
        .and_then(|wire| wire.price)
}

/// Update the cached Rajomon price for a method (called when a response header is received).
#[cfg(feature = "ac_rajomon")]
pub fn update_rajomon_price(method: &tonic::CowGrpcMethod, price: u64) {
    masa_policy::CLIENT_TOKEN_BUCKET.update_price(method, price);
}

/// Check the client-side Rajomon bucket and draw a random token count for an
/// outgoing request. Returns `None` when the bucket can't cover the cached
/// price for the method (client-side shed). Use this from loadgens that build
/// their own `Context` (and therefore can't use `try_create_context`).
#[cfg(feature = "ac_rajomon")]
pub fn try_acquire_tokens(api: &str) -> Option<u64> {
    use masa_policy::CLIENT_TOKEN_BUCKET;
    let method = tonic::CowGrpcMethod::new("", api.to_string());
    masa_policy::ClientTokenBucket::ensure_worker_started();
    CLIENT_TOKEN_BUCKET.try_acquire(&method)
}

/// Initial token value for Rajomon admission control (runtime-configurable).
#[cfg(feature = "ac_rajomon")]
pub fn tokens_left_init() -> u64 {
    masa_policy::PolicyParams::global().rajomon.tokens_left_init
}
