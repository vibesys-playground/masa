// Layer module — composable policy modules layered on the base request
// lifecycle.
//
// A policy module is a type implementing `Layer`. Modules are composed into a
// stack with `policy_stack!`, and `PolicyHooks<S>` dispatches every lifecycle
// hook through the stack in order. The first `Err` short-circuits. `()` is the
// empty module, so a disabled slot in a stack costs nothing.
//
// Masa's built-in modules:
// - **Guard** (feature `abort_slo`): `E2eDeadlineGuardLayer` — rejects
//   past-deadline requests.
// - **Estimation** (feature `estimator`): `EstimationLayer` — latency tracking,
//   deadline tightening, reprioritization, feasibility checks.
// - **Oracle** (feature `sched_oracle`): `OracleLayer` — perfect-information
//   child deadline and priority assignment for synthetic experiments.
// - **Admission**: `predictive` (feature `ac_pred`) or `rajomon`
//   (feature `ac_rajomon`).
// - **Observer** (feature `trace_queue_latency`): `QueueLatencyLayer`.
//
// Which modules make up the default stack is decided in `masa_stack.rs`.
// All dispatch is monomorphic — zero runtime cost.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::task::Poll;

use masa_core::{Context, PriorityHint};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::{CowGrpcMethod, Response, Status};

use crate::wire::{assert_valid_name, WireIn, WireOut};

// ── Submodules ──────────────────────────────────────────────────────────

pub(crate) mod admission;

#[cfg(feature = "estimator")]
pub(crate) mod est;

#[cfg(feature = "abort_slo")]
mod e2e_deadline_guard;
#[cfg(feature = "sched_oracle")]
mod oracle;
#[cfg(feature = "trace_queue_latency")]
mod queue_latency;

// ── Traits ──────────────────────────────────────────────────────────────

/// Server-level module state, created once per service and shared across all
/// requests.
pub trait LayerServer: Send + Sync + std::fmt::Debug + Sized {
    /// Construct the server state.
    ///
    /// Modules earlier in the stack are constructed first, so a module can
    /// consume state that an earlier module published with
    /// [`ServerInit::provide`].
    fn new(init: &mut ServerInit) -> Self;
}

/// Construction context passed to every [`LayerServer::new`] in stack order.
///
/// Carries the service name and a typed store through which modules share
/// server-level state (e.g., the estimation module publishes its latency
/// estimators and predictive admission control reads them).
#[derive(Debug)]
pub struct ServerInit {
    service_name: &'static str,
    resources: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl ServerInit {
    pub(crate) fn new(service_name: &'static str) -> Self {
        Self {
            service_name,
            resources: HashMap::new(),
        }
    }

    /// Name of the service whose hooks are being constructed.
    pub fn service_name(&self) -> &'static str {
        self.service_name
    }

    /// Publish a value for modules later in the stack. Replaces any value
    /// previously published under the same type.
    pub fn provide<T: Any + Send + Sync + Clone>(&mut self, value: T) {
        self.resources.insert(TypeId::of::<T>(), Box::new(value));
    }

    /// Read a value published by an earlier module.
    pub fn get<T: Any + Send + Sync + Clone>(&self) -> Option<T> {
        self.resources
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
            .cloned()
    }
}

/// Mutable state populated by modules in [`Layer::before_child_rpc`].
///
/// Initialized from the parent context. Each module in the stack may mutate
/// fields (e.g., tighten deadline, adjust priority). After all modules have
/// run, the hooks build the child `Context` from these fields. Module-specific
/// data does not belong here: it goes in the module's [`Layer::Wire`].
#[derive(Debug)]
#[non_exhaustive]
pub struct ChildRpcContext {
    pub deadline: u64,
    pub prio_hint: PriorityHint,
    #[cfg(feature = "estimator")]
    pub hop_count: u8,
}

impl ChildRpcContext {
    pub(crate) fn from_parent(ctx: &Context) -> Self {
        // hop_count is only incremented when estimation is active — it uses
        // hop_count to distinguish ingress from internal hops.
        Self {
            deadline: ctx.deadline(),
            prio_hint: ctx.prio_hint(),
            #[cfg(feature = "estimator")]
            hop_count: ctx.hop_count().saturating_add(1),
        }
    }
}

/// A policy module: per-request state that hooks into the request lifecycle
/// at key points to implement policy-specific logic (estimation, admission
/// control, etc.).
///
/// All methods have default no-op implementations so that modules only need
/// to override the hooks they care about.
pub trait Layer: Send + Sync + std::fmt::Debug {
    type Server: LayerServer;
    type Child: LayerChild;

    /// Name of this module's section in the wire envelope. Must be unique
    /// among the modules of a stack that have wire data (checked when the
    /// server state is built), ASCII letters, digits, `_` or `-`, and stable
    /// across the binaries of a deployment. Unused when `Wire` is `()`.
    const NAME: &'static str;

    /// The type of the data this module exchanges with other hops. The module
    /// reads it with [`WireIn::get`] and writes it with [`WireOut::put`]; the
    /// framework never inspects, defaults or forwards it. A module with no
    /// wire data uses `()`.
    ///
    /// Associated type defaults are unstable, so every module declares this
    /// explicitly. Wrap a field in `Option` when its absence must be told
    /// apart from any value, such as zero.
    type Wire: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Construct per-request module state.
    ///
    /// `wire` is the inbound request's wire sections; the module reads its own
    /// with `wire.get::<Self>()`, which is `None` if the sender attached
    /// none. May inspect and mutate `ctx`.
    fn new(
        method: &CowGrpcMethod,
        server: &Self::Server,
        ctx: &mut Context,
        wire: &WireIn<'_>,
    ) -> Self;

    /// Called before each poll of the handler future.
    ///
    /// Returns `Err` to abort the request.
    fn before_poll<Ret>(&self, _ctx: &Context) -> Result<(), Result<Response<Ret>, Status>> {
        Ok(())
    }

    /// Called before each outbound child RPC.
    ///
    /// The module may reject the child RPC (returning `Err`) or mutate
    /// `child_rpc` to tighten the deadline or adjust priority. `child_wire`
    /// starts empty and becomes the child request's wire sections: a module
    /// that does not `put` anything sends nothing, and nothing is carried
    /// over from this request's inbound wire.
    fn before_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child_method: &CowGrpcMethod,
        _child_ctx: &mut Self::Child,
        _request: &mut tonic::Request<T>,
        _child_rpc: &mut ChildRpcContext,
        _child_wire: &mut WireOut,
    ) -> Result<(), Status> {
        Ok(())
    }

    /// Called after a child RPC response is received.
    ///
    /// `response_wire` is the wire sections the child's modules attached to
    /// the response (or to the error status); read this module's own with
    /// `response_wire.get::<Self>()`. It borrows from `response`, which is
    /// therefore read-only here. The framework carries none of it forward:
    /// whatever this module wants to report upstream it must `put` in
    /// [`Layer::finalize`].
    fn after_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child_method: &CowGrpcMethod,
        _response: &Result<Response<T>, Status>,
        _response_wire: &WireIn<'_>,
        _child_ctx: &Self::Child,
    ) -> Result<(), Status> {
        Ok(())
    }

    /// Called after each poll of the handler future.
    ///
    /// Returns `Err` to abort the request (e.g., deadline guard on `Pending`).
    fn after_poll<Ret>(
        &self,
        _ctx: &Context,
        _poll: &Poll<Result<Response<Ret>, Status>>,
    ) -> Result<(), Result<Response<Ret>, Status>> {
        Ok(())
    }

    /// Called before the response is serialized and sent.
    ///
    /// Modules should mutate `ctx` directly (e.g., set `response_meta` or
    /// `queue_latencies`). The caller serializes the context once after all
    /// modules have run. `wire` starts empty and becomes the response's wire
    /// sections.
    fn finalize<Ret>(
        &self,
        _ctx: &mut Context,
        _result: &mut Result<Response<Ret>, Status>,
        _wire: &mut WireOut,
    ) {
    }

    #[doc(hidden)]
    fn collect_names(names: &mut Vec<&'static str>) {
        if TypeId::of::<Self::Wire>() != TypeId::of::<()>() {
            names.push(Self::NAME);
        }
    }
}

/// Panics if two modules in the stack `S` share a wire name, which would make
/// their data overwrite each other.
pub(crate) fn assert_unique_names<S: Layer>() {
    let mut names = Vec::new();
    S::collect_names(&mut names);
    for (i, name) in names.iter().enumerate() {
        assert_valid_name(name);
        assert!(
            !names[..i].contains(name),
            "two modules in the policy stack use the wire name `{name}`; `Layer::NAME` must be unique"
        );
    }
}

/// Per-child-RPC module state.
pub trait LayerChild: Send + Sync + Clone + std::fmt::Debug {
    fn new() -> Self;
}

// ── Composition ─────────────────────────────────────────────────────────

/// The empty module. Used to terminate a stack and to fill a disabled slot.
impl LayerServer for () {
    fn new(_init: &mut ServerInit) -> Self {}
}

impl LayerChild for () {
    fn new() -> Self {}
}

impl Layer for () {
    type Server = ();
    type Child = ();
    const NAME: &'static str = "";
    type Wire = ();

    fn new(_method: &CowGrpcMethod, _server: &(), _ctx: &mut Context, _wire: &WireIn<'_>) -> Self {}
}

impl<A: LayerServer, B: LayerServer> LayerServer for (A, B) {
    fn new(init: &mut ServerInit) -> Self {
        let head = A::new(init);
        (head, B::new(init))
    }
}

impl<A: LayerChild, B: LayerChild> LayerChild for (A, B) {
    fn new() -> Self {
        (A::new(), B::new())
    }
}

/// A module stack: runs `head` before every module in `tail`.
///
/// Build stacks with [`policy_stack!`](crate::policy_stack) rather than by
/// hand.
#[derive(Debug)]
pub struct Stack<H, T> {
    head: H,
    tail: T,
}

impl<H: Layer, T: Layer> Layer for Stack<H, T> {
    type Server = (H::Server, T::Server);
    type Child = (H::Child, T::Child);
    const NAME: &'static str = "";
    type Wire = ();

    fn new(
        method: &CowGrpcMethod,
        server: &Self::Server,
        ctx: &mut Context,
        wire: &WireIn<'_>,
    ) -> Self {
        let head = H::new(method, &server.0, ctx, wire);
        Self {
            head,
            tail: T::new(method, &server.1, ctx, wire),
        }
    }

    fn collect_names(names: &mut Vec<&'static str>) {
        H::collect_names(names);
        T::collect_names(names);
    }

    #[inline]
    fn before_poll<Ret>(&self, ctx: &Context) -> Result<(), Result<Response<Ret>, Status>> {
        self.head.before_poll(ctx)?;
        self.tail.before_poll(ctx)
    }

    #[inline]
    fn before_child_rpc<R>(
        &self,
        ctx: &Context,
        child_method: &CowGrpcMethod,
        child_ctx: &mut Self::Child,
        request: &mut tonic::Request<R>,
        child_rpc: &mut ChildRpcContext,
        child_wire: &mut WireOut,
    ) -> Result<(), Status> {
        self.head.before_child_rpc(
            ctx,
            child_method,
            &mut child_ctx.0,
            request,
            child_rpc,
            child_wire,
        )?;
        self.tail.before_child_rpc(
            ctx,
            child_method,
            &mut child_ctx.1,
            request,
            child_rpc,
            child_wire,
        )
    }

    #[inline]
    fn after_child_rpc<R>(
        &self,
        ctx: &Context,
        child_method: &CowGrpcMethod,
        response: &Result<Response<R>, Status>,
        response_wire: &WireIn<'_>,
        child_ctx: &Self::Child,
    ) -> Result<(), Status> {
        self.head
            .after_child_rpc(ctx, child_method, response, response_wire, &child_ctx.0)?;
        self.tail
            .after_child_rpc(ctx, child_method, response, response_wire, &child_ctx.1)
    }

    #[inline]
    fn after_poll<Ret>(
        &self,
        ctx: &Context,
        poll: &Poll<Result<Response<Ret>, Status>>,
    ) -> Result<(), Result<Response<Ret>, Status>> {
        self.head.after_poll(ctx, poll)?;
        self.tail.after_poll(ctx, poll)
    }

    #[inline]
    fn finalize<Ret>(
        &self,
        ctx: &mut Context,
        result: &mut Result<Response<Ret>, Status>,
        wire: &mut WireOut,
    ) {
        self.head.finalize(ctx, result, wire);
        self.tail.finalize(ctx, result, wire);
    }
}

/// Compose policy modules into a stack type, run in the listed order.
///
/// ```ignore
/// type MyStack = masa_policy::policy_stack![MyGuard, MyAdmission];
/// type MyHooks = masa_policy::PolicyHooks<MyStack>;
/// ```
#[macro_export]
macro_rules! policy_stack {
    () => { () };
    ($head:ty $(, $tail:ty)* $(,)?) => {
        $crate::Stack<$head, $crate::policy_stack![$($tail),*]>
    };
}

// ── Re-exports ──────────────────────────────────────────────────────────

#[cfg(feature = "ac_pred")]
pub use admission::predictive::PredAdmissionLayer;
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
pub use admission::rajomon::RajomonLayer;
#[cfg(feature = "abort_slo")]
pub use e2e_deadline_guard::E2eDeadlineGuardLayer;
#[cfg(feature = "estimator")]
pub use est::EstimationLayer;
#[cfg(feature = "sched_oracle")]
pub use oracle::OracleLayer;
#[cfg(feature = "trace_queue_latency")]
pub use queue_latency::QueueLatencyLayer;
