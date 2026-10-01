// Budget module — Masa's request facts and time budget.
//
// A request's API, id, SLO, gateway entry time, deadline and priority are the
// wire data of the budget module (`BudgetLayer`, the `budget` section of the
// `ctx` header). The framework carries none of it: this module reads it from
// the inbound request, shares a read-only view with the modules after it, and
// writes the child's and the response's sections itself.
//
// A child request's deadline and priority are decided by several modules
// (estimation, oracle). `BudgetLayer`, first in the stack, opens the decision
// with the parent's values in `before_child_rpc`; the modules after it
// overwrite the fields they decide, the last writer winning; and
// `seal_child_rpc`, which the framework runs after all of them, writes the
// child's section from the result.

use std::sync::Arc;

use masa_core::{Api, Context, Latency, PriorityHint, RequestId, Timestamp, BUDGET_SECTION};
use tonic::{CowGrpcMethod, Response, Status};

use super::{ChildState, Extensions, Layer, LayerServer, MissingDependency, Outcome, ServerInit};
use crate::wire::{WireIn, WireOut};

// ── Root priority ───────────────────────────────────────────────────────

/// The priority a root request gets when its creator does not choose one.
///
/// This is where the scheduling feature decides what "urgent" means: the
/// gateway entry time for TailClipper (oldest request first), the time left to
/// the deadline under `sched_pred`, the deadline otherwise. A root request with
/// deadline 0 (no SLO) therefore gets priority 0, which is `PriorityHint::infra()`
/// and always runs first.
pub fn root_priority(gateway_entry: Timestamp, deadline: Timestamp) -> PriorityHint {
    #[cfg(feature = "sched_tailclipper")]
    {
        let _ = deadline;
        PriorityHint::new(gateway_entry)
    }
    #[cfg(all(not(feature = "sched_tailclipper"), feature = "sched_pred"))]
    {
        let _ = gateway_entry;
        PriorityHint::new(deadline.saturating_sub(masa_core::time_now()))
    }
    #[cfg(not(any(feature = "sched_tailclipper", feature = "sched_pred")))]
    {
        let _ = gateway_entry;
        PriorityHint::new(deadline)
    }
}

/// Builds the [`Context`] of a root request, assigning its priority with
/// [`root_priority`] unless one is set.
pub struct ContextBuilder {
    api: Api,
    request_id: RequestId,
    slo: Latency,
    gateway_entry: Timestamp,
    deadline: Timestamp,
    prio_hint: Option<PriorityHint>,
}

impl ContextBuilder {
    pub fn new(api: impl Into<Api>, request_id: RequestId) -> Self {
        Self {
            api: api.into(),
            request_id,
            slo: 0,
            gateway_entry: 0,
            deadline: 0,
            prio_hint: None,
        }
    }

    /// A builder starting from `ctx`, including its priority.
    pub fn from(ctx: &Context) -> Self {
        Self {
            api: ctx.api().clone(),
            request_id: ctx.request_id(),
            slo: ctx.slo(),
            gateway_entry: ctx.gateway_entry(),
            deadline: ctx.deadline(),
            prio_hint: Some(ctx.prio_hint()),
        }
    }

    pub fn slo(mut self, slo: Latency) -> Self {
        self.slo = slo;
        self
    }

    pub fn gateway_entry(mut self, gateway_entry: Timestamp) -> Self {
        self.gateway_entry = gateway_entry;
        self
    }

    pub fn deadline(mut self, deadline: Timestamp) -> Self {
        self.deadline = deadline;
        self
    }

    pub fn prio_hint(mut self, prio_hint: PriorityHint) -> Self {
        self.prio_hint = Some(prio_hint);
        self
    }

    pub fn build(self) -> Context {
        let prio_hint = self
            .prio_hint
            .unwrap_or_else(|| root_priority(self.gateway_entry, self.deadline));
        Context::new(
            self.api,
            self.request_id,
            self.slo,
            self.gateway_entry,
            self.deadline,
            prio_hint,
        )
    }
}

// ── Shared with later modules ───────────────────────────────────────────

/// What the budget module knows about the request being served, read-only:
/// the facts the sender attached. Inserted into [`Extensions`] by
/// [`BudgetLayer::new`], so modules after it read it in their own `new`.
#[derive(Debug, Clone)]
pub struct BudgetInfo(Arc<Context>);

impl From<Context> for BudgetInfo {
    fn from(request: Context) -> Self {
        Self(Arc::new(request))
    }
}

impl BudgetInfo {
    /// The view the budget module published for this request. Panics if the
    /// module is missing; a module that declares [`BudgetLayer`] in
    /// [`Layer::requires`] cannot hit that.
    pub fn of(ext: &Extensions) -> Self {
        ext.get::<Self>()
            .unwrap_or_else(|| {
                panic!(
                    "the `BudgetInfo` that `BudgetLayer` publishes is missing; put `BudgetLayer` \
                     first in the policy stack"
                )
            })
            .clone()
    }

    pub fn api(&self) -> &Api {
        self.0.api()
    }

    pub fn request_id(&self) -> RequestId {
        self.0.request_id()
    }

    pub fn slo(&self) -> Latency {
        self.0.slo()
    }

    pub fn gateway_entry(&self) -> Timestamp {
        self.0.gateway_entry()
    }

    /// This hop's deadline, as the sender set it. It may be earlier than
    /// [`e2e_deadline`](Self::e2e_deadline) when an upstream module tightened it.
    pub fn deadline(&self) -> Timestamp {
        self.0.deadline()
    }

    /// The end-to-end deadline: gateway entry plus SLO.
    pub fn e2e_deadline(&self) -> Timestamp {
        self.0.e2e_deadline()
    }

    pub fn prio_hint(&self) -> PriorityHint {
        self.0.prio_hint()
    }
}

/// The deadline and priority the child request being set up will carry, in
/// its [`ChildState`]. [`BudgetLayer`] opens it in `before_child_rpc` with the
/// parent's values; modules after it (estimation, oracle) overwrite the fields
/// they decide, the last writer winning; [`BudgetLayer`] then writes the
/// child's budget section from it in `seal_child_rpc`. Reach it with
/// [`ChildBudget::of`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildBudget {
    pub deadline: Timestamp,
    pub prio_hint: PriorityHint,
}

impl ChildBudget {
    /// The child budget being decided for this child RPC. Panics if the
    /// module asking runs before [`BudgetLayer`]; a module that declares
    /// [`BudgetLayer`] in [`Layer::requires`] cannot hit that.
    pub fn of(child: &mut ChildState) -> &mut Self {
        child.get_mut::<Self>().unwrap_or_else(|| {
            panic!(
                "no `ChildBudget` is open; a module that sets a child's deadline or priority \
                 must sit after `BudgetLayer` in the policy stack"
            )
        })
    }
}

// ── Budget ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct BudgetServer;

impl LayerServer for BudgetServer {
    fn new(_init: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(Self)
    }
}

/// Reads the request's budget section, shares it, writes the child's section
/// and reports the request's own back in the response.
///
/// The request must carry a budget section: every Masa client attaches one, and
/// a request without it is a misconfigured sender, so `new` panics with a
/// message saying so (as for any malformed wire data).
#[derive(Debug)]
pub struct BudgetLayer {
    info: BudgetInfo,
    /// The inbound section as the sender encoded it. The response carries the
    /// request's own facts unchanged, and so does a child whose budget no
    /// module changed, so both reuse it instead of encoding the context again.
    encoded: String,
}

impl Layer for BudgetLayer {
    type Server = BudgetServer;
    const NAME: &'static str = BUDGET_SECTION;
    type Wire = Context;

    fn new(
        _method: &CowGrpcMethod,
        _server: &BudgetServer,
        wire: &WireIn<'_>,
        ext: &mut Extensions,
    ) -> Self {
        let request = wire
            .get::<Self>()
            .unwrap_or_else(|err| panic!("{err}"))
            .unwrap_or_else(|| panic!("{}", masa_core::missing_budget_section_message()));
        let encoded = wire
            .get_encoded::<Self>()
            .unwrap_or_else(|| panic!("{}", masa_core::missing_budget_section_message()))
            .to_owned();
        let info = BudgetInfo::from(request);
        ext.insert(info.clone());
        Self { info, encoded }
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _request: &mut tonic::Request<T>,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        child.insert(self.parent_budget());
        Ok(())
    }

    fn seal_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _request: &mut tonic::Request<T>,
        child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) {
        let settled = *ChildBudget::of(child);
        if settled == self.parent_budget() {
            child_wire.put_encoded::<Self>(&self.encoded);
            return;
        }
        let context = Context::new(
            self.info.api().clone(),
            self.info.request_id(),
            self.info.slo(),
            self.info.gateway_entry(),
            settled.deadline,
            settled.prio_hint,
        );
        child_wire
            .put::<Self>(&context)
            .unwrap_or_else(|err| panic!("{err}"));
    }

    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        wire.put_encoded::<Self>(&self.encoded);
    }
}

impl BudgetLayer {
    /// The budget of a child that no module changed.
    fn parent_budget(&self) -> ChildBudget {
        ChildBudget {
            deadline: self.info.deadline(),
            prio_hint: self.info.prio_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(any(feature = "sched_tailclipper", feature = "sched_pred")))]
    #[test]
    fn root_priority_is_the_deadline_by_default() {
        assert_eq!(root_priority(10, 110), PriorityHint::new(110));
    }

    #[cfg(feature = "sched_tailclipper")]
    #[test]
    fn root_priority_is_the_gateway_entry_for_tailclipper() {
        assert_eq!(root_priority(10, 110), PriorityHint::new(10));
    }

    #[cfg(all(feature = "sched_pred", not(feature = "sched_tailclipper")))]
    #[test]
    fn root_priority_is_the_time_left_under_sched_pred() {
        let now = masa_core::time_now();
        let prio = root_priority(now, now + 1_000_000).value();
        assert!(prio <= 1_000_000 && prio > 900_000, "prio {prio}");
        assert_eq!(root_priority(now, 0), PriorityHint::new(0));
    }

    #[test]
    fn a_root_without_a_deadline_gets_priority_zero() {
        let ctx = ContextBuilder::new("api", 1).build();
        assert_eq!(ctx.prio_hint(), PriorityHint::infra());
    }

    #[test]
    fn an_explicit_priority_wins() {
        let ctx = ContextBuilder::new("api", 1)
            .deadline(5)
            .prio_hint(PriorityHint::new(42))
            .build();
        assert_eq!(ctx.prio_hint(), PriorityHint::new(42));
        assert_eq!(
            ContextBuilder::from(&ctx).build().prio_hint(),
            ctx.prio_hint()
        );
    }
}
