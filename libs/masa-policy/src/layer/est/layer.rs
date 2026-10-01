// Estimation layer — latency tracking, deadline tightening, reprioritization,
// and local deadline checks (ABORT_SLACK / SIGNAL_SLACK).
//
// Active when the `estimator` feature is enabled. Runs independently of the
// admission control layer (ac_pred / ac_rajomon / noop).

use crate::wire::{WireIn, WireOut};
use std::sync::Arc;
use std::task::Poll;

use masa_core::{PriorityHint, ABORT_SLACK};
use tonic::{Code, CowGrpcMethod, Response, Status};

use super::super::{
    BudgetInfo, BudgetLayer, ChildDeadline, ChildPriority, ChildState, Extensions, Layer,
    LayerServer, MissingDependency, Outcome, Requires, ServerInit,
};
use super::default_estimator::DefaultLatencyEstimator;
use super::state::{
    child_report, is_early_return_response, ChildRPCTracker, EstimationTracker, LatencyEstimators,
    RequestMetadataTracker,
};
use super::wire::{EstimationInfo, EstimationWire, RootMethod};
use crate::MethodRegistry;

// ── Server ──────────────────────────────────────────────────────────────

/// Server-level estimation state (shared across requests).
#[derive(Debug)]
pub struct EstimationServer {
    pub(crate) est: LatencyEstimators<DefaultLatencyEstimator>,
}

impl LayerServer for EstimationServer {
    /// Publishes the latency estimators so later modules (e.g., predictive
    /// admission control) share this service's estimates.
    fn new(init: &mut ServerInit) -> Result<Self, MissingDependency> {
        let est = LatencyEstimators::<DefaultLatencyEstimator>::new();
        init.provide(est.clone());
        Ok(Self { est })
    }
}

// ── Per-Request ─────────────────────────────────────────────────────────

/// Per-request estimation layer state.
///
/// Tracks latency distributions, tightens child deadlines (when `sched_pred`
/// is enabled), handles local deadline checks (ABORT_SLACK aborts the request,
/// SIGNAL_SLACK only signals admission control), and manages response metadata
/// propagation.
#[derive(Debug)]
pub struct EstimationLayer {
    pub(crate) estimation: EstimationTracker<DefaultLatencyEstimator>,
    rpc: CowGrpcMethod,
    info: EstimationInfo,
    budget: BudgetInfo,
}

impl Layer for EstimationLayer {
    type Server = EstimationServer;
    const NAME: &'static str = "estimation";
    type Wire = EstimationWire;

    fn requires(requires: &mut Requires) {
        requires.module::<BudgetLayer>();
    }

    fn new(
        method: &CowGrpcMethod,
        server: &EstimationServer,
        wire: &WireIn<'_>,
        ext: &mut Extensions,
    ) -> Self {
        let resolved_method_id = MethodRegistry::global().get_or_register(method.clone());
        let inbound = wire
            .get::<Self>()
            .unwrap_or_else(|err| panic!("{err}"))
            .and_then(|wire| wire.request);
        // A request without an estimation section comes from a sender that
        // runs no estimation (a load generator, say), so it is at ingress.
        let hop_count = inbound.as_ref().map_or(0, |request| request.hop_count);
        let (root_method, root_method_id) = if hop_count == 0 {
            let root = RootMethod {
                service: method.service().to_string(),
                method: method.method().to_string(),
            };
            (Some(Arc::new(root)), Some(resolved_method_id))
        } else {
            let root = inbound.and_then(|request| request.root_method);
            let id = root.as_ref().map(|root| {
                MethodRegistry::global().get_or_register(CowGrpcMethod::new(
                    root.service.clone(),
                    root.method.clone(),
                ))
            });
            (root.map(Arc::new), id)
        };
        let info = EstimationInfo {
            hop_count,
            root_method,
            root_method_id,
        };
        ext.insert(info.clone());
        // The tally of this request's polls and children lives in the
        // extensions, where modules after estimation read it as
        // `SubtreeHealth`, rather than behind a handle shared with them.
        ext.insert(RequestMetadataTracker::new());
        Self {
            estimation: EstimationTracker::new(
                resolved_method_id,
                root_method_id,
                server.est.clone(),
            ),
            rpc: method.clone(),
            info,
            budget: BudgetInfo::of(ext),
        }
    }

    /// Reprioritize the current task and check the local deadline
    /// (ABORT_SLACK aborts; SIGNAL_SLACK marks the soft signal).
    #[inline]
    fn before_poll<Ret>(&self, ext: &mut Extensions) -> Result<(), Result<Response<Ret>, Status>> {
        let request_metadata = Self::tally(ext);
        if ABORT_SLACK {
            let local_deadline = self.budget.deadline();
            if local_deadline != 0 && masa_core::time_now() > local_deadline {
                return Err(Err(Status::new(
                    Code::DeadlineExceeded,
                    format!(
                        "/EarlyReturn?src={}::{}&reason=LocalDeadlineExceeded",
                        self.rpc.service(),
                        self.rpc.method(),
                    ),
                )));
            }
        }

        super::signal_slack::mark_if_late(&self.budget, request_metadata);

        #[cfg(feature = "sched_pred")]
        {
            let remaining = self.budget.deadline().saturating_sub(masa_core::time_now());
            tokio::task::reprioritize(tokio::task::TaskPriority::new(remaining));
        }

        request_metadata.start_poll();
        Ok(())
    }

    /// Compute latency estimates, tighten child deadline, and begin child
    /// RPC tracking. Feasibility checks are handled by the admission layer.
    #[inline]
    fn before_child_rpc<T>(
        &self,
        child_method_name: &CowGrpcMethod,
        child: &mut ChildState,
        _request: &mut tonic::Request<T>,
        child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        // Each hop below ingress is one further from it; the root is passed on.
        child_wire
            .put::<Self>(&EstimationWire::request(
                self.info.hop_count.saturating_add(1),
                self.info.root_method().cloned(),
            ))
            .unwrap_or_else(|err| panic!("{err}"));

        let child_tracker = self.estimation.begin_child(child_method_name);
        let time_left = self
            .budget
            .e2e_deadline()
            .saturating_sub(masa_core::time_now());
        let root = self
            .estimation
            .root_method_id
            .unwrap_or(self.estimation.resolved_method_id);
        let remaining = self.estimation.est.est_after_child_wallclock_for_group(
            root,
            self.estimation.resolved_method_id,
            child_tracker.path_prefix,
            &child_tracker.base_signature,
            child_tracker.service_path_prefix,
            &child_tracker.base_service_signature,
            child_tracker.child_id,
            time_left,
        );

        self.estimation
            .est
            .log_estimates(&child_tracker.key, &remaining);

        // Wallclock-time decay applied to BOTH the deadline-tightening floor
        // and the priority-tightening full estimate. Stale samples (no fresh
        // observation in TAU_DECAY_US) shrink toward zero — without this, a
        // floor inflated by sustained queueing keeps tightening child
        // deadlines indefinitely, causing `abort_slack` to kill mid-flight
        // requests that could have completed (the same metastable trap the
        // BCF check now avoids).
        let decay = crate::layer::est::state::decay_factor(
            masa_core::time_now(),
            self.estimation
                .est
                .after_child_wallclock_last_obs(child_tracker.key),
        );
        let decayed_full = (remaining.full as f64 * decay) as u64;
        let decayed_floor = (remaining.floor as f64 * decay) as u64;

        let (deadline, prio_hint) =
            Self::child_deadline_and_prio(&self.budget, decayed_full, decayed_floor);
        child.propose(ChildDeadline(deadline))?;
        child.propose(ChildPriority(prio_hint))?;

        child.insert(child_tracker);

        Ok(())
    }

    /// Record child RPC completion: track latencies, absorb metadata.
    #[inline]
    fn after_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        response: &Result<Response<T>, Status>,
        response_wire: &WireIn<'_>,
        child: &ChildState,
        ext: &Extensions,
    ) -> Result<(), Status> {
        if let Some(child_tracker) = child.get::<ChildRPCTracker>() {
            let report = child_report(
                response,
                response_wire
                    .get::<Self>()
                    .unwrap_or_else(|err| panic!("{err}"))
                    .and_then(|wire| wire.response),
            );
            self.estimation
                .record_child_complete(child_tracker, response, report.as_ref());
            Self::tally(ext).absorb_child(response, report.as_ref());
        }

        #[cfg(feature = "sched_pred")]
        if let Err(status) = response {
            return Err(status.clone());
        }

        Ok(())
    }

    /// Stop compute tracking and check the local deadline.
    ///
    /// ABORT_SLACK can only replace a Pending poll with an early return.
    /// SIGNAL_SLACK records a soft signal on every post-poll check, including
    /// Ready, because it does not alter the response.
    #[inline]
    fn after_poll<Ret>(
        &self,
        poll: &Poll<Result<Response<Ret>, Status>>,
        ext: &Extensions,
    ) -> Result<(), Result<Response<Ret>, Status>> {
        let request_metadata = Self::tally(ext);
        request_metadata.end_poll();
        if let Poll::Pending = poll {
            if ABORT_SLACK {
                let local_deadline = self.budget.deadline();
                if local_deadline != 0 && masa_core::time_now() > local_deadline {
                    return Err(Err(Status::new(
                        Code::DeadlineExceeded,
                        format!(
                            "/EarlyReturn?src={}::{}&reason=LocalDeadlineExceeded",
                            self.rpc.service(),
                            self.rpc.method(),
                        ),
                    )));
                }
            }
        }
        super::signal_slack::mark_if_late(&self.budget, request_metadata);
        Ok(())
    }

    /// Flush estimation observations and build response metadata.
    ///
    /// Three response classes drive different bookkeeping:
    /// 1. `Err(EarlyReturn)` — abort path. Mark local early-return; skip
    ///    flush so the latency estimator only learns from on-time work.
    /// 2. `Ok` but the subtree tripped `signal_slack` — request finished
    ///    successfully, but its wallclock was inflated by the
    ///    signal-but-continue runtime. Skip flush for the same reason: an
    ///    inflated observation poisons the estimator, which then
    ///    over-tightens child deadlines in the next requests and triggers
    ///    even more signals.
    /// 3. `Ok` and on-time — the only case where the estimator should learn.
    #[inline]
    fn finalize<Ret>(
        &self,
        result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        ext: &Extensions,
    ) {
        let request_metadata = Self::tally(ext);
        if is_early_return_response(result) {
            request_metadata.mark_early_return();
        } else if !super::signal_slack::should_skip_flush(request_metadata) {
            self.estimation.flush();
        }
        wire.put::<Self>(&EstimationWire::response(request_metadata.response_wire()))
            .unwrap_or_else(|err| panic!("{err}"));
    }
}

impl EstimationLayer {
    /// This request's tally, inserted by `new`.
    fn tally(ext: &Extensions) -> &RequestMetadataTracker {
        ext.get::<RequestMetadataTracker>()
            .expect("estimation inserts its tally in `new`")
    }

    /// Compute child deadline and priority hint.
    ///
    /// When `sched_pred` is enabled, tightens the deadline by subtracting
    /// `est_remaining`. When disabled, passes through the parent values.
    #[inline]
    fn child_deadline_and_prio(
        budget: &BudgetInfo,
        #[cfg_attr(not(feature = "sched_pred"), allow(unused_variables))]
        priority_est_remaining: u64,
        deadline_est_remaining: u64,
    ) -> (u64, PriorityHint) {
        #[cfg(feature = "sched_pred")]
        {
            // Priority is a soft scheduling signal, so use the full estimate.
            // The propagated deadline is a hard abort threshold; use the floor
            // estimate to avoid converting estimator variance into false ERs.
            let hard_deadline_estimate =
                hard_deadline_estimate(priority_est_remaining, deadline_est_remaining);
            let deadline = budget.deadline().saturating_sub(hard_deadline_estimate);
            let priority_deadline = budget.deadline().saturating_sub(priority_est_remaining);
            let priority_remaining = priority_deadline.saturating_sub(masa_core::time_now());
            (deadline, PriorityHint::new(priority_remaining))
        }
        #[cfg(not(feature = "sched_pred"))]
        {
            let d = budget.deadline().saturating_sub(deadline_est_remaining);
            (d, budget.prio_hint())
        }
    }
}

#[inline]
#[cfg(any(feature = "sched_pred", test))]
fn hard_deadline_estimate(slack_estimate: u64, deadline_estimate: u64) -> u64 {
    if cfg!(feature = "deadline_equals_slack") {
        slack_estimate
    } else {
        deadline_estimate
    }
}

#[cfg(test)]
mod tests {
    use super::hard_deadline_estimate;

    #[cfg(feature = "deadline_equals_slack")]
    #[test]
    fn tied_deadline_uses_slack_estimate() {
        assert_eq!(hard_deadline_estimate(90, 40), 90);
    }

    #[cfg(not(feature = "deadline_equals_slack"))]
    #[test]
    fn default_deadline_uses_conservative_estimate() {
        assert_eq!(hard_deadline_estimate(90, 40), 40);
    }
}
