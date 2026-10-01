use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;

use serde::{Deserialize, Serialize};
use tonic::{CowGrpcMethod, Response, Status};

use super::shared::{RajomonSharedState, RAJOMON_STATE};
use crate::layer::{
    ChildOutcome, ChildState, Extensions, Layer, LayerServer, MissingDependency, Outcome,
    ServerInit,
};
use crate::policy_params::PolicyParams;
use crate::wire::{WireIn, WireOut};

// ── Wire data ───────────────────────────────────────────────────────────

/// Token budget Rajomon assumes for a request that arrives with no Rajomon
/// wire data at all. Absence is Rajomon's to interpret; zero is a real budget.
pub(super) const DEFAULT_TOKENS: u64 = 100;

/// Rajomon's data on the wire. A request carries the token budget down the
/// call tree; a response echoes the request's budget and, when lazy price
/// propagation selects it, carries the price the responding service would
/// charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RajomonWire {
    pub tokens: u64,
    /// Absent unless the responder chose to propagate its price; zero is a
    /// real price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<u64>,
}

impl RajomonWire {
    /// The wire data of a request with a budget of `tokens`.
    pub fn request(tokens: u64) -> Self {
        Self {
            tokens,
            price: None,
        }
    }

    /// The wire data of a response that propagates `price`.
    pub fn response(tokens: u64, price: u64) -> Self {
        Self {
            tokens,
            price: Some(price),
        }
    }
}

// ── Layer Implementation ────────────────────────────────────────────────

#[derive(Debug)]
pub struct RajomonServer;

impl LayerServer for RajomonServer {
    fn new(_init: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(Self)
    }
}

#[derive(Debug)]
pub struct RajomonLayer {
    pub(super) rpc: CowGrpcMethod,
    pub(super) should_drop: bool,
    /// Remaining token budget for this request, shared across fan-out branches.
    pub(super) remaining_tokens: AtomicU64,
    /// Inbound token count, echoed in the response.
    pub(super) inbound_tokens: AtomicU64,
}

impl Layer for RajomonLayer {
    type Server = RajomonServer;
    const NAME: &'static str = "rajomon";
    type Wire = RajomonWire;

    fn new(
        method: &CowGrpcMethod,
        _server: &RajomonServer,
        wire: &WireIn<'_>,
        _ext: &mut Extensions,
    ) -> Self {
        RajomonSharedState::ensure_worker_started();

        let mut layer = Self {
            rpc: method.clone(),
            should_drop: false,
            remaining_tokens: AtomicU64::new(0),
            inbound_tokens: AtomicU64::new(0),
        };

        // Inbound admission check: accepted iff tokens >= accumulated_price.
        // Paper §3.4 / Go LoadShedding both allow tokens == accumulated == 0
        // to pass (nothing to charge for). No artificial minimum price.
        let tokens = wire
            .get::<Self>()
            .unwrap_or_else(|err| panic!("{err}"))
            .map_or(DEFAULT_TOKENS, |wire| wire.tokens);
        let accumulated = RAJOMON_STATE.accumulated_price(&layer.rpc);
        let own = RAJOMON_STATE.own_price.load(Ordering::Relaxed);
        layer.inbound_tokens.store(tokens, Ordering::Relaxed);
        if tokens < accumulated {
            layer.should_drop = true;
            RAJOMON_STATE.diag_rejected.fetch_add(1, Ordering::Relaxed);
            RAJOMON_STATE
                .diag_token_deficit_sum
                .fetch_add(accumulated - tokens, Ordering::Relaxed);
        } else {
            layer
                .remaining_tokens
                .store(tokens - own, Ordering::Relaxed);
            RAJOMON_STATE.diag_admitted.fetch_add(1, Ordering::Relaxed);
        }

        layer
    }

    #[inline]
    fn before_poll<Ret>(&self, _ext: &mut Extensions) -> Result<(), Result<Response<Ret>, Status>> {
        // Track queue delay for price updates
        let q_lat_us = tokio::task::obtain_task_queue_latency().as_micros() as u64;
        RAJOMON_STATE
            .queue_stats
            .window_max
            .fetch_max(q_lat_us, Ordering::Relaxed);

        // Check if request was marked for drop
        if self.should_drop {
            return Err(Err(self.issue_error(None, "RajomonAdmissionRej")));
        }
        Ok(())
    }

    #[inline]
    fn before_child_rpc<T>(
        &self,
        child_method: &CowGrpcMethod,
        _child: &mut ChildState,
        _request: &mut tonic::Request<T>,
        child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        // Check if request was marked for drop before initiating child RPC
        if self.should_drop {
            return Err(self.issue_error(None, "RajomonAdmissionRej"));
        }

        // Check outbound budget
        let price = RAJOMON_STATE.child_price(child_method);
        let current = self.remaining_tokens.load(Ordering::Relaxed);
        if current < price {
            RAJOMON_STATE
                .diag_child_budget_rej
                .fetch_add(1, Ordering::Relaxed);
            return Err(self.issue_error(Some(child_method), "RajomonChildBudgetRej"));
        }

        child_wire
            .put::<Self>(&RajomonWire::request(current))
            .unwrap_or_else(|err| panic!("{err}"));

        Ok(())
    }

    #[inline]
    fn after_poll<Ret>(
        &self,
        poll: &Poll<Result<Response<Ret>, Status>>,
        _ext: &Extensions,
    ) -> Result<(), Result<Response<Ret>, Status>> {
        if let Poll::Pending = poll {
            if self.should_drop {
                return Err(Err(self.issue_error(None, "RajomonAdmissionRej")));
            }
        }
        Ok(())
    }

    #[inline]
    fn after_child_rpc<T>(
        &self,
        child_method: &CowGrpcMethod,
        outcome: ChildOutcome<'_, T>,
        response_wire: &WireIn<'_>,
        _child: &ChildState,
        _ext: &Extensions,
    ) -> Result<(), Status> {
        // A rejected child was never sent, so no price came back.
        if outcome.sent().is_none() {
            return Ok(());
        }
        // Extract and cache downstream prices from the child's response, which
        // may be an error status: the price is read regardless of outcome.
        let price = response_wire
            .get::<Self>()
            .unwrap_or_else(|err| panic!("{err}"))
            .and_then(|wire| wire.price);

        if let Some(price) = price {
            // Store per-child price keyed by (parent, child)
            RAJOMON_STATE
                .downstream_prices
                .insert((self.rpc.clone(), child_method.clone()), price);
            // Recompute max for this parent method across all children
            let max_price = RAJOMON_STATE
                .downstream_prices
                .iter()
                .filter(|e| e.key().0 == self.rpc)
                .map(|e| *e.value())
                .max()
                .unwrap_or(0);
            RAJOMON_STATE
                .max_downstream_for_method
                .insert(self.rpc.clone(), max_price);
        }

        Ok(())
    }

    #[inline]
    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        let tokens = self.inbound_tokens.load(Ordering::Relaxed);
        // Paper §3.4 "Lazy Price Propagation": probabilistic per-response.
        // Paper §3.4: propagate the raw accumulated price — no artificial floor.
        let response = if self.should_propagate_price() {
            RajomonWire::response(tokens, RAJOMON_STATE.accumulated_price(&self.rpc))
        } else {
            RajomonWire::request(tokens)
        };
        wire.put::<Self>(&response)
            .unwrap_or_else(|err| panic!("{err}"));
    }
}

impl RajomonLayer {
    /// Build a rejection `Status` carrying a structured `/EarlyReturn?...` message.
    ///
    /// Mirrors the format used by predictive admission so the experiment plotting code
    /// (`exp_runner/runner/plotting/util.py::_parse_error_columns`) can extract a
    /// `reason` column for each rejected request.
    fn issue_error(&self, child_method: Option<&CowGrpcMethod>, reason: &str) -> Status {
        let msg = match child_method {
            Some(child) => format!(
                "/EarlyReturn?src={}::{}?last_rpc={}::{}&reason={}",
                self.rpc.service(),
                self.rpc.method(),
                child.service(),
                child.method(),
                reason,
            ),
            None => format!(
                "/EarlyReturn?src={}::{}&reason={}",
                self.rpc.service(),
                self.rpc.method(),
                reason,
            ),
        };

        Status::resource_exhausted(msg)
    }

    /// Paper §3.4 "Lazy Price Propagation": *"Upon sending a response, the
    /// controller attaches its price information to the response with a
    /// configured probability, e.g., 20%, updating the upstream services
    /// with the current local prices."*
    ///
    /// `price_freq` is interpreted as the inverse propagation probability:
    /// `1/price_freq` is the probability of attaching the price on each
    /// response. So `price_freq = 5` matches the paper's 20% example,
    /// `price_freq = 1` means always propagate, and `price_freq = 0` means
    /// never propagate.
    ///
    /// This replaces the prior `inbound_tokens % price_freq == 0` predicate
    /// inherited from the Go reference (`3rd_party/rajomon/rajomon.go:415` /
    /// `:442`). The Go predicate is biased — it depends on the distribution
    /// of `tok` values across requests, and under deterministic "all-in"
    /// client spending it degenerates to "always" or "never." See
    /// `3rd_party/rajomon/RUST_PORT_ALIGNMENT.md` §0.5 / §10 item B.
    pub(super) fn should_propagate_price(&self) -> bool {
        use rand::Rng;
        let p = PolicyParams::global().rajomon.price_freq;
        match p {
            0 => false,
            1 => true,
            n => rand::thread_rng().gen_range(0..n) == 0,
        }
    }
}
