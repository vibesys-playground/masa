use masa_core::Instant;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    Mutex,
};

use tonic::{Code, Response, Status};

use crate::layer::est::wire::EstimationResponseWire;

// ══════════════════════════════════════════════════════════════════════════
// Response metadata assembly
// ══════════════════════════════════════════════════════════════════════════

/// Tracks cumulative compute time (CPU time spent in poll) for a single request.
#[derive(Debug)]
pub(crate) struct ComputeTracker {
    /// Accumulated compute microseconds across all polls.
    poll_compute_us: AtomicU64,
    /// Start instant of the current poll (`None` when not inside a poll).
    poll_start: Mutex<Option<Instant>>,
}

impl ComputeTracker {
    pub(super) fn new() -> Self {
        Self {
            poll_compute_us: AtomicU64::new(0),
            poll_start: Mutex::new(None),
        }
    }

    /// Start tracking compute time for the current poll.
    pub(super) fn start(&self) {
        *self.poll_start.lock().unwrap() = Some(Instant::now());
    }

    /// Stop tracking compute time and accumulate elapsed time.
    pub(super) fn stop(&self) {
        if let Some(start) = self.poll_start.lock().unwrap().take() {
            let elapsed_us = start.elapsed().as_micros() as u64;
            self.poll_compute_us
                .fetch_add(elapsed_us, Ordering::Relaxed);
        }
    }

    /// Read the accumulated compute time in microseconds.
    pub(super) fn compute_us(&self) -> u64 {
        self.poll_compute_us.load(Ordering::Relaxed)
    }
}

/// Per-request metadata tracker.
///
/// Accumulates local compute time, downstream utilization, and subtree
/// compute cost throughout the request lifecycle. Builds the
/// `EstimationResponseWire` for the outgoing response at finalization.
#[derive(Debug)]
pub(crate) struct RequestMetadataTracker {
    compute: ComputeTracker,
    max_child_downstream_util: Mutex<f32>,
    accumulated_child_compute_us: AtomicU64,
    accumulated_child_early_returns: AtomicU32,
    local_early_return: AtomicBool,
    // A single user-facing request that signals at multiple hops should still
    // count as one event; otherwise a 5-hop request that signals at every hop
    // looks like 5 separate failures. We track presence (AtomicBool), not a
    // running tally, so the ingress sees deadline_signal_count in {0, 1}.
    pub(super) child_deadline_signal: AtomicBool,
    local_deadline_signal: AtomicBool,
}

impl RequestMetadataTracker {
    pub(crate) fn new() -> Self {
        Self {
            compute: ComputeTracker::new(),
            max_child_downstream_util: Mutex::new(0.0),
            accumulated_child_compute_us: AtomicU64::new(0),
            accumulated_child_early_returns: AtomicU32::new(0),
            local_early_return: AtomicBool::new(false),
            child_deadline_signal: AtomicBool::new(false),
            local_deadline_signal: AtomicBool::new(false),
        }
    }

    /// Start tracking compute time for the current poll.
    pub(crate) fn start_poll(&self) {
        self.compute.start();
    }

    /// Stop tracking compute time after a poll.
    pub(crate) fn end_poll(&self) {
        self.compute.stop();
    }

    /// Fold a child RPC's outcome into this request's totals.
    ///
    /// `report` is the estimation section of a successful child response (see
    /// [`child_report`]); it updates max downstream utilization and
    /// accumulated subtree compute cost.
    pub(crate) fn absorb_child<T>(
        &self,
        response: &Result<Response<T>, Status>,
        report: Option<&EstimationResponseWire>,
    ) {
        if is_early_return_response(response) {
            // Child early-returned with Err(Status) — we know at least 1 early
            // return occurred, whatever the status carries.
            self.accumulated_child_early_returns
                .fetch_add(1, Ordering::Relaxed);
        } else if let Some(report) = report {
            let mut max_util = self.max_child_downstream_util.lock().unwrap();
            if report.max_downstream_util > *max_util {
                *max_util = report.max_downstream_util;
            }
            self.accumulated_child_compute_us
                .fetch_add(report.accumulated_compute_us, Ordering::Relaxed);
            self.accumulated_child_early_returns
                .fetch_add(report.early_return_count, Ordering::Relaxed);
            if report.deadline_signal_count > 0 {
                self.child_deadline_signal.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Mark this request as having triggered a local early return.
    pub(crate) fn mark_early_return(&self) {
        self.local_early_return.store(true, Ordering::Relaxed);
    }

    /// Mark this request as having tripped a soft deadline signal
    /// (`signal_slack`) - request continues, but the ingress AC sees the
    /// signal via `EstimationResponseWire::deadline_signal_count`.
    pub(crate) fn mark_deadline_signal(&self) {
        self.local_deadline_signal.store(true, Ordering::Relaxed);
    }

    /// True when this hop or any descendant tripped its local deadline under
    /// `signal_slack`. Used to gate latency-estimator updates so a
    /// signal-but-continue request's inflated wallclock doesn't poison the
    /// estimator.
    pub(crate) fn is_subtree_signaled(&self) -> bool {
        self.local_deadline_signal.load(Ordering::Relaxed)
            || self.child_deadline_signal.load(Ordering::Relaxed)
    }

    /// Whether this hop or its subtree has recorded an early return or a
    /// deadline signal.
    pub(crate) fn has_early_return_or_signal(&self) -> bool {
        self.local_early_return.load(Ordering::Relaxed)
            || self.accumulated_child_early_returns.load(Ordering::Relaxed) > 0
            || self.is_subtree_signaled()
    }

    /// The report for the outgoing response.
    pub(crate) fn response_wire(&self) -> EstimationResponseWire {
        let compute_time_us = self.compute.compute_us();
        let accumulated_compute_us =
            compute_time_us + self.accumulated_child_compute_us.load(Ordering::Relaxed);
        let utilization = tokio::task::current_utilization() as f32;
        let max_child_util = *self.max_child_downstream_util.lock().unwrap();
        let max_downstream_util = utilization.max(max_child_util);

        let local_er = if self.local_early_return.load(Ordering::Relaxed) {
            1
        } else {
            0
        };
        let early_return_count =
            local_er + self.accumulated_child_early_returns.load(Ordering::Relaxed);

        // Saturate at 1: a single ingress request signals at most once,
        // regardless of how many hops in its subtree tripped their local
        // deadline. The AC reads this as a boolean (`> 0`) anyway.
        let signaled = self.local_deadline_signal.load(Ordering::Relaxed)
            || self.child_deadline_signal.load(Ordering::Relaxed);
        let deadline_signal_count = if signaled { 1 } else { 0 };

        EstimationResponseWire {
            compute_time_us,
            accumulated_compute_us,
            utilization,
            max_downstream_util,
            early_return_count,
            deadline_signal_count,
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Helpers
// ═════════════════════════════════════════════════════════��════════════════

pub(crate) fn is_early_return_response<T>(response: &Result<Response<T>, Status>) -> bool {
    match response {
        Ok(_) => false,
        Err(status) => status.code() == Code::DeadlineExceeded,
    }
}

/// The estimation report of a child response: its `estimation` section if the
/// child succeeded and attached one. An error status contributes no report; an
/// early return is counted from the status alone.
pub(crate) fn child_report<T>(
    response: &Result<Response<T>, Status>,
    wire: Option<EstimationResponseWire>,
) -> Option<EstimationResponseWire> {
    response.as_ref().ok().and(wire)
}

/// True when a child's report carries a non-zero `deadline_signal_count` —
/// i.e., the request returned successfully but tripped its local deadline at
/// some hop under `signal_slack`. The wallclock for such a request is
/// inflated by signal-but-continue runtime, so the latency estimator should
/// skip these observations the same way it skips Err early-returns.
pub(crate) fn is_signaled_report(report: Option<&EstimationResponseWire>) -> bool {
    report.is_some_and(|report| report.deadline_signal_count > 0)
}
