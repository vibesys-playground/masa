//! Helpers shared by scenarios, written against the harness API only.

#![allow(dead_code)]

use std::time::Duration;

use crate::harness::*;

/// One fully served request through `svc.parent`: it calls `child_method` on
/// `child` (which takes `child_us`) and then works `after_us` more. The end-to-end SLO
/// is generous, so nothing is ever late.
pub fn train<H: Hooks>(
    w: &World<H>,
    svc: &Service<H>,
    parent: &'static str,
    child: &Service<H>,
    child_method: &'static str,
    child_us: u64,
    after_us: u64,
) -> Reply {
    let req = w.ingress("TrainApi", Duration::from_secs(10));
    svc.serve(parent, &req, |h| {
        h.call(child, child_method)
            .run_for(Duration::from_micros(child_us))?;
        h.work(Duration::from_micros(after_us))
    })
}

/// What a service hands to a child when it is asked to call it, observed
/// without finishing (and so without teaching) the parent request.
pub struct Probe {
    pub inbound: CtxView,
    pub outbound: CtxView,
}

impl Probe {
    /// How much earlier the child's deadline is than the parent's.
    pub fn deadline_tightening(&self) -> u64 {
        self.inbound.deadline - self.outbound.deadline
    }
}

/// Serve a fresh request with end-to-end `slo` to `svc.parent` and issue a
/// call to `child` from it, then abandon the request. The request is never
/// finalized, so it teaches no estimator anything.
pub fn probe<H: Hooks>(
    w: &World<H>,
    svc: &Service<H>,
    parent: &'static str,
    slo: Duration,
    child: &Service<H>,
    child_method: &'static str,
) -> Result<Probe, tonic::Status> {
    let req = w.ingress("ProbeApi", slo);
    probe_request(svc, parent, &req, child, child_method)
}

/// Like [`probe`] for an already built request.
pub fn probe_request<H: Hooks>(
    svc: &Service<H>,
    parent: &'static str,
    req: &Inbound,
    child: &Service<H>,
    child_method: &'static str,
) -> Result<Probe, tonic::Status> {
    let handler = svc.accept(parent, req);
    let out = handler.call(child, child_method).outbound()?;
    Ok(Probe {
        inbound: req.view().clone(),
        outbound: out.view(),
    })
}
