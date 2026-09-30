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

/// Assert that `observed` out of `draws` matches a probability `p` to within six
/// standard deviations of the binomial count (a spurious failure has
/// probability around 2e-9), exactly when `p` is 0 or 1.
pub fn assert_frequency(observed: usize, draws: usize, p: f64, what: &str) {
    let expected = draws as f64 * p;
    let sigma = (draws as f64 * p * (1.0 - p)).sqrt();
    let tolerance = 6.0 * sigma + 1.0;
    assert!(
        (observed as f64 - expected).abs() <= tolerance,
        "{what}: observed {observed} of {draws}, expected {expected:.1} +- {tolerance:.1}"
    );
}

// ── Predictive admission helpers ────────────────────────────────────────

/// The ingress method used by admission scenarios.
#[cfg(feature = "ac_pred")]
pub const INGRESS: &str = "Ingress";

/// Enough overload windows for the admission probability to underflow to
/// exactly zero (0.875^6000 is below the smallest f64).
#[cfg(feature = "ac_pred")]
pub const WINDOWS_TO_CLOSE: usize = 6000;

/// Record one ingress outcome for `method` without polling the request:
/// `early_return` decides whether it ended as an early return or a success.
#[cfg(feature = "ac_pred")]
pub fn outcome<H: Hooks>(w: &World<H>, svc: &Service<H>, method: &'static str, early_return: bool) {
    let req = w.ingress("AcApi", dur_ms(10_000));
    let handler = svc.accept(method, &req);
    let result = if early_return {
        Err(tonic::Status::new(
            tonic::Code::DeadlineExceeded,
            "/EarlyReturn?src=injected",
        ))
    } else {
        Ok(())
    };
    handler.finalize_now(result);
}

/// Does the admission check at the first poll of a fresh ingress request
/// admit it? The request is abandoned, so the answer records no outcome.
#[cfg(feature = "ac_pred")]
pub fn admitted<H: Hooks>(w: &World<H>, svc: &Service<H>, method: &'static str) -> bool {
    let req = w.ingress("AcApi", dur_ms(10_000));
    svc.accept(method, &req).poll_begin().is_ok()
}

#[cfg(feature = "ac_pred")]
pub fn count_admitted<H: Hooks>(
    w: &World<H>,
    svc: &Service<H>,
    method: &'static str,
    draws: usize,
) -> usize {
    (0..draws).filter(|_| admitted(w, svc, method)).count()
}

/// Drive `method` into full overload: every 60 ms window is all early returns.
#[cfg(feature = "ac_pred")]
pub fn close_admission<H: Hooks>(w: &World<H>, svc: &Service<H>, method: &'static str) {
    for _ in 0..WINDOWS_TO_CLOSE {
        w.advance_ms(60);
        outcome(w, svc, method, true);
    }
}

// ── Rajomon helpers ─────────────────────────────────────────────────────

/// Teach `svc` that `child_service::child_method`, called from `parent`,
/// advertises `price`: one request whose child replies with that price.
#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
pub fn learn_price<H: Hooks>(
    w: &World<H>,
    svc: &Service<H>,
    parent: &'static str,
    child_service: &'static str,
    child_method: &'static str,
    price: u64,
) {
    let req = w
        .crafted("RajApi", dur_ms(10_000))
        .tokens(1_000_000)
        .build();
    let h = svc.accept(parent, &req);
    h.poll_begin().expect("seeding request is admitted");
    let spec = ReplySpec {
        price: Some(price),
        ..ReplySpec::default()
    };
    h.call_remote(child_service, child_method)
        .returns(Duration::from_millis(1), Reply::synthetic(spec))
        .expect("child call within budget");
    h.finalize_now(Ok(()));
}
