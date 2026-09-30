//! Reference models of Masa's latency estimators, small enough to read as a
//! specification. Scenarios compare Masa's observable behavior against them
//! over randomized observation sequences.
//!
//! Only the default estimator (`est_mean_var`, an asymmetric exponential moving
//! average) is modeled: the other estimator kinds are pinned as inert by
//! `estimator_kinds`.

#![allow(dead_code)]

/// Estimates derived from a stream of non-negative observations (microseconds).
pub trait Model: Default {
    fn track(&mut self, x: u64);
    /// Estimate used for soft signals (priority) and, with
    /// `deadline_equals_slack`, hard deadlines.
    fn full(&self) -> u64;
    /// Conservative estimate used for hard deadline tightening.
    fn floor(&self) -> u64;
}

/// Exponential moving average that inflates slowly and deflates quickly.
///
/// Mean: weight 0.05 for an observation above the mean, 0.2 otherwise.
/// Floor: weight 0.01 above the floor, 0.3 below it. The first observation
/// initializes both.
#[derive(Default)]
pub struct MeanVar {
    mean: f64,
    floor: f64,
    started: bool,
}

impl Model for MeanVar {
    fn track(&mut self, x: u64) {
        let x = x as f64;
        if !self.started {
            self.mean = x;
            self.floor = x;
            self.started = true;
            return;
        }
        let alpha = if x > self.mean { 0.05 } else { 0.2 };
        self.mean += alpha * (x - self.mean);
        let alpha_floor = if x < self.floor { 0.3 } else { 0.01 };
        self.floor += alpha_floor * (x - self.floor);
    }

    fn full(&self) -> u64 {
        self.mean as u64
    }

    fn floor(&self) -> u64 {
        self.floor as u64
    }
}
