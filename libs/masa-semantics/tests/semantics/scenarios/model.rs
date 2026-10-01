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

/// Reference model of the predictive admission controller's AIMD loop for one
/// scope (all requests, or one root API).
///
/// Outcomes accumulate in a window. The first outcome that arrives more than
/// 50 ms after the window opened closes it: if the window's early-return
/// fraction exceeds 10% the admission probability is multiplied by 0.875,
/// otherwise 0.05 is added (capped at 1). Rejection probability is
/// `(1 - admit_p) * exp(-idle / 2 s)`, where idle is the time since the last
/// window closed.
#[cfg(feature = "ac_pred")]
pub struct Aimd {
    pub admit_p: f64,
    window_total: u64,
    window_er: u64,
    window_opened_at: u64,
    last_closed_at: u64,
}

#[cfg(feature = "ac_pred")]
impl Aimd {
    pub fn new(now_us: u64) -> Self {
        Self {
            admit_p: 1.0,
            window_total: 0,
            window_er: 0,
            window_opened_at: now_us,
            last_closed_at: now_us,
        }
    }

    pub fn record(&mut self, now_us: u64, early_return: bool) {
        self.window_total += 1;
        if early_return {
            self.window_er += 1;
        }
        let elapsed =
            std::time::Duration::from_micros(now_us - self.window_opened_at).as_secs_f64();
        if elapsed > 0.05 {
            let fraction = self.window_er as f64 / self.window_total as f64;
            if fraction > 0.10 {
                self.admit_p = (self.admit_p * 0.875).max(0.0);
            } else {
                self.admit_p = (self.admit_p + 0.05).min(1.0);
            }
            self.last_closed_at = now_us;
            self.window_er = 0;
            self.window_total = 0;
            self.window_opened_at = now_us;
        }
    }

    pub fn reject_prob(&self, now_us: u64) -> f64 {
        let idle = std::time::Duration::from_micros(now_us - self.last_closed_at).as_secs_f64();
        ((1.0 - self.admit_p) * (-idle / 2.0).exp()).min(1.0)
    }
}
