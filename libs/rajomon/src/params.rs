// Runtime-configurable Rajomon parameters.
//
// The parameters are read once, on first use, from the JSON file named by the
// `RAJOMON_PARAMS_PATH` environment variable, or else by
// `MASA_POLICY_PARAMS_PATH`, the variable experiment configs already set. The
// file holds the parameters under a top-level `"rajomon"` key; any other key
// is ignored, so a Masa params file (`{"rajomon": {...}, "pred": {...}}`) works
// unchanged. An unset variable, an unreadable file or a parse error falls back
// to the defaults.

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// Primary environment variable naming the params file.
pub const PARAMS_PATH_ENV: &str = "RAJOMON_PARAMS_PATH";
/// Alias read when `RAJOMON_PARAMS_PATH` is unset: the variable existing
/// experiment configs set for the whole policy params file.
pub const LEGACY_PARAMS_PATH_ENV: &str = "MASA_POLICY_PARAMS_PATH";

/// Tunable parameters for the Rajomon token-based admission control policy.
///
/// Implements the algorithm described in the NSDI '25 paper *"Rajomon:
/// Decentralized and Coordinated Overload Control for Latency-Sensitive
/// Microservices"* (Xing et al.). For an exhaustive comparison against the
/// paper and the upstream Go reference (`3rd_party/rajomon/`), see
/// `3rd_party/rajomon/RUST_PORT_ALIGNMENT.md`.
///
/// **Algorithm summary** (paper §3.4 "Proportional Price Updates"):
/// At each tick (`price_update_rate_ms`), the server reads the maximum
/// queueing delay observed in the previous window. If it exceeds the
/// threshold, the price is *increased proportionally* to the excess
/// (`(excess_us * price_step_up) / 1000`, paper says ~3-13 tokens per 1ms
/// excess is typical). If queueing falls below half the threshold, the
/// price is *decreased by 1* (hardcoded in the paper). Otherwise, the
/// price is held (hysteresis dead band `[threshold/2, threshold]`).
///
/// **Total price** (paper §3.4 "Maximum Total Price"):
/// `total_price(service) = own_price + max(downstream_total_prices)`.
/// A request with `tokens` is admitted iff `tokens >= total_price`.
///
/// **Lazy price propagation** (paper §3.4):
/// Each response attaches the local price with probability `1/price_freq`
/// (probabilistic per-call Bernoulli draw, not a deterministic modulo).
///
/// **Client-side token bucket** (paper §3.3):
/// Tokens replenish via a Poisson process at rate
/// `token_update_step / token_update_rate_ms` tokens/ms.
/// Each outgoing request spends a *uniform random* amount
/// from `[0, current_balance]` (paper §3.3 "Randomized Token Spending");
/// deterministic "all-in" spending is explicitly called out by the paper
/// as making AQM ineffective.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RajomonParams {
    /// Server-side price-update tick interval in milliseconds.
    /// Paper §3.4 suggests this typically ranges from 1ms to 20ms.
    pub price_update_rate_ms: u64,

    /// Queueing-delay threshold in microseconds.
    /// The price climbs when observed queue latency exceeds this value
    /// and decays when it drops below half this value (`threshold / 2`).
    /// Paper §3.4 suggests this typically ranges from 1ms to 20ms.
    pub latency_threshold_us: u64,

    /// Proportional price-increase coefficient: tokens added per 1ms of
    /// queueing-delay excess over `latency_threshold_us`.
    /// The full increment per tick is
    /// `((excess_us * price_step_up) / 1000).max(1)`.
    /// Paper §3.4: typical range is 3-13. Bumping this above the paper
    /// range makes the controller more aggressive on the climb side.
    pub price_step_up: u64,

    /// Initial server price on worker startup.
    /// Default 0 matches the Go reference. The paper doesn't specify.
    pub init_price: u64,

    /// Inverse propagation probability: each response attaches the price
    /// with probability `1 / price_freq` via a per-call Bernoulli draw.
    /// Paper §3.4 example: 20% propagation rate, i.e. `price_freq = 5`.
    /// `price_freq = 1` means always propagate. `price_freq = 0` disables
    /// propagation entirely.
    pub price_freq: u64,

    /// Initial value of the client-side `CLIENT_TOKEN_BUCKET` on process
    /// startup.
    pub tokens_left_init: u64,

    /// Mean inter-replenishment interval (in milliseconds) for the
    /// client-side token bucket. Paper §3.3 specifies a Poisson process,
    /// which our implementation realizes via an exponential distribution
    /// with rate `1 / token_update_rate_ms`.
    pub token_update_rate_ms: u64,

    /// Tokens added on each client-bucket replenishment event.
    pub token_update_step: u64,
}

impl Default for RajomonParams {
    fn default() -> Self {
        Self {
            price_update_rate_ms: 10,
            latency_threshold_us: 5_000,
            price_step_up: 8,
            init_price: 0,
            price_freq: 5,
            tokens_left_init: 10,
            token_update_rate_ms: 10,
            token_update_step: 5,
        }
    }
}

/// The params file's shape: Rajomon's section, whatever else the file holds.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ParamsFile {
    rajomon: RajomonParams,
}

static PARAMS: OnceLock<RajomonParams> = OnceLock::new();

impl RajomonParams {
    /// The process-wide parameters, loaded on first call (see the file comment
    /// for where from).
    pub fn global() -> &'static RajomonParams {
        PARAMS.get_or_init(|| {
            let path =
                std::env::var(PARAMS_PATH_ENV).or_else(|_| std::env::var(LEGACY_PARAMS_PATH_ENV));
            if let Ok(path) = path {
                match std::fs::File::open(&path) {
                    Ok(file) => {
                        match serde_json::from_reader::<_, ParamsFile>(std::io::BufReader::new(
                            file,
                        )) {
                            Ok(file) => {
                                log::info!("Loaded rajomon params from {}", path);
                                return file.rajomon;
                            }
                            Err(e) => log::warn!(
                                "Failed to parse rajomon params from {}: {}, using defaults",
                                path,
                                e
                            ),
                        }
                    }
                    Err(e) => log::warn!(
                        "Failed to open rajomon params file {}: {}, using defaults",
                        path,
                        e
                    ),
                }
            }
            RajomonParams::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_documented_values() {
        let p = RajomonParams::default();
        assert_eq!(p.price_update_rate_ms, 10);
        assert_eq!(p.latency_threshold_us, 5_000);
        assert_eq!(p.price_step_up, 8);
        assert_eq!(p.init_price, 0);
        assert_eq!(p.price_freq, 5);
        assert_eq!(p.tokens_left_init, 10);
        assert_eq!(p.token_update_rate_ms, 10);
        assert_eq!(p.token_update_step, 5);
    }

    #[test]
    fn a_masa_params_file_yields_the_rajomon_section() {
        let json = r#"{"rajomon": {"token_update_step": 200}, "pred": {"tau_er": 9.0}}"#;
        let file: ParamsFile = serde_json::from_str(json).unwrap();
        assert_eq!(file.rajomon.token_update_step, 200);
        assert_eq!(file.rajomon.price_update_rate_ms, 10);
    }

    #[test]
    fn a_file_without_the_section_yields_defaults() {
        let file: ParamsFile = serde_json::from_str(r#"{"pred": {}}"#).unwrap();
        assert_eq!(file.rajomon.token_update_step, 5);
        let file: ParamsFile = serde_json::from_str("{}").unwrap();
        assert_eq!(file.rajomon.price_freq, 5);
    }
}
