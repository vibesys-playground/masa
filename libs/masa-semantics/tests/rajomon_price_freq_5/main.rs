//! Rajomon price propagation is lazy: with `price_freq = 5` (the shipped
//! default) each response carries the price with probability one in five.
//!
//! The probability is process-wide configuration, so this runs in its own
//! process. The draw is random and unseedable; the count is compared with the
//! binomial expectation to within six standard deviations (a spurious failure
//! has probability around 2e-9).

#[path = "../semantics/harness.rs"]
mod harness;

#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
#[test]
fn one_response_in_five_carries_the_price() {
    use harness::*;

    let carrying = run_with::<Under, _>(
        Params {
            rajomon_price_freq: 5,
        },
        true,
        |w| {
            let svc = w.service("RwLazy");
            let draws = 5000;
            (0..draws)
                .filter(|_| {
                    let req = w.crafted("RwApi", dur_ms(1000)).tokens(100).build();
                    svc.accept("Leaf", &req)
                        .finalize_now(Ok(()))
                        .price()
                        .is_some()
                })
                .count()
        },
    );
    let expected = 1000.0;
    let tolerance = 6.0 * (5000.0_f64 * 0.2 * 0.8).sqrt();
    assert!(
        (carrying as f64 - expected).abs() <= tolerance,
        "{carrying} of 5000 responses carried the price; expected {expected} +- {tolerance:.0}"
    );
}
