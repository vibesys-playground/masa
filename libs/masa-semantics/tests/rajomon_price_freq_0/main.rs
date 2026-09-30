//! With `price_freq = 0` Rajomon never attaches its price to responses.
//!
//! The probability is process-wide configuration, so this runs in its own
//! process.

#[path = "../semantics/harness.rs"]
mod harness;

#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
#[test]
fn price_propagation_can_be_disabled() {
    use harness::*;

    let carrying = run_with::<Under, _>(
        Params {
            rajomon_price_freq: 0,
        },
        true,
        |w| {
            let svc = w.service("RwNever");
            (0..2000)
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
    assert_eq!(carrying, 0);
}
