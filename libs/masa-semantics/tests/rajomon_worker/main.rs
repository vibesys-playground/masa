//! Rajomon's time-driven behavior: the server's price moving with queueing
//! delay, and the client's token bucket refilling.
//!
//! Both are driven by background workers that start once per process, so these
//! scenarios run in a process of their own, in one test, sharing one runtime
//! whose clock is paused and advances only when a scenario says so.
//!
//! The price reacts to how long requests waited in the runtime's queue, which
//! the runtime measures in real time. The scenario makes a request wait for a
//! real 30 ms and asserts only what follows from waiting at least that long;
//! a slower machine can only see a longer wait.

#[path = "../semantics/harness.rs"]
mod harness;

#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
mod scenarios {
    use std::time::Duration;

    use crate::harness::*;

    /// The price `svc` advertises for `method` right now. Reading it runs no
    /// poll, so it adds no queueing observation.
    fn price_of<H: Hooks>(w: &World<H>, svc: &Service<H>, method: &'static str) -> u64 {
        let req = w
            .crafted("RwApi", Duration::from_secs(3600))
            .tokens(u64::MAX / 2)
            .build();
        svc.accept(method, &req)
            .finalize_now(Ok(()))
            .price()
            .expect("price attached to every reply")
    }

    /// Make one request wait `delay` in the queue and then start (its first
    /// poll reports the wait to the price controller).
    async fn start_request_after_waiting<H: Hooks>(
        w: &World<H>,
        svc: &Service<H>,
        delay: Duration,
    ) {
        let w2 = w.clone();
        let svc = svc.clone();
        w.run_queued(delay, move || {
            let req = w2
                .crafted("RwApi", Duration::from_secs(3600))
                .tokens(u64::MAX / 2)
                .build();
            svc.accept("Leaf", &req).poll_begin().expect("admitted");
        })
        .await;
    }

    /// The price rises in proportion to queueing delay above 5 ms, holds while
    /// the delay signal is between 2.5 and 5 ms, then falls by exactly one per
    /// 10 ms tick. A request whose tokens do not cover the price is rejected.
    pub async fn price_follows_queueing_delay<H: Hooks>(w: &World<H>) {
        let svc = w.service("RwPrice");
        w.elapse(Duration::from_millis(50)).await;
        assert_eq!(price_of(w, &svc, "Leaf"), 0, "no queueing, no price");

        start_request_after_waiting(w, &svc, Duration::from_millis(30)).await;
        let mut prices = vec![price_of(w, &svc, "Leaf")];
        assert_eq!(prices[0], 0, "the price moves only on a tick");
        for _ in 0..500 {
            w.elapse(Duration::from_millis(10)).await;
            prices.push(price_of(w, &svc, "Leaf"));
        }

        // A wait of at least 30 ms is 25 ms over the threshold, worth at least
        // 25 * 8 = 200 tokens on the first tick.
        assert!(
            prices[1] >= 200,
            "first tick raised the price to {}",
            prices[1]
        );

        let steps: Vec<i64> = prices
            .windows(2)
            .map(|p| p[1] as i64 - p[0] as i64)
            .collect();
        let rises = steps.iter().take_while(|s| **s > 0).count();
        let holds = steps[rises..].iter().take_while(|s| **s == 0).count();
        let falls = &steps[rises + holds..];
        assert!(
            rises >= 1,
            "price rose while the delay was above the threshold"
        );
        assert!(
            holds >= 1,
            "price held while the signal was inside the dead band"
        );
        let descending = falls.iter().take_while(|s| **s == -1).count();
        assert!(
            descending >= 100,
            "price fell by one per tick: {descending} ticks"
        );
        assert!(
            falls[descending..].iter().all(|s| *s == 0),
            "after falling, the price only stays put (at zero)"
        );

        // Gate: with the price at p, p tokens are enough and p - 1 are not.
        start_request_after_waiting(w, &svc, Duration::from_millis(30)).await;
        w.elapse(Duration::from_millis(10)).await;
        let p = price_of(w, &svc, "Leaf");
        assert!(p > 0);
        let poor = w
            .crafted("RwApi", Duration::from_secs(3600))
            .tokens(p - 1)
            .build();
        assert!(svc.accept("Leaf", &poor).poll_begin().is_err());
        let enough = w
            .crafted("RwApi", Duration::from_secs(3600))
            .tokens(p)
            .build();
        assert!(svc.accept("Leaf", &enough).poll_begin().is_ok());
    }

    /// Clients hold a bucket of tokens that refills with time. A request is
    /// shed at the client when the bucket cannot cover the price it has
    /// learned for the API; the tokens a request carries are paid from the
    /// bucket.
    pub async fn client_bucket_refills_with_time<H: Hooks>(w: &World<H>) {
        // Free API: every acquisition succeeds, and what is handed out never
        // exceeds the initial 10 tokens while no time passes.
        let mut handed_out = 0;
        for _ in 0..200 {
            handed_out += w
                .client_acquires_tokens("RwClientFree")
                .expect("free API is never shed");
        }
        assert!(
            handed_out <= 10,
            "handed out {handed_out} of a 10-token bucket"
        );

        // An API priced beyond the bucket is shed, however often asked.
        w.client_learns_price("RwClientCostly", 200);
        for _ in 0..50 {
            assert_eq!(w.client_acquires_tokens("RwClientCostly"), None);
        }
        // Every token count actually handed out covers the price.
        w.client_learns_price("RwClientCheap", 3);
        for _ in 0..200 {
            if let Some(t) = w.client_acquires_tokens("RwClientCheap") {
                assert!(t >= 3);
            }
        }

        // A second of refilling (about 100 refills of 5 tokens) reopens the
        // costly API.
        w.elapse(Duration::from_secs(1)).await;
        let reopened = (0..100)
            .filter_map(|_| w.client_acquires_tokens("RwClientCostly"))
            .collect::<Vec<_>>();
        assert!(!reopened.is_empty(), "the bucket refilled");
        assert!(reopened.iter().all(|t| *t >= 200));
    }
}

#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
#[test]
fn rajomon_time_driven_behavior() {
    harness::run_timed::<harness::Under, _, _>(harness::Params::default(), |w| async move {
        scenarios::price_follows_queueing_delay(&w).await;
        scenarios::client_bucket_refills_with_time(&w).await;
    });
}
