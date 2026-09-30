//! Rajomon admission control (`ac_rajomon`): token-gated admission with prices
//! that propagate up the call graph in responses.
//!
//! Rajomon keeps process-wide state keyed by method name, so each scenario
//! uses method names of its own. The process runs with a price attached to
//! every response, and no background price-updating worker (see
//! `tests/rajomon_worker` for the dynamics of the price itself).

use std::time::Duration;

use tonic::Code;

use super::common::*;
use crate::harness::*;

fn request_with_tokens<H: Hooks>(w: &World<H>, tokens: u64) -> Inbound {
    w.crafted("RajApi", dur_ms(10_000)).tokens(tokens).build()
}

const REJECTED: &str = "reason=RajomonAdmissionRej";

scenario! {
    /// A request is admitted exactly when its tokens cover the price of the
    /// method it calls: that method's own price plus the highest price any of
    /// its children advertised. Rejection is ResourceExhausted with an
    /// EarlyReturn message, and the rejected request stays rejected at every
    /// later hook (child calls, pending polls) though it may still finish.
    fn admission_requires_tokens_to_cover_the_accumulated_price(w) {
        let svc = w.service("RajGate");
        learn_price(w, &svc, "Gate", "RajGateDown", "Down", 50);

        let poor = request_with_tokens(w, 10);
        let h = svc.accept("Gate", &poor);
        let st = h.poll_begin().unwrap_err();
        assert_eq!(st.code(), Code::ResourceExhausted);
        assert_eq!(st.message(), format!("/EarlyReturn?src=RajGate::Gate&{REJECTED}"));
        match h.call_remote("RajGateDown", "Down").outbound_or_untouched() {
            Ok(_) => panic!("a rejected request must not call children"),
            Err((status, untouched)) => {
                assert_eq!(status.code(), Code::ResourceExhausted);
                assert_eq!(status.message(), format!("/EarlyReturn?src=RajGate::Gate&{REJECTED}"));
                assert!(untouched);
            }
        }
        assert!(h.poll_yield().is_err());

        for tokens in [50, 51, 100] {
            let rich = request_with_tokens(w, tokens);
            let h = svc.accept("Gate", &rich);
            assert!(h.poll_begin().is_ok(), "tokens {tokens} cover price 50");
        }
        let edge = request_with_tokens(w, 49);
        assert!(svc.accept("Gate", &edge).poll_begin().is_err());
    }
}

scenario! {
    /// Replies echo the token budget the request arrived with (not what was
    /// left after its own price or its children), on success and on error.
    fn replies_echo_the_inbound_tokens(w) {
        let svc = w.service("RajEcho");
        let req = request_with_tokens(w, 33);
        let reply = svc.serve("Entry", &req, |h| {
            h.call_remote("RajEchoDown", "Down")
                .returns(Duration::from_millis(1), Reply::bare_ok())?;
            Ok(())
        });
        assert_eq!(reply.view().unwrap().tokens, Some(33));
        let req = request_with_tokens(w, 0);
        let reply = svc.serve("Entry", &req, |_h| Err(tonic::Status::internal("boom")));
        assert_eq!(reply.view().unwrap().tokens, Some(0));
    }
}

scenario! {
    /// A method with no known downstream price costs nothing: even a request
    /// with zero tokens is admitted, and zero is forwarded as zero, not
    /// replaced by a default budget.
    fn zero_tokens_are_a_real_budget(w) {
        let svc = w.service("RajZero");
        let req = request_with_tokens(w, 0);
        let h = svc.accept("Free", &req);
        assert!(h.poll_begin().is_ok());
        let out = h.call_remote("RajZeroDown", "Down").outbound().unwrap();
        assert_eq!(out.view().tokens, Some(0));
    }
}

scenario! {
    /// A request that carries no token budget at all is treated as having 100.
    fn missing_budget_defaults_to_one_hundred(w) {
        let svc = w.service("RajDefault");
        let req = w.ingress("RajApi", dur_ms(10_000));
        let h = svc.accept("Entry", &req);
        let out = h.call_remote("RajDefaultDown", "Down").outbound().unwrap();
        assert_eq!(out.view().tokens, Some(100));
    }
}

scenario! {
    /// An admitted request forwards all its remaining tokens to its children:
    /// this process's own price is deducted first (zero here), so a child
    /// receives the tokens the parent arrived with.
    fn child_receives_the_remaining_tokens(w) {
        let svc = w.service("RajForward");
        for tokens in [1, 7, 100, 12_345] {
            let req = request_with_tokens(w, tokens);
            let h = svc.accept("Entry", &req);
            let out = h.call_remote("RajForwardDown", "Down").outbound().unwrap();
            assert_eq!(out.view().tokens, Some(tokens));
        }
    }
}

scenario! {
    /// An admitted request may only call children whose advertised price its
    /// remaining tokens cover: with 10 tokens and a child priced at 50 the child
    /// call is rejected before anything is sent, naming the child as `last_rpc`.
    fn child_call_is_rejected_beyond_the_budget(w) {
        let svc = w.service("RajBudget");
        learn_price(w, &svc, "Learn", "RajBudgetDown", "Down", 50);

        // A different parent method has no downstream price, so it is admitted
        // with 10 tokens, yet cannot afford the priced child.
        let req = request_with_tokens(w, 10);
        let h = svc.accept("Other", &req);
        assert!(h.poll_begin().is_ok());
        match h.call_remote("RajBudgetDown", "Down").outbound_or_untouched() {
            Ok(_) => panic!("the child call exceeds the budget"),
            Err((status, untouched)) => {
                assert_eq!(status.code(), Code::ResourceExhausted);
                assert_eq!(
                    status.message(),
                    "/EarlyReturn?src=RajBudget::Other?last_rpc=RajBudgetDown::Down&reason=RajomonChildBudgetRej"
                );
                assert!(untouched);
            }
        }
        // With 50 the same call goes through.
        let req = request_with_tokens(w, 50);
        let h = svc.accept("Other", &req);
        assert!(h.call_remote("RajBudgetDown", "Down").outbound().is_ok());
    }
}

scenario! {
    /// A service advertises its accumulated price in every response it sends:
    /// its own price (zero) plus the highest price among its children.
    fn responses_advertise_the_accumulated_price(w) {
        let svc = w.service("RajAdvertise");
        let req = request_with_tokens(w, 100);
        assert_eq!(svc.serve("Leaf", &req, |_h| Ok(())).price(), Some(0));

        learn_price(w, &svc, "Gate", "RajAdvChildA", "A", 30);
        learn_price(w, &svc, "Gate", "RajAdvChildB", "B", 70);
        let req = request_with_tokens(w, 100);
        assert_eq!(svc.serve("Gate", &req, |_h| Ok(())).price(), Some(70));
    }
}

scenario! {
    /// Error replies advertise the price too, and so do rejections.
    fn error_and_rejected_replies_advertise_the_price(w) {
        let svc = w.service("RajErrPrice");
        learn_price(w, &svc, "Gate", "RajErrChild", "Down", 40);
        let req = request_with_tokens(w, 100);
        let reply = svc.serve("Gate", &req, |_h| Err(tonic::Status::internal("boom")));
        assert_eq!(reply.code(), Code::Internal);
        assert_eq!(reply.price(), Some(40));

        let poor = request_with_tokens(w, 1);
        let reply = svc.serve("Gate", &poor, |_h| Ok(()));
        assert_eq!(reply.code(), Code::ResourceExhausted);
        assert_eq!(reply.price(), Some(40));
    }
}

scenario! {
    /// The downstream price is the maximum over children and follows updates:
    /// when the priciest child advertises a lower price later, the parent's
    /// price falls to the next highest.
    fn downstream_price_is_the_max_and_follows_updates(w) {
        let svc = w.service("RajMax");
        learn_price(w, &svc, "Gate", "RajMaxA", "A", 30);
        learn_price(w, &svc, "Gate", "RajMaxB", "B", 70);
        let req = request_with_tokens(w, 100);
        assert_eq!(svc.serve("Gate", &req, |_h| Ok(())).price(), Some(70));
        learn_price(w, &svc, "Gate", "RajMaxB", "B", 10);
        let req = request_with_tokens(w, 100);
        assert_eq!(svc.serve("Gate", &req, |_h| Ok(())).price(), Some(30));
    }
}

scenario! {
    /// A child that fails still teaches its price: error replies are read for
    /// the price header just like successful ones.
    fn failed_child_still_advertises_its_price(w) {
        let svc = w.service("RajFail");
        let req = request_with_tokens(w, 1_000_000);
        let h = svc.accept("Gate", &req);
        assert!(h.poll_begin().is_ok());
        let failed = Reply::err_with_price(tonic::Status::internal("child broke"), 25);
        let _ = h.call_remote("RajFailDown", "Down").returns(Duration::from_millis(1), failed);
        h.finalize_now(Ok(()));
        let req = request_with_tokens(w, 24);
        assert!(svc.accept("Gate", &req).poll_begin().is_err());
        let req = request_with_tokens(w, 25);
        assert!(svc.accept("Gate", &req).poll_begin().is_ok());
    }
}

scenario! {
    /// Prices are cached per parent method: a price learned on behalf of one
    /// method does not make a different method of the same service reject.
    fn prices_belong_to_the_calling_method(w) {
        let svc = w.service("RajScope");
        learn_price(w, &svc, "Gate", "RajScopeDown", "Down", 90);
        let req = request_with_tokens(w, 5);
        assert!(svc.accept("Unrelated", &req).poll_begin().is_ok());
        assert!(svc.accept("Gate", &req).poll_begin().is_err());
    }
}
