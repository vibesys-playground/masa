# rajomon

Rajomon admission control (NSDI '25, "Rajomon: Decentralized and Coordinated
Overload Control for Latency-Sensitive Microservices") as an `rpcstack` module.
It depends on the framework crates and generic libraries only, never on a Masa
crate or hyper; `scripts/validate_rpcstack_boundary.py` checks this.

- `RajomonModule`: the module. Admits a request iff its tokens cover the
  accumulated price (own price plus the largest downstream price), forwards the
  remaining tokens to child RPCs, learns downstream prices from child responses
  and, with probability `1/price_freq`, advertises its price in its own response.
- `RajomonWire`: the module's wire section (`tokens`, optional `price`).
- `RAJOMON_STATE` / `RajomonSharedState`: process-wide price tables and the
  price-update worker, started by the first request inside a tokio runtime.
- `CLIENT_TOKEN_BUCKET` / `ClientTokenBucket`: the client-side token bucket that
  root clients spend from and a replenishment worker refills.
- `RajomonParams`: tunables. `RajomonParams::global()` reads, once, the JSON file
  named by `RAJOMON_PARAMS_PATH`, or else `MASA_POLICY_PARAMS_PATH`, and takes
  its `"rajomon"` section; other sections are ignored, so a Masa params file
  works unchanged. Unset or unreadable means defaults.

`tests/toy_stack.rs` runs the module in a stack of the framework alone.
