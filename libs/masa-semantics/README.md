# masa-semantics

Server-free tests of what Masa's policy does to a request: deadlines,
priorities, admission, aborts and response metadata as it flows through a
virtual call graph. The suite must keep passing across refactors of the policy
machinery, so it depends only on public surfaces (`masa::DefaultHooks`, the
`tonic::masa` hook traits, `masa-core`'s `Context`, and `masa`'s
request/response/status extensions).

## Layout

- `tests/semantics/harness.rs`: the only file that knows how hooks are driven.
  It builds inbound requests, runs the hook lifecycle of virtual services,
  feeds one service's outbound request to the next, and reads results back as
  plain views. It is generic over `H: tonic::masa::Hooks`; `Under` selects the
  implementation under test.
- `tests/semantics/scenarios/*.rs`: pure semantics written against the harness
  API. Each test's doc comment states the rule it pins.
- `tests/rajomon_worker/`, `tests/rajomon_price_freq_5/`,
  `tests/rajomon_price_freq_0/`: Rajomon behavior that depends on process-wide
  state (its background price worker and refill worker start once per process,
  and the price-propagation probability is configuration read once), so each
  gets a process of its own. `rajomon_worker` runs with tokio's clock paused and
  advances it explicitly; it is the only place where a real delay enters, as a
  lower bound on how long a request waited in the runtime queue.

## Running

```bash
cargo test -p masa-semantics --features sched_slo,abort_slo
```

Scenarios are gated by the same feature names as the rest of Masa. The sets CI
runs are listed in `scripts/test.sh` (`semantics_combos`).

## Determinism

Virtual time replaces the wall clock: the `test_clock` feature of `masa-core`
(enabled by this crate) swaps `time_now()` and `masa_core::Instant` for a
per-thread virtual counter that only moves when a scenario advances it. Nothing
sleeps. Where Masa draws random numbers (admission coin flips, price
propagation) scenarios either pin the probability to 0 or 1, or compare
frequencies over many draws with a tolerance of at least six standard
deviations.
