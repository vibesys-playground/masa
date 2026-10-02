# Masa Hotel Reservation: goodput through custom RPC policy

Raise the **goodput** of Masa's Rust Hotel Reservation app (`apps/hotel`) by
changing the **RPC framework's policy**, not the application. Goodput is the
number of requests per second that succeed within their end-to-end SLO. Search
has a 200 ms SLO and Reservation a 100 ms SLO.

The benchmark drives Hotel with open-loop Poisson arrivals at a sweep of rates,
most of them past saturation. The objective `mean_goodput_rps` is goodput
averaged over the load points. Past saturation, the run queue fills, so which
work runs first, which work is abandoned, and which work is admitted decide
how many requests finish in time.

## What you may change

Every Hotel service is built with the fixed features
`sched_slo,stack_custom,sched_custom`. Those features route policy through two
agent-owned slots. Only these files may change; the accuracy gate rejects a
candidate that touches anything else (apps, built-in modules, framework crates,
vendored Tokio/Hyper/Tonic, `Cargo.toml`/`Cargo.lock`, `exp_runner`, scripts):

1. **Policy stack**: `libs/masa-policy/src/agent/` (new files plus `mod.rs`).
   `AgentStack` is the module stack every server and client stub runs. It starts
   as `crate::MasaStack`, which under `sched_slo` only propagates the deadline
   budget. Modules implement `rpcstack::Module` hooks:
   - `new`: a request arrives;
   - `before_poll`/`after_poll`: around each poll of the handler; can end the
     request, or reprioritize it with `tokio::task::reprioritize`;
   - `before_child_rpc`/`seal_child_rpc`: before each downstream call; can set
     the child's deadline and priority, or reject the call;
   - `after_child_rpc`: a downstream response arrived (record latencies);
   - `finalize`: before the response is serialized.
2. **Run queue**: `libs/rpcstack-sched/src/custom.rs`. `custom::Queue` decides
   which runnable task the current-thread Tokio runtime polls next. It starts as
   the deadline-ordered priority heap. Update the expected pop orders in
   `libs/rpcstack-sched/src/replay_tests.rs` (`custom_replay_scripts`) when you
   change the order on purpose.

Read `docs/POLICY_MODULES.md` first. It documents the module contract, hook
order, decisions, wire data and both slots. `libs/masa-policy/src/module/`
holds the built-in modules (budget, deadline guard, estimation, admission) as
examples. Built-in modules other than `BudgetModule` are compiled out under the
task features, so write your own modules in `agent/` (copying and adapting is
fine).

## Rules

- Keep `BudgetModule` first in `AgentStack`. Without it, requests carry no
  deadline or priority to the next hop.
- Never delay infrastructure work (`PriorityHint::infra()`, `Meta(0)`). The
  queue must drain it first.
- Use `masa::DefaultHooks` everywhere; one hooks type per binary.
- Every service is built with the same features, so wire sections stay
  compatible. Do not add dependencies.
- Rejecting or aborting a request (a `ResourceExhausted` or
  `DeadlineExceeded` status) never counts as goodput. The gates fail a
  candidate that does not serve at least 97% of requests within SLO, per API,
  at light load, or that fails more than 1% of requests in the accuracy run.
- Hard-coding knowledge of Hotel's services and methods (by service or method
  name) is allowed when it measurably helps.

## Measuring

- The framework runs both gates itself; see `.vibesys/tasks/hotel-goodput/README.md`.
- Check a change quickly with
  `RUSTFLAGS="-D warnings" cargo check --features sched_slo,stack_custom,sched_custom`
  and `cargo test -p rpcstack-sched --features sched_custom`.
- A full benchmark takes several minutes and needs the Docker host to itself.
  To measure locally, run
  `python3 .vibesys/tasks/hotel-goodput/benchmark/masa_hotel.py benchmark`
  from the repository root. It prints goodput per load point.
