# `hotel-goodput` task

A VibeSys task in which the agent optimizes Masa's RPC policy (not application
code) for goodput on the Rust Hotel Reservation app. `OBJECTIVE.md` is the
agent-facing brief.

## Evaluator

`benchmark/masa_hotel.py` is the shim between VibeSys and Masa's experiment
runner (`exp_runner`). Both gates call it:

| Gate | What it does |
| --- | --- |
| `accuracy` | Scope check against `BASELINE_COMMIT` (only `libs/masa-policy/src/agent/`, `libs/rpcstack-sched/src/custom.rs` and `replay_tests.rs` may change); `cargo check` with `-D warnings`, `rpcstack-sched` tests and the slot integration tests under the task features; then `exp_runner` on `benchmark/experiments/accuracy` (light load, 300 rps), where at most 1% of requests may fail and at least 97% must be within SLO, per API. |
| `benchmark` | Scope check, then `exp_runner` on `benchmark/experiments/benchmark` (Poisson sweep past saturation). Goodput is computed from the load generator's per-request CSVs and written with the VibeSys evaluator result protocol v2 (`benchmark/vs_protocol.py`). The lightest load point must also pass the 97% check. |

A request counts toward goodput when its status is OK (`/None`) and its latency
is at most its SLO. Goodput at a load point is that count divided by the
measured window (`DurationSecs`). `mean_goodput_rps` averages it over the
sweep.

The shim copies an experiment config into `exp/hotel/in/vibesys_<gate>/`
(`exp/` is git-ignored) and writes the build features into its `policies`
file. Runs are serialized with a lock on `/tmp/vibesys-masa-hotel.lock`,
because `exp_runner` image tags and compose resources are shared on a Docker
host.

## Host requirements

Docker Engine reachable without `sudo`; rustup (the repository pins its
toolchain in `rust-toolchain.toml`); `protoc` (`PROTOC` or on `PATH`); `uv`;
Python 3.11 or newer. The shim also looks for `cargo` and `uv` in
`~/.cargo/bin` and `~/.local/bin`.

## Run it by hand

```bash
python3 .vibesys/tasks/hotel-goodput/benchmark/masa_hotel.py accuracy
python3 .vibesys/tasks/hotel-goodput/benchmark/masa_hotel.py benchmark --vs-output /tmp/hotel.jsonl
```
