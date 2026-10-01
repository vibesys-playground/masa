# masa-bench

A dev-only benchmark suite (`publish = false`, nothing depends on it) that
measures what the Masa RPC stack costs per RPC, and compares two git refs so a
change to the stack can be judged before it reaches an experiment.

It answers questions like "did this refactor make a request through
`sched_slo,abort_slo` slower, and where?". It does not answer "does goodput
under load improve" (see [What is not measured](#what-is-not-measured)).

## What each benchmark measures

Each benchmark is a `harness = false` binary under `benches/` that prints
`key value` lines on stdout (`<bench>.<variant>.<metric> <number>`, always
lower-is-better) and a human-readable summary on stderr. Run one directly with
`cargo bench -p masa-bench --bench hook_cost --features sched_slo`.

| Benchmark | Measures |
|---|---|
| `hook_cost` | Hook overhead of one RPC with one child call, through the stable `tonic::masa` hook traits on `masa::DefaultHooks`: total ns per RPC; ns per phase (client request creation, `begin`, `before_poll` x3, `after_poll` x3, `before_child_rpc`, the callee's hops, `after_child_rpc`, finalize, drop); the cost of one `before_poll` + `after_poll` pair; allocations and bytes allocated per RPC (a counting global allocator); and the size in bytes of the `ctx` headers of a request, a child request and a response. No network and no handler logic run. |
| `priority_read` | `masa::read_priority_from_headers`, the call the vendored Hyper makes for every HTTP/2 stream before spawning the handler, and the full context decode for comparison. |
| `wire_codec` | Encoding a root request's `ctx` header, copying metadata into HTTP headers, decoding the whole header, decoding one section only, and the sum of these for one hop. |
| `spawn_poll` | Tokio current-thread spawn and poll throughput (ns per poll) under one run queue: FIFO (default), `queue_prio` (the priority heap, i.e. `sched_slo`), `queue_tailclipper` or `queue_custom`. Tasks yield twice, so each is polled three times. |
| `run_queue` | The built-in queues of `rpcstack-sched` (FIFO, priority heap, TailClipper, and the editable `custom` queue) driven directly, without a runtime: `push` + `pop` per step, and the full lifecycle (`pop`, `on_poll_start`, `on_poll_end`, `push`, ..., `on_task_exit`) per poll, at queue depths 16 and 4096. This is the real queue code; `libs/tokio/benches/prio_bh_bench` benchmarks a mock copy. |

The Masa feature set is chosen when the benchmark is built (Cargo features of
this crate: `sched_slo`, `sched_pred`, `abort_slo`, `abort_slack`, `ac_pred`,
`ac_rajomon`, `est_mean_var`, `estimator`, `trace_queue_latency`). Output keys
carry the set, for example `hook_cost.sched_slo,abort_slo.total_ns`.

Other timing tests in the repository, not part of this suite:
`apps/benchmark` (`cargo bench -p masa-benchmark`, criterion, end-to-end
latency over a loopback socket; it binds a fixed port, so run it alone) and the
ignored tests `libs/masa-policy/tests/wire_bench.rs` and
`estimation_hook_bench.rs`.

## Comparing two refs

```bash
scripts/bench.sh <baseline-ref> [<candidate-ref>=HEAD] \
    [--features SET]... [--queues fifo,prio,tailclipper,custom] \
    [--rounds N] [--core C] [--workdir DIR] [--out FILE] [--keep]
```

For example, `scripts/bench.sh origin/main origin/rpcstack/integration
--rounds 5 --workdir /mnt/data/me/bench-work`. The script

1. refuses to start with less than 150 GB free under the work directory (two
   release builds of the workspace need many gigabytes);
2. checks out each ref in a detached worktree, copies this checkout's
   `libs/masa-bench` into a ref that lacks it, and picks the compat layer (see
   below);
3. builds each side in release mode with its own target directory and
   `CARGO_INCREMENTAL=0`, one build per Masa feature set and per run queue;
4. runs every benchmark binary in interleaved rounds (round 1: baseline then
   candidate, round 2: candidate then baseline, ...), pinned with `taskset` to
   the idlest core unless `--core` is given, under `perf stat -e
   cycles,instructions` when `perf` works (`perf.*.cycles_G` and
   `perf.*.instructions_G` rows, in billions, for the whole process);
5. prints the comparison table and removes the worktrees and target
   directories (`--keep` retains them and the raw results).

The default hook feature sets are `sched_slo`, `sched_slo,abort_slo`,
`sched_slo,ac_rajomon`, `sched_pred,abort_slack,ac_pred,est_mean_var` and
`sched_slo,trace_queue_latency`. Pass `--features` (repeatable) to replace them.
The default work directory is under `/tmp/claude-$UID` when that exists; if
`/tmp` is a network file system, pass a `--workdir` on a local disk.

### Reading the table

`scripts/bench_compare.py <results-dir>` produces one row per metric:

```
metric  base min  base med  base spr%  cand min  cand med  cand spr%  delta%  verdict
```

- `min` and `med` are the minimum and median over rounds. The comparison uses
  the minimum: noise on a shared machine only ever adds time, so the fastest
  round is the best estimate of the cost.
- `spr%` is the spread, `(max - min) / min`, of the rounds. A spread near or
  above the delta means the row cannot be trusted.
- `delta%` is the change of the minimum, candidate relative to baseline.
  Positive is slower or larger.
- `verdict` is `regression` or `improvement` when the minimum moved by more
  than 3% and the median moved the same way, `noise` otherwise. A metric that
  did not vary at all on either side (allocation and byte counts) is judged on
  any change. `new` and `removed` mark metrics only one side has.

### Noise

- Pin to one core that nothing else uses; the script picks the idlest one.
  Another process on the same core or its hyper-thread sibling adds 10-30%.
- The machine can be bimodal: the same binary runs in one of two speeds,
  depending on which core and frequency state it lands in, for the whole
  process lifetime. Interleaving puts both sides in the same state per round,
  and the minimum over several rounds picks the fast mode. If a row's spread is
  large, run more rounds (`--rounds 9`) before believing it.
- Do not run two benchmark sessions at once on the same cores, or the
  `apps/benchmark` end-to-end bench (fixed port) alongside.
- Absolute numbers depend on the machine; compare only numbers from one
  session. The reference numbers below are for orientation.

## Supporting an older or newer ref (compat)

The benchmarks use only the `tonic::masa` hook traits and `masa` facade items
that every supported ref has. The one API difference, how a root request gets
its context, is isolated in `src/compat.rs`:

- current refs: `masa::RootContext::from(context)`, wire data such as
  `with_rajomon_tokens`, `attach(request)`;
- `legacy-main` refs (before the rpcstack refactor, e.g. `origin/main` at
  7ad21552b): tokens in the context builder, `Request::set_masa_context`.

`scripts/bench.sh` enables `legacy-main` when the ref's `libs/masa/src/lib.rs`
has no `RootContext`, and strips the Cargo.toml blocks fenced by `# BEGIN
rpcstack-only` / `# END rpcstack-only` (the `rpcstack-sched` dependency, the
`queue_custom` feature and the `run_queue` benchmark) when the ref has no
`libs/rpcstack-sched`.

To add a compat layer for a future ref whose API differs again:

1. Add a module in `src/compat.rs` with the same functions (today only
   `root_request`), gated on a new feature such as `legacy-foo`, and add the
   feature to `Cargo.toml`. If a benchmark needs another difference isolated,
   grow `compat.rs`; do not add `cfg`s to a benchmark.
2. Teach `prepare_worktree` in `scripts/bench.sh` how to detect the ref
   (a `grep` for a symbol, as for `RootContext`) and print the feature.
3. If the ref lacks crates the benchmarks import, fence those lines in
   `Cargo.toml` and the benchmark with the rpcstack-only markers or add a
   similar pair of markers and a strip rule.
4. Check that `scripts/bench.sh <old-ref> <old-ref>` runs and shows only noise.

## What is not measured

- Goodput (requests completed within their SLO) under load. That needs
  workloads, many processes and long runs: use the experiment harness
  (`exp_runner`, see `docs/experiments/workflow.md`). This suite is not a
  substitute for it; it tells you only the per-request overhead a policy adds.
- The multi-threaded scheduler (`sched_mt`, `sched_mt_multiqueue`). The
  benchmarks use the current-thread runtime, as the applications do.
- Network, serialization of message bodies, the transport and Hyper beyond the
  priority read, and handler work.
- Background workers (Rajomon price updates, estimator state updates) beyond
  what runs inline in the hooks.

## Reference numbers

`scripts/bench.sh origin/main origin/rpcstack/integration`, release, pinned,
minimum over interleaved rounds. Baseline: `origin/main` at 7ad21552b.
Candidate: the rpcstack integration branch at 99a385914 (reference measurement
by the original scratch benchmarks).

Per-RPC hook cost (`hook_cost`, ns per RPC with one child call; the first
column is the Masa feature set; "ref" is the original scratch measurement at
99a385914, "run" is this suite on 6a025009e, 5 rounds, core 15):

| Feature set | baseline ref | candidate ref | baseline run | candidate run |
|---|---|---|---|---|
| `sched_slo` | 2383 | 3453 | 2411 | 3470 |
| `sched_slo,abort_slo` | 2696 | 3866 | 2759 | 3841 |
| `sched_slo,ac_rajomon` | 3168 | 5851 | 3150 | 5384 |
| `sched_pred,abort_slack,ac_pred,est_mean_var` | 8568 | 9671 | 8319 | 9850 |
| `sched_slo,trace_queue_latency` | 3589 | 4846 | 3613 | 4858 |

Other metrics (`sched_slo`, baseline to candidate; ref in parentheses):

| Metric | Baseline | Candidate |
|---|---|---|
| allocations per RPC | 34 (34) | 45 (45) |
| bytes allocated per RPC | 3874 | 5800 |
| `ctx` header, request / response, bytes | 75 / 75 | 106 / 106 |
| `before_poll` + `after_poll` pair, ns | 3.9 (0.8) | 37.2 (38.6) |
| `read_priority_from_headers`, ns | 186 (192.6) | 158 (160.2) |
| tokio spawn+poll, `queue_prio`, ns per poll | 539 (566) | 414 (428) |
| tokio spawn+poll, FIFO, ns per poll | 241 | 246 |

Per RPC the candidate costs more time (+1000 ns for `sched_slo`), 11 more
allocations and a larger header (106 bytes instead of 75), and polls the
priority queue and reads a stream's priority faster. The poll pair of the
baseline is below 4 ns (its hooks are nearly empty, so the figure is dominated
by loop overhead and varies between 0.8 and 3.9 ns between builds); only the
candidate's 37 ns is a stable figure. The Rajomon candidate row differs from the reference by 8%, the
other rows by 4% or less. Spread of the baseline `sched_pred,...` rows is about
30% (the machine's slow mode was hit in some rounds), which is why the minimum
is compared.

`run_queue` (candidate only, the baseline has no `rpcstack-sched`; ns per
step): FIFO 2.5 for push+pop, priority heap 18.4 at depth 16 and 51.4 at depth
4096, TailClipper 27.5 and 66.3.
