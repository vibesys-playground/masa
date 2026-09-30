# Context consumers survey (rpcstack refactor)

Snapshot of `origin/rpcstack/integration` at 03ffb9955 (plus this doc). Every claim is a `file:line` from `git grep` on that tree.
R = read, W = write, S = serialize or parse on the wire. "No consumer found" means the grep over `*.rs` and `*.py` found none.

Abbreviations: `core` = `libs/masa-core/src`, `pol` = `libs/masa-policy/src`, `polT` = `libs/masa-policy/tests`, `tonT` = `libs/tonic/tests/masa_integration_tests/tests`.

## 0. Facts that change the plan

1. The wire format today is **bincode (positional, no field names) then base64**, not JSON: `core/context.rs:452-463`. The plan doc (`docs/RPCSTACK_REFACTOR.md`) says JSON keyed by module name; that is a new format. JSON exists only for debugging (`core/context.rs:442-449`).
2. The struct layout on the wire depends on Cargo features: `Context` fields are `#[cfg(feature=...)]` at `core/context.rs:144-159`. The comment at `core/context.rs:138-142` says all binaries must share a feature set. `#[serde(default)]` does not help bincode (see comment at `core/context.rs:68-70`).
3. **Hyper parses the full Context on every incoming HTTP/2 stream just to get one integer.** `libs/hyper/src/common/exec.rs:97-101` calls `masa_core::read_priority_from_headers`, which calls `read_context_from_headers(...).prio_hint()` (`core/header.rs:20-22`). Hyper therefore depends on `masa-core` (`libs/hyper/Cargo.toml:39`, `:94`). Tonic does not (enforced by `scripts/validate_tonic_masa_boundary.py:12`, which checks only tonic, not hyper).
4. Tokio does **not** depend on `masa-core`. It has its own `TaskPriority` (`libs/tokio/tokio/src/masa/priority.rs:6`) and hyper converts: `TaskPriority::new(hint.value())` (`exec.rs:100`). Priority 0 is special-cased in tokio queues (`libs/tokio/tokio/src/masa/scheduler/prio_heap.rs:49`, `tailclipper.rs:50`).
5. `ContextBuilder::build` assigns the priority from cfg flags (`core/context.rs:296-309`): `sched_tailclipper` uses gateway_entry, `sched_pred` uses `deadline - now`, else `deadline`. Every root context goes through it, so the root priority rule lives in masa-core.
6. The child request's context is built by `ContextBuilder::from(&parent_ctx)` and overriding four fields (`pol/hooks.rs:131-141`). Everything not overridden (slo, gateway_entry, api, request_id, `response`, `root_method`, queue latencies, frontend_elapse) is copied from the parent into the child request. In particular the parent's `estimator.response` and `queue.latencies` are copied into outgoing requests (`core/context.rs:218-226`), which is probably accidental. A new per-module wire type must decide copy vs reset explicitly.
7. `ctx` is read by literal name in two apps, bypassing `MASA_CONTEXT_HEADER`: `apps/app-utils/src/load_gen.rs:416` and `apps/tracebench/generic-service/src/replay.rs:322`. `apps/benchmark/src/main.rs:67` removes a key `"x-masa-context"` that is never the real key (`ctx`), so the benchmark's "cleanup" is a no-op.
8. The same `ctx` header is used in both directions: requests carry it client to server, responses (and error `Status` metadata) carry it server to client with response-only data (`estimator.response`, `queue.latencies`): `pol/hooks.rs:167-174`. So module wire types need a direction story (request-side vs response-side data), which `Context` currently mixes into one struct.
9. No Python, shell, Lua, Go or gateway code parses or builds `ctx`. `git grep` for `ctx`, `bincode`, `base64`, `MASA_CONTEXT` over `*.py`, `*.sh`, `*.lua`, `*.go`, `*.yaml` found nothing relevant (only `exp_runner/runner/apps/tracebench.py:498`, a ConfigMap size comment). Python consumes only CSV columns that Rust load generators write (`start_at`, `queue_lengths`, ...; see section 2.3).
10. `hop_count == 0` is the de facto "am I ingress" test, used by estimation and predictive admission (`pol/layer/est/layer.rs:60`, `pol/layer/admission/predictive/mod.rs:105,234,253`). It is incremented only when feature `estimator` is on (`pol/layer/mod.rs:119-120`).

## 1. Field-by-field consumer table

### 1.1 `Context` (`core/context.rs:144-159`), `RequestContext` (`:51`), `ContextBuilder` (`:167`)

Builder setters are in `core/context.rs:232-285`; `ContextBuilder::from` copies every field (`:210-230`); `build()` at `:287`. Only consumers outside `core/context.rs` are listed.

| Field / accessor | Gate | Consumers |
|---|---|---|
| `request.api` / `api()` (`:334`) | none | **masa-core** R: `core/header.rs:74` (test). **masa-policy** R: `pol/context_ext.rs:285` (test), `polT/stack_characterization.rs:236,356`. **apps** R: `apps/app-utils/src/load_gen.rs:469`. **core tests** R: `core/tests/context_roundtrip.rs:30`. Set via `ContextBuilder::new(api, id)`, see section 2.2. No hyper/tonic/tokio consumer. |
| `request.request_id` / `request_id()` (`:339`) | none | R: `apps/app-utils/src/load_gen.rs:470`; tests `core/header.rs:75`, `pol/context_ext.rs:284`, `polT/stack_characterization.rs:237,357,392`, `core/tests/context_roundtrip.rs:31`. W: `ContextBuilder::new` (all callers in 2.2). `masa::create_context` assigns from a process-wide counter (`libs/masa/src/lib.rs:77`). No policy module reads it. |
| `request.slo` / `slo()` (`:344`) | none | R: `apps/app-utils/src/load_gen.rs:471`; `e2e_deadline()` (`core/context.rs:359`) reads slo + gateway_entry, consumers below. W: builders in 2.2. tests `polT/stack_characterization.rs:238,834`. |
| `request.gateway_entry` / `gateway_entry()` (`:349`) | none | R: `apps/app-utils/src/load_gen.rs:472`; `build()` for tailclipper priority (`core/context.rs:298`). W: builders in 2.2. |
| `request.deadline` / `deadline()` (`:354`) | none | **masa-policy** R: `pol/layer/mod.rs:117` (ChildRpcContext seed), `pol/layer/e2e_deadline_guard.rs:62`, `pol/layer/est/layer.rs:86,103,205,264,265,271`, `pol/layer/est/signal_slack.rs:24`. **apps** R: `apps/app-utils/src/load_gen.rs:473`. W: `pol/hooks.rs:132` (child). Also read by `build()` for default priority (`core/context.rs:304,308`). Tests: many. |
| `e2e_deadline()` (derived, `:359`) | none | R: `pol/layer/e2e_deadline_guard.rs:53`, `pol/layer/est/layer.rs:123`, `pol/layer/admission/predictive/mod.rs:156`, `pol/layer/oracle.rs:44`. Comment at `e2e_deadline_guard.rs:50` explains why the guard uses this and not `deadline()`. |
| `request.prio_hint` / `prio_hint()` (`:380`) | none; default computed by cfg in `build()` | **hyper** R+S (parses whole ctx to read it): `libs/hyper/src/common/exec.rs:100` via `core/header.rs:20-22`. **masa-policy** R: `pol/layer/mod.rs:118`, `pol/layer/est/layer.rs:272`. W: `pol/hooks.rs:133`, via `child_rpc.prio_hint` set at `pol/layer/est/layer.rs:161`, `pol/layer/oracle.rs:48`. **tests** W/R: `polT/custom_stack.rs:36,79,205,244-287`, `polT/stack_characterization.rs:227,245,249,317,321`, `core/tests/*`, `hyper exec.rs:179` (test). |
| `request.frontend_elapse` / `frontend_elapse()` (`:385`), `set_frontend_elapse` (`:390`), builder `frontend_elapse` (`:252`) | none | W: `apps/hotel/src/frontend/server.rs:229,281`. **No reader found anywhere** (grep over `*.rs`, `*.py`). Copied by `ContextBuilder::from` (`core/context.rs:218`). Likely dead. Note: the server then sets `response.set_masa_context(&ctx)` (`server.rs:230,282`) but `finalize_before_serialization` re-sets the ctx header from the hook's own clone (`pol/hooks.rs:167-174`); I did not run it to see which wins, so whether the value ever reaches a client is undetermined. |
| `request()` (`:394`) | none | No consumer outside `core/context.rs` found. |
| `Context::default()` (`:161-165`) | none | W (root-ish, api default, slo 0, deadline 0): `apps/hotel/src/client/client_bench.rs:72`; tests `pol/layer/est/state/tests.rs:349,360,374,391,404`, `pol/layer/admission/predictive/mod.rs:626`. |
| `to_json` / `from_json` (`:447,442`) | none | S: `core/tests/context_roundtrip.rs:45-46` only. Docs mention it (`docs/MASA_POLICY_IMPL.md:111`). |
| `to_header_string` / `from_header_string` (`:452,461`) | none | S, see section 2.1. |

### 1.2 Estimator state (feature `estimator`): `EstimatorContext` (`:116`), `EstimatorResponse`/`ResponseMeta` (`:83`, `:103`), `RootMethod` (`:109`)

| Field / accessor | Consumers |
|---|---|
| `estimator.hop_count` / `hop_count()` (`:425`) / builder `hop_count` (`:270`) | R: `pol/layer/mod.rs:120` (+1 to seed child), `pol/layer/est/layer.rs:60`, `pol/layer/admission/predictive/mod.rs:105,234,253`. W: `pol/hooks.rs:136` (child). Tests: `polT/stack_characterization.rs:253,271,283,313,364`. Also `ChildRpcContext.hop_count` (below). Semantics: 0 means ingress (`pol/layer/est/layer.rs:59`). |
| `estimator.root_method` / `root_method()` (`:431`), `set_root_method()` (`:437`), builder `root_method` (`:282`) | W: `pol/layer/est/layer.rs:61` (ingress only). R: `pol/layer/est/layer.rs:66`, `pol/layer/admission/predictive/mod.rs:84` (**predictive admission reads a value written by the estimation module**). Not overridden on the child, so it propagates by `ContextBuilder::from` (`core/context.rs:226`). Tests: `polT/stack_characterization.rs:254,272,284,365`. |
| `estimator.response` / `response_meta()` (`:413`), `set_response_meta()` (`:419`), builder `response_meta` (`:264`) | W: `pol/layer/est/state/metadata.rs:189` (finalize), test helper `pol/layer/admission/predictive/mod.rs:627`. R: `pol/layer/est/state/metadata.rs:119-120` (parent reads child response), `:219` (signal check on response), `pol/layer/admission/predictive/mod.rs:236-237` (subtree compute at ingress), `:269-271` (early return at ingress finalize). Tests `polT/stack_characterization.rs:203,419,889,...`. Copied into child requests unintentionally (fact 6). |
| `EstimatorResponse.compute_time_us` | W: `metadata.rs:190`. R: only tests (`polT/stack_characterization.rs:434,558`). |
| `.accumulated_compute_us` | W: `metadata.rs:191`. R: `metadata.rs:127,133`, `pol/layer/admission/predictive/mod.rs:241`. |
| `.utilization` | W: `metadata.rs:170,192`. R: no production reader found; only used to compute `max_downstream_util` (`metadata.rs:172`). |
| `.max_downstream_util` | W: `metadata.rs:193`. R: `metadata.rs:122-125`. |
| `.early_return_count` | W: `metadata.rs:194`. R: `metadata.rs:129`, `pol/layer/admission/predictive/mod.rs:270`. |
| `.deadline_signal_count` | W: `metadata.rs:195`. R: `metadata.rs:130,220`, `pol/layer/admission/predictive/mod.rs:270`. Producer is est (`signal_slack.rs`), consumer is predictive admission. |
| `RootMethod { service, method }` | R: `pol/layer/est/layer.rs:68`, `pol/layer/admission/predictive/mod.rs:86`. W: `est/layer.rs:61`. |

### 1.3 Queue telemetry (feature `trace_queue_latency`): `QueueContext` (`:76`), `QueueLatencies` (`:63`)

| Field / accessor | Consumers |
|---|---|
| `queue.latencies` / `queue_latencies()` (`:401`), `set_queue_latencies()` (`:407`), builder `queue_latencies` (`:258`) | W: `pol/layer/queue_latency.rs:114` (finalize). R: `pol/layer/queue_latency.rs:89` (parent aggregates child). **Apps R+S** (parse `ctx` from response metadata by hand): `apps/app-utils/src/load_gen.rs:416-420`, `apps/tracebench/generic-service/src/replay.rs:322-324`; typed: `apps/hotel/src/frontend/server.rs:42`. **tonic tests** R: `tonT/policy_behavior.rs:131`. core tests: `core/tests/context_roundtrip.rs:13,38`. |
| `QueueLatencies.initial`, `.resume` | W: `queue_latency.rs:115-116`. R: `queue_latency.rs:90-91`, `load_gen.rs:422-423`, `replay.rs:326-327`, `hotel/frontend/server.rs:43`. |
| `QueueLatencies.queue_lengths` (HashMap) | W: `queue_latency.rs:113,117`. R: `queue_latency.rs:92-94`, `load_gen.rs:424`, `replay.rs:328`. |

### 1.4 Rajomon (feature `ac_rajomon`): `RajomonContext` (`:127`, default 100 at `:132-136`)

| Field / accessor | Consumers |
|---|---|
| `rajomon.tokens` / `tokens()` (`:365`), `consume_tokens()` (`:371`), builder `tokens` (`:276`) | R: `pol/layer/mod.rs:122` (seed), `pol/layer/admission/rajomon/layer.rs:51,52,57,61`. W: `pol/hooks.rs:140` (child), via `child_rpc.tokens` set at `rajomon/layer.rs:108`; root creators: `libs/masa/src/lib.rs:107-112` (`try_create_context`), `apps/tracebench/generic-service/src/loadgen.rs:343`. `consume_tokens()`: no caller found in `*.rs`. Tests: `pol/layer/admission/rajomon/tests.rs:204-532`, `polT/stack_characterization.rs:104,259,378,630`, `tonT/policy_behavior.rs:254,327`. Default 100 (`core/context.rs:132-136`) vs `PolicyParams.rajomon.tokens_left_init` (`libs/masa/src/lib.rs:141-143`): two sources of the default. |

### 1.5 `ChildRpcContext` (`pol/layer/mod.rs:103-125`, `#[non_exhaustive]`)

| Field | Consumers |
|---|---|
| `deadline` | seeded `pol/layer/mod.rs:117`; W: `pol/layer/est/layer.rs:160`, `pol/layer/oracle.rs:47`; R: `pol/hooks.rs:132`. Other modules receive `&mut ChildRpcContext` but do not touch it: `e2e_deadline_guard.rs:152`, `predictive/mod.rs:144`. |
| `prio_hint` | seeded `:118`; W: `est/layer.rs:161`, `oracle.rs:48`, tests `polT/custom_stack.rs:79,205`; R: `pol/hooks.rs:133`. |
| `hop_count` (feature `estimator`) | seeded `:119-120` (parent+1); R: `pol/hooks.rs:136`. No module writes it. |
| `tokens` (feature `ac_rajomon`) | seeded `:121-122`; W: `rajomon/layer.rs:108`; R: `pol/hooks.rs:140`. |

The only constructor is `ChildRpcContext::from_parent` (`pol/layer/mod.rs:113`, `pub(crate)`); consumer of the finished value is `PolicyHooks::before_child_rpc` (`pol/hooks.rs:131-143`). Because it is `#[non_exhaustive]` and created in masa-policy, external stacks (for example `polT/custom_stack.rs`) can only mutate it.

### 1.6 Not in `Context` but adjacent

- `FutureSpan` (`core/context.rs:36-47`): only `apps/hotel/src/profile_layer.rs:1,11-18` uses the masa-core type; `apps/app-utils/src/load_gen.rs:236` defines its own same-named struct. Not part of `Context`.
- Side headers written by policy modules, separate from `ctx`: `x-masa-rajomon-price` (`pol/layer/admission/rajomon/layer.rs:141,177,180`, `apps/app-utils/src/load_gen.rs:323`), `x-masa-method-name` / `x-masa-service-name` (`pol/context_ext.rs:10-11`), oracle headers (`core/lib.rs:36,39`, `pol/layer/oracle.rs:72,74`, `apps/synthbench/src/oracle.rs:4,15,25,28,108,113`). These already follow the "module owns its own wire data" pattern.
- Cargo feature constants used as `if` guards: `ABORT_SLACK`, `SIGNAL_SLACK` from `core/flag.rs` (used at `pol/layer/est/layer.rs:85,204`, `pol/layer/est/signal_slack.rs:12,21,35`).

## 2. Serialization, parsing and root creation

### 2.1 Every place that encodes or decodes the `ctx` header

| Location | Dir | What |
|---|---|---|
| `core/context.rs:452-463` | S | `from_header_string` / `to_header_string` (base64 + bincode). Panics on bad input (`:453-457`). |
| `core/header.rs:4-22` | S parse | `read_context_from_headers`, `read_context`, `read_priority_from_headers` (panics if the header is missing, `:7`). |
| `core/lib.rs:33` | const | `MASA_CONTEXT_HEADER = "ctx"`; re-exported at `pol/context_ext.rs:8`, `libs/masa/src/lib.rs:11`. |
| `libs/hyper/src/common/exec.rs:97-101` | parse (server, per stream) | priority only, via `read_priority_from_headers`. Hyper test at `:178-186`. |
| `pol/context_ext.rs:14-30` | S | `get_masa_context_from_metadata`, `set_masa_context_in_metadata` (tonic `MetadataMap`); trait ext at `:104-210` (`set_masa_context`, `with_masa_context`, `get_masa_context` on Request, Response, Status). |
| `pol/hooks.rs:98` | parse (server entry) | `read_context(req)` in `ParentContext::begin`. |
| `pol/hooks.rs:143` | write (client side of child call) | `request.set_masa_context(&child_recv_ctx)`. |
| `pol/hooks.rs:167-174` | write (response/status) | `finalize_before_serialization`. |
| `apps/app-utils/src/load_gen.rs:416-419` | parse (response, by literal `"ctx"`) | queue latency for CSV. |
| `apps/tracebench/generic-service/src/replay.rs:322-323` | parse (response, by literal `"ctx"`) | same. |
| `apps/benchmark/src/main.rs:40-83,113-121`, `apps/benchmark/benches/serialization.rs:7-42`, `apps/benchmark/benches/e2e.rs:71-75`, `apps/benchmark/README.md:20` | S | microbenchmarks of the current encoding; need rebaselining. |
| Tests that hand-build the header | S | `core/header.rs:68,110`, `core/tests/context_roundtrip.rs:25-67`, `pol/hooks.rs:337`, `pol/layer/e2e_deadline_guard.rs:226`, `polT/custom_stack.rs:39`, `polT/stack_characterization.rs:113`, `tonT/metadata_helpers.rs:87,91`, `libs/hyper/src/common/exec.rs:182-183`. |
| Docs describing the format | - | `docs/MASA_POLICY_IMPL.md:201,214,415,417`; `AGENTS.md` Data Flow; `docs/RPCSTACK_REFACTOR.md` (JSON, differs from current). |

Non-Rust: **none found.** `exp_runner/` (Python), `scripts/*.sh`, `eval/**/run.sh`, `apps/**/*.sh`, Dockerfiles do not touch `ctx`. There is no gateway or ingress proxy in the repo; the "gateway" is the Rust load generators in 2.2. `scripts/validate_tonic_masa_boundary.py` is relevant only as a rule that tonic must not import masa crates (`:12`).

### 2.2 Entry points that create a ROOT context (must all move to the new wire format)

| Entry point | File:line | Sets |
|---|---|---|
| `masa::create_context` | `libs/masa/src/lib.rs:76-88` | api, request_id (counter), slo, gateway_entry, deadline, priority (defaulted in `build()`). Used by `apps/app-utils/src/load_gen.rs:822` (the generic load generator for hotel, socialnet, synthbench), `apps/tracebench/generic-service/src/replay.rs:275`, `apps/benchmark/*` (`main.rs:40,113`, `benches/e2e.rs:71`, `benches/serialization.rs:7,24`), `attach_context` (`libs/masa/src/lib.rs:127`). |
| `masa::try_create_context` (feature `ac_rajomon`) | `libs/masa/src/lib.rs:95-116` | as above plus `tokens` from the client token bucket. Caller `apps/app-utils/src/load_gen.rs:810`. |
| Tracebench root load generator | `apps/tracebench/generic-service/src/loadgen.rs:336-346` (api `"root"`, tokens from `masa::try_acquire_tokens` at `:302`) | slo from trace entry, gateway_entry, deadline, tokens. |
| Tracebench service replay (**not a true root**, it mints a fresh context per child RPC) | `apps/tracebench/generic-service/src/service_replay.rs:75-79` | api `"replay"`, request_id copied, slo/start_at/deadline copied from the replay payload. Bypasses the hooks: loses hop_count, root_method, tokens (defaults). |
| Client benches | `apps/socialnet/src/client/client_bench.rs:38-43`, `apps/synthbench/src/client/client_bench.rs:37-43` (`ping`), `apps/hotel/src/client/client_bench.rs:72` (`Context::default()`, slo 0 and deadline 0) | minimal roots for ping. |
| Tests | `apps/synthbench/src/tests/override_test.rs:79`, `tonT/*.rs` (see `git grep ContextBuilder libs/tonic/tests`), `polT/*`, `core/tests/*`, `pol/hooks.rs:330`, `pol/context_ext.rs:253,273`, `pol/layer/e2e_deadline_guard.rs:219`, `pol/layer/admission/rajomon/tests.rs:204-532`. | |

All external callers of `ContextBuilder` go through these. Methods used by app code: `new`, `slo`, `gateway_entry`, `deadline`, `tokens`. No app sets `prio_hint`, `hop_count`, `root_method`, `response_meta`, `queue_latencies` or `frontend_elapse` on a builder; only masa-policy and tests do.

Important: the client-side hook `ChildContext`/`before_child_rpc` only runs for RPCs a server makes; a root client must call the builder itself, so root creators cannot be hidden behind hooks unless the new framework adds a `create_root` API. That is the natural place for the "priority assignment hook" in step 5 of the plan.

### 2.3 Python consumption (indirect)

Python never decodes `ctx`. It reads load-generator CSV output (`start_at`, `latency` in `exp_runner/runner/plotting/goodput.py:92-115`; `slo` in `exp_runner/runner/optimizer.py:139-151`). Those columns are produced by `apps/app-utils/src/load_gen.rs:469-478` from `Context` accessors (api, request_id, slo, gateway_entry, deadline, and queue latency fields). Queue-related columns are named `queue_lengths` in the CSV (`load_gen.rs:311,344`). I did not find a Python reader of `queue_lengths` (`git grep` over `*.py`), so whether any plot uses it is undetermined from this grep.

## 3. `PriorityHint`, `TaskPriority`, `::infra()`, `spawn_with_prio`

| Symbol | Where | Assumption |
|---|---|---|
| `PriorityHint(u64)` | `core/priority.rs:6`; `infra()` = 0 at `:11`; `Default` = infra at `:28-30`; reversed `Ord` for a max-heap (`:32-42`) | Smaller = more urgent; 0 is reserved for infra and is the default. A request whose computed hint is 0 (e.g. `deadline = 0` or `sched_pred` with an expired deadline: `deadline.saturating_sub(now)` in `core/context.rs:304`) is indistinguishable from infra and jumps ahead of everything. `Context::default()` has deadline 0 so its priority is 0 too (`hotel client_bench.rs:72`). |
| `Prioritize` trait | `core/priority.rs:46`, re-export `core/lib.rs:28` | no implementor in `*.rs` outside tokio's own trait. Tokio has a separate `TaskPrioritize` (`libs/tokio/tokio/src/masa/priority.rs:46`, used at `tailclipper.rs:8`, `runtime/task/mod.rs:190`). |
| `TaskPriority(u64)` | `libs/tokio/tokio/src/masa/priority.rs:6`; re-exported at `libs/tokio/tokio/src/task/mod.rs:325` | duplicate of `PriorityHint` with the same semantics; converted by value in hyper (`exec.rs:100`) and in `pol/layer/est/layer.rs:104`. |
| `TaskPriority::infra()` / `PriorityHint::infra()` | hyper default and foreign executor: `exec.rs:23,102`; tokio: default for every plain spawn `libs/tokio/tokio/src/task/spawn.rs:169`, `runtime/handle.rs:332`, `runtime/task/mod.rs:406`, `task/local.rs:371,653`; queues: `masa/scheduler/prio_heap.rs:49` (`value()==0` goes to a FIFO `infra_q`, popped first at `:72`), `masa/scheduler/tailclipper.rs:50,79,168` (`USE_INFRA_QUEUE`) | Tokio itself defines "infra" and runs it first. Plan step 4 removes this. Every non-annotated `tokio::spawn` (including user handlers that spawn children) runs at the highest priority, not at its parent's priority; `meta_for_unannotated_spawn(spawner)` in the plan addresses this. |
| `spawn_with_prio` | def `libs/tokio/tokio/src/task/spawn.rs:174`; caller `libs/hyper/src/common/exec.rs:109` (only under hyper `masa` feature and `Exec::Default`); example `libs/tonic/examples/src/helloworld/server.rs:159`; tests `libs/tokio/tokio/tests/masa_priority.rs`, `tests/sched_mt_multiqueue.rs`; benches `libs/tokio/benches/rt_multi_threaded.rs` | The hyper call site is the only production spawn with a non-infra priority. With a user-provided `Executor`, `exec.rs:102,137` fall back to infra and ignore the context: custom executors silently disable scheduling. |
| `tokio::task::reprioritize` | def `spawn.rs:210`; caller `pol/layer/est/layer.rs:104` (`sched_pred`: priority = remaining time to local deadline) | Masa-policy calls tokio directly to change priority in a `before_poll` path; this must also become a `Meta` operation. |
| Hyper trait default | `exec.rs:22-23` (`h2_stream_priority` default returns infra) and `:85-88` (non-masa: ignore prio) | - |
| Priority computation sites | `core/context.rs:296-309` (root default), `pol/layer/est/layer.rs:256-272` (child priority under sched_pred), `pol/layer/oracle.rs:48` (oracle), `polT/custom_stack.rs:79,205` (custom) | Three different formulas depending on Cargo features: deadline, `deadline - now`, gateway_entry (tailclipper). Children re-derive priority from the parent in `ChildRpcContext::from_parent` (`pol/layer/mod.rs:118`), so with `sched_slo` the child keeps the root's absolute deadline as its priority. |

## 4. Proposed classification of every field

Legend: (a) fixed framework fact, (b) owned by one module, (c) shared concept used by several modules.

| Field | Class | Owner / users | Reasoning and risk |
|---|---|---|---|
| `api` | (a) | framework | Fixed fact; only used for logging and CSV (`load_gen.rs:469`). Risk: root `Api` today is a free string (`"ping"`, `"root"`, `"replay"`); the plan says the framework owns service/method names, which are known only at the first server. At root creation there is no service/method pair, so keep a root-label field or drop it. Do not use it for estimation (estimation uses `root_method`, `est/layer.rs:61`). |
| `request_id` | (a) | framework ("open" per plan) | Read by no module; needed for tracing. Risk: low. |
| `slo`, `gateway_entry`, `deadline`, `e2e_deadline` | (c) | guard (`abort_slo`), est, predictive admission, oracle, sched priority defaults; also CSV in apps | Used by at least four modules plus priority defaults (see 1.1). This is the real shared Masa concept: an end-to-end time budget. Not a framework fact in the plan's sense, but it is cheaper to keep as one small shared "budget" type in a Masa-shared crate than to duplicate it in each module. Risk: `deadline` is a mutable per-hop value (child deadline tightened by est at `est/layer.rs:160`, oracle at `oracle.rs:47`) while `gateway_entry + slo` is immutable. Keep both semantics separate; the guard intentionally reads the immutable one (`e2e_deadline_guard.rs:50-53`). |
| `prio_hint` | (b) scheduling-assignment module (plan step 5) plus the scheduler `Meta` | hyper reads it today (`exec.rs:100`) | Wire-wise a scheduling module should own it, but hyper and tokio need it before any policy code runs. Risk (high): hyper would have to call into the framework to decode (hyper -> framework dependency, or framework registers a decoder). Options: (1) hyper stays generic and is given a `fn(&HeaderMap) -> Meta` hook at compile time; (2) carry a separate tiny header (for example `x-prio`) that hyper reads without decoding `ctx`. Option 2 also removes the full bincode decode per stream (fact 3), at the cost of a second header. Root assignment by feature flags in `build()` (`core/context.rs:296-309`) must move to the assignment hook. |
| `frontend_elapse` | (b) hotel app diagnostics; candidate to delete | `hotel` frontend only | No reader found; drop during the move, or move to a hotel-owned module. Risk: minimal. |
| `estimator.hop_count` | (c) framework candidate | est, predictive admission | Two modules test `== 0` to detect ingress. It is a call fact (depth), not an estimation concept; make it a framework fact (`is_ingress`/`depth`) so predictive admission does not depend on est being in the stack. Risk: today it only increments when `estimator` is on (`pol/layer/mod.rs:119`), so moving it to the framework changes wire content for non-estimator stacks (+1 byte). Also ingress detection must be reliable for `service_replay.rs:75` which mints a fresh ctx per child. |
| `estimator.root_method` | (c) | set by est, read by est and predictive admission | Writer and reader are in different modules (`est/layer.rs:61` vs `predictive/mod.rs:84`). If it moves to est's wire type, predictive admission needs est's type, creating a module dependency. Better: framework call fact "root method" (service, method), which is the same "fixed facts" category the plan already names (caller method). Risk: medium; `est` also keys latency maps on it. |
| `estimator.response` (all six `EstimatorResponse` fields) | (b) est (producer) with predictive admission as consumer (c) | `est/state/metadata.rs`, `predictive/mod.rs` | Response-direction data. `compute_time_us`, `utilization`, `max_downstream_util`, `accumulated_compute_us`: est only plus predictive's `accumulated_compute_us` read (`predictive/mod.rs:241`). `early_return_count` and `deadline_signal_count` are read by predictive at ingress (`:270`). So predictive admission reads est's wire type. Risk: a hard coupling; acceptable if predictive and est ship together (same `MasaStack`, same crate), not if predictive must work with other estimators. Alternative: est exposes a public `EstWire` and predictive imports it from masa-policy internals (same crate), so no cross-crate problem. |
| `queue.latencies` (`initial`, `resume`, `queue_lengths`) | (b) `QueueLatencyLayer` (observer) | apps read it out of the response ctx by hand (`load_gen.rs:416`, `replay.rs:322`, `hotel server.rs:42`) | Self-contained module; consumers outside the stack need the wire type. Risk: apps parse `ctx` by literal name and decode the whole `Context`; they must switch to a module accessor (`resp.module_wire::<QueueWire>()`) or the framework codec. |
| `rajomon.tokens` | (b) rajomon module | `rajomon/layer.rs`, root creators `masa/src/lib.rs:107`, `tracebench loadgen.rs:343` | Clean candidate (plan step 1 prototype). Risk: root creators need a typed builder to set tokens before the first RPC; `ChildRpcContext.tokens` moves with it. Default differs between `RajomonContext::default` (100, `core/context.rs:132-136`) and `PolicyParams` (`masa/src/lib.rs:141`). |
| `ChildRpcContext.deadline` | (c) same as `deadline` | est, oracle write | Stay with the shared budget type. |
| `ChildRpcContext.prio_hint` | (b) with `prio_hint` | est, oracle write | Follows the priority decision. |
| `ChildRpcContext.hop_count` | (a) with `hop_count` | none writes it | Delete as a module-writable field once depth is a framework fact (the framework increments it). |
| `ChildRpcContext.tokens` | (b) rajomon | rajomon layer | Replace by rajomon's own child wire value. |

### Cross-cutting risks for the classification

- hyper and tokio need `prio_hint` before any policy code. See the `prio_hint` row.
- `ContextBuilder::from(parent)` copies all fields into the child (fact 6). With per-module wire types the framework must call a per-module `child_wire(parent_wire)` hook and default to `Default` (reset), otherwise response data leaks into requests.
- Request vs response direction is mixed in one struct today (fact 8).
- `Context` is cloned on every child RPC and every finalize (`pol/hooks.rs:169`, child ctx built from parent at `:131`); the plan's JSON object keyed by module name will be larger and slower than bincode. `apps/benchmark` exists to measure this (`apps/benchmark/README.md:20`).

## 5. Top risks and recommended migration order

### Top risks (most to least severe)

1. **hyper needs the priority before policy code exists** (`exec.rs:97-101`). Whichever design is chosen changes the hyper patch, and `libs/hyper/Cargo.toml:39` already depends on masa-core, so the boundary script does not catch it.
2. **Panics on wire mismatch**: decoders panic on bad input (`core/context.rs:453-457`, `core/header.rs:7,10`); every binary (services, load generators, benches) must be rebuilt together. Old test fixtures and any stored headers break.
3. **Priority 0 == infra collision** (`core/priority.rs:11,28`; tokio queues) for roots with deadline 0 (`Context::default()`), or for `sched_pred` when the deadline has passed (`core/context.rs:304`).
4. **Hidden cross-module reads**: predictive admission reads est's `root_method`, `response.*` and `hop_count` (section 1.2). Moving wire types per module exposes this as real type dependencies.
5. **Root contexts are built in app code** (8 sites, 2.2) and two apps decode `ctx` by string literal (`load_gen.rs:416`, `replay.rs:322`). Missing one is a runtime panic, not a compile error, unless the `"ctx"` literal and `Context` type disappear.
6. **Feature-dependent layout**: any test that compiles under different `check.sh` feature sets (`AGENTS.md`) checks different wire layouts. The per-module design removes that, but `polT/stack_characterization.rs` accessors (`:203,236-284,356-378`) must be rewritten per feature.
7. `ContextBuilder::from` copy semantics (fact 6) silently change behavior if not reproduced.
8. Benchmarks (`apps/benchmark`) and docs (`docs/MASA_POLICY_IMPL.md:111,201,214,415,417`, `AGENTS.md`) describe the old format.

### Recommended order

1. **`tokens` (rajomon)**: one field, one reader module, two root creators (`masa/src/lib.rs:107`, `loadgen.rs:343`), `ChildRpcContext.tokens`. Matches plan step 1. Lets you define the codec, the typed root builder and the child-wire hook on the smallest case.
2. **`queue.latencies`**: self-contained module; the only external readers are three app sites (`load_gen.rs:416`, `replay.rs:322`, `hotel server.rs:42`) plus one test (`tonT/policy_behavior.rs:131`). Forces the "outside code reads a module's response wire data" API.
3. **`frontend_elapse`**: delete or move to hotel (no reader).
4. **Framework facts**: `hop_count` (depth/ingress) and `root_method`, so that est and predictive admission stop sharing `Context` fields. Do this before touching est's wire type.
5. **`estimator.response`** into est's wire type; predictive admission imports it (same crate). Largest behavioral surface (`est/state/metadata.rs`, `predictive/mod.rs`); `stack_characterization.rs` is the safety net.
6. **Budget (`slo`, `gateway_entry`, `deadline`)**: decide last the shared-type vs per-module question, since four modules and the apps read it (1.1).
7. **`prio_hint`** with plan steps 4-5 (scheduler `Meta`, assignment hook), because it needs the hyper decision. Until then keep it in the shared header.
8. Remove `Context`'s `#[cfg]` fields and `ContextBuilder` cfg branches, update `apps/benchmark`, docs, `AGENTS.md`.

## 6. Could not determine

- Whether `frontend_elapse` set by hotel (`server.rs:229-230,281-282`) reaches the client, given `finalize_before_serialization` (`pol/hooks.rs:167-174`) overwrites the ctx header; not executed. No reader exists either way.
- Whether any Python plot reads the `queue_lengths` CSV column (no match in `*.py` outside unrelated `rajomon`/`start_at` uses, but CSV columns can be read by dynamic name).
- Whether any out-of-repo client or load generator sends `ctx` (nothing in-repo does outside Rust; `trace-analysis/` and `traces/` were not searched beyond the file-type listing).
- `tonic/tests` consumers beyond the `ContextBuilder` setters and `queue_latencies` at `policy_behavior.rs:131` were enumerated with the accessor grep only; compile-time feature combinations in those tests were not built.
- I did not compile or run anything (read-only survey).
