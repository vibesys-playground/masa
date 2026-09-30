# rpcstack refactor plan

Goal: turn Masa into one instantiation of a generic RPC policy framework, so that scheduling,
admission and execution policies (Masa, rajomon, and new ones) are written as self-contained
modules on top of shared crates.

This file is the plan that the integration branch (`rpcstack/integration`) implements. Working names
(`rpcstack`, `rpcstack-tonic`, `rpcstack-sched`) are placeholders and may be renamed.

## Target layout

| Crate | Role | Depends on |
|---|---|---|
| `rpcstack` | Generic framework: `Module` trait (today's `Layer`), `Stack`, `policy_stack!`, `ServerInit`, wire codec, call facts | `tonic` (types only) |
| `rpcstack-tonic` | Adapter implementing tonic's `Hooks` / `ParentHooks` / `ClientHooks` for any stack | `rpcstack`, `tonic` |
| `rpcstack-sched` | Scheduler interface: `RunQueue` trait, `Meta`, built-in queues. Leaf crate: no tokio, no tonic | none |
| `masa-policy` | Masa's modules (guard, estimation, oracle, predictive admission, queue latency), `MasaStack` | `rpcstack*` |
| `rajomon` | Rajomon admission module and its state | `rpcstack*` |
| `masa` | Facade: `DefaultHooks`, feature flags | all of the above |

Dependency direction is one-way: `rpcstack` knows nothing about Masa or rajomon; Tokio knows nothing
about any policy, only the `rpcstack-sched` interface.

## Design decisions

- `Layer` is renamed `Module` (avoids confusion with `tower::Layer`).
- The framework owns only fixed facts (service and method names; request id and caller method are open).
  Everything else is policy-defined data.
- Each module declares `type Wire: Serialize + DeserializeOwned + Default`. The framework owns the codec
  (serde, JSON object keyed by module name for now). All binaries are assumed to be built from the same
  stack, as with protobuf/gRPC stubs. Zero is a real value on the wire (use `Option`).
- Scheduling: queue and `Meta` are selected at compile time (Cargo features), like today. There is no
  global registration. Tokio has no notion of "infra" or a priority-0 invariant; queues decide.
  Unannotated spawns call `meta_for_unannotated_spawn(spawner: Option<&Meta>) -> Meta`.
  `spawn_with_prio` becomes `spawn_with_meta`. A `sched_custom` slot mirrors `stack_custom`.
  Only the `current_thread` runtime is in scope.
- Priority assignment becomes a module hook at request entry, replacing the `cfg` branches in
  `ContextBuilder::build`.

## Sequence

1. Policy-owned wire data (prototype on rajomon first).
2. Extract `rpcstack` and `rpcstack-tonic`; rename `Layer` to `Module`.
3. Move rajomon into its own crate.
4. Scheduler track (parallel to 1-3): `Meta` and the unannotated-spawn hook; move "infra" out of Tokio;
   extract `rpcstack-sched` and port the queues; `sched_custom` slot.
5. Priority assignment module hook (joins the two tracks).
6. Retire `sched_*` flags as mechanisms; docs.

## Safety net

`libs/masa-policy/tests/stack_characterization.rs` pins the behavior of the policy hooks and passes on
the code from before the stack refactor and after it. It must keep passing with unchanged expected values
at every step (only its accessors may change when `Context` moves). Queue refactors are guarded by replaying
recorded pop orders of the old queues.
