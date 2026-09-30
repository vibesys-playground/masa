# Writing Policy Modules

A Masa policy is a stack of **policy modules**. Each module implements
`masa_policy::Layer` and hooks into the RPC lifecycle; `PolicyHooks<S>` runs
the stack `S` for every request. Masa's own policies (deadline guard,
estimation, oracle, predictive/Rajomon admission, queue-latency tracing) are
built-in modules, and `MasaStack` is the feature-selected default. A new
policy is a new module, composed into a new stack. It does not need edits to
`hooks.rs` or new `cfg` branches.

## The contract

| Item | Role |
|---|---|
| `Layer` | Per-request state and lifecycle hooks. All hooks default to no-ops. |
| `Layer::Server: LayerServer` | Per-service state, built once by `LayerServer::new(&mut ServerInit)`, which returns `Err(MissingDependency)` when a prerequisite module is missing or misordered. |
| `Layer::Child: LayerChild` | Per-child-RPC state, created before `before_child_rpc` and passed back to `after_child_rpc`. |
| `ChildRpcContext` | What the child will receive in the shared `Context`: `deadline` and `prio_hint`. Modules mutate it in `before_child_rpc`. |
| `Layer::NAME`, `Layer::Wire` | The module's own wire data: a serde type and the unique name of its section in the `ctx` header. `()` means none. See "Wire data". |
| `WireIn` / `WireOut` | Typed access to the wire sections: `wire.get::<Self>()` on an inbound message (a request, or a child's response), `out.put::<Self>(&value)` on an outbound one. |
| `Extensions` | A per-request typed map (one value per type) that the hooks of all modules share; the framework never reads or fills it. |
| `ServerInit` | Service name plus a typed store: `provide::<T>()` publishes server state, `get::<T>()` and `require::<T>()` read state from an earlier module. |
| `()` | The empty module. Terminates stacks and fills disabled slots. |

Lifecycle, per inbound request:

| Hook | When | Can |
|---|---|---|
| `Layer::new` | Request arrives, context decoded | Read or mutate the inbound `Context`; read the inbound wire data (`wire.get::<Self>()`, `None` if absent); insert into `Extensions` |
| `before_poll` | Before each handler poll | Abort (`Err`), reprioritize the task (`tokio::task::reprioritize`) |
| `before_child_rpc` | Before each outbound RPC | Reject the child (`Err`), set child deadline/priority, write the child's wire data |
| `after_child_rpc` | Child response received | Record latencies, read the child response's wire data (`response_wire.get::<Self>()`) |
| `after_poll` | After each handler poll | Abort (`Err`), e.g. on `Pending` past deadline |
| `finalize` | Before response serialization | Write response metadata into `Context`, write the response's wire data |

Modules run in stack order and the first `Err` short-circuits the rest. For
`ChildRpcContext`, the last writer wins. `Extensions` is passed mutably to
`new`, `before_poll` and `before_child_rpc`, and shared to the `after_*` hooks
and `finalize`.

## Example

```rust
use masa_core::{Context, PriorityHint};
use masa_policy::{policy_stack, ChildRpcContext, Extensions, Layer, PolicyHooks, WireIn, WireOut};
use tonic::{CowGrpcMethod, Request, Status};

/// Earliest-deadline-first for children: child priority = child deadline.
#[derive(Debug)]
pub struct ChildEdf;

impl Layer for ChildEdf {
    type Server = ();
    type Child = ();
    const NAME: &'static str = "child_edf";
    type Wire = ();

    fn new(
        _m: &CowGrpcMethod,
        _s: &(),
        _ctx: &mut Context,
        _wire: &WireIn<'_>,
        _ext: &mut Extensions,
    ) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child: &CowGrpcMethod,
        _child_ctx: &mut (),
        _req: &mut Request<T>,
        child_rpc: &mut ChildRpcContext,
        _child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        child_rpc.prio_hint = PriorityHint::new(child_rpc.deadline);
        Ok(())
    }
}

// Reuse Masa's estimation module, replace everything else.
pub type MyStack = policy_stack![masa_policy::modules::EstimationLayer, ChildEdf];
pub type MyHooks = PolicyHooks<MyStack>;
```

`libs/masa-policy/tests/custom_stack.rs` has runnable modules covering
priority assignment, ordering, short-circuiting, and shared server state.

## Wire data

A module that needs to send data to other hops (tokens, prices, hints) defines
a serde type and exchanges it through the framework's codec, instead of adding
fields to `Context`:

```rust
#[derive(Serialize, Deserialize)]
pub struct MyWire { pub budget: u64 }

impl Layer for MyModule {
    const NAME: &'static str = "my_module";
    type Wire = MyWire;
    // ...
    fn new(
        _m: &CowGrpcMethod,
        _s: &(),
        _ctx: &mut Context,
        wire: &WireIn<'_>,
        _ext: &mut Extensions,
    ) -> Self {
        // `None` means the sender attached nothing; what that implies is the
        // module's decision, and zero is an ordinary value.
        let budget = wire.get::<Self>().unwrap().map_or(100, |w| w.budget);
        // ...
    }
    // before_child_rpc / finalize: `child_wire.put::<Self>(&MyWire { budget })`
    // after_child_rpc: `response_wire.get::<Self>()` is what the child's
    // module put in its response.
}
```

The framework carries nothing by itself, in either direction: a child request
or a response has wire data only for modules that `put` it, including values
received from the parent or from a child. A module that aggregates over its
children (Masa's queue-latency module sums the totals in the children's
responses and reports the sum) reads the child's section in `after_child_rpc`
and writes its own in `finalize`. The response is read-only there because
`response_wire` borrows from it; the error status of a failed child carries
wire sections the same way a successful response does.
Each section travels in the `ctx` header as `.<NAME>:<base64 JSON>` after the
unchanged `Context` blob (`libs/masa-policy/src/wire.rs` documents the layout).
Sections are decoded independently, and `masa_policy::peek::<M>(&headers)`
decodes one module's section without decoding the `Context`. Sections that are
built and parsed on every RPC, like estimation's, encode their fields as JSON
arrays instead of objects (`#[serde(from = ..., into = ...)]` on a tuple
struct), which made them about a third the size and the per-RPC hook cost
equal to what it was with the data in `Context`. `NAME` must be
unique among modules with wire data and use only ASCII letters, digits, `_` or
`-`; a stack that violates this panics when the server is constructed. Root
clients attach wire data with `masa::RootContext` (for example
`with_rajomon_tokens`). Apps read a response's wire data through `masa`, for
example `masa::queue_latencies_from_metadata` and
`masa::rajomon_price_from_metadata`.

## Sharing state between modules

Publish server state from the producer's `LayerServer::new` and read it in a
later module:

```rust
// producer
init.provide(estimators.clone());
// consumer (must come later in the stack)
let est = init.require::<LatencyEstimators<_>>()?;
```

`PredAdmissionLayer` reads `EstimationLayer`'s estimators this way. A stack
that orders them wrongly fails when the server is constructed, not while it
serves requests: `ServerContext::try_new` returns the `MissingDependency`
error (naming the missing type and the requiring module), and the hooks'
`ServerHooks::new` panics with its message. Published values should be cheap
handles (`Arc`-backed) so producer and consumer share one instance.

Per-request data that several modules share goes in `Extensions` instead: a
module inserts a value in `new`, and modules later in the stack see it in `new`
and every module sees it in later hooks. Its type is the key, so a module that
wants private data defines a private type for it. The `after_*` hooks and
`finalize` get `Extensions` shared, so a value that changes during the request
is published as a handle with interior mutability (an `Arc` of atomics), not
re-inserted.

Estimation uses both mechanisms for predictive admission. Estimation's server
publishes the marker `PublishesEstimationInfo`, which admission's server
requires, so a stack that puts admission first fails at construction. In `new`,
estimation inserts an `EstimationInfo` (hop count and ingress flag, root
method and its registry id, and a live view of the subtree's early-return and
deadline-signal state); admission reads it in `new` and uses it in place of
any `Context` field.

### Estimation's wire data

`EstimationLayer`'s section (`EstimationWire`) has two halves, one per
direction. A request carries `EstimationRequestWire { hop_count, root_method }`:
estimation itself writes the child's section in `before_child_rpc` with the hop
count incremented (saturating at 255) and the root passed on unchanged, and the
framework does not touch either. A response carries
`EstimationResponseWire { compute_time_us, accumulated_compute_us,
utilization, max_downstream_util, early_return_count, deadline_signal_count }`,
written in `finalize`; a parent reads each child's in `after_child_rpc` and
sums or maximizes the fields into its own.

What estimation decides when a section is absent:

- **Request**: the sender runs no estimation (a load generator, for example),
  so the request is at ingress: hop count 0, and this method is the root. A
  present section with hop count 0 means the same. Nothing else distinguishes
  the two, and the framework supplies no default for either.
- **Response**: the child reported nothing, so it adds nothing to its parent's
  totals. Only successful responses are read; a child that returned a
  `DeadlineExceeded` status counts as one early return without reading its
  section, and any other failed child adds nothing.

## Selecting a stack

- **Default Masa policies**: build with features as before. `masa::DefaultHooks`
  is `PolicyHooks<MasaStack>`, and `libs/masa-policy/src/masa_stack.rs` maps
  features to modules.
- **Agent-owned stack (`stack_custom`)**: the way to run a new policy in the
  existing apps and experiments. See the next section.
- **Per-app stack**: point code generation at your hooks type so that server
  **and** client stubs agree, e.g. `tonic_build::configure().default_hooks_path(...)`
  set to `crate::MyHooks`. Tests can also instantiate servers directly with
  `FooServer::<_, MyHooks>::with_custom_context(svc)`.

## Agent-owned stack (`stack_custom`)

`libs/masa-policy/src/agent/` is reserved for policies written outside Masa's
built-in modules, whether by a person or by an optimizing agent. It defines
`AgentStack`. With the `stack_custom` feature, `masa::DefaultHooks` becomes
`PolicyHooks<AgentStack>`, so every generated server and client stub in every
app uses it, with no app or `build.rs` changes.

To try a policy:

1. Write modules as files under `libs/masa-policy/src/agent/` and declare them
   in `agent/mod.rs`.
2. Set `AgentStack` in `agent/mod.rs`, e.g.
   `pub type AgentStack = policy_stack![crate::modules::EstimationLayer, my_policy::MyAdmission];`.
   It starts as `crate::MasaStack`, so `<features>,stack_custom` behaves like
   `<features>` until you change it.
3. Build with a scheduling feature plus `stack_custom`, and add the features
   the reused built-in modules need, e.g.
   `cargo build -p hotel --release --features "sched_pred,est_mean_var,stack_custom"`.
   `stack_custom` without a scheduling feature is a compile error.
4. Run it as an experiment policy: list `sched_slo,stack_custom` (or any
   other combination) in the experiment's `policies` file. Plots label it
   `Custom stack (...)` and do not reuse the built-in policy's style.

Rules for the agent stack:

- Edit only `libs/masa-policy/src/agent/`. Built-in modules, `masa_stack.rs`,
  `hooks.rs`, and the vendored libraries are out of scope for a policy change.
- Keep one hooks type per binary: use `masa::DefaultHooks` everywhere rather
  than naming `PolicyHooks<...>` in app code (see below).
- Never delay `PriorityHint::infra()` work: it is reserved for infrastructure
  tasks.
- Built-in modules are reusable only when their feature is enabled (e.g.
  `crate::modules::EstimationLayer` needs `estimator`). Modules' wire
  sections depend on the stack, so build every service with the same features.
- Validate with `./scripts/check.sh "sched_slo,stack_custom"` and
  `./scripts/test.sh --feature "sched_slo,stack_custom"`.
  `libs/tonic/tests/masa_integration_tests/tests/custom_stack_serve.rs`
  checks that `DefaultHooks` resolves to `AgentStack` and that custom modules
  run on served requests.

The parent context reaches client stubs through an untyped thread-local
(`tonic::masa::thread_local`). A server and the client stubs its handlers call
**must** use the same hooks type. Mixing `PolicyHooks<MyStack>` servers with
`masa::DefaultHooks` clients is undefined behavior.

The runtime still needs a scheduling feature (`sched_slo`, `sched_fifo`, ...)
so that Hyper spawns handlers with priorities and Tokio orders its queue by
them.

## Agent-owned run queue (`sched_custom`)

`libs/rpcstack-sched/src/custom.rs` is the slot for a new run queue (the
structure that decides which runnable task the Tokio current-thread runtime
polls next). `custom::Queue` starts as a copy of the default priority-heap
queue. With the `sched_custom` feature, the runtime uses it instead of the
queue that `sched_slo`, `sched_fifo` or `sched_tailclipper` would select. This
describes shipped behavior; the feature only selects the slot and encodes no
policy.

To try a queue:

1. Edit `custom::Queue` in `libs/rpcstack-sched/src/custom.rs`: implement
   `RunQueue` and read each task's `Meta` from the `TaskView` passed to
   `push`. Edit only that file.
2. Update the expected pop orders in `custom_replay_scripts` (in
   `libs/rpcstack-sched/src/replay_tests.rs`) if the new ordering is
   intentional, and run them with
   `cargo test -p rpcstack-sched --features sched_custom`.
3. Build with a scheduling feature plus `sched_custom`, e.g.
   `cargo build -p hotel --release --features "sched_slo,sched_custom"`.
   `sched_custom` without a scheduling feature is a compile error, because
   Hyper only passes priorities to Tokio under one. It composes with
   `stack_custom` and with every modifier. It only affects the current-thread
   runtime, so `sched_mt*` ignores it.
4. Run it as an experiment policy: list `sched_slo,sched_custom` in the
   experiment's `policies` file and sweep load as usual (see
   `docs/experiments/workflow.md`). Plots label it `Custom scheduler (...)`
   (`Custom stack + Custom scheduler (...)` with both custom features) and do
   not reuse the built-in policy's style.
5. Validate with `./scripts/check.sh "sched_slo,sched_custom"` and
   `./scripts/test.sh --feature "sched_slo,sched_custom"`.
   `libs/tonic/tests/masa_integration_tests/tests/custom_sched_serve.rs`
   checks that the runtime selects `custom::Queue` and that generated stubs
   serve through it.

## Not yet modular

These policy decisions are still selected by features outside `masa-policy`:

- **Root priority**: `ContextBuilder::build` in `masa-core` picks deadline,
  gateway entry time (TailClipper), or remaining budget (`sched_pred`) when the
  gateway does not set `prio_hint`. Modules can override it per task with
  `tokio::task::reprioritize` in `before_poll`.
- **Queue discipline**: FIFO, binary heap and TailClipper round-robin live in
  `rpcstack-sched` (new queues go in `custom.rs`, see above); the
  multi-threaded heap/multiqueue live in the patched Tokio (`sched_mt*`).
- **Wire context schema**: `ChildRpcContext` and `Context` fields are fixed by
  `masa-core`. A module that needs new propagated metadata should use its own
  wire section. Estimation and predictive admission no longer keep anything in
  `Context`; what remains there is request identity, SLO, deadline and
  priority.
- **Behavior toggles inside built-in modules**: e.g., `abort_slack`,
  `signal_slack`, `deadline_equals_slack` and the `est_*` estimator choice are
  still `cfg`/`const` switches inside `EstimationLayer`.
