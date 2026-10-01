# Writing Policy Modules

A Masa policy is a stack of **policy modules**. Each module implements
`masa_policy::Module` and hooks into the RPC lifecycle; `PolicyHooks<S>` runs
the stack `S` for every request. Masa's own policies (deadline guard,
estimation, oracle, predictive/Rajomon admission, queue-latency tracing) are
built-in modules, and `MasaStack` is the feature-selected default. A new
policy is a new module, composed into a new stack. It does not need edits to
the hook adapter or new `cfg` branches.

## Crates

The framework owns every mechanism (hook order, which modules run a hook,
shared per-request state, decision points, outcomes, declared dependencies,
the wire codec) and has no policy: it knows a request only by its service and
method name, and never defaults or interprets a value a module carries. It
lives in three crates that know nothing about Masa
(`scripts/validate_rpcstack_boundary.py` checks their dependencies):

| Crate | Contents |
|---|---|
| `libs/rpcstack-wire` | The `ctx` header's section codec. A leaf crate, so Hyper can read one section without depending on tonic. |
| `libs/rpcstack` | `Module`, `ModuleServer`, `Stack`/`policy_stack!`, `Extensions`/`ChildState` with decision points, `Outcome`/`ChildOutcome`, `Requires`/`MissingDependency`/`ServerInit`, `build_server`, and the typed wire codec (`WireIn`, `WireOut`, `peek`). |
| `libs/rpcstack-tonic` | `PolicyHooks<S>` (tonic's `Hooks` for any stack), and `RequestExt`/`ResponseExt`/`StatusExt` for module wire data and method-name overrides on tonic messages. |

`libs/rajomon` is a policy built on those crates alone (`RajomonModule`, its
state, wire data and parameters; no Masa crate in its dependencies, which the
same script checks). `masa-policy` depends on it under `ac_rajomon`.

`libs/masa-policy` holds Masa's own modules and stacks (budget, guard,
estimation, oracle, admission, queue latency, `MasaStack`) and re-exports the
framework, so `masa_policy::Module`, `masa_policy::policy_stack!` and
`masa_policy::PolicyHooks<S = MasaStack>` name the same items as the
framework crates. Each framework crate has a README listing its public
surface.

## The contract

| Item | Role |
|---|---|
| `Module` | Per-request state and lifecycle hooks. All hooks default to no-ops. |
| `Module::Server: ModuleServer` | Per-service state, built once by `ModuleServer::new(&mut ServerInit)`, which returns `Err(MissingDependency)` when the server state it needs is missing. |
| `Module::requires` | Declares the modules that must precede this one in the stack; checked when the server state is built. |
| `ChildState` | A typed map for one child RPC (like `Extensions`, but per child): what a module keeps for that child, keyed by type. |
| `Extensions::propose` / `proposals` / `resolve` | Typed decision points: many modules propose, the owner resolves. See "Decisions". |
| `Outcome`, `ChildOutcome` | How the request, or one child RPC, ended; passed to `finalize` and `after_child_rpc`. |
| `BudgetModule` | Masa's budget module: the request's facts and time budget, and the child's. Owns the `ChildDeadline` and `ChildPriority` decisions. See "Budget module". |
| `Module::NAME`, `Module::Wire` | The module's name (in `Outcome` and the wire envelope) and its own wire data: a serde type whose section is named `NAME` in the `ctx` header. `()` means none. See "Wire data". |
| `Module::POLL_HOOKS` | `true` by default. A module that overrides neither `before_poll` nor `after_poll` sets it to `false`; a stack where no module has poll hooks skips them, and the per-request lock that calling them takes, on every poll. Debug builds still call the hooks of such modules and panic if one was overridden, so a wrong `false` is caught by any test that polls. |
| `WireIn` / `WireOut` | Typed access to the wire sections: `wire.get::<Self>()` on an inbound message (a request, or a child's response), `out.put::<Self>(&value)` on an outbound one. |
| `Extensions` | A per-request typed map (one value per type) that the hooks of all modules share; the framework never reads or fills it. |
| `ServerInit` | Service name plus a typed store: `provide::<T>()` publishes server state, `get::<T>()` and `require::<T>()` read state from an earlier module. |
| `()` | The empty module. Fills disabled slots. |

### Hook order

The framework, not the module, decides in which order a stack's modules run a
hook and which of them run it. With a stack `[A, B, C]`:

| Hook | When | Order | Modules that run it |
|---|---|---|---|
| `new` | Request arrives | A, B, C | all |
| `before_poll` | Before each handler poll | A, B, C | until the first `Err`, which ends the request |
| `before_child_rpc` | Before each outbound RPC | A, B, C | until the first `Err`, which rejects the child |
| `seal_child_rpc` | Once per child, after every `before_child_rpc` accepted it | C, B, A | until the first `Err`, which rejects the child |
| `after_child_rpc` | The child is over: answered (`ChildOutcome::Sent`) or rejected, not sent (`ChildOutcome::Rejected`) | C, B, A | every module whose `before_child_rpc` ran, whichever way the child ended |
| `after_poll` | After each handler poll | A, B, C | until the first `Err`, which ends the request |
| `finalize` | Before response serialization | C, B, A | all |

Two rules explain the table.

- **Pre-hooks go head first, post-hooks tail first, and a module gets a
  post-hook only if its pre-hook ran.** A child RPC rejected by `B` is
  reported to `B` (the rejecter included) and then to `A`, never to `C`, whose
  `before_child_rpc` did not run. So a module that counts a child in
  `before_child_rpc` always sees it end in `after_child_rpc`, sent or not, and
  a module nearer the head of the stack wraps everything after it. `finalize`
  pairs with `new`, which cannot reject, so every module finalizes; the
  `Outcome` it is given says which module, if any, ended the request.
- **A hook that decides goes in stack order, a hook that reports goes in
  reverse.** `before_poll` and `after_poll` decide whether the request goes on;
  the first module in stack order to say no wins, before and after the poll
  alike (`after_poll` is not reversed: a poll has no nesting to unwind, and
  reversing it would change which module's rejection wins). Only the hooks that
  close what a pre-hook opened are reversed.

`Extensions` is passed mutably to `new`, `before_poll`, `before_child_rpc` and
`seal_child_rpc`, and shared to the other hooks. `ChildState` is passed mutably
to the first two child hooks and shared to the rest. The request-level map is
guarded by a lock held for a whole `before_child_rpc` plus `seal_child_rpc`
call, so each child is set up atomically. No hook receives a request context:
the framework carries no request, child or response data of its own, so a child
request or a response carries exactly the sections the modules `put`.

An `Err` from `after_child_rpc` does not skip the other modules' hooks. For a
sent child the first `Err` in hook order (the tail-most module's) fails the
child RPC at the handler; for a rejected child the rejection stands.

`Outcome` is `Handled` (the handler produced the result, which may itself be an
error, such as a status built from a rejected child), `Rejected { by, status }`
(a module's `before_poll` or `after_poll` ended the request with a status; `by`
is that module's `NAME`) or `Replied { by }` (it ended the request with a
response). It is recorded when the module ends the request and does not change
if a module rewrites the result in `finalize`, so an observer sees both the
cause (`outcome`) and the final `result`.

## Dependencies between modules

A module that reads what another module provides declares it:

```rust
impl Module for MyAdmission {
    // ...
    fn requires(requires: &mut Requires) {
        requires.module::<BudgetModule>().module::<EstimationModule>();
    }
}
```

`ServerContext::try_new` (called by `ServerHooks::new`, which panics with the
same message) walks the stack and fails if a required module is absent or comes
later, with an error naming both: *module `MyAdmission` requires module
`EstimationModule` earlier in the stack, but it comes later; move
`EstimationModule` before `MyAdmission`*. A module placed in an illegal position
therefore fails when the server is built, never at the first request. Server
resources (`ServerInit::provide`/`require`) are for state that a module's
server shares with another's, such as estimation's latency estimators; their
`MissingDependency` error names the missing type and the module that needed it.

## Decisions

Several modules often contribute to one decision: the deadline a child
request carries, say. The framework supplies the mechanism and no rule:

1. The *owner* defines the decision type, usually a newtype, and documents how
   it combines proposals (`ChildDeadline(Timestamp)`, owned by `BudgetModule`).
2. Any module proposes with `child.propose(value)?` (or `ext.propose` on the
   request map) while the child is being set up. The framework records the value
   with the proposing module's `NAME`, in the order the proposals were made.
   `propose` borrows the map only for the call, so it never conflicts with
   other reads.
3. A module that runs later can read what earlier ones proposed with
   `proposals::<T>()`, which returns the values with their provenance
   (`Proposal { by, value }`).
4. The owner calls `resolve::<T>()` once, in `seal_child_rpc`, which the
   framework runs after every `before_child_rpc` (in reverse order, so a module
   near the head of the stack, the budget module, sees what all the others
   proposed). It applies its own rule to the proposals: last wins, the
   smallest, or `Err` to veto the child call. `resolve` closes the decision.
5. A proposal that arrives after `resolve` (a module before the owner proposing
   in its own `seal_child_rpc`) fails with `DecisionClosed`, which converts into
   a `Status::internal` naming the proposer, the owner and the type.

Per-child decisions live in `ChildState`, so they belong to one child however
many are in flight, and are dropped with it, including when the child is
rejected.

## Example

```rust
use masa_core::PriorityHint;
use masa_policy::{
    policy_stack, BudgetModule, ChildDeadline, ChildPriority, ChildState, Extensions, Module,
    PolicyHooks, Requires, WireIn, WireOut,
};
use tonic::{CowGrpcMethod, Request, Status};

/// Earliest-deadline-first for children: propose the child's priority from the
/// deadline the parent has (here: the parent's own, read from the budget module).
#[derive(Debug)]
pub struct ChildEdf;

impl Module for ChildEdf {
    type Server = ();
    const NAME: &'static str = "child_edf";
    type Wire = ();

    fn requires(requires: &mut Requires) {
        requires.module::<BudgetModule>();
    }

    fn new(
        _m: &CowGrpcMethod,
        _s: &(),
        _wire: &WireIn<'_>,
        _ext: &mut Extensions,
    ) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        child: &mut ChildState,
        _req: &mut Request<T>,
        _child_wire: &mut WireOut,
        ext: &mut Extensions,
    ) -> Result<(), Status> {
        let deadline = masa_policy::BudgetInfo::of(ext).deadline();
        child.propose(ChildDeadline(deadline))?;
        child.propose(ChildPriority(PriorityHint::new(deadline)))?;
        Ok(())
    }
}

// Reuse Masa's budget and estimation modules, add the new one. The module
// nearest the end of the stack wins a decision under the budget module's rule.
pub type MyStack = policy_stack![
    BudgetModule,
    masa_policy::modules::EstimationModule,
    ChildEdf,
];
pub type MyHooks = PolicyHooks<MyStack>;
```

An observer is a module that implements only the post-hooks. Placed first, it
finalizes last and sees every module's rejection:

```rust
fn finalize<Ret>(
    &self,
    result: &mut Result<Response<Ret>, Status>,
    outcome: Outcome<'_>,
    _wire: &mut WireOut,
    _ext: &Extensions,
) {
    if let Outcome::Rejected { by, status } = outcome {
        metrics::rejected(by, status.code());
    }
}
```

`libs/rpcstack-tonic/tests/stack_semantics.rs` has runnable toy modules for every
rule above (order, symmetry, outcome, dependencies, per-child state, decisions
with different owner rules and a veto, concurrent children), and
`libs/masa-policy/tests/custom_stack.rs` has modules covering priority
assignment, ordering, short-circuiting, and shared server state.

## Wire data

A module that needs to send data to other hops (tokens, prices, hints) defines
a serde type and exchanges it through the framework's codec:

```rust
#[derive(Serialize, Deserialize)]
pub struct MyWire { pub budget: u64 }

impl Module for MyModule {
    const NAME: &'static str = "my_module";
    type Wire = MyWire;
    // ...
    fn new(
        _m: &CowGrpcMethod,
        _s: &(),
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
(from `ChildOutcome::Sent`; a rejected child has no response) and writes its own
in `finalize`. The response is read-only there because
`response_wire` borrows from it; the error status of a failed child carries
wire sections the same way a successful response does.
Each section travels in the `ctx` header as `<NAME>:<base64 bincode>`, joined by
`.`; the header holds nothing else (`libs/rpcstack/src/wire.rs` documents
the layout, and `libs/rpcstack-wire/src/lib.rs` holds the primitives Hyper also
uses through `masa_core`). Sections are decoded independently, and `masa_policy::peek::<M>(&headers)`
decodes one module's section without decoding any other. A payload is the wire
type serialized with `bincode` (little endian, variable-length integers: a
`u64` below 251 is one byte, a larger one takes 3, 5 or 9), so a plain request
header is `ctx: budget:<base64 of about 40 bytes>` and a section is built and
parsed with no number or name parsing. `bincode` does not describe itself, so
a wire type must serialize every field every time: express "may be absent" as
an `Option` field, which is always written (an absent value is its tag byte,
distinct from any value including zero), and use no `skip_serializing_if`,
`default`, `flatten` or tagged enums. A type that is built and parsed on every
RPC, like `Context` and estimation's, is a tuple struct
(`#[serde(from = ..., into = ...)]`). The header is not readable by eye:
`masa_policy::describe(header)` (or `Display` for `WireIn`) prints each
section's bytes in hex, and `masa_policy::peek::<M>` or `get_wire` decode one
as its type. A module that forwards a section it received without changing it
keeps the `EncodedSection` from `WireIn::get_with_encoded` and gives it to
`WireOut::put_encoded`, so the section is not encoded again (the budget module
does this for the response and for a child whose budget it did not change).
`NAME` must be
unique among the modules of a stack (the empty module `()` is exempt), and a
module with wire data must also use only ASCII letters, digits, `_` or `-`; a
stack that violates this panics when the server is constructed, naming both
modules of a duplicate. Root
clients attach wire data with `masa::RootContext` (for example
`with_rajomon_tokens`). Apps read a response's wire data through `masa`, for
example `masa::queue_latencies_from_metadata` and
`masa::rajomon_price_from_metadata`.

## Sharing state between modules

Publish server state from the producer's `ModuleServer::new` and read it in a
later module:

```rust
// producer
init.provide(estimators.clone());
// consumer (must come later in the stack)
let est = init.require::<LatencyEstimators<_>>()?;
```

`PredAdmissionModule` reads `EstimationModule`'s estimators this way. Published
values should be cheap handles (`Arc`-backed) so producer and consumer share one
instance.

Per-request data that several modules share goes in `Extensions` instead: a
module inserts a value in `new`, and modules later in the stack see it in `new`
and every module sees it in later hooks. Its type is the key, so a module that
wants private data defines a private type for it. Values with interior
mutability (atomics) can change during the request even though the `after_*`
hooks and `finalize` get `Extensions` shared.

Estimation and predictive admission use both mechanisms. Estimation inserts an
`EstimationInfo` (hop count and ingress flag, root method and its registry id)
in `new`, and keeps its running tally of the request's polls and children in
`Extensions` too. Admission declares `EstimationModule` in `requires`, reads the
`EstimationInfo` in `new`, and in `finalize` takes a `SubtreeHealth::of(ext)`
snapshot: whether a child returned early and whether any hop tripped
`signal_slack`. Post-hooks run in reverse order, so admission finalizes before
estimation; that is safe because the tally is updated while the request runs
(`before_poll`, `after_poll`, `after_child_rpc`), not in estimation's
`finalize`, so it is complete whichever module finalizes first. What the
tally cannot hold is this hop's own early return, which only exists once the
result does: admission reads it from the `result` it is given in `finalize`
(estimation does the same to decide whether to flush), and recognizes its own
ingress rejection from `Outcome::Rejected { by: "pred_admission", .. }` rather
than keeping a flag.

### Budget module

A request's API, id, SLO, gateway entry time, deadline and priority (the
`masa_core::Context`) are the wire data of `BudgetModule`, in the `budget`
section. The framework neither reads nor forwards them. `BudgetModule`:

- reads the request's section in `new` (a request without one is a
  misconfigured sender and panics, as for any malformed wire data) and
  inserts a read-only `BudgetInfo` into `Extensions`: API, request id, SLO,
  gateway entry, this hop's deadline, the end-to-end deadline
  (`gateway_entry + slo`) and priority. The guard, estimation, oracle and
  predictive admission read it in their own `new` and keep a clone. They
  declare `BudgetModule` in `requires`, so a stack that puts one of them before
  it, or omits it, fails at construction.
- owns the child's `ChildDeadline` and `ChildPriority` decisions. Modules that
  decide them (estimation, oracle) `propose` in `before_child_rpc`;
  `BudgetModule` resolves both in `seal_child_rpc` with the rule *the last
  proposal wins* (so the module nearest the end of the stack decides), and the
  parent's own value when nobody proposed. The two decisions resolve
  independently, so a module that proposes only a priority leaves the deadline
  alone. It then writes the child's `budget` section; a child whose deadline and
  priority equal the parent's gets the parent's encoded section verbatim.
- writes the request's own section into the response in `finalize`, again
  reusing the encoded inbound section.

A stack needs no writer module: any stack that starts with `BudgetModule` sends
its children a budget section. A stack without it sends none (the framework
sends nothing by default), which Masa's next hop cannot serve.

Root clients build a `Context` with `masa::ContextBuilder`
(`masa_policy::ContextBuilder`), which sets the root priority with
`masa_policy::root_priority` unless the caller gives one: the gateway entry
time under `sched_tailclipper`, the time left to the deadline under
`sched_pred`, the deadline otherwise (so a root with deadline 0 gets priority 0,
which is `PriorityHint::infra()`). `masa::RootContext` and
`MasaRequestExt::set_masa_context` attach a `Context` as the budget section.
Hyper reads only the section's priority to schedule a stream
(`masa_core::read_priority_from_headers`).

### Estimation's wire data

`EstimationModule`'s section (`EstimationWire`) has two halves, one per
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
   `pub type AgentStack = policy_stack![crate::modules::BudgetModule, crate::modules::EstimationModule, my_policy::MyAdmission];`.
   Keep `BudgetModule` first (see "Budget module"): without it requests carry no
   deadline or priority to the next hop.
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
  the framework crates, and the vendored libraries are out of scope for a
  policy change.
- Keep one hooks type per binary: use `masa::DefaultHooks` everywhere rather
  than naming `PolicyHooks<...>` in app code (see below).
- Never delay `PriorityHint::infra()` work: it is reserved for infrastructure
  tasks.
- Built-in modules are reusable only when their feature is enabled (e.g.
  `crate::modules::EstimationModule` needs `estimator`). Modules' wire
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
   `RunQueue` and read each task's `Meta` and timing facts from the `TaskView`
   passed to `push`. Edit only that file.
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

### What a queue is told

The runtime gives a queue facts and events and attaches no meaning to them.
`TaskView` carries `task_id`, `meta`, `enqueued_at` (when this enqueue
happened, an `Instant`), `first_enqueued_at` (when the task first entered any
queue) and `polls` (polls started so far). The runtime supplies the times, so a
queue needs no clock and tests can pass any values. A re-pushed task keeps its
id, first enqueue time and poll count.

Besides `push` and `pop`, `RunQueue` has callbacks that default to doing
nothing and that the current-thread run loop calls:

- `on_poll_start(view)` just before a task is polled, and
  `on_poll_end(view, PollOutcome::{Pending, Ready})` right after. `Ready`
  means the task is finished for good (it completed, panicked, or its
  cancellation was observed at the poll).
- `on_idle()` when nothing is runnable, just before the thread parks or yields
  to the driver.
- `on_task_exit(task_id)` once per task, after the `on_poll_end(.., Ready)` of
  the poll it ended in, so a queue that keeps per-task state can free it. A
  task cancelled while queued during runtime shutdown gets it with no poll
  around it.

Tasks taken from the cross-thread injection queue never pass through `push`,
but they do get the poll and exit callbacks. The multi-thread runtime
(`sched_mt*`) does not make any of these calls yet. Tests of the call order use
`custom::Queue` built with the `lifecycle_trace` feature, which records the
calls it receives; it has no other effect.

## Not yet modular

These policy decisions are still selected by features outside `masa-policy`:

- **Root priority**: `masa_policy::root_priority` (used by
  `masa_policy::ContextBuilder`) picks deadline, gateway entry time
  (TailClipper), or remaining budget (`sched_pred`) when the gateway does not
  set `prio_hint`, and is selected by features at the client. Hyper schedules
  the stream with the priority in the header before any module runs; modules can
  override it per task with `tokio::task::reprioritize` in `before_poll`.
- **Queue discipline**: FIFO, binary heap and TailClipper round-robin live in
  `rpcstack-sched` (new queues go in `custom.rs`, see above); the
  multi-threaded heap/multiqueue live in the patched Tokio (`sched_mt*`).
- **Budget schema**: `Context` (the budget section) is fixed by `masa-core`
  because Hyper reads its priority. A module that needs new propagated metadata
  should use its own wire section.
- **Behavior toggles inside built-in modules**: e.g., `abort_slack`,
  `signal_slack`, `deadline_equals_slack` and the `est_*` estimator choice are
  still `cfg`/`const` switches inside `EstimationModule`.
