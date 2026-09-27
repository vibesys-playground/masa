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
| `Layer::Server: LayerServer` | Per-service state, built once by `LayerServer::new(&mut ServerInit)`. |
| `Layer::Child: LayerChild` | Per-child-RPC state, created before `before_child_rpc` and passed back to `after_child_rpc`. |
| `ChildRpcContext` | What the child will receive: `deadline`, `prio_hint`, plus feature-gated fields (`hop_count`, `tokens`). Modules mutate it in `before_child_rpc`. |
| `ServerInit` | Service name plus a typed store: `provide::<T>()` publishes server state and `get::<T>()` reads state from an earlier module. |
| `()` | The empty module. Terminates stacks and fills disabled slots. |

Lifecycle, per inbound request:

| Hook | When | Can |
|---|---|---|
| `Layer::new` | Request arrives, context decoded | Read or mutate the inbound `Context` |
| `before_poll` | Before each handler poll | Abort (`Err`), reprioritize the task (`tokio::task::reprioritize`) |
| `before_child_rpc` | Before each outbound RPC | Reject the child (`Err`), set child deadline/priority |
| `after_child_rpc` | Child response received | Record latencies, inspect response context |
| `after_poll` | After each handler poll | Abort (`Err`), e.g. on `Pending` past deadline |
| `finalize` | Before response serialization | Write response metadata into `Context` |

Modules run in stack order and the first `Err` short-circuits the rest. For
`ChildRpcContext`, the last writer wins.

## Example

```rust
use masa_core::{Context, PriorityHint};
use masa_policy::{policy_stack, ChildRpcContext, Layer, PolicyHooks};
use tonic::{CowGrpcMethod, Request, Status};

/// Earliest-deadline-first for children: child priority = child deadline.
#[derive(Debug)]
pub struct ChildEdf;

impl Layer for ChildEdf {
    type Server = ();
    type Child = ();

    fn new(_m: &CowGrpcMethod, _s: &(), _ctx: &mut Context) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child: &CowGrpcMethod,
        _child_ctx: &mut (),
        _req: &mut Request<T>,
        child_rpc: &mut ChildRpcContext,
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

## Sharing state between modules

Publish server state from the producer's `LayerServer::new` and read it in a
later module:

```rust
// producer
init.provide(estimators.clone());
// consumer (must come later in the stack)
let est = init.get::<LatencyEstimators<_>>().expect("needs EstimationLayer earlier");
```

`PredAdmissionLayer` reads `EstimationLayer`'s estimators this way. A stack
that orders them wrongly panics when the server is constructed, not while it
serves requests. Published values should be cheap handles (`Arc`-backed) so
producer and consumer share one instance.

## Selecting a stack

- **Default Masa policies**: build with features as before. `masa::DefaultHooks`
  is `PolicyHooks<MasaStack>`, and `libs/masa-policy/src/masa_stack.rs` maps
  features to modules.
- **Custom stack**: point code generation at your hooks type so that server
  **and** client stubs agree, e.g. `tonic_build::configure().default_hooks_path(...)`
  set to `crate::MyHooks`. Tests can also instantiate servers directly with
  `FooServer::<_, MyHooks>::with_custom_context(svc)`.

The parent context reaches client stubs through an untyped thread-local
(`tonic::masa::thread_local`). A server and the client stubs its handlers call
**must** use the same hooks type. Mixing `PolicyHooks<MyStack>` servers with
`masa::DefaultHooks` clients is undefined behavior.

The runtime still needs a scheduling feature (`sched_slo`, `sched_fifo`, ...)
so that Hyper spawns handlers with priorities and Tokio orders its queue by
them.

## Not yet modular

These policy decisions are still selected by features outside `masa-policy`:

- **Root priority**: `ContextBuilder::build` in `masa-core` picks deadline,
  gateway entry time (TailClipper), or remaining budget (`sched_pred`) when the
  gateway does not set `prio_hint`. Modules can override it per task with
  `tokio::task::reprioritize` in `before_poll`.
- **Queue discipline**: FIFO, binary heap, TailClipper round-robin, and the
  multi-threaded heap/multiqueue live in the patched Tokio (`sched_prio`,
  `sched_fifo`, `tailclipper`, `sched_mt*`).
- **Wire context schema**: `ChildRpcContext` and `Context` fields are fixed by
  `masa-core`. A module that needs new propagated metadata must extend the
  context there.
- **Behavior toggles inside built-in modules**: e.g., `abort_slack`,
  `signal_slack`, `deadline_equals_slack` and the `est_*` estimator choice are
  still `cfg`/`const` switches inside `EstimationLayer`.
