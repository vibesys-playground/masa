# rpcstack-tonic

Runs an `rpcstack` module stack as tonic's request hooks. Vendored tonic does
not depend on this crate (`scripts/validate_tonic_masa_boundary.py`); an
application selects the hooks as a type parameter of its generated server.
Like `rpcstack`, it depends on no Masa crate, hyper or tokio runtime policy
crate (`scripts/validate_rpcstack_boundary.py`).

Public surface:

- `PolicyHooks<S>`: tonic's `Hooks` for any `rpcstack::ModuleStack` `S`. It
  splits the inbound wire sections, resolves method names, drives the stack
  through every lifecycle hook, and installs the wire sections the modules
  produce on child requests and responses. `ServerContext<S>`,
  `ParentContext<S>` and `ChildContext<S>` are its three contexts;
  `ServerContext::try_new` reports a misconfigured stack as an error, and
  `ChildContext::state` exposes the child RPC's `ChildState`.
- `RequestExt`, `ResponseExt`, `StatusExt`: `set_wire::<M>` and `get_wire::<M>`
  for a module's wire data on tonic messages, and, on `Request`, method-name
  and service-name overrides.
- `get_wire_from_metadata`, `set_wire_in_metadata`, and the
  `get_/set_*_name_override_*` helpers for metadata and HTTP headers. The
  overrides travel in `x-rpcstack-method-name` and `x-rpcstack-service-name`.
