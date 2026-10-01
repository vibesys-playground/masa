//! Runs an [`rpcstack`] module stack as tonic's request hooks.
//!
//! [`PolicyHooks<S>`] implements tonic's `Hooks` for any [`rpcstack::ModuleStack`]
//! `S`: it splits the inbound wire sections, resolves method names, drives the
//! stack through every lifecycle hook, and installs the wire sections the
//! modules produce on child requests and responses. The [`metadata`] module
//! has the helpers for reading and writing those sections, and the method-name
//! overrides, on tonic requests, responses and statuses.

mod hooks;
pub mod metadata;

pub use hooks::{ChildContext, ParentContext, PolicyHooks, ServerContext};
pub use metadata::{
    get_method_name_override_from_headers, get_method_name_override_from_metadata,
    get_service_name_override_from_headers, get_service_name_override_from_metadata,
    get_wire_from_metadata, set_method_name_override_in_headers,
    set_service_name_override_in_headers, set_wire_in_metadata, RequestExt, ResponseExt, StatusExt,
};
