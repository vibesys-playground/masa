//! A module framework for RPC policies.
//!
//! A policy is a stack of [`Module`]s. The framework owns every mechanism:
//! the order in which a stack's modules run each hook and which of them run
//! it, the typed per-request state modules share ([`Extensions`],
//! [`ChildState`]), the decision points through which several modules
//! contribute to one decision ([`Extensions::propose`]), how a request or child
//! RPC ended ([`Outcome`], [`ChildOutcome`]), declared dependencies between
//! modules ([`Requires`], [`MissingDependency`]), and the layout of the wire
//! data modules exchange with other hops ([`WireIn`], [`WireOut`]). A module
//! contributes only what it does at each hook.
//!
//! The framework knows a request only by its service and method name. It has
//! no notion of deadlines, priorities or any other value a module may carry:
//! it never defaults or interprets one.
//!
//! This crate defines modules and stacks. `rpcstack-tonic` runs a stack as
//! tonic's request hooks.

mod extensions;
mod module;
pub mod wire;

pub use extensions::{ChildState, DecisionClosed, Extensions, Proposal, Proposals};
pub use module::{
    build_server, ChildOutcome, Early, MissingDependency, Module, ModuleDecl, ModuleServer,
    ModuleStack, Outcome, Rejection, Requires, ServerInit, Stack,
};
pub use wire::{peek, WireError, WireIn, WireOut};
