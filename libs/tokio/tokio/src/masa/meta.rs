//! Per-task scheduling metadata: the mechanism side.
//!
//! `Meta` is an opaque value the run queues order tasks by. Code that only
//! moves a `Meta` around (task headers, spawn plumbing) names `Meta` and never
//! interprets it. Both the concrete type and the decision below come from the
//! active policy in `default_policy.rs`.

use super::default_policy;

pub use super::default_policy::Meta;

/// Hook called by every spawn that supplied no `Meta`; returns the `Meta` the
/// new task gets.
///
/// `spawner` is the `Meta` of the task performing the spawn, or `None` when the
/// spawn happens outside any task (for example from `block_on`, from a plain
/// thread holding a runtime handle, or from the blocking pool).
pub fn meta_for_unannotated_spawn(spawner: Option<&Meta>) -> Meta {
    default_policy::meta_for_unannotated_spawn(spawner)
}
