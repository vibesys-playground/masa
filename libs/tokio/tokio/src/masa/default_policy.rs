//! The default (Masa) scheduling policy: the parts of task scheduling that are
//! policy decisions rather than runtime mechanism.
//!
//! Two choices live here, and nothing else in the runtime's spawn paths makes
//! them:
//! - the concrete type of `Meta`, which is a priority number, and
//! - what `Meta` a spawn gets when its caller supplied none, which is the
//!   infrastructure priority (0, served before all other work).
//!
//! A different policy replaces this module; `meta.rs` is the only caller.

use super::priority::TaskPriority;

/// The concrete scheduling metadata type under the default policy.
pub type Meta = TaskPriority;

/// Default policy for spawns that supplied no `Meta`: infrastructure priority,
/// whatever the spawner's own `Meta` is.
pub(crate) fn meta_for_unannotated_spawn(_spawner: Option<&Meta>) -> Meta {
    Meta::infra()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unannotated_spawn_is_infra_regardless_of_spawner() {
        assert_eq!(meta_for_unannotated_spawn(None), TaskPriority::infra());
        assert_eq!(
            meta_for_unannotated_spawn(Some(&TaskPriority::new(42))),
            TaskPriority::infra()
        );
    }
}
