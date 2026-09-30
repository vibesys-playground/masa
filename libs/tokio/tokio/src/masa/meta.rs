//! Per-task scheduling metadata.
//!
//! `Meta` is the value the run queues order tasks by. Its concrete type is
//! chosen at compile time; today it is `TaskPriority`. Code that only moves a
//! `Meta` around (task headers, spawn plumbing) should name `Meta`, not
//! `TaskPriority`, so the concrete type can change in one place.

use super::priority::TaskPriority;

/// Scheduling metadata attached to every task.
pub type Meta = TaskPriority;

/// Decides the `Meta` of a spawn that did not supply one.
///
/// `spawner` is the `Meta` of the task performing the spawn, or `None` when the
/// spawn happens outside any task (for example from `block_on`, from a plain
/// thread holding a runtime handle, or from the blocking pool).
///
/// The default policy gives every unannotated spawn the infrastructure
/// priority and ignores `spawner`.
pub fn meta_for_unannotated_spawn(spawner: Option<&Meta>) -> Meta {
    let _ = spawner;
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
