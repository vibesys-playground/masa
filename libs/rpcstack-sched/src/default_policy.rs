//! The default scheduling policy: decisions about what a [`Meta`] means.
//!
//! Policy decisions live here and in the queues under [`crate::builtin`], and
//! nowhere in the runtime that calls them:
//! - a `Meta` is a priority number where smaller runs first,
//! - `Meta::new(0)` is reserved for infrastructure work and runs before all
//!   other work, and
//! - a spawn that supplied no `Meta` is infrastructure work.

use crate::Meta;

/// The `Meta` of infrastructure work (runtime and transport internals).
pub const INFRA: Meta = Meta::new(0);

/// Whether `meta` is infrastructure work.
pub fn is_infra(meta: &Meta) -> bool {
    *meta == INFRA
}

/// Default policy for spawns that supplied no `Meta`: infrastructure priority,
/// whatever the spawner's own `Meta` is.
///
/// `spawner` is the `Meta` of the task performing the spawn, or `None` when the
/// spawn happens outside any task.
pub fn meta_for_unannotated_spawn(_spawner: Option<&Meta>) -> Meta {
    INFRA
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unannotated_spawn_is_infra_regardless_of_spawner() {
        assert_eq!(meta_for_unannotated_spawn(None), INFRA);
        assert_eq!(meta_for_unannotated_spawn(Some(&Meta::new(42))), INFRA);
    }

    #[test]
    fn only_zero_is_infra() {
        assert!(is_infra(&Meta::new(0)));
        assert!(!is_infra(&Meta::new(1)));
    }
}
