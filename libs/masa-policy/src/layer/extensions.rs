//! Per-request typed extensions.

use std::any::{Any, TypeId};
use std::collections::HashMap;

/// A per-request map holding at most one value of each type, through which the
/// modules of a stack share data about one request without the framework
/// having a field for it (the same idea as `http::Extensions`).
///
/// The framework creates an empty map when a request begins and hands it to
/// every hook of that request: mutably to [`Layer::new`](super::Layer::new),
/// [`Layer::before_poll`](super::Layer::before_poll) and
/// [`Layer::before_child_rpc`](super::Layer::before_child_rpc), shared to the
/// `after_*` hooks and [`Layer::finalize`](super::Layer::finalize). It never
/// inserts, reads or interprets a value. The key is the value's type, so a
/// module that wants its data private should define a private type for it;
/// data meant for other modules is published by making its type public.
///
/// Stack order decides what a module finds: a value inserted by a module in
/// `new` is visible to modules later in the stack during `new`, and to every
/// module in later hooks.
#[derive(Debug, Default)]
pub struct Extensions {
    values: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Extensions {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `value`, returning the value of the same type it replaces.
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) -> Option<T> {
        self.values
            .insert(TypeId::of::<T>(), Box::new(value))
            .and_then(|old| old.downcast().ok().map(|old| *old))
    }

    /// The stored value of type `T`.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref())
    }

    /// The stored value of type `T`, mutably.
    pub fn get_mut<T: Any + Send + Sync>(&mut self) -> Option<&mut T> {
        self.values
            .get_mut(&TypeId::of::<T>())
            .and_then(|value| value.downcast_mut())
    }

    /// Remove and return the stored value of type `T`.
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<T> {
        self.values
            .remove(&TypeId::of::<T>())
            .and_then(|old| old.downcast().ok().map(|old| *old))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_value_per_type() {
        let mut ext = Extensions::new();
        assert_eq!(ext.insert(1u32), None);
        assert_eq!(ext.insert("a"), None);
        assert_eq!(ext.insert(2u32), Some(1));
        assert_eq!(ext.get::<u32>(), Some(&2));
        assert_eq!(ext.get::<&str>(), Some(&"a"));
        assert_eq!(ext.get::<u64>(), None);
    }

    #[test]
    fn zero_is_a_value() {
        let mut ext = Extensions::new();
        ext.insert(0u64);
        assert_eq!(ext.get::<u64>(), Some(&0));
    }

    #[test]
    fn get_mut_and_remove() {
        let mut ext = Extensions::new();
        ext.insert(vec![1]);
        ext.get_mut::<Vec<i32>>().unwrap().push(2);
        assert_eq!(ext.remove::<Vec<i32>>(), Some(vec![1, 2]));
        assert_eq!(ext.get::<Vec<i32>>(), None);
    }
}
