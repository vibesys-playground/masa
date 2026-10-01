//! Typed maps through which the modules of a stack share data.

use std::any::{Any, TypeId};
use std::ops::{Deref, DerefMut};

/// A map holding at most one value of each type, through which the modules of
/// a stack share data without the framework having a field for it (the same
/// idea as `http::Extensions`).
///
/// The framework creates one empty map when a request begins and hands it to
/// every hook of that request: mutably to [`Layer::new`](super::Layer::new),
/// [`Layer::before_poll`](super::Layer::before_poll),
/// [`Layer::before_child_rpc`](super::Layer::before_child_rpc) and
/// [`Layer::seal_child_rpc`](super::Layer::seal_child_rpc), shared to the
/// other hooks. It never inserts, reads or interprets a value. The key is the
/// value's type, so a module that wants its data private should define a
/// private type for it; data meant for other modules is published by making
/// its type public.
///
/// A value inserted by a module in `new` is visible to modules later in the
/// stack during `new`, and to every module in later hooks. Values are looked
/// up by a linear scan: a request holds a handful of them, which is cheaper
/// than hashing.
#[derive(Debug, Default)]
pub struct Extensions {
    values: Vec<(TypeId, Box<dyn Any + Send + Sync>)>,
}

impl Extensions {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }

    fn position<T: Any>(&self) -> Option<usize> {
        let id = TypeId::of::<T>();
        self.values.iter().position(|(key, _)| *key == id)
    }

    /// Store `value`, returning the value of the same type it replaces.
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) -> Option<T> {
        match self.position::<T>() {
            Some(at) => {
                let old = std::mem::replace(&mut self.values[at].1, Box::new(value));
                old.downcast().ok().map(|old| *old)
            }
            None => {
                self.values.push((TypeId::of::<T>(), Box::new(value)));
                None
            }
        }
    }

    /// The stored value of type `T`.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        let at = self.position::<T>()?;
        self.values[at].1.downcast_ref()
    }

    /// The stored value of type `T`, mutably.
    pub fn get_mut<T: Any + Send + Sync>(&mut self) -> Option<&mut T> {
        let at = self.position::<T>()?;
        self.values[at].1.downcast_mut()
    }

    /// Remove and return the stored value of type `T`.
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<T> {
        let at = self.position::<T>()?;
        self.values
            .swap_remove(at)
            .1
            .downcast()
            .ok()
            .map(|old| *old)
    }
}

/// The state of one child RPC: an [`Extensions`] map that lives from the
/// child's `before_child_rpc` to its `after_child_rpc`, or to its rejection.
///
/// Each child RPC has its own map, so what modules store here (the deadline
/// and priority being decided for this child, a latency tracker) belongs to
/// that child alone, however many children are in flight. It is keyed by type
/// like [`Extensions`], so a module finds its state, and a module that wants
/// to read another's, by naming the type, never by position in the stack.
#[derive(Debug, Default)]
pub struct ChildState(Extensions);

impl ChildState {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Deref for ChildState {
    type Target = Extensions;

    fn deref(&self) -> &Extensions {
        &self.0
    }
}

impl DerefMut for ChildState {
    fn deref_mut(&mut self) -> &mut Extensions {
        &mut self.0
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
