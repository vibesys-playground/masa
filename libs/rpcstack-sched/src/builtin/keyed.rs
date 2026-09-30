use std::cmp::Ordering;

use crate::Meta;

/// An item stored next to its ordering key.
///
/// Equality and order look at the key only, which is what makes a
/// `BinaryHeap<Keyed<T>>` sift exactly as a heap of the items ordered by
/// priority would, so tie order is preserved.
pub(crate) struct Keyed<T> {
    meta: Meta,
    pub(crate) item: T,
}

impl<T> Keyed<T> {
    pub(crate) fn new(meta: Meta, item: T) -> Self {
        Self { meta, item }
    }
}

impl<T> PartialEq for Keyed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.meta == other.meta
    }
}

impl<T> Eq for Keyed<T> {}

impl<T> PartialOrd for Keyed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Keyed<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.meta.cmp(&other.meta)
    }
}
