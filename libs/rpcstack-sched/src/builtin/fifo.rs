use std::collections::VecDeque;

use crate::{RunQueue, SchedFlavor, TaskView};

/// First-in-first-out queue; ignores `Meta`.
pub struct FifoQueue<T> {
    inner: VecDeque<T>,
}

impl<T> RunQueue<T> for FifoQueue<T> {
    const FLAVOR: SchedFlavor = SchedFlavor::Fifo;

    fn with_capacity(cap: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(cap),
        }
    }

    fn push(&mut self, item: T, _view: &TaskView<'_>) {
        self.inner.push_back(item);
    }

    fn pop(&mut self) -> Option<T> {
        self.inner.pop_front()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_queue_sched_flavor() {
        assert_eq!(<FifoQueue<u64> as RunQueue<u64>>::FLAVOR, SchedFlavor::Fifo);
    }
}
