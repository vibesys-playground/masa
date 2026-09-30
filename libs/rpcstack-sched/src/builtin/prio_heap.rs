use std::collections::{BinaryHeap, VecDeque};

use super::keyed::Keyed;
use crate::{default_policy, RunQueue, SchedFlavor, TaskView};

/// Priority-heap queue: smaller `Meta` values first. Infrastructure work
/// (`default_policy::is_infra`) goes to a FIFO lane that is always drained
/// before the heap.
pub struct BinaryHeapQueue<T> {
    q: BinaryHeap<Keyed<T>>,
    infra_q: VecDeque<T>,
}

impl<T> RunQueue<T> for BinaryHeapQueue<T> {
    const FLAVOR: SchedFlavor = SchedFlavor::Prio;

    fn with_capacity(cap: usize) -> Self {
        Self {
            q: BinaryHeap::with_capacity(cap),
            infra_q: VecDeque::new(),
        }
    }

    fn push(&mut self, item: T, view: &TaskView<'_>) {
        if default_policy::is_infra(view.meta) {
            self.infra_q.push_back(item);
        } else {
            self.q.push(Keyed::new(*view.meta, item));
        }
    }

    fn pop(&mut self) -> Option<T> {
        if let Some(item) = self.infra_q.pop_front() {
            return Some(item);
        }
        self.q.pop().map(|keyed| keyed.item)
    }

    fn len(&self) -> usize {
        self.q.len() + self.infra_q.len()
    }

    fn capacity(&self) -> usize {
        self.q.capacity() + self.infra_q.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prio_queue_sched_flavor() {
        assert_eq!(
            <BinaryHeapQueue<u64> as RunQueue<u64>>::FLAVOR,
            SchedFlavor::Prio
        );
    }
}
