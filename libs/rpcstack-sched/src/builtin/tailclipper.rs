use std::collections::{BinaryHeap, VecDeque};

use super::keyed::Keyed;
use crate::{default_policy, RunQueue, SchedFlavor, TaskView};

/// Size of the round-robin window.
const N: usize = 6;

/// TailClipper queue: a priority heap fronted by a small round-robin window
/// holding the `N` best tasks. A new task evicts the window's worst task back
/// to the heap only if it is strictly better.
///
/// With `USE_INFRA_QUEUE`, infrastructure work (`default_policy::is_infra`)
/// goes to a FIFO lane drained before everything else.
pub struct BinaryHeapRoundRobinQueue<T, const USE_INFRA_QUEUE: bool = false> {
    heap: BinaryHeap<Keyed<T>>,
    rr_queue: VecDeque<Keyed<T>>,
    infra_rr_queue: VecDeque<T>,
}

impl<T, const USE_INFRA_QUEUE: bool> RunQueue<T> for BinaryHeapRoundRobinQueue<T, USE_INFRA_QUEUE> {
    const FLAVOR: SchedFlavor = SchedFlavor::Prio;

    fn with_capacity(cap: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(cap),
            rr_queue: VecDeque::with_capacity(N),
            infra_rr_queue: VecDeque::with_capacity(cap),
        }
    }

    fn push(&mut self, item: T, view: &TaskView<'_>) {
        if USE_INFRA_QUEUE && default_policy::is_infra(view.meta) {
            self.infra_rr_queue.push_back(item);
            return;
        }

        let item = Keyed::new(*view.meta, item);

        if !self.rr_queue.is_empty() {
            // Find index of item with minimum priority in rr_queue
            let mut min_idx = 0;
            for i in 1..self.rr_queue.len() {
                if self.rr_queue[i] < self.rr_queue[min_idx] {
                    min_idx = i;
                }
            }

            // If the new item has higher priority than the lowest-priority item in rr_queue, replace it.
            if item > self.rr_queue[min_idx] {
                if let Some(evicted) = self.rr_queue.remove(min_idx) {
                    self.heap.push(evicted);
                }
                self.rr_queue.push_back(item);
                return;
            }
        }

        self.heap.push(item);
    }

    fn pop(&mut self) -> Option<T> {
        if USE_INFRA_QUEUE {
            if let Some(item) = self.infra_rr_queue.pop_front() {
                return Some(item);
            }
        }

        if self.rr_queue.is_empty() {
            // Refill the round-robin queue with the top N items from the heap
            for _ in 0..N {
                if let Some(item) = self.heap.pop() {
                    self.rr_queue.push_back(item);
                }
            }
        }

        self.rr_queue.pop_front().map(|keyed| keyed.item)
    }

    fn len(&self) -> usize {
        self.heap.len() + self.rr_queue.len() + self.infra_rr_queue.len()
    }

    fn capacity(&self) -> usize {
        self.heap.capacity() + self.rr_queue.capacity() + self.infra_rr_queue.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Meta;

    type Queue<const INFRA: bool = false> = BinaryHeapRoundRobinQueue<u64, INFRA>;

    /// Pushes task `id` with priority value `prio`.
    fn push<const INFRA: bool>(queue: &mut Queue<INFRA>, id: u64, prio: u64) {
        let t0 = std::time::Instant::now();
        queue.push(id, &TaskView::new(id, &Meta::new(prio), t0, t0, 0));
    }

    #[test]
    fn prio_bh_rr_queue_sched_flavor() {
        assert_eq!(<Queue as RunQueue<u64>>::FLAVOR, SchedFlavor::Prio);
    }

    #[test]
    fn infra_priority() {
        let mut queue = Queue::<true>::with_capacity(0);

        push(&mut queue, 1, 100);
        push(&mut queue, 2, 0);
        push(&mut queue, 3, 50);

        // Infra task should come out first
        assert_eq!(queue.pop(), Some(2));
        // Then the higher priority normal task (smaller prio value = higher priority)
        assert_eq!(queue.pop(), Some(3));
        // Then the last one
        assert_eq!(queue.pop(), Some(1));
    }

    #[test]
    fn round_robin_refill() {
        let mut queue = Queue::<false>::with_capacity(0);

        // Push N+1 tasks. Higher priority (smaller val) comes out of heap first.
        for i in 0..N as u64 + 1 {
            push(&mut queue, i, (i + 1) * 10);
        }

        // 1st pop: RR empty. Refills from heap with the top N priorities.
        // The next N-1 pops come from that refill.
        for i in 0..N as u64 {
            assert_eq!(queue.pop(), Some(i));
        }

        // RR empty again: refills from the heap with the last item.
        assert_eq!(queue.pop(), Some(N as u64));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn eviction_logic() {
        let mut queue = Queue::<false>::with_capacity(0);

        for i in 0..N as u64 {
            push(&mut queue, i, (i + 1) * 10);
        }

        // Pop one to refill RR with N items; pop returns T0, leaving N-1.
        assert_eq!(queue.pop(), Some(0));

        // A high priority task evicts the lowest priority item in RR, T_{N-1},
        // and is appended after the remaining RR items.
        push(&mut queue, 100, 5);

        for i in 1..N as u64 - 1 {
            assert_eq!(queue.pop(), Some(i));
        }
        assert_eq!(queue.pop(), Some(100));

        // Finally, the evicted T_{N-1} from the heap.
        assert_eq!(queue.pop(), Some(N as u64 - 1));
    }

    #[test]
    fn push_no_eviction_when_priority_low() {
        let mut queue = Queue::<false>::with_capacity(0);

        for i in 0..N as u64 {
            push(&mut queue, i, (i + 1) * 10);
        }
        assert_eq!(queue.pop(), Some(0));

        // A task worse than anything in RR stays in the heap.
        push(&mut queue, 100, (N as u64 + 1) * 10);

        for i in 1..N as u64 {
            assert_eq!(queue.pop(), Some(i));
        }
        assert_eq!(queue.pop(), Some(100));
    }

    #[test]
    fn len_and_capacity() {
        let mut queue = Queue::<false>::with_capacity(0);
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());

        push(&mut queue, 1, 10);
        assert_eq!(queue.len(), 1);
        assert!(!queue.is_empty());

        push(&mut queue, 2, 0);
        assert_eq!(queue.len(), 2);

        assert!(queue.capacity() >= 2);

        queue.pop();
        queue.pop();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
    }
}
