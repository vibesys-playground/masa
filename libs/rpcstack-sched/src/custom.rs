//! The slot for writing a new run queue.
//!
//! `Queue` starts as a copy of the default priority-heap queue
//! (`builtin::prio_heap::BinaryHeapQueue`). Build with the `sched_custom`
//! feature (Tokio forwards it) to make the runtime use it, then edit it freely:
//! implement [`RunQueue`], keep any ordering state next to the item, and read
//! the task's [`Meta`](crate::Meta) and timing facts from the [`TaskView`]
//! passed to `push`. The `on_*` callbacks of [`RunQueue`] tell a queue that
//! keeps per-task state when to update and clean it up.
//! The replay tests in this crate run against `Queue` under `sched_custom`;
//! update their expected pop orders when you change the behavior on purpose.

use std::collections::{BinaryHeap, VecDeque};

use crate::builtin::keyed::Keyed;
use crate::{default_policy, PollOutcome, RunQueue, SchedFlavor, TaskView};

/// Priority-heap queue: smaller `Meta` values first, with infrastructure work
/// in a FIFO lane drained before the heap.
pub struct Queue<T> {
    q: BinaryHeap<Keyed<T>>,
    infra_q: VecDeque<T>,
}

impl<T> RunQueue<T> for Queue<T> {
    const FLAVOR: SchedFlavor = SchedFlavor::Prio;

    fn with_capacity(cap: usize) -> Self {
        Self {
            q: BinaryHeap::with_capacity(cap),
            infra_q: VecDeque::new(),
        }
    }

    fn push(&mut self, item: T, view: &TaskView<'_>) {
        #[cfg(feature = "lifecycle_trace")]
        crate::trace::record(crate::trace::Event::Push {
            task_id: view.task_id,
            polls: view.polls,
            enqueued_at: view.enqueued_at,
            first_enqueued_at: view.first_enqueued_at,
        });
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

    fn on_poll_start(&mut self, _view: &TaskView<'_>) {
        #[cfg(feature = "lifecycle_trace")]
        crate::trace::record(crate::trace::Event::PollStart {
            task_id: _view.task_id,
            polls: _view.polls,
        });
    }

    fn on_poll_end(&mut self, _view: &TaskView<'_>, _outcome: PollOutcome) {
        #[cfg(feature = "lifecycle_trace")]
        crate::trace::record(crate::trace::Event::PollEnd {
            task_id: _view.task_id,
            polls: _view.polls,
            outcome: _outcome,
        });
    }

    fn on_idle(&mut self) {
        #[cfg(feature = "lifecycle_trace")]
        crate::trace::record(crate::trace::Event::Idle);
    }

    fn on_task_exit(&mut self, _task_id: u64) {
        #[cfg(feature = "lifecycle_trace")]
        crate::trace::record(crate::trace::Event::Exit { task_id: _task_id });
    }
}
