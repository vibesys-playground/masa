//! Tests of the callback contract and the facts in `TaskView`.
//!
//! This crate has no runtime, so a small scripted driver plays its part: it
//! owns the per-task facts (enqueue times, poll count) the way the runtime
//! does, builds the `TaskView`s and makes the calls in the documented order.
//! Times are plain numbers, so every scenario is deterministic.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::builtin::fifo::FifoQueue;
use crate::builtin::prio_heap::BinaryHeapQueue;
use crate::builtin::tailclipper::BinaryHeapRoundRobinQueue;
use crate::custom;
use crate::{Meta, PollOutcome, RunQueue, TaskView};

/// The instant `ns` nanoseconds after a fixed origin, so scripts can use plain
/// numbers for times.
fn at(ns: u64) -> Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    *ORIGIN.get_or_init(Instant::now) + Duration::from_nanos(ns)
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Call {
    Push {
        id: u64,
        enqueued: Instant,
        first: Instant,
        polls: u32,
    },
    PollStart(u64, u32),
    PollEnd(u64, u32, PollOutcome),
    Idle,
    Exit(u64),
}

/// A FIFO queue that logs every call it receives.
#[derive(Default)]
struct Recording {
    items: std::collections::VecDeque<u64>,
    log: Vec<Call>,
}

impl RunQueue<u64> for Recording {
    const FLAVOR: crate::SchedFlavor = crate::SchedFlavor::Fifo;

    fn with_capacity(_cap: usize) -> Self {
        Self::default()
    }

    fn push(&mut self, item: u64, view: &TaskView<'_>) {
        self.log.push(Call::Push {
            id: view.task_id,
            enqueued: view.enqueued_at,
            first: view.first_enqueued_at,
            polls: view.polls,
        });
        self.items.push_back(item);
    }

    fn pop(&mut self) -> Option<u64> {
        self.items.pop_front()
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn capacity(&self) -> usize {
        self.items.capacity()
    }

    fn on_poll_start(&mut self, view: &TaskView<'_>) {
        self.log.push(Call::PollStart(view.task_id, view.polls));
    }

    fn on_poll_end(&mut self, view: &TaskView<'_>, outcome: PollOutcome) {
        self.log
            .push(Call::PollEnd(view.task_id, view.polls, outcome));
    }

    fn on_idle(&mut self) {
        self.log.push(Call::Idle);
    }

    fn on_task_exit(&mut self, task_id: u64) {
        self.log.push(Call::Exit(task_id));
    }
}

/// Plays the runtime's part for a queue: tracks per-task facts, builds views
/// and makes the calls in the documented order.
struct Driver<Q> {
    queue: Q,
    /// `(meta, first_enqueued_at_ns, last enqueued_at_ns, polls)` per live task.
    tasks: HashMap<u64, (Meta, u64, u64, u32)>,
}

impl<Q: RunQueue<u64>> Driver<Q> {
    fn new(queue: Q) -> Self {
        Self {
            queue,
            tasks: HashMap::new(),
        }
    }

    fn push(&mut self, id: u64, now: u64) {
        self.push_with_prio(id, now, 10);
    }

    /// Push a task. `prio` is used only the first time a task is pushed.
    fn push_with_prio(&mut self, id: u64, now: u64, prio: u64) {
        let (meta, first, last, polls) =
            self.tasks
                .entry(id)
                .or_insert((Meta::new(prio), now, now, 0));
        *last = now;
        let view = TaskView::new(id, meta, at(*last), at(*first), *polls);
        self.queue.push(id, &view);
    }

    /// Pop a task and poll it; `ready` says whether the poll finishes it.
    fn poll_next(&mut self, ready: bool) -> Option<u64> {
        self.poll_next_where(|_| ready)
    }

    /// Pop a task and poll it; `finishes(id)` says whether the poll finishes it.
    fn poll_next_where(&mut self, finishes: impl Fn(u64) -> bool) -> Option<u64> {
        let id = self.queue.pop()?;
        let facts = self.tasks.get_mut(&id).unwrap();
        facts.3 += 1;
        let (meta, first, last, polls) = *facts;
        let view = TaskView::new(id, &meta, at(last), at(first), polls);
        self.queue.on_poll_start(&view);
        let ready = finishes(id);
        let outcome = if ready {
            PollOutcome::Ready
        } else {
            PollOutcome::Pending
        };
        self.queue.on_poll_end(&view, outcome);
        if ready {
            self.tasks.remove(&id);
            self.queue.on_task_exit(id);
        }
        Some(id)
    }
}

#[test]
fn views_carry_enqueue_times_and_poll_counts_across_repushes() {
    let mut d = Driver::new(Recording::default());
    d.push(1, 100);
    d.push(2, 150);
    assert_eq!(d.poll_next(false), Some(1));
    assert_eq!(d.poll_next(false), Some(2));
    d.push(2, 400);
    d.push(1, 500);
    assert_eq!(d.poll_next(true), Some(2));
    assert_eq!(d.poll_next(true), Some(1));
    d.queue.on_idle();

    let push = |id, enqueued, first, polls| Call::Push {
        id,
        enqueued: at(enqueued),
        first: at(first),
        polls,
    };
    assert_eq!(
        d.queue.log,
        [
            push(1, 100, 100, 0),
            push(2, 150, 150, 0),
            Call::PollStart(1, 1),
            Call::PollEnd(1, 1, PollOutcome::Pending),
            Call::PollStart(2, 1),
            Call::PollEnd(2, 1, PollOutcome::Pending),
            // Re-pushes: the enqueue time is new, the first time is kept and
            // the poll count grew.
            push(2, 400, 150, 1),
            push(1, 500, 100, 1),
            Call::PollStart(2, 2),
            Call::PollEnd(2, 2, PollOutcome::Ready),
            Call::Exit(2),
            Call::PollStart(1, 2),
            Call::PollEnd(1, 2, PollOutcome::Ready),
            Call::Exit(1),
            Call::Idle,
        ]
    );
}

#[test]
fn exit_fires_once_per_task_and_after_its_last_poll_end() {
    let mut d = Driver::new(Recording::default());
    for id in 1..=3 {
        d.push(id, id * 10);
    }
    while d.poll_next(true).is_some() {}
    for id in 1..=3 {
        let exits: Vec<_> = d
            .queue
            .log
            .iter()
            .enumerate()
            .filter(|(_, c)| **c == Call::Exit(id))
            .collect();
        assert_eq!(exits.len(), 1);
        let end = d
            .queue
            .log
            .iter()
            .position(|c| *c == Call::PollEnd(id, 1, PollOutcome::Ready))
            .unwrap();
        assert!(end < exits[0].0);
    }
    assert!(d.tasks.is_empty());
}

/// Run the same script against `Q` with and without the callbacks; the pop
/// order must be the same, since built-in queues ignore the callbacks.
fn pop_order<Q: RunQueue<u64>>(call_callbacks: bool) -> Vec<u64> {
    let mut q = Q::with_capacity(8);
    let metas = [Meta::new(30), Meta::new(0), Meta::new(20), Meta::new(10)];
    let mut order = Vec::new();
    for (i, meta) in metas.iter().enumerate() {
        let id = i as u64 + 1;
        q.push(id, &TaskView::new(id, meta, at(0), at(0), 0));
    }
    while let Some(id) = q.pop() {
        if call_callbacks {
            let view = TaskView::new(id, &metas[0], at(5), at(0), 1);
            q.on_poll_start(&view);
            q.on_poll_end(&view, PollOutcome::Pending);
            q.on_idle();
            q.on_task_exit(id);
        }
        order.push(id);
    }
    order
}

#[test]
fn builtin_queues_ignore_callbacks() {
    assert_eq!(
        pop_order::<FifoQueue<u64>>(true),
        pop_order::<FifoQueue<u64>>(false)
    );
    assert_eq!(
        pop_order::<BinaryHeapQueue<u64>>(true),
        pop_order::<BinaryHeapQueue<u64>>(false)
    );
    assert_eq!(
        pop_order::<BinaryHeapRoundRobinQueue<u64, false>>(true),
        pop_order::<BinaryHeapRoundRobinQueue<u64, false>>(false)
    );
    assert_eq!(
        pop_order::<BinaryHeapRoundRobinQueue<u64, true>>(true),
        pop_order::<BinaryHeapRoundRobinQueue<u64, true>>(false)
    );
    assert_eq!(
        pop_order::<custom::Queue<u64>>(true),
        pop_order::<custom::Queue<u64>>(false)
    );
}

/// An aging queue, written only with what `TaskView` and the callbacks give:
/// a queued task's effective priority improves by one for every `STEP_NS` it
/// has existed, so a low-priority task is eventually served even while new
/// high-priority tasks keep arriving.
///
/// The queue has no clock; "now" is the newest enqueue time it has seen. It
/// keeps per-task state and drops it in `on_task_exit`.
#[derive(Default)]
struct AgingQueue {
    queued: Vec<(u64, u64)>,
    /// `task_id -> (base priority, first enqueue time)`.
    state: HashMap<u64, (u64, Instant)>,
    now: Option<Instant>,
}

impl AgingQueue {
    const STEP_NS: u64 = 100;

    fn effective(&self, id: u64) -> u64 {
        let (base, first) = self.state[&id];
        let age = self
            .now
            .map_or(Duration::ZERO, |now| now.duration_since(first));
        base.saturating_sub(age.as_nanos() as u64 / Self::STEP_NS)
    }
}

impl RunQueue<u64> for AgingQueue {
    const FLAVOR: crate::SchedFlavor = crate::SchedFlavor::Prio;

    fn with_capacity(_cap: usize) -> Self {
        Self::default()
    }

    fn push(&mut self, item: u64, view: &TaskView<'_>) {
        self.now = self.now.max(Some(view.enqueued_at));
        self.state
            .entry(view.task_id)
            .or_insert((view.meta.value(), view.first_enqueued_at));
        self.queued.push((view.task_id, item));
    }

    fn pop(&mut self) -> Option<u64> {
        let best = (0..self.queued.len()).min_by_key(|&i| (self.effective(self.queued[i].0), i))?;
        Some(self.queued.remove(best).1)
    }

    fn len(&self) -> usize {
        self.queued.len()
    }

    fn capacity(&self) -> usize {
        self.queued.capacity()
    }

    fn on_task_exit(&mut self, task_id: u64) {
        self.state.remove(&task_id);
    }
}

/// Task 1 has priority 50. From time 100 a new priority-10 task arrives every
/// 100 ns and each round polls one task to completion. Returns the round in
/// which task 1 was first polled, if it was within `rounds`.
fn round_of_low_priority_poll<Q: RunQueue<u64>>(queue: Q, rounds: u64) -> Option<u64> {
    let mut d = Driver::new(queue);
    d.push_with_prio(1, 0, 50);
    for round in 1..=rounds {
        d.push_with_prio(round + 1, round * 100, 10);
        if d.poll_next_where(|_| true) == Some(1) {
            return Some(round);
        }
    }
    None
}

#[test]
fn aging_queue_serves_old_low_priority_task_before_newer_high_priority_ones() {
    // Without aging the priority heap never reaches task 1.
    assert_eq!(
        round_of_low_priority_poll(BinaryHeapQueue::<u64>::with_capacity(8), 200),
        None
    );
    // Aging: the task's priority 50 improves by one per 100 ns and ties with
    // the newcomers' 10 after 40 steps; the older task wins the tie.
    assert_eq!(
        round_of_low_priority_poll(AgingQueue::with_capacity(8), 200),
        Some(40)
    );
}

#[test]
fn aging_queue_keeps_no_state_for_finished_tasks() {
    let mut d = Driver::new(AgingQueue::default());
    for round in 0..100u64 {
        d.push_with_prio(round, round * 100, round % 7);
        if round % 2 == 1 {
            d.poll_next_where(|_| true);
            d.poll_next_where(|_| true);
        }
        assert!(
            d.queue.state.len() <= d.queue.len(),
            "state for {} tasks, {} queued",
            d.queue.state.len(),
            d.queue.len()
        );
    }
    while d.poll_next_where(|_| true).is_some() {}
    assert!(d.queue.state.is_empty());
}
