use std::time::Instant;

use crate::Meta;

/// Read-only facts about a task that a queue may use to place it.
///
/// The queue sees a task only through this view, so the queued item `T` needs
/// no trait bounds: the runtime passes whatever it stores. The view carries
/// facts only; what they mean for ordering is up to the queue.
///
/// The runtime supplies the times, so a queue needs no clock of its own and a
/// test can build a view with any times (an `Instant` plus a `Duration`).
#[derive(Debug, Clone, Copy)]
pub struct TaskView<'a> {
    /// The runtime's identifier of the task. It stays the same every time the
    /// task is pushed again and is unique among live tasks.
    pub task_id: u64,
    /// The task's scheduling metadata.
    pub meta: &'a Meta,
    /// When the enqueue this view describes happened. During `on_poll_start`
    /// and `on_poll_end` it is the time of the enqueue that led to the poll.
    pub enqueued_at: Instant,
    /// When the task first entered any queue. Equal to `enqueued_at` on
    /// the first push, and unchanged by later pushes.
    pub first_enqueued_at: Instant,
    /// Number of polls the task has started so far, counting the poll in
    /// progress during `on_poll_start` and `on_poll_end`. It is 0 at a task's
    /// first push and 1 when it is pushed again after one poll.
    pub polls: u32,
}

impl<'a> TaskView<'a> {
    /// Create a view of a task.
    pub fn new(
        task_id: u64,
        meta: &'a Meta,
        enqueued_at: Instant,
        first_enqueued_at: Instant,
        polls: u32,
    ) -> Self {
        Self {
            task_id,
            meta,
            enqueued_at,
            first_enqueued_at,
            polls,
        }
    }
}

/// How a poll ended, as reported to [`RunQueue::on_poll_end`].
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum PollOutcome {
    /// The task is not finished. It will be pushed again when something wakes
    /// it, unless it already woke itself during the poll.
    Pending,
    /// The task is finished and will never be pushed again: its future
    /// completed or panicked, or it was cancelled and the runtime observed
    /// that when it picked the task up. [`RunQueue::on_task_exit`] follows.
    Ready,
}

/// Describes the strategy a queue implements.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum SchedFlavor {
    /// The scheduler executes tasks in first-in-first-out order.
    Fifo,
    /// The scheduler executes tasks based on priority.
    Prio,
}

/// The run queue of a single-threaded scheduler: holds runnable tasks of type
/// `T` and decides the order they are polled in.
///
/// The runtime records the times in [`TaskView`] itself, around `push` and
/// `pop`. A queue only orders. The `on_*` callbacks give a queue that keeps
/// per-task state the events it needs to maintain and clean up that state;
/// they default to doing nothing.
pub trait RunQueue<T> {
    /// The strategy this queue implements.
    const FLAVOR: SchedFlavor;

    /// Create an empty queue. `cap` is a capacity hint and must not affect the
    /// pop order.
    fn with_capacity(cap: usize) -> Self;

    /// Add a runnable task. `view` describes `item`.
    fn push(&mut self, item: T, view: &TaskView<'_>);

    /// Remove and return the next task to poll, or `None` when empty.
    fn pop(&mut self) -> Option<T>;

    /// Number of queued tasks.
    fn len(&self) -> usize;

    /// Whether no task is queued.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of tasks the queue can hold without reallocating.
    fn capacity(&self) -> usize;

    /// Called just before the runtime polls a task it took from this queue
    /// (or from the cross-thread injection queue, which bypasses `push`).
    fn on_poll_start(&mut self, _view: &TaskView<'_>) {}

    /// Called right after that poll returned, before the runtime picks the
    /// next task. `outcome` says whether the task finished. The task may
    /// already have been pushed again during the poll (for example by
    /// `yield_now`), in which case that `push` came before this call.
    fn on_poll_end(&mut self, _view: &TaskView<'_>, _outcome: PollOutcome) {}

    /// Called when the runtime found nothing runnable, just before it parks
    /// the thread or yields to the driver. It can fire many times per run.
    fn on_idle(&mut self) {}

    /// Called once when a task is gone for good: it completed, panicked or was
    /// cancelled. Use it to drop per-task state kept for `task_id`.
    ///
    /// It follows the `on_poll_end(.., Ready)` of the poll the task ended in.
    /// A task that ends without a poll (cancelled while queued during runtime
    /// shutdown) gets this call with no `on_poll_start` or `on_poll_end`.
    fn on_task_exit(&mut self, _task_id: u64) {}
}
