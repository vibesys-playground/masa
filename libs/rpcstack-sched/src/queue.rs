use crate::Meta;

/// Read-only facts about a task that a queue may use to place it.
///
/// The queue sees a task only through this view, so the queued item `T` needs
/// no trait bounds: the runtime passes whatever it stores.
#[derive(Debug, Clone, Copy)]
pub struct TaskView<'a> {
    /// The runtime's identifier of the task.
    pub task_id: u64,
    /// The task's scheduling metadata.
    pub meta: &'a Meta,
}

impl<'a> TaskView<'a> {
    /// Create a view of a task.
    pub fn new(task_id: u64, meta: &'a Meta) -> Self {
        Self { task_id, meta }
    }
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
/// The runtime records enqueue and dequeue timestamps itself, around `push` and
/// `pop`. A queue only orders.
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

    /// Called before the runtime polls a task. Not yet called by the runtime.
    fn on_poll_start(&mut self, _view: &TaskView<'_>) {}

    /// Called after the runtime polled a task. Not yet called by the runtime.
    fn on_poll_end(&mut self, _view: &TaskView<'_>) {}

    /// Called when the runtime has no runnable task. Not yet called by the
    /// runtime.
    fn on_idle(&mut self) {}
}
