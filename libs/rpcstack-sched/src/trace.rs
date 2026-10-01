//! Test-only recording of the calls `custom::Queue` receives.
//!
//! Compiled only with the `lifecycle_trace` feature, which exists so tests of
//! the runtime can observe the order of run-queue calls. The log is per
//! thread, which matches the current-thread runtime: it runs on the thread
//! that calls `block_on`.

use std::cell::RefCell;

use crate::PollOutcome;

/// One call received by the traced queue.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum Event {
    /// `push` of a task, with the times and poll count of its `TaskView`.
    Push {
        task_id: u64,
        polls: u32,
        enqueued_at_ns: u64,
        first_enqueued_at_ns: u64,
    },
    /// `on_poll_start`.
    PollStart { task_id: u64, polls: u32 },
    /// `on_poll_end`.
    PollEnd {
        task_id: u64,
        polls: u32,
        outcome: PollOutcome,
    },
    /// `on_idle`.
    Idle,
    /// `on_task_exit`.
    Exit { task_id: u64 },
}

thread_local! {
    static LOG: RefCell<Vec<Event>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn record(event: Event) {
    LOG.with(|log| log.borrow_mut().push(event));
}

/// Return and clear the events recorded on this thread.
pub fn take() -> Vec<Event> {
    LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
}
