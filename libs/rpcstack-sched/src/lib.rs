//! Scheduling building blocks for the Masa runtime.
//!
//! The runtime (a vendored Tokio) owns mechanisms only: it stores a [`Meta`]
//! per task, hands each runnable task to a [`RunQueue`], and asks the queue
//! which task to poll next. Everything that decides what a `Meta` means, or
//! which task should go first, lives in this crate:
//!
//! - [`RunQueue`], [`TaskView`], [`SchedFlavor`]: the interface (mechanism).
//! - [`Meta`]: the per-task scheduling metadata type.
//! - [`builtin`]: the built-in queues (FIFO, priority heap, TailClipper).
//!   They interpret `Meta` and so embody policy.
//! - [`custom`]: a copy of the priority-heap queue, meant to be edited to
//!   write a new queue.
//! - [`default_policy`]: which `Meta` a spawn gets when its caller gave none.
//!
//! The queue the runtime uses is chosen at compile time by Cargo features and
//! exposed as [`SelectedQueue`].
//!
//! This is a leaf crate: it depends on no runtime, RPC or Masa crate.

pub mod builtin;
pub mod custom;
pub mod default_policy;
mod meta;
mod queue;

#[cfg(test)]
mod replay_tests;

pub use meta::Meta;
pub use queue::{RunQueue, SchedFlavor, TaskView};

/// The run queue selected by Cargo features.
///
/// `sched_custom` selects [`custom::Queue`]. Otherwise `tailclipper` selects
/// the TailClipper queue, `sched_prio` the priority heap, and with none of
/// them the queue is FIFO.
#[cfg(feature = "sched_custom")]
pub type SelectedQueue<T> = custom::Queue<T>;

/// The run queue selected by Cargo features.
#[cfg(all(
    feature = "sched_prio",
    feature = "tailclipper",
    not(feature = "sched_custom")
))]
pub type SelectedQueue<T> = builtin::tailclipper::BinaryHeapRoundRobinQueue<T, false>;

/// The run queue selected by Cargo features.
#[cfg(all(
    feature = "sched_prio",
    not(feature = "tailclipper"),
    not(feature = "sched_custom")
))]
pub type SelectedQueue<T> = builtin::prio_heap::BinaryHeapQueue<T>;

/// The run queue selected by Cargo features.
#[cfg(not(any(feature = "sched_prio", feature = "sched_custom")))]
pub type SelectedQueue<T> = builtin::fifo::FifoQueue<T>;
