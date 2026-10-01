//! The mechanism side of run queues: Tokio stores runnable tasks in the queue
//! chosen by `rpcstack-sched` and never inspects how it orders them.

use rpcstack_sched::{RunQueue, SelectedQueue};

pub use rpcstack_sched::SchedFlavor;

/// The run queue of the current-thread scheduler, selected by Cargo features.
pub(crate) type LocalRunQueue<T> = SelectedQueue<T>;

/// Get the scheduling flavor used by this runtime instantiation.
pub fn get_sched_flavor() -> SchedFlavor {
    <SelectedQueue<u64> as RunQueue<u64>>::FLAVOR
}

/// Get the current queue length for the current_thread runtime.
///
/// # Panics
///
/// This function will panic if:
/// - Called from outside a Tokio runtime context
/// - Called from a multi-threaded runtime (only works with current_thread runtime)
/// - Called when the scheduler core is not available (e.g., outside of `block_on`)
pub fn current_thread_queue_len() -> usize {
    use crate::runtime::context;
    use crate::runtime::scheduler::Context;

    context::with_scheduler(|maybe_context| {
        let context = match maybe_context {
            Some(Context::CurrentThread(ctx)) => ctx,
            #[cfg(feature = "rt-multi-thread")]
            Some(_) => panic!(
                "current_thread_queue_len() can only be called from a current_thread runtime"
            ),
            None => panic!(
                "current_thread_queue_len() must be called from within a Tokio runtime context"
            ),
        };

        match context.queue_len() {
            Some(len) => len,
            None => {
                panic!("current_thread_queue_len() called when scheduler core is not available")
            }
        }
    })
}
