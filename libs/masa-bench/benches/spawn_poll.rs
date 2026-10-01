//! Tokio current-thread spawn and poll throughput under the selected run queue.
//!
//! Spawns a batch of tasks with scattered priorities, each of which yields
//! twice (so it is polled three times and re-enqueued twice), and awaits them
//! all. The cost per poll covers the run queue's push and pop, the task
//! header's metadata and the scheduler loop; the future does no work. The run
//! queue is chosen at compile time: FIFO by default, or one of the
//! `queue_prio`, `queue_tailclipper` and `queue_custom` features.
//!
//! Usage: `spawn_poll [tasks per batch] [batches]` (default 300000, 5).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use masa_bench::report::{metric, note};

/// A future that returns `Pending` (after waking itself) a fixed number of
/// times, then `Ready`.
struct YieldN(u32);

impl Future for YieldN {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            Poll::Ready(())
        } else {
            self.0 -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(any(
    feature = "queue_prio",
    feature = "queue_tailclipper",
    feature = "queue_custom"
))]
const QUEUE: &str = if cfg!(feature = "queue_custom") {
    "custom"
} else if cfg!(feature = "queue_tailclipper") {
    "tailclipper"
} else {
    "prio"
};

#[cfg(not(any(
    feature = "queue_prio",
    feature = "queue_tailclipper",
    feature = "queue_custom"
)))]
const QUEUE: &str = "fifo";

#[cfg(any(
    feature = "queue_prio",
    feature = "queue_tailclipper",
    feature = "queue_custom"
))]
fn spawn_one(i: u64) -> tokio::task::JoinHandle<()> {
    // A multiplicative hash scatters the priorities so the heap does real work.
    let priority = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) + 1;
    tokio::task::spawn_with_prio(YieldN(2), tokio::task::TaskPriority::new(priority))
}

#[cfg(not(any(
    feature = "queue_prio",
    feature = "queue_tailclipper",
    feature = "queue_custom"
)))]
fn spawn_one(_i: u64) -> tokio::task::JoinHandle<()> {
    tokio::spawn(YieldN(2))
}

fn main() {
    let tasks: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(300_000);
    let batches: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(|err| panic!("cannot build the tokio runtime: {err}"));

    // The first batch warms the allocator and caches and is not reported.
    let mut ns_per_poll = Vec::with_capacity(batches);
    for batch in 0..=batches {
        let start = Instant::now();
        runtime.block_on(async {
            let handles: Vec<_> = (0..tasks).map(spawn_one).collect();
            for handle in handles {
                handle
                    .await
                    .unwrap_or_else(|err| panic!("task failed: {err}"));
            }
        });
        if batch > 0 {
            ns_per_poll.push(start.elapsed().as_nanos() as f64 / (tasks * 3) as f64);
        }
    }
    ns_per_poll.sort_by(f64::total_cmp);
    let best = ns_per_poll[0];
    let median = ns_per_poll[ns_per_poll.len() / 2];
    metric("spawn_poll", QUEUE, "ns_per_poll", best);
    metric("spawn_poll", QUEUE, "median_ns_per_poll", median);
    note(&format!(
        "spawn_poll [{QUEUE}] {best:.1} ns/poll best, {median:.1} median ({tasks} tasks x {batches} batches, 3 polls each)"
    ));
}
