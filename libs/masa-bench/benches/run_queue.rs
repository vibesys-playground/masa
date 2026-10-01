//! The built-in run queues of `rpcstack-sched`, measured directly.
//!
//! Unlike the `spawn_poll` benchmark this involves no tokio runtime, so it
//! isolates the queue itself. Both scenarios keep the queue at a steady depth
//! of live tasks, with scattered priorities:
//!
//! - `push_pop`: one `push` plus one `pop` per step.
//! - `lifecycle`: a task's full life as the runtime drives it: `pop`,
//!   `on_poll_start`, `on_poll_end`, then `push` again, until its third poll
//!   returns `Ready` and `on_task_exit` runs; a new task takes its place. The
//!   reported time is per poll.
//!
//! Queues: `fifo`, `prio_heap`, `tailclipper` and `custom` (the copy of the
//! priority heap meant for editing). A depth of 16 is a lightly loaded service
//! and 4096 a saturated one.
//!
//! Usage: `run_queue [steps per run]` (default 2000000; best of 5 runs).

use std::hint::black_box;
use std::time::Instant;

use masa_bench::report::{metric, note};
use rpcstack_sched::builtin::fifo::FifoQueue;
use rpcstack_sched::builtin::prio_heap::BinaryHeapQueue;
use rpcstack_sched::builtin::tailclipper::BinaryHeapRoundRobinQueue;
use rpcstack_sched::{custom, Meta, PollOutcome, RunQueue, TaskView};

const DEPTHS: [usize; 2] = [16, 4096];
const POLLS_PER_TASK: u32 = 3;

/// A deterministic scatter of priorities, so the heap does real work.
fn priority(i: u64) -> Meta {
    Meta::new((i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) + 1)
}

/// Nanoseconds per step of `step`, best of `runs` runs of `steps` steps.
fn best_ns(runs: u32, steps: u64, mut step: impl FnMut(u64)) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..runs {
        let start = Instant::now();
        for i in 0..steps {
            step(i);
        }
        best = best.min(start.elapsed().as_nanos() as f64 / steps as f64);
    }
    best
}

fn push_pop<Q: RunQueue<u64>>(depth: usize, steps: u64, now: Instant) -> f64 {
    let mut queue = Q::with_capacity(depth + 1);
    for i in 0..depth as u64 {
        let meta = priority(i);
        queue.push(i, &TaskView::new(i, &meta, now, now, 0));
    }
    best_ns(5, steps, |i| {
        let id = depth as u64 + i;
        let meta = priority(id);
        queue.push(id, &TaskView::new(id, &meta, now, now, 0));
        black_box(queue.pop());
    })
}

fn lifecycle<Q: RunQueue<u64>>(depth: usize, steps: u64, now: Instant) -> f64 {
    let mut queue = Q::with_capacity(depth + 1);
    // The queue holds slot numbers. Each slot has a live task, whose id and
    // started polls are kept here; a finished task's slot starts a new task.
    let mut polls = vec![0u32; depth];
    let mut next_id = depth as u64;
    let mut ids: Vec<u64> = (0..depth as u64).collect();
    for (slot, id) in ids.iter().enumerate() {
        let meta = priority(*id);
        queue.push(slot as u64, &TaskView::new(*id, &meta, now, now, 0));
    }
    best_ns(5, steps, |_| {
        let Some(slot) = queue.pop() else { return };
        let slot_index = slot as usize;
        let id = ids[slot_index];
        let meta = priority(id);
        polls[slot_index] += 1;
        let view = TaskView::new(id, &meta, now, now, polls[slot_index]);
        queue.on_poll_start(&view);
        if polls[slot_index] < POLLS_PER_TASK {
            queue.on_poll_end(&view, PollOutcome::Pending);
            queue.push(slot, &view);
        } else {
            queue.on_poll_end(&view, PollOutcome::Ready);
            queue.on_task_exit(id);
            polls[slot_index] = 0;
            let new_id = next_id;
            next_id += 1;
            ids[slot_index] = new_id;
            let new_meta = priority(new_id);
            queue.push(slot, &TaskView::new(new_id, &new_meta, now, now, 0));
        }
    })
}

fn measure<Q: RunQueue<u64>>(name: &str, steps: u64, now: Instant) {
    for depth in DEPTHS {
        let push_pop_ns = push_pop::<Q>(depth, steps, now);
        let lifecycle_ns = lifecycle::<Q>(depth, steps, now);
        metric(
            "run_queue",
            name,
            &format!("push_pop_depth{depth}_ns"),
            push_pop_ns,
        );
        metric(
            "run_queue",
            name,
            &format!("lifecycle_depth{depth}_ns"),
            lifecycle_ns,
        );
        note(&format!(
            "run_queue [{name}] depth {depth:>4}: push+pop {push_pop_ns:>6.1} ns, lifecycle poll {lifecycle_ns:>6.1} ns"
        ));
    }
}

fn main() {
    let steps: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let now = Instant::now();
    measure::<FifoQueue<u64>>("fifo", steps, now);
    measure::<BinaryHeapQueue<u64>>("prio_heap", steps, now);
    measure::<BinaryHeapRoundRobinQueue<u64, false>>("tailclipper", steps, now);
    measure::<custom::Queue<u64>>("custom", steps, now);
}
