//! Replay tests that pin the pop order of every local run queue.
//!
//! Each script is a sequence of pushes and pops. The expected pop order and
//! queue length after every step were recorded from the queues as implemented
//! before the `Meta` refactor and must not change when that refactor lands.
//! A pop on an empty queue is recorded as `-1`.

use super::Queue;
use crate::runtime::task::{Id, Identifiable, TraceTimer, Traceable};
use crate::task::{TaskPrioritize, TaskPriority};

#[derive(Clone, Copy)]
enum Op {
    /// Push a task with `(id, priority)`.
    Push(u64, u64),
    Pop,
}

use Op::{Pop, Push};

struct MockTask {
    id: Id,
    priority: TaskPriority,
    timer: TraceTimer,
}

impl MockTask {
    fn new(id: u64, priority: u64) -> Self {
        Self {
            id: Id(id),
            priority: TaskPriority::new(priority),
            timer: TraceTimer::new(),
        }
    }
}

impl Traceable for MockTask {
    fn timer(&mut self) -> &mut TraceTimer {
        &mut self.timer
    }
}

impl Identifiable for MockTask {
    fn id(&self) -> Id {
        self.id
    }
}

impl TaskPrioritize for MockTask {
    fn priority(&self) -> TaskPriority {
        self.priority
    }
}

impl PartialEq for MockTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for MockTask {}

impl PartialOrd for MockTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MockTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

/// Runs `script` against a fresh queue; returns the popped ids and the queue
/// length after every step.
fn replay<Q: Queue<Item = MockTask>>(cap: usize, script: &[Op]) -> (Vec<i64>, Vec<usize>) {
    let mut q = Q::with_capacity(cap);
    let mut popped = Vec::new();
    let mut lens = Vec::new();
    for op in script {
        match *op {
            Push(id, prio) => {
                assert!(q.push(MockTask::new(id, prio)).is_ok());
                assert!(!q.is_full());
            }
            Pop => popped.push(match q.pop() {
                Ok(t) => t.id.0 as i64,
                Err(_) => -1,
            }),
        }
        lens.push(q.len());
    }
    (popped, lens)
}

const TIES: &[Op] = &[
    Push(1, 10),
    Push(2, 10),
    Push(3, 10),
    Push(4, 10),
    Push(5, 10),
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
];

const MIXED_WITH_INFRA: &[Op] = &[
    Push(1, 30),
    Push(2, 0),
    Push(3, 20),
    Push(4, 0),
    Push(5, 20),
    Push(6, 10),
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
];

const INTERLEAVED: &[Op] = &[
    Push(1, 50),
    Push(2, 40),
    Pop,
    Push(3, 30),
    Push(4, 0),
    Pop,
    Pop,
    Push(5, 45),
    Pop,
    Pop,
    Pop,
    Pop,
];

const ONLY_INFRA: &[Op] = &[
    Push(1, 0),
    Push(2, 0),
    Push(3, 0),
    Pop,
    Push(4, 0),
    Pop,
    Pop,
    Pop,
    Pop,
];

const EMPTY_POPS: &[Op] = &[Pop, Pop, Push(1, 5), Pop, Pop, Push(2, 7), Pop];

/// More tasks than the round-robin window (6) with distinct priorities, pushed
/// in an order that differs from priority order.
const RR_DISTINCT: &[Op] = &[
    Push(1, 80),
    Push(2, 20),
    Push(3, 70),
    Push(4, 30),
    Push(5, 60),
    Push(6, 40),
    Push(7, 50),
    Push(8, 10),
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
];

/// Pushes after the round-robin window is filled: one that evicts, one that
/// does not, and a tie with the window's worst element.
const RR_EVICTION: &[Op] = &[
    Push(1, 10),
    Push(2, 20),
    Push(3, 30),
    Push(4, 40),
    Push(5, 50),
    Push(6, 60),
    Push(7, 70),
    Pop,
    Push(8, 5),
    Push(9, 80),
    Push(10, 60),
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
];

const RR_TIES: &[Op] = &[
    Push(1, 10),
    Push(2, 10),
    Push(3, 10),
    Push(4, 10),
    Push(5, 10),
    Push(6, 10),
    Push(7, 10),
    Push(8, 10),
    Push(9, 10),
    Pop,
    Push(10, 10),
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
];

const RR_INFRA_INTERLEAVED: &[Op] = &[
    Push(1, 30),
    Push(2, 20),
    Pop,
    Push(3, 0),
    Push(4, 10),
    Push(5, 0),
    Pop,
    Pop,
    Push(6, 0),
    Pop,
    Pop,
    Pop,
    Pop,
    Pop,
];

/// Scripts with the queue length expected after every step. Lengths do not
/// depend on the queue implementation.
const CASES: &[(&str, &[Op], &[usize])] = &[
    ("TIES", TIES, &[1, 2, 3, 4, 5, 4, 3, 2, 1, 0, 0]),
    (
        "MIXED_WITH_INFRA",
        MIXED_WITH_INFRA,
        &[1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 0, 0],
    ),
    (
        "INTERLEAVED",
        INTERLEAVED,
        &[1, 2, 1, 2, 3, 2, 1, 2, 1, 0, 0, 0],
    ),
    ("ONLY_INFRA", ONLY_INFRA, &[1, 2, 3, 2, 3, 2, 1, 0, 0]),
    ("EMPTY_POPS", EMPTY_POPS, &[0, 0, 1, 0, 0, 1, 0]),
    (
        "RR_DISTINCT",
        RR_DISTINCT,
        &[1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0],
    ),
    (
        "RR_EVICTION",
        RR_EVICTION,
        &[
            1, 2, 3, 4, 5, 6, 7, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0,
        ],
    ),
    (
        "RR_TIES",
        RR_TIES,
        &[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0,
        ],
    ),
    (
        "RR_INFRA_INTERLEAVED",
        RR_INFRA_INTERLEAVED,
        &[1, 2, 1, 2, 3, 4, 3, 2, 3, 2, 1, 0, 0, 0],
    ),
];

/// Capacities the queues are constructed with: zero, one, and an amount larger
/// than any script. The pop order must not depend on the capacity hint.
const CAPS: [usize; 3] = [0, 1, 64];

/// Replays every case against `Q` and compares with `expected_pops`, which is
/// aligned with `CASES`.
fn check<Q: Queue<Item = MockTask>>(expected_pops: &[&[i64]]) {
    assert_eq!(expected_pops.len(), CASES.len());
    for ((name, script, lens), pops) in CASES.iter().zip(expected_pops) {
        for cap in CAPS {
            let (p, l) = replay::<Q>(cap, script);
            assert_eq!(&p, pops, "{name}: pop order at capacity {cap}");
            assert_eq!(&l, lens, "{name}: queue length trace at capacity {cap}");
        }
    }
}

#[cfg(not(feature = "sched_prio"))]
mod fifo {
    use super::*;
    use crate::masa::scheduler::fifo::FifoQueue;

    #[test]
    fn replay_scripts() {
        check::<FifoQueue<MockTask>>(&[
            &[1, 2, 3, 4, 5, -1],
            &[1, 2, 3, 4, 5, 6, -1],
            &[1, 2, 3, 4, 5, -1, -1],
            &[1, 2, 3, 4, -1],
            &[-1, -1, 1, -1, 2],
            &[1, 2, 3, 4, 5, 6, 7, 8, -1],
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1],
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1],
            &[1, 2, 3, 4, 5, 6, -1, -1],
        ]);
    }
}

#[cfg(all(feature = "sched_prio", not(feature = "tailclipper")))]
mod prio_heap {
    use super::*;
    use crate::masa::scheduler::prio_heap::BinaryHeapQueue;

    #[test]
    fn replay_scripts() {
        check::<BinaryHeapQueue<MockTask>>(&[
            &[1, 3, 5, 2, 4, -1],
            &[2, 4, 6, 5, 3, 1, -1],
            &[2, 4, 3, 5, 1, -1, -1],
            &[1, 2, 3, 4, -1],
            &[-1, -1, 1, -1, 2],
            &[8, 2, 4, 6, 7, 5, 3, 1, -1],
            &[1, 8, 2, 3, 4, 5, 6, 10, 7, 9, -1],
            &[1, 3, 7, 9, 10, 6, 8, 5, 2, 4, -1],
            &[2, 3, 5, 6, 4, 1, -1, -1],
        ]);
    }
}

#[cfg(all(feature = "sched_prio", feature = "tailclipper"))]
mod tailclipper {
    use super::*;
    use crate::masa::scheduler::tailclipper::BinaryHeapRoundRobinQueue;

    #[test]
    fn replay_scripts_without_infra_queue() {
        check::<BinaryHeapRoundRobinQueue<MockTask, false>>(&[
            &[1, 3, 5, 2, 4, -1],
            &[2, 4, 6, 3, 5, 1, -1],
            &[2, 4, 3, 5, 1, -1, -1],
            &[1, 2, 3, 4, -1],
            &[-1, -1, 1, -1, 2],
            &[8, 2, 4, 6, 7, 5, 3, 1, -1],
            &[1, 2, 3, 4, 5, 8, 6, 10, 7, 9, -1],
            &[1, 3, 7, 9, 6, 8, 5, 4, 2, 10, -1],
            &[2, 3, 5, 4, 6, 1, -1, -1],
        ]);
    }

    #[test]
    fn replay_scripts_with_infra_queue() {
        check::<BinaryHeapRoundRobinQueue<MockTask, true>>(&[
            &[1, 3, 5, 2, 4, -1],
            &[2, 4, 6, 5, 3, 1, -1],
            &[2, 4, 3, 5, 1, -1, -1],
            &[1, 2, 3, 4, -1],
            &[-1, -1, 1, -1, 2],
            &[8, 2, 4, 6, 7, 5, 3, 1, -1],
            &[1, 2, 3, 4, 5, 8, 6, 10, 7, 9, -1],
            &[1, 3, 7, 9, 6, 8, 5, 4, 2, 10, -1],
            &[2, 3, 5, 6, 4, 1, -1, -1],
        ]);
    }
}
