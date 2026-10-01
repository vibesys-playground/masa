//! Sample 5 (round 2): an aging run queue on the FINAL scheduler interface
//! (`TaskView` times and poll count, `on_poll_*`, `on_task_exit`), tested
//! (a) against a scripted driver, (b) against the call trace the REAL
//! current-thread runtime produces (`custom::Queue` + `lifecycle_trace`;
//! queue selection is compile-time, so the aging queue itself cannot be the
//! runtime's queue without editing `custom.rs`), and (c) `reprioritize`.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rpcstack_sched::builtin::prio_heap::BinaryHeapQueue;
use rpcstack_sched::trace::{self, Event};
use rpcstack_sched::{default_policy, Meta, PollOutcome, RunQueue, SchedFlavor, TaskView};

// ── The queue ───────────────────────────────────────────────────────────

/// Effective key (smaller first) = priority (taken as microseconds) + time of
/// the task's FIRST enqueue. A task that has waited `w` longer than another
/// therefore beats it by `w` of priority: linear aging with rate 1. Because
/// the key is static while queued, a plain heap implements it; `TaskView`
/// carries `first_enqueued_at`, so no per-task table is needed for the age.
///
/// The one table kept is per-task state for the demotion of tasks that keep
/// yielding (`pends`), to exercise the callbacks and `on_task_exit`.
struct AgingQueue<T> {
    heap: BinaryHeap<Entry<T>>,
    infra: VecDeque<T>,
    seq: u64,
    origin: Option<Instant>,
    pends: HashMap<u64, u32>,
}

struct Entry<T> {
    key: u128,
    seq: u64,
    item: T,
}
impl<T> PartialEq for Entry<T> {
    fn eq(&self, o: &Self) -> bool {
        (self.key, self.seq) == (o.key, o.seq)
    }
}
impl<T> Eq for Entry<T> {}
impl<T> PartialOrd for Entry<T> {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl<T> Ord for Entry<T> {
    fn cmp(&self, o: &Self) -> Ordering {
        (o.key, o.seq).cmp(&(self.key, self.seq))
    }
}

const DEMOTE_US: u128 = 1_000;

impl<T> RunQueue<T> for AgingQueue<T> {
    const FLAVOR: SchedFlavor = SchedFlavor::Prio;

    fn with_capacity(cap: usize) -> Self {
        // No way to pass a rate or a clock: `with_capacity(cap)` is the only
        // constructor, so the rate is a constant.
        Self {
            heap: BinaryHeap::with_capacity(cap),
            infra: VecDeque::new(),
            seq: 0,
            origin: None,
            pends: HashMap::new(),
        }
    }

    fn push(&mut self, item: T, view: &TaskView<'_>) {
        if default_policy::is_infra(view.meta) {
            self.infra.push_back(item);
            return;
        }
        let origin = *self.origin.get_or_insert(view.first_enqueued_at);
        let age_us = view
            .first_enqueued_at
            .checked_duration_since(origin)
            .unwrap_or_default()
            .as_micros();
        let demotion = DEMOTE_US * u128::from(self.pends.get(&view.task_id).copied().unwrap_or(0));
        self.seq += 1;
        self.heap.push(Entry {
            key: u128::from(view.meta.value()) + age_us + demotion,
            seq: self.seq,
            item,
        });
    }

    fn pop(&mut self) -> Option<T> {
        self.infra.pop_front().or_else(|| self.heap.pop().map(|e| e.item))
    }

    fn len(&self) -> usize {
        self.heap.len() + self.infra.len()
    }

    fn capacity(&self) -> usize {
        self.heap.capacity()
    }

    fn on_poll_end(&mut self, view: &TaskView<'_>, outcome: PollOutcome) {
        // Also reached for tasks that never passed `push` (injected from
        // another thread), hence `entry`, hence the need for `on_task_exit`.
        if outcome == PollOutcome::Pending {
            *self.pends.entry(view.task_id).or_default() += 1;
        }
    }

    fn on_task_exit(&mut self, task_id: u64) {
        self.pends.remove(&task_id);
    }
}

// ── A scripted driver (plays the runtime) ───────────────────────────────

fn origin() -> Instant {
    static O: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *O.get_or_init(Instant::now)
}
fn at(us: u64) -> Instant {
    origin() + Duration::from_micros(us)
}

struct Facts {
    meta: Meta,
    first: u64,
    enq: u64,
    polls: u32,
}

struct Driver<Q> {
    q: Q,
    tasks: HashMap<u64, Facts>,
}

impl<Q: RunQueue<u64>> Driver<Q> {
    fn new() -> Self {
        Self { q: Q::with_capacity(8), tasks: HashMap::new() }
    }
    fn spawn(&mut self, id: u64, prio: u64, now: u64) {
        let f = Facts { meta: Meta::new(prio), first: now, enq: now, polls: 0 };
        let view = TaskView::new(id, &f.meta, at(f.enq), at(f.first), f.polls);
        self.q.push(id, &view);
        self.tasks.insert(id, f);
    }
    fn requeue(&mut self, id: u64, now: u64) {
        let f = self.tasks.get_mut(&id).unwrap();
        f.enq = now;
        let view = TaskView::new(id, &f.meta, at(f.enq), at(f.first), f.polls);
        self.q.push(id, &view);
    }
    /// Pop and poll one task; `finishes(id)` decides whether it completes.
    fn run(&mut self, finishes: impl Fn(u64) -> bool) -> Option<u64> {
        let id = self.q.pop()?;
        let f = self.tasks.get_mut(&id).unwrap();
        f.polls += 1;
        let view = TaskView::new(id, &f.meta, at(f.enq), at(f.first), f.polls);
        self.q.on_poll_start(&view);
        let ready = finishes(id);
        self.q.on_poll_end(&view, if ready { PollOutcome::Ready } else { PollOutcome::Pending });
        if ready {
            self.tasks.remove(&id);
            self.q.on_task_exit(id);
        }
        Some(id)
    }
}

/// Bronze (priority 100_000) waits from t=0; one gold task (1_000) arrives
/// every 10_000 and one task is served per step. Step at which bronze runs.
fn bronze_served_at<Q: RunQueue<u64>>() -> Option<u64> {
    let mut d = Driver::<Q>::new();
    d.spawn(1, 100_000, 0);
    for step in 0..40u64 {
        d.spawn(100 + step, 1_000, step * 10_000);
        if d.run(|_| true) == Some(1) {
            return Some(step);
        }
    }
    None
}

#[test]
fn a_plain_priority_heap_starves_bronze_and_aging_does_not() {
    assert_eq!(bronze_served_at::<BinaryHeapQueue<u64>>(), None);
    // key(bronze) = 100_000; key(gold@t) = 1_000 + t: bronze first once t > 99_000.
    assert_eq!(bronze_served_at::<AgingQueue<u64>>(), Some(10));
}

#[test]
fn a_repushed_task_keeps_its_age_because_the_view_carries_first_enqueued_at() {
    // Old probe: a re-pushed task lost its age unless the queue kept a table.
    let mut d = Driver::<AgingQueue<u64>>::new();
    d.spawn(1, 100_000, 0);
    assert_eq!(d.run(|_| false), Some(1)); // polled at t=60_000, pends
    d.requeue(1, 60_000);
    d.spawn(2, 1_000, 100_000); // gold arrives later
    assert_eq!(d.run(|_| true), Some(1)); // bronze still wins: age kept
}

#[test]
fn per_task_state_is_dropped_on_exit_across_many_shapes_of_task() {
    let mut d = Driver::<AgingQueue<u64>>::new();
    // 1000 short tasks.
    for id in 0..1000u64 {
        d.spawn(id, id % 7 + 1, id * 10);
        d.run(|_| true);
    }
    assert!(d.q.pends.is_empty());
    // Tasks that pend several times, then finish; interleaved with new arrivals.
    for id in 2000..2050u64 {
        d.spawn(id, 5, 20_000);
    }
    let mut rounds = 0;
    while d.q.len() > 0 {
        let id = d.q.pop().unwrap();
        let f = d.tasks.get_mut(&id).unwrap();
        f.polls += 1;
        let v = TaskView::new(id, &f.meta, at(f.enq), at(f.first), f.polls);
        d.q.on_poll_start(&v);
        let done = f.polls >= 3 + (id % 3) as u32;
        d.q.on_poll_end(&v, if done { PollOutcome::Ready } else { PollOutcome::Pending });
        if done {
            d.tasks.remove(&id);
            d.q.on_task_exit(id);
        } else {
            d.requeue(id, 20_000 + rounds);
        }
        rounds += 1;
        assert!(d.q.pends.len() <= d.tasks.len());
    }
    assert!(d.q.pends.is_empty() && d.tasks.is_empty());
}

#[test]
fn task_ids_may_be_reused_after_exit_without_inheriting_state() {
    let mut d = Driver::<AgingQueue<u64>>::new();
    d.spawn(7, 10, 0);
    d.run(|_| false);
    d.requeue(7, 1);
    d.run(|_| true); // exits after two pends... one pend recorded then exit
    assert!(d.q.pends.is_empty());
    d.spawn(7, 10, 100); // reuse
    assert_eq!(d.q.pends.get(&7), None);
}

#[test]
fn a_task_that_is_polled_without_a_push_and_a_task_cancelled_before_any_poll_do_not_leak() {
    let mut q = AgingQueue::<u64>::with_capacity(4);
    // Injected task: poll callbacks without push (as the runtime does for the
    // cross-thread injection queue).
    let m = Meta::new(5);
    let v = TaskView::new(9, &m, at(0), at(0), 1);
    q.on_poll_start(&v);
    q.on_poll_end(&v, PollOutcome::Pending);
    assert_eq!(q.pends.len(), 1, "state was created lazily by on_poll_end");
    q.on_poll_end(&v, PollOutcome::Ready);
    q.on_task_exit(9);
    assert!(q.pends.is_empty());
    // Cancelled while queued during shutdown: exit with no poll at all.
    q.on_task_exit(10);
    assert!(q.pends.is_empty());
}

// ── Real runtime: replay its actual call trace into the aging queue ─────

/// Feed the events the real run loop produced to `q`, maintaining the task
/// facts the way the runtime does.
fn replay(events: &[Event], q: &mut AgingQueue<u64>) -> (usize, usize) {
    let mut facts: HashMap<u64, (Meta, Instant, Instant)> = HashMap::new();
    let (mut pushes, mut exits) = (0, 0);
    for e in events {
        match *e {
            Event::Push { task_id, polls, enqueued_at, first_enqueued_at } => {
                let meta = Meta::new(task_id + 1);
                facts.insert(task_id, (meta, enqueued_at, first_enqueued_at));
                q.push(task_id, &TaskView::new(task_id, &meta, enqueued_at, first_enqueued_at, polls));
                pushes += 1;
            }
            Event::PollStart { task_id, polls } => {
                let (m, enq, first) = facts.get(&task_id).copied().unwrap_or((Meta::new(1), at(0), at(0)));
                q.pop(); // keep the heap bounded; order is the runtime's
                q.on_poll_start(&TaskView::new(task_id, &m, enq, first, polls));
            }
            Event::PollEnd { task_id, polls, outcome } => {
                let (m, enq, first) = facts.get(&task_id).copied().unwrap_or((Meta::new(1), at(0), at(0)));
                q.on_poll_end(&TaskView::new(task_id, &m, enq, first, polls), outcome);
            }
            Event::Idle => q.on_idle(),
            Event::Exit { task_id } => {
                q.on_task_exit(task_id);
                exits += 1;
            }
        }
    }
    (pushes, exits)
}

#[test]
fn the_real_runtime_calls_on_task_exit_for_completed_panicked_and_aborted_tasks() {
    trace::take();
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let mut joins = Vec::new();
        for i in 0..12u64 {
            joins.push(tokio::task::spawn_with_prio(
                async move {
                    if i % 3 == 0 {
                        tokio::task::yield_now().await;
                        tokio::task::yield_now().await;
                    }
                    if i == 4 {
                        panic!("probe: deliberate task panic");
                    }
                    i
                },
                tokio::task::TaskPriority::new(10 + i),
            ));
        }
        // Aborted while pending forever.
        let h = tokio::task::spawn_with_prio(
            std::future::pending::<()>(),
            tokio::task::TaskPriority::new(5),
        );
        tokio::task::yield_now().await;
        h.abort();
        assert!(h.await.unwrap_err().is_cancelled());
        for j in joins {
            let _ = j.await;
        }
    });
    drop(rt);
    let events = trace::take();
    let mut q = AgingQueue::<u64>::with_capacity(8);
    let (pushes, exits) = replay(&events, &mut q);
    assert!(pushes >= 13, "pushes {pushes}");
    // Every task that was ever pushed exited exactly once.
    let mut pushed: Vec<u64> = events
        .iter()
        .filter_map(|e| if let Event::Push { task_id, polls: 0, .. } = e { Some(*task_id) } else { None })
        .collect();
    pushed.sort_unstable();
    pushed.dedup();
    let mut exited: Vec<u64> = events
        .iter()
        .filter_map(|e| if let Event::Exit { task_id } = e { Some(*task_id) } else { None })
        .collect();
    exited.sort_unstable();
    assert_eq!(pushed, exited, "pushed {pushed:?} exited {exited:?}");
    assert_eq!(exits, exited.len());
    assert!(q.pends.is_empty(), "leaked per-task state: {:?}", q.pends);
}

#[test]
fn a_task_pending_when_the_runtime_is_dropped_still_gets_its_exit() {
    trace::take();
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let _forever = tokio::task::spawn_with_prio(
            std::future::pending::<()>(),
            tokio::task::TaskPriority::new(5),
        );
        tokio::task::yield_now().await;
    });
    drop(rt);
    let events = trace::take();
    let ids: Vec<u64> = events
        .iter()
        .filter_map(|e| if let Event::Push { task_id, polls: 0, .. } = e { Some(*task_id) } else { None })
        .collect();
    let exited = events.iter().filter(|e| matches!(e, Event::Exit { .. })).count();
    // Documented behavior probed: report the observed count rather than assume.
    eprintln!("pushed {ids:?}, exits observed {exited}");
    // Whatever the runtime does at shutdown, per-task state lives inside the
    // queue, which is dropped with the runtime: nothing can outlive it.
    let mut q = AgingQueue::<u64>::with_capacity(8);
    replay(&events, &mut q);
    // (If no exit was reported for the forever-pending task its table entry
    // would remain until the queue is dropped.)
    assert_eq!(exited, ids.len(), "observed: shutdown reports Exit for a task that was pending, not queued");
    assert!(q.pends.is_empty());
}

// ── reprioritize ────────────────────────────────────────────────────────

fn two_polls(new_prios: [(&'static str, u64, u64); 2]) -> (Vec<&'static str>, Vec<&'static str>) {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let mut joins = vec![];
        for (label, prio, new_prio) in new_prios {
            let (f, s) = (first.clone(), second.clone());
            joins.push(tokio::task::spawn_with_prio(
                async move {
                    f.lock().unwrap().push(label);
                    tokio::task::reprioritize(tokio::task::TaskPriority::new(new_prio));
                    tokio::task::yield_now().await;
                    s.lock().unwrap().push(label);
                },
                tokio::task::TaskPriority::new(prio),
            ));
        }
        for j in joins {
            j.await.unwrap();
        }
        let a = first.lock().unwrap().clone();
        let b = second.lock().unwrap().clone();
        (a, b)
    })
}

#[test]
fn reprioritize_still_changes_only_the_next_enqueue_of_the_running_task() {
    let (first, second) = two_polls([("x", 10, 30), ("y", 20, 20)]);
    assert_eq!(first, vec!["x", "y"]);
    assert_eq!(second, vec!["y", "x"]);
}

#[test]
fn reprioritize_keeps_the_task_identity_and_age_in_the_view() {
    trace::take();
    two_polls([("x", 10, 30), ("y", 20, 20)]);
    let events = trace::take();
    // For every task: the first push has polls 0, the re-push polls 1, and
    // first_enqueued_at is unchanged by the re-push even though the priority
    // was changed in between.
    let mut by_task: HashMap<u64, Vec<(u32, Instant, Instant)>> = HashMap::new();
    for e in &events {
        if let Event::Push { task_id, polls, enqueued_at, first_enqueued_at } = *e {
            by_task.entry(task_id).or_default().push((polls, enqueued_at, first_enqueued_at));
        }
    }
    let spawned: Vec<_> = by_task.values().filter(|v| v.len() >= 2 && v[0].0 == 0).collect();
    assert_eq!(spawned.len(), 2);
    for v in spawned {
        assert_eq!(v[0].0, 0);
        assert_eq!(v[1].0, 1);
        assert_eq!(v[0].2, v[1].2, "first_enqueued_at unchanged");
        assert!(v[1].1 >= v[0].1);
    }
}

#[test]
fn reprioritize_to_zero_makes_a_task_infrastructure_and_outside_a_task_is_a_noop() {
    // Footgun: a computed priority of 0 (e.g. "no deadline") is the reserved
    // infra priority and jumps every queue.
    let (_, second) = two_polls([("x", 10, 0), ("y", 20, 20)]);
    assert_eq!(second, vec!["x", "y"]); // x was first anyway: check ordering with y first below
    let (_, second) = two_polls([("y", 10, 20), ("x", 20, 0)]);
    assert_eq!(second, vec!["x", "y"], "x demoted by its own prio 20 on first poll, then infra on the second");
    // Outside any task (block_on's main future is not a spawned task) and
    // outside any runtime it does nothing and does not panic.
    tokio::task::reprioritize(tokio::task::TaskPriority::new(1));
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async { tokio::task::reprioritize(tokio::task::TaskPriority::new(1)) });
}
