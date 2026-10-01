#![allow(unknown_lints, unexpected_cfgs)]
#![cfg(all(
    feature = "full",
    feature = "sched_custom",
    feature = "lifecycle_trace"
))]

//! The run-queue callbacks the current-thread runtime makes around real polls.
//!
//! `custom::Queue` records every call it receives when built with
//! `lifecycle_trace`; these tests read that log after running a runtime.

use rpcstack_sched::trace::{self, Event};
use rpcstack_sched::PollOutcome;
use tokio::runtime::Builder;
use tokio::sync::oneshot;

fn runtime() -> tokio::runtime::Runtime {
    Builder::new_current_thread().build().unwrap()
}

/// The id of the task the first `Push` in `events` is for.
fn first_task_id(events: &[Event]) -> u64 {
    events
        .iter()
        .find_map(|e| match e {
            Event::Push { task_id, .. } => Some(*task_id),
            _ => None,
        })
        .expect("a push was recorded")
}

/// The calls made for `task_id` plus every `Idle`, as short strings.
fn shape(events: &[Event], task_id: u64) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match *e {
            Event::Push {
                task_id: t, polls, ..
            } if t == task_id => Some(format!("push({polls})")),
            Event::PollStart { task_id: t, polls } if t == task_id => {
                Some(format!("start({polls})"))
            }
            Event::PollEnd {
                task_id: t,
                polls,
                outcome,
            } if t == task_id => Some(format!("end({polls},{outcome:?})")),
            Event::Exit { task_id: t } if t == task_id => Some("exit".to_string()),
            Event::Idle => Some("idle".to_string()),
            _ => None,
        })
        .collect()
}

fn exits_of(events: &[Event], task_id: u64) -> usize {
    events
        .iter()
        .filter(|e| **e == Event::Exit { task_id })
        .count()
}

#[test]
fn task_that_pends_once_sees_every_callback_in_order() {
    trace::take();
    let rt = runtime();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move { rx.await.unwrap() });
        // Let the task run once and pend on `rx`.
        tokio::task::yield_now().await;
        tx.send(7).unwrap();
        assert_eq!(handle.await.unwrap(), 7);
    });
    let events = trace::take();
    let id = first_task_id(&events);
    assert_eq!(
        shape(&events, id),
        [
            "push(0)",
            "start(1)",
            "end(1,Pending)",
            "idle",
            "push(1)",
            "start(2)",
            "end(2,Ready)",
            "exit",
            "idle",
        ]
    );
}

#[test]
fn repush_keeps_first_enqueue_time_and_counts_polls() {
    trace::take();
    let rt = runtime();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move { rx.await.unwrap() });
        tokio::task::yield_now().await;
        std::thread::sleep(std::time::Duration::from_millis(5));
        tx.send(()).unwrap();
        handle.await.unwrap();
    });
    let events = trace::take();
    let id = first_task_id(&events);
    let pushes: Vec<_> = events
        .iter()
        .filter_map(|e| match *e {
            Event::Push {
                task_id,
                polls,
                enqueued_at,
                first_enqueued_at,
            } if task_id == id => Some((polls, enqueued_at, first_enqueued_at)),
            _ => None,
        })
        .collect();
    assert_eq!(pushes.len(), 2);
    let (first, second) = (pushes[0], pushes[1]);
    assert_eq!((first.0, second.0), (0, 1));
    assert_eq!(
        first.1, first.2,
        "first push: both times are the same enqueue"
    );
    assert_eq!(second.2, first.2, "re-push keeps the first enqueue time");
    assert!(
        second.1 >= first.1 + std::time::Duration::from_millis(5),
        "second enqueue is at least the 5 ms sleep later: {first:?} {second:?}"
    );
}

#[test]
fn cancelled_after_a_poll_exits_once() {
    trace::take();
    let rt = runtime();
    rt.block_on(async {
        let handle = tokio::spawn(std::future::pending::<()>());
        tokio::task::yield_now().await;
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
    });
    let events = trace::take();
    let id = first_task_id(&events);
    assert_eq!(exits_of(&events, id), 1);
    assert_eq!(
        shape(&events, id),
        [
            "push(0)",
            "start(1)",
            "end(1,Pending)",
            "idle",
            "push(1)",
            "start(2)",
            "end(2,Ready)",
            "exit",
            "idle",
        ]
    );
}

#[test]
fn cancelled_before_first_poll_exits_once() {
    trace::take();
    let rt = runtime();
    rt.block_on(async {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
    });
    let events = trace::take();
    let id = first_task_id(&events);
    assert_eq!(exits_of(&events, id), 1);
    let calls: Vec<_> = shape(&events, id)
        .into_iter()
        .filter(|c| c != "idle")
        .collect();
    assert_eq!(calls, ["push(0)", "start(1)", "end(1,Ready)", "exit"]);
}

#[test]
fn panicking_task_ends_ready_and_exits_once() {
    trace::take();
    let rt = runtime();
    rt.block_on(async {
        let handle = tokio::spawn(async { panic!("expected by the test") });
        assert!(handle.await.unwrap_err().is_panic());
    });
    let events = trace::take();
    let id = first_task_id(&events);
    assert_eq!(exits_of(&events, id), 1);
    assert!(events.contains(&Event::PollEnd {
        task_id: id,
        polls: 1,
        outcome: PollOutcome::Ready
    }));
}

#[test]
fn runtime_shutdown_exits_every_remaining_task_once() {
    trace::take();
    let rt = runtime();
    rt.block_on(async {
        let polled = tokio::spawn(std::future::pending::<()>());
        tokio::task::yield_now().await;
        let unpolled = tokio::spawn(std::future::pending::<()>());
        drop((polled, unpolled));
    });
    drop(rt);
    let events = trace::take();
    let pushed: Vec<u64> = events
        .iter()
        .filter_map(|e| match e {
            Event::Push {
                task_id, polls: 0, ..
            } => Some(*task_id),
            _ => None,
        })
        .collect();
    assert_eq!(pushed.len(), 2);
    for id in pushed {
        assert_eq!(exits_of(&events, id), 1, "task {id}");
    }
}

#[test]
fn idle_fires_each_time_the_queue_drains() {
    trace::take();
    let rt = runtime();
    rt.block_on(async {
        for _ in 0..3 {
            tokio::spawn(async {}).await.unwrap();
        }
    });
    let events = trace::take();
    let idles = events.iter().filter(|e| **e == Event::Idle).count();
    assert!(idles >= 3, "idle fired {idles} times");
    // Nothing is queued when the run loop reports idle: every push is
    // followed by its poll before the next idle.
    let mut queued = 0i32;
    for e in &events {
        match e {
            Event::Push { .. } => queued += 1,
            Event::PollStart { .. } => queued -= 1,
            Event::Idle => assert_eq!(queued, 0, "idle with a task queued"),
            _ => {}
        }
    }
}
