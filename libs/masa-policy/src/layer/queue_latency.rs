// Queue latency tracking layer — observes queue latencies and propagates
// them through the request tree.
//
// Included in the default stack only with `trace_queue_latency`. Tracks
// initial and resume queue latencies via the tokio runtime, aggregates child
// queue latencies from responses, and injects the totals into the response
// context.

use crate::wire::{WireIn, WireOut};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use masa_core::{Context, QueueLatencies};
use tonic::{CowGrpcMethod, Response, Status};

use crate::context_ext::MasaResponseExt;

use super::{Extensions, Layer, LayerChild, LayerServer, MissingDependency, ServerInit};

// ── Server ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct QueueLatencyServer;

impl LayerServer for QueueLatencyServer {
    fn new(_init: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(Self)
    }
}

// ── Per-Request ─────────────────────────────────────────────────────────

static SERVICE_NAME: OnceLock<String> = OnceLock::new();

fn service_name() -> &'static str {
    SERVICE_NAME
        .get_or_init(|| std::env::var("SERVICE_NAME").unwrap_or_else(|_| "unknown".to_string()))
}

#[derive(Debug)]
pub struct QueueLatencyLayer {
    initial_q_lat: AtomicU64,
    resume_q_lat: AtomicU64,
    is_first_poll: AtomicBool,
    own_queue_len: AtomicU64,
    child_queue_lengths: Mutex<HashMap<String, u64>>,
}

impl Layer for QueueLatencyLayer {
    type Server = QueueLatencyServer;
    type Child = QueueLatencyChild;
    const NAME: &'static str = "queue_latency";
    type Wire = ();

    fn new(
        _method: &CowGrpcMethod,
        _server: &QueueLatencyServer,
        _ctx: &mut Context,
        _wire: &WireIn<'_>,
        _ext: &mut Extensions,
    ) -> Self {
        Self {
            initial_q_lat: AtomicU64::new(0),
            resume_q_lat: AtomicU64::new(0),
            is_first_poll: AtomicBool::new(true),
            own_queue_len: AtomicU64::new(0),
            child_queue_lengths: Mutex::new(HashMap::new()),
        }
    }

    #[inline]
    fn before_poll<Ret>(
        &self,
        _ctx: &Context,
        _ext: &mut Extensions,
    ) -> Result<(), Result<Response<Ret>, Status>> {
        let queue_latency = tokio::task::obtain_task_queue_latency().as_micros() as u64;
        if self.is_first_poll.swap(false, Ordering::Relaxed) {
            if queue_latency > 0 {
                self.initial_q_lat
                    .fetch_add(queue_latency, Ordering::AcqRel);
            }
            let q_len = tokio::runtime::current_thread_queue_len() as u64;
            self.own_queue_len.store(q_len, Ordering::Release);
        } else if queue_latency > 0 {
            self.resume_q_lat.fetch_add(queue_latency, Ordering::AcqRel);
        }
        Ok(())
    }

    #[inline]
    fn after_child_rpc<T>(
        &self,
        _ctx: &Context,
        _child_method: &CowGrpcMethod,
        response: &Result<Response<T>, Status>,
        _response_wire: &WireIn<'_>,
        _child_ctx: &QueueLatencyChild,
        _ext: &Extensions,
    ) -> Result<(), Status> {
        if let Ok(resp) = response {
            if let Some(ctx) = resp.get_masa_context() {
                if let Some(ql) = ctx.queue_latencies() {
                    self.initial_q_lat.fetch_add(ql.initial, Ordering::AcqRel);
                    self.resume_q_lat.fetch_add(ql.resume, Ordering::AcqRel);
                    if !ql.queue_lengths.is_empty() {
                        let mut child_qls = self.child_queue_lengths.lock().unwrap();
                        for (svc, len) in ql.queue_lengths.iter() {
                            child_qls
                                .entry(svc.clone())
                                .and_modify(|e| *e = (*e).max(*len))
                                .or_insert(*len);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn finalize<Ret>(
        &self,
        ctx: &mut Context,
        _result: &mut Result<Response<Ret>, Status>,
        _wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        let initial = self.initial_q_lat.load(Ordering::Acquire);
        let resume = self.resume_q_lat.load(Ordering::Acquire);
        let own_len = self.own_queue_len.load(Ordering::Acquire);
        let mut queue_lengths = std::mem::take(&mut *self.child_queue_lengths.lock().unwrap());
        queue_lengths.insert(service_name().to_string(), own_len);
        ctx.set_queue_latencies(QueueLatencies {
            initial,
            resume,
            queue_lengths,
        });
    }
}

#[derive(Debug, Clone)]
pub struct QueueLatencyChild;

impl LayerChild for QueueLatencyChild {
    fn new() -> Self {
        Self
    }
}
