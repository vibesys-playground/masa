use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(all(feature = "server", any(feature = "http1", feature = "http2")))]
use crate::body::Body;
#[cfg(feature = "server")]
use crate::body::HttpBody;
#[cfg(all(feature = "http2", feature = "server"))]
use crate::proto::h2::server::H2Stream;
use crate::rt::Executor;
#[cfg(all(feature = "server", any(feature = "http1", feature = "http2")))]
use crate::server::server::{new_svc::NewSvcTask, Watcher};
#[cfg(all(feature = "server", any(feature = "http1", feature = "http2")))]
use crate::service::HttpService;
use tokio::task::{meta_for_unannotated_spawn, TaskPriority};

/// What to do with a new HTTP/2 stream, decided before anything is spawned.
#[cfg(feature = "server")]
#[derive(Debug)]
pub enum StreamVerdict {
    /// Serve the stream with a task queued with this priority.
    Spawn(TaskPriority),
    /// Answer the stream with this response head, ending the stream, and spawn
    /// nothing: the service never sees the request.
    Reject(http::response::Parts),
}

/// Decides what happens to each new HTTP/2 stream from its request head,
/// before a task is spawned for it. Knows nothing of what the requests mean:
/// the implementor does.
#[cfg(feature = "server")]
pub trait StreamIngress: Send + Sync {
    /// Called once per stream, on the connection's task; keep it cheap.
    fn ingress(&self, req: &http::Request<crate::body::Body>) -> StreamVerdict;
}

#[cfg(feature = "server")]
pub trait ConnStreamExec<F, B: HttpBody>: Clone {
    fn h2_stream_ingress(&self, _req: &http::Request<crate::body::Body>) -> StreamVerdict {
        StreamVerdict::Spawn(meta_for_unannotated_spawn(None))
    }

    fn execute_h2stream_with_prio(&mut self, fut: H2Stream<F, B>, prio: TaskPriority);
}

#[cfg(all(feature = "server", any(feature = "http1", feature = "http2")))]
pub trait NewSvcExec<I, N, S: HttpService<Body>, E, W: Watcher<I, S, E>>: Clone {
    fn execute_new_svc(&mut self, fut: NewSvcTask<I, N, S, E, W>);
}

pub(crate) type BoxSendFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Specify what executor to use.
// Either the user provides an executor for background tasks, or we use
// `tokio::spawn`.
#[derive(Clone)]
pub enum Exec {
    /// Use tokio by default.
    Default,
    /// Use custom executor.
    Executor(Arc<dyn Executor<BoxSendFuture> + Send + Sync>),
    /// Use tokio, deciding what happens to each HTTP/2 stream from its request
    /// head before a task is spawned for it: spawn it with a priority, or
    /// answer it without spawning.
    #[cfg(all(feature = "masa", feature = "server"))]
    Ingress(Arc<dyn StreamIngress>),
}

// ===== impl Exec =====

impl Exec {
    pub(crate) fn execute<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match *self {
            Exec::Executor(ref e) => {
                e.execute(Box::pin(fut));
            }
            _ => {
                #[cfg(feature = "tcp")]
                {
                    tokio::task::spawn(fut);
                }
                #[cfg(not(feature = "tcp"))]
                {
                    // If no runtime, we need an executor!
                    panic!("executor must be set")
                }
            }
        }
    }
}

impl fmt::Debug for Exec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exec").finish()
    }
}

#[cfg(all(feature = "server", not(feature = "masa")))]
impl<F, B> ConnStreamExec<F, B> for Exec
where
    H2Stream<F, B>: Future<Output = ()> + Send + 'static,
    B: HttpBody,
{
    fn execute_h2stream_with_prio(&mut self, fut: H2Stream<F, B>, prio: TaskPriority) {
        let _ = prio;
        self.execute(fut);
    }
}

#[cfg(all(feature = "server", feature = "masa"))]
impl<F, B> ConnStreamExec<F, B> for Exec
where
    H2Stream<F, B>: Future<Output = ()> + Send + 'static,
    B: HttpBody,
{
    fn h2_stream_ingress(&self, req: &http::Request<Body>) -> StreamVerdict {
        match self {
            Exec::Ingress(ingress) => ingress.ingress(req),
            Exec::Default | Exec::Executor(_) => {
                StreamVerdict::Spawn(meta_for_unannotated_spawn(None))
            }
        }
    }

    fn execute_h2stream_with_prio(&mut self, fut: H2Stream<F, B>, prio: TaskPriority) {
        match self {
            Exec::Ingress(_) => {
                tokio::task::spawn_with_prio(fut, prio);
            }
            Exec::Default | Exec::Executor(_) => self.execute(fut),
        }
    }
}

#[cfg(all(feature = "server", any(feature = "http1", feature = "http2")))]
impl<I, N, S, E, W> NewSvcExec<I, N, S, E, W> for Exec
where
    NewSvcTask<I, N, S, E, W>: Future<Output = ()> + Send + 'static,
    S: HttpService<Body>,
    W: Watcher<I, S, E>,
{
    fn execute_new_svc(&mut self, fut: NewSvcTask<I, N, S, E, W>) {
        self.execute(fut)
    }
}

// ==== impl Executor =====

#[cfg(feature = "server")]
impl<E, F, B> ConnStreamExec<F, B> for E
where
    E: Executor<H2Stream<F, B>> + Clone,
    H2Stream<F, B>: Future<Output = ()>,
    B: HttpBody,
{
    fn execute_h2stream_with_prio(&mut self, fut: H2Stream<F, B>, prio: TaskPriority) {
        let _ = prio;
        self.execute(fut)
    }
}

#[cfg(all(feature = "server", any(feature = "http1", feature = "http2")))]
impl<I, N, S, E, W> NewSvcExec<I, N, S, E, W> for E
where
    E: Executor<NewSvcTask<I, N, S, E, W>> + Clone,
    NewSvcTask<I, N, S, E, W>: Future<Output = ()>,
    S: HttpService<Body>,
    W: Watcher<I, S, E>,
{
    fn execute_new_svc(&mut self, fut: NewSvcTask<I, N, S, E, W>) {
        self.execute(fut)
    }
}

#[cfg(all(test, feature = "server"))]
mod h2_ingress_tests {
    use super::*;

    type TestFuture = std::future::Ready<Result<http::Response<Body>, crate::Error>>;

    fn verdict(exec: &Exec, req: &http::Request<Body>) -> StreamVerdict {
        <Exec as ConnStreamExec<TestFuture, Body>>::h2_stream_ingress(exec, req)
    }

    fn spawned(verdict: StreamVerdict) -> TaskPriority {
        match verdict {
            StreamVerdict::Spawn(priority) => priority,
            StreamVerdict::Reject(_) => panic!("the stream was rejected"),
        }
    }

    #[test]
    fn default_executor_spawns_with_the_unannotated_default() {
        let req = http::Request::new(Body::empty());

        assert_eq!(
            spawned(verdict(&Exec::Default, &req)),
            meta_for_unannotated_spawn(None)
        );
    }

    #[cfg(feature = "masa")]
    struct ByHeader;

    #[cfg(feature = "masa")]
    impl StreamIngress for ByHeader {
        fn ingress(&self, req: &http::Request<Body>) -> StreamVerdict {
            match req.headers().get("x-prio") {
                Some(value) => StreamVerdict::Spawn(TaskPriority::new(
                    value.to_str().unwrap().parse().unwrap(),
                )),
                None => {
                    let (parts, ()) = http::Response::builder()
                        .status(403)
                        .body(())
                        .unwrap()
                        .into_parts();
                    StreamVerdict::Reject(parts)
                }
            }
        }
    }

    #[test]
    #[cfg(feature = "masa")]
    fn ingress_executor_spawns_with_the_decided_priority() {
        let req = http::Request::builder()
            .header("x-prio", "42")
            .body(Body::empty())
            .unwrap();

        assert_eq!(
            spawned(verdict(&Exec::Ingress(Arc::new(ByHeader)), &req)),
            TaskPriority::new(42)
        );
    }

    #[test]
    #[cfg(feature = "masa")]
    fn ingress_executor_can_reject_a_stream() {
        let req = http::Request::new(Body::empty());

        match verdict(&Exec::Ingress(Arc::new(ByHeader)), &req) {
            StreamVerdict::Reject(parts) => assert_eq!(parts.status, 403),
            StreamVerdict::Spawn(_) => panic!("the stream was spawned"),
        }
    }
}

// If http2 is not enable, we just have a stub here, so that the trait bounds
// that *would* have been needed are still checked. Why?
//
// Because enabling `http2` shouldn't suddenly add new trait bounds that cause
// a compilation error.
#[cfg(not(feature = "http2"))]
#[allow(missing_debug_implementations)]
pub struct H2Stream<F, B>(std::marker::PhantomData<(F, B)>);

#[cfg(not(feature = "http2"))]
impl<F, B, E> Future for H2Stream<F, B>
where
    F: Future<Output = Result<http::Response<B>, E>>,
    B: crate::body::HttpBody,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unreachable!()
    }
}
