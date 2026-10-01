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
use http::HeaderMap;
#[cfg(feature = "masa")]
use tokio::task::Meta;
use tokio::task::{meta_for_unannotated_spawn, TaskPriority};

/// Decides the [`Meta`] of the task that serves an HTTP/2 stream from the
/// stream's request headers, before the task is queued. `None` means no
/// decision, and the task gets the runtime's default for unannotated spawns.
#[cfg(feature = "masa")]
pub type IngressFn = fn(&HeaderMap) -> Option<Meta>;

#[cfg(feature = "server")]
pub trait ConnStreamExec<F, B: HttpBody>: Clone {
    fn h2_stream_priority(&self, _headers: &HeaderMap) -> TaskPriority {
        meta_for_unannotated_spawn(None)
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
    /// Use tokio, queueing each HTTP/2 stream's task with the `Meta` that the
    /// function decides from the stream's request headers.
    #[cfg(feature = "masa")]
    Ingress(IngressFn),
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
    fn h2_stream_priority(&self, headers: &HeaderMap) -> TaskPriority {
        match self {
            Exec::Ingress(ingress) => {
                ingress(headers).unwrap_or_else(|| meta_for_unannotated_spawn(None))
            }
            Exec::Default | Exec::Executor(_) => meta_for_unannotated_spawn(None),
        }
    }

    fn execute_h2stream_with_prio(&mut self, fut: H2Stream<F, B>, prio: TaskPriority) {
        match self {
            Exec::Default | Exec::Ingress(_) => {
                tokio::task::spawn_with_prio(fut, prio);
            }
            Exec::Executor(_) => self.execute(fut),
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
mod h2_priority_tests {
    use super::*;

    type TestFuture = std::future::Ready<Result<http::Response<Body>, crate::Error>>;

    fn priority(exec: &Exec, headers: &HeaderMap) -> TaskPriority {
        <Exec as ConnStreamExec<TestFuture, Body>>::h2_stream_priority(exec, headers)
    }

    #[test]
    fn default_executor_uses_infra_priority() {
        let headers = HeaderMap::new();

        assert_eq!(
            priority(&Exec::Default, &headers),
            meta_for_unannotated_spawn(None)
        );
    }

    #[test]
    #[cfg(feature = "masa")]
    fn ingress_executor_uses_the_decided_meta() {
        fn ingress(headers: &HeaderMap) -> Option<Meta> {
            headers
                .get("x-prio")
                .map(|value| Meta::new(value.to_str().unwrap().parse().unwrap()))
        }
        let mut headers = HeaderMap::new();
        headers.insert("x-prio", "42".parse().unwrap());

        assert_eq!(
            priority(&Exec::Ingress(ingress), &headers),
            TaskPriority::new(42)
        );
    }

    #[test]
    #[cfg(feature = "masa")]
    fn ingress_executor_without_a_decision_uses_the_unannotated_default() {
        assert_eq!(
            priority(&Exec::Ingress(|_| None), &HeaderMap::new()),
            meta_for_unannotated_spawn(None)
        );
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
