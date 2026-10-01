use crate::{
    body::{boxed, BoxBody},
    server::NamedService,
};
use http::{Request, Response};
use hyper::Body;
use pin_project::pin_project;
use std::{
    convert::Infallible,
    fmt,
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tower::ServiceExt;
use tower_service::Service;

/// A [`Service`] router.
#[derive(Default, Clone)]
pub struct Routes {
    router: axum::Router,
    /// The ingress decision of each added service that supplies one, by
    /// service name.
    #[cfg(feature = "masa")]
    ingress: Vec<(
        &'static str,
        std::sync::Arc<dyn crate::masa::ServiceIngress>,
    )>,
}

impl fmt::Debug for Routes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Routes")
            .field("router", &self.router)
            .finish()
    }
}

#[derive(Debug, Default, Clone)]
/// Allows adding new services to routes by passing a mutable reference to this builder.
pub struct RoutesBuilder {
    routes: Option<Routes>,
}

impl RoutesBuilder {
    /// Add a new service.
    pub fn add_service<S>(&mut self, svc: S) -> &mut Self
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<crate::Error> + Send,
    {
        let routes = self.routes.take().unwrap_or_default();
        self.routes.replace(routes.add_service(svc));
        self
    }

    /// Returns the routes with added services or empty [`Routes`] if no service was added
    pub fn routes(self) -> Routes {
        self.routes.unwrap_or_default()
    }
}
impl Routes {
    /// Create a new routes with `svc` already added to it.
    pub fn new<S>(svc: S) -> Self
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<crate::Error> + Send,
    {
        let router = axum::Router::new().fallback(unimplemented);
        Self {
            router,
            #[cfg(feature = "masa")]
            ingress: Vec::new(),
        }
        .add_service(svc)
    }

    /// Add a new service.
    pub fn add_service<S>(mut self, svc: S) -> Self
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<crate::Error> + Send,
    {
        #[cfg(feature = "masa")]
        if let Some(handler) = svc.ingress_handler() {
            self.ingress.retain(|(name, _)| *name != S::NAME);
            self.ingress.push((S::NAME, handler));
        }
        let svc = svc.map_response(|res| res.map(axum::body::boxed));
        self.router = self
            .router
            .route_service(&format!("/{}/*rest", S::NAME), svc);
        self
    }

    /// The executor for the server: when a service supplies an ingress decision,
    /// each HTTP/2 stream goes to the decision of the service its path names
    /// before any task is spawned for it; tokio's default otherwise.
    pub(crate) fn exec(&self) -> hyper::rt::Exec {
        #[cfg(feature = "masa")]
        if !self.ingress.is_empty() {
            return hyper::rt::Exec::Ingress(std::sync::Arc::new(
                super::ingress::IngressTable::new(self.ingress.clone()),
            ));
        }
        hyper::rt::Exec::Default
    }

    pub(crate) fn prepare(self) -> Self {
        Self {
            // this makes axum perform update some internals of the router that improves perf
            // see https://docs.rs/axum/latest/axum/routing/struct.Router.html#a-note-about-performance
            router: self.router.with_state(()),
            ..self
        }
    }

    /// Convert this `Routes` into an [`axum::Router`].
    pub fn into_router(self) -> axum::Router {
        self.router
    }
}

async fn unimplemented() -> impl axum::response::IntoResponse {
    let status = http::StatusCode::OK;
    let headers = [("grpc-status", "12"), ("content-type", "application/grpc")];
    (status, headers)
}

impl Service<Request<Body>> for Routes {
    type Response = Response<BoxBody>;
    type Error = crate::Error;
    type Future = RoutesFuture;

    #[inline]
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        RoutesFuture(self.router.call(req))
    }
}

#[pin_project]
pub struct RoutesFuture(#[pin] axum::routing::future::RouteFuture<Body, Infallible>);

impl fmt::Debug for RoutesFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RoutesFuture").finish()
    }
}

impl Future for RoutesFuture {
    type Output = Result<Response<BoxBody>, crate::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.project().0.poll(cx)) {
            Ok(res) => Ok(res.map(boxed)).into(),
            Err(err) => match err {},
        }
    }
}
