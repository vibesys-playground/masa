// Masa hooks implementation.
//
// `PolicyHooks<S>` is the single concrete `Hooks` implementation. It owns
// request plumbing (splitting the inbound wire sections, resolving method
// names, installing the child's and the response's wire sections) and
// delegates every policy decision to the module stack `S`: what a request
// carries, what a child request carries, and what a response carries are all
// whatever the modules put. The default stack,
// `MasaStack`, is selected by Cargo features in `masa_stack.rs`; any other
// stack built with `policy_stack!` plugs in the same way.
//
// Scheduling order itself is enforced by the tokio runtime from the priority
// hints that the stack's modules assign.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;

use crate::context_ext::{
    get_method_name_override_from_headers, get_method_name_override_from_metadata,
    get_service_name_override_from_headers, get_service_name_override_from_metadata,
};
use crate::layer::{
    assert_unique_names, Extensions, Layer, LayerChild, LayerServer, MissingDependency, ServerInit,
};
use crate::masa_stack::MasaStack;
use crate::wire::{WireIn, WireOut};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

/// `Hooks` implementation that runs the policy module stack `S`.
pub struct PolicyHooks<S = MasaStack>(PhantomData<fn() -> S>);

impl<S> std::fmt::Debug for PolicyHooks<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PolicyHooks")
    }
}

fn resolve_method_name_impl(
    method: GrpcMethod,
    method_override: Option<&str>,
    service_override: Option<&str>,
) -> CowGrpcMethod {
    if let Some(method_name) = method_override {
        if let Some(service_name) = service_override {
            return CowGrpcMethod::new(service_name.to_string(), method_name.to_string());
        }
        return CowGrpcMethod::new(method.service(), method_name.to_string());
    }
    CowGrpcMethod::new(method.service(), method.method())
}

fn resolve_method_name_from_http<B>(method: GrpcMethod, req: &http::Request<B>) -> CowGrpcMethod {
    resolve_method_name_impl(
        method,
        get_method_name_override_from_headers(req.headers()),
        get_service_name_override_from_headers(req.headers()),
    )
}

fn resolve_method_name_from_request<T>(method: GrpcMethod, request: &Request<T>) -> CowGrpcMethod {
    resolve_method_name_impl(
        method,
        get_method_name_override_from_metadata(request.metadata()),
        get_service_name_override_from_metadata(request.metadata()),
    )
}

impl<S: Layer + 'static> Hooks for PolicyHooks<S> {
    type ServerContext = ServerContext<S>;
    type ChildContext = ChildContext<S>;
    type ParentContext = ParentContext<S>;
}

#[derive(Debug)]
pub struct ServerContext<S: Layer = MasaStack> {
    layers: S::Server,
}

impl<S: Layer> ServerContext<S> {
    /// Build the server state of the stack `S`, or report the first module
    /// whose prerequisite an earlier module did not publish.
    /// [`ServerHooks::new`] panics with the same message.
    ///
    /// Panics if two modules share a wire name.
    pub fn try_new(service_name: &'static str) -> Result<Self, MissingDependency> {
        assert_unique_names::<S>();
        Ok(Self {
            layers: S::Server::new(&mut ServerInit::new(service_name))?,
        })
    }
}

impl<S: Layer + 'static> ServerHooks for ServerContext<S> {
    fn new(service_name: &'static str) -> Self {
        Self::try_new(service_name).unwrap_or_else(|err| panic!("{err}"))
    }
}

#[derive(Debug)]
pub struct ParentContext<S: Layer = MasaStack> {
    layers: S,
    /// Hooks take `&self`, so the per-request extensions sit behind a lock;
    /// hooks run one at a time, so it is never contended.
    ext: Mutex<Extensions>,
}

impl<S: Layer> ParentContext<S> {
    fn ext(&self) -> MutexGuard<'_, Extensions> {
        self.ext.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<S: Layer + 'static> ParentHooks<ChildContext<S>, ServerContext<S>> for ParentContext<S> {
    fn begin<B>(
        method: GrpcMethod,
        req: &http::Request<B>,
        server_ctx: Arc<ServerContext<S>>,
    ) -> Self {
        let resolved_method = resolve_method_name_from_http(method, req);
        let wire = WireIn::from_headers(req.headers()).unwrap_or_else(|err| panic!("{err}"));
        let mut ext = Extensions::new();
        let layers = S::new(&resolved_method, &server_ctx.layers, &wire, &mut ext);

        Self {
            layers,
            ext: Mutex::new(ext),
        }
    }

    fn before_poll<Ret>(&self) -> Result<(), Result<Response<Ret>, Status>> {
        self.layers.before_poll(&mut self.ext())
    }

    fn before_child_rpc<T>(
        &self,
        child_method: GrpcMethod,
        request: &mut Request<T>,
        child_ctx: &mut ChildContext<S>,
    ) -> Result<(), Status> {
        let child_method_name = resolve_method_name_from_request(child_method, request);
        child_ctx.set_method_name(child_method_name.clone());

        let mut child_wire = WireOut::new();
        self.layers.before_child_rpc(
            &child_method_name,
            &mut child_ctx.layers,
            request,
            &mut child_wire,
            &mut self.ext(),
        )?;

        // The child request carries exactly what the modules put: nothing is
        // copied down from this request.
        child_wire.install(request.metadata_mut());

        Ok(())
    }

    fn after_child_rpc<T>(
        &self,
        _method: GrpcMethod,
        response: &mut Result<Response<T>, Status>,
        child_ctx: ChildContext<S>,
    ) -> Result<(), Status> {
        if let Some(child_method) = child_ctx.child_method_name.as_ref() {
            let metadata = match &*response {
                Ok(resp) => resp.metadata(),
                Err(status) => status.metadata(),
            };
            let response_wire =
                WireIn::from_metadata(metadata).unwrap_or_else(|err| panic!("{err}"));
            self.layers.after_child_rpc(
                child_method,
                response,
                &response_wire,
                &child_ctx.layers,
                &self.ext(),
            )?;
        }
        Ok(())
    }

    fn after_poll<Ret>(
        &self,
        poll: &Poll<Result<Response<Ret>, Status>>,
    ) -> Result<(), Result<Response<Ret>, Status>> {
        self.layers.after_poll(poll, &self.ext())
    }

    fn finalize_before_serialization<Ret>(&self, result: &mut Result<Response<Ret>, Status>) {
        let mut wire = WireOut::new();
        self.layers.finalize(result, &mut wire, &self.ext());
        let metadata = match result {
            Ok(resp) => resp.metadata_mut(),
            Err(status) => status.metadata_mut(),
        };
        wire.install(metadata);
    }
}

#[derive(Debug)]
pub struct ChildContext<S: Layer = MasaStack> {
    pub child_method_name: Option<CowGrpcMethod>,
    layers: S::Child,
}

impl<S: Layer> Clone for ChildContext<S> {
    fn clone(&self) -> Self {
        Self {
            child_method_name: self.child_method_name.clone(),
            layers: self.layers.clone(),
        }
    }
}

impl<S: Layer> ClientHooks for ChildContext<S> {
    fn new<T>(_method: GrpcMethod, _request: &Request<T>) -> Self {
        Self {
            child_method_name: None,
            layers: S::Child::new(),
        }
    }
}

impl<S: Layer> ChildContext<S> {
    pub fn set_method_name(&mut self, name: CowGrpcMethod) {
        self.child_method_name = Some(name);
    }
}

// Type-position aliases pin tests to the default stack; expression-position
// paths such as `ParentContext::begin` do not apply default type parameters.
#[cfg(all(test, any(feature = "abort_slo", feature = "estimator")))]
type DefaultParentContext = ParentContext;
#[cfg(all(test, any(feature = "abort_slo", feature = "estimator")))]
type DefaultServerContext = ServerContext;
#[cfg(all(test, any(feature = "abort_slo", feature = "estimator")))]
type DefaultChildContext = ChildContext;

#[cfg(test)]
mod tests {
    #[cfg(feature = "abort_slo")]
    crate::generate_abort_slo_test!(
        DefaultParentContext,
        DefaultServerContext,
        DefaultChildContext
    );

    #[cfg(feature = "estimator")]
    mod est_tests {
        use super::super::{
            resolve_method_name_from_http, resolve_method_name_from_request,
            DefaultChildContext as ChildContext, DefaultParentContext as ParentContext,
            DefaultServerContext as ServerContext,
        };
        use crate::context_ext::MASA_CONTEXT_HEADER;
        use crate::layer::est::latency_map::ParentToChildKey;
        use crate::layer::est::state::LatencyEstimators;
        use crate::ContextBuilder;
        use crate::MethodRegistry;
        use masa_core::LatencyRms;
        use std::sync::Arc;
        use tonic::masa::{ClientHooks, ParentHooks, ServerHooks};
        use tonic::{CowGrpcMethod, GrpcMethod, Request, Response};

        #[test]
        fn test_server_context_rms_integration() {
            let est = LatencyEstimators::<LatencyRms>::new();
            let registry = MethodRegistry::global();
            let parent_mid =
                registry.get_or_register(CowGrpcMethod::new("TestIntegration", "Parent"));
            let child_mid =
                registry.get_or_register(CowGrpcMethod::new("TestIntegration", "Child"));
            let root_mid = registry.get_or_register(CowGrpcMethod::new("TestIntegration", "Root"));
            let key = ParentToChildKey::root_rpc_method(root_mid)
                .parent_rpc_method(parent_mid)
                .child_rpc_method(child_mid);

            {
                est.child_wallclock_map().insert(key, LatencyRms::new(2));
            }

            est.track_child_wallclock(key, 10);
            let val = est.est_child_wallclock(key);
            assert_eq!(val, Some(0));

            est.track_child_wallclock(key, 10);
            let val = est.est_child_wallclock(key);
            assert_eq!(val, Some(10));

            est.track_child_wallclock(key, 20);
            let val = est.est_child_wallclock(key);
            assert_eq!(val, Some(10));

            est.track_child_wallclock(key, 20);
            let val = est.est_child_wallclock(key);
            assert_eq!(val, Some(15));
        }

        #[test]
        fn test_resolve_method_name_from_http_with_overrides() {
            use crate::context_ext::{
                set_method_name_override_in_headers, set_service_name_override_in_headers,
            };

            let method = GrpcMethod::new("TestService", "TestMethod");
            let mut req = http::Request::new(());

            let resolved = resolve_method_name_from_http(method, &req);
            assert_eq!(resolved.service(), "TestService");
            assert_eq!(resolved.method(), "TestMethod");

            set_method_name_override_in_headers(req.headers_mut(), "OverriddenMethod").unwrap();
            let resolved = resolve_method_name_from_http(method, &req);
            assert_eq!(resolved.service(), "TestService");
            assert_eq!(resolved.method(), "OverriddenMethod");

            set_service_name_override_in_headers(req.headers_mut(), "OverriddenService").unwrap();
            let resolved = resolve_method_name_from_http(method, &req);
            assert_eq!(resolved.service(), "OverriddenService");
            assert_eq!(resolved.method(), "OverriddenMethod");
        }

        #[test]
        fn test_resolve_method_name_from_request_with_overrides() {
            use crate::MasaRequestExt;

            let method = GrpcMethod::new("TestService", "TestMethod");
            let mut req = Request::new(());

            let resolved = resolve_method_name_from_request(method, &req);
            assert_eq!(resolved.service(), "TestService");
            assert_eq!(resolved.method(), "TestMethod");

            req.set_method_name_override("OverriddenMethod").unwrap();
            let resolved = resolve_method_name_from_request(method, &req);
            assert_eq!(resolved.service(), "TestService");
            assert_eq!(resolved.method(), "OverriddenMethod");

            req.set_service_name_override("OverriddenService").unwrap();
            let resolved = resolve_method_name_from_request(method, &req);
            assert_eq!(resolved.service(), "OverriddenService");
            assert_eq!(resolved.method(), "OverriddenMethod");
        }

        #[test]
        fn test_local_deadline_policy_integration() {
            let server_ctx = Arc::new(ServerContext::new("IntegrationService"));

            let method = GrpcMethod::new("IntegrationService", "ParentMethod");
            let now = masa_core::time_now();
            let slo_us = 100_000u64;
            let deadline = now + slo_us;
            let ctx = ContextBuilder::new("IntegrationService", 123)
                .slo(slo_us)
                .gateway_entry(now)
                .deadline(deadline)
                .build();

            let req = http::Request::builder()
                .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
                .body(())
                .unwrap();

            let parent_ctx = ParentContext::begin(method, &req, server_ctx.clone());

            let child_method = GrpcMethod::new("IntegrationService", "ChildMethod");
            let mut child_req = Request::new(());
            let mut child_ctx = ChildContext::new(child_method, &child_req);

            let _ = parent_ctx
                .before_child_rpc(child_method, &mut child_req, &mut child_ctx)
                .unwrap();

            // Verify child tracker was initialized by the estimation layer
            let (_budget, (_guard, (estimation, _))) = &child_ctx.layers;
            let child_tracker = estimation
                .child_tracker
                .as_ref()
                .expect("child_tracker should be initialized after before_child_rpc");

            let registry = MethodRegistry::global();
            let parent_id =
                registry.get_or_register(CowGrpcMethod::new("IntegrationService", "ParentMethod"));
            let child_id =
                registry.get_or_register(CowGrpcMethod::new("IntegrationService", "ChildMethod"));

            let key = child_tracker.key;
            assert_eq!(key.parent(), parent_id);
            assert_eq!(key.child(), child_id);

            let mut response = Ok(Response::new(()));
            let _ = parent_ctx
                .after_child_rpc(child_method, &mut response, child_ctx)
                .unwrap();

            let mut response_result = Ok(Response::new(()));
            parent_ctx.finalize_before_serialization(&mut response_result);

            let parent_method = registry.get_method_name(parent_id).unwrap();
            assert_eq!(parent_method.service(), "IntegrationService");
            assert_eq!(parent_method.method(), "ParentMethod");
        }
    }
}
