// Masa's tonic hooks: the `rpcstack-tonic` adapter, defaulting to `MasaStack`.
//
// `PolicyHooks<S>` runs any module stack `S`; `MasaStack`, selected by Cargo
// features in `masa_stack.rs`, is the default, so `PolicyHooks` alone is Masa's
// hook implementation.

use crate::masa_stack::MasaStack;

/// `Hooks` implementation that runs the policy module stack `S`.
pub type PolicyHooks<S = MasaStack> = rpcstack_tonic::PolicyHooks<S>;

/// Server-level state of the module stack `S`.
pub type ServerContext<S = MasaStack> = rpcstack_tonic::ServerContext<S>;

/// Per-request state of the module stack `S`.
pub type ParentContext<S = MasaStack> = rpcstack_tonic::ParentContext<S>;

/// Per-child-RPC state of the module stack `S`.
pub type ChildContext<S = MasaStack> = rpcstack_tonic::ChildContext<S>;

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
            DefaultChildContext as ChildContext, DefaultParentContext as ParentContext,
            DefaultServerContext as ServerContext,
        };
        use crate::context_ext::MASA_CONTEXT_HEADER;
        use crate::module::est::latency_map::ParentToChildKey;
        use crate::module::est::state::{ChildRPCTracker, LatencyEstimators};
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

            // Verify child tracker was initialized by the estimation module
            let child_tracker = child_ctx
                .state()
                .get::<ChildRPCTracker>()
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
