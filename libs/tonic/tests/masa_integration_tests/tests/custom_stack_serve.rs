// Custom policy stacks on a real served gRPC path.
//
// `libs/masa-policy/tests/custom_stack.rs` drives stacks through the hook
// traits directly. These tests serve generated stubs over HTTP/2 so that a
// custom stack runs where applications run it: Hyper spawns the handler, and
// generated server code calls the hooks around each poll and at finalize.

#![cfg(feature = "sched_slo")]

use std::net::{SocketAddr, TcpListener};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use masa::MasaRequestExt;
#[cfg(feature = "stack_custom")]
use masa::MasaResponseExt;
use masa_core::{time_now, Context, ContextBuilder};
use masa_integration_tests::pb::{
    child_service_client::ChildServiceClient,
    child_service_server::{ChildService, ChildServiceServer},
    Input1, Input2, Output1, Output2,
};
use masa_policy::{policy_stack, Extensions, Layer, PolicyHooks, WireIn, WireOut};
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Code, CowGrpcMethod, Request, Response, Status};

const BLOCKED_REQUEST_ID: u64 = 7;
const STAMP_HEADER: &str = "x-custom-stack";

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral test port");
    listener.local_addr().expect("read ephemeral test port")
}

fn request_with_context(request_id: u64) -> Request<Input1> {
    let now = time_now();
    let ctx = ContextBuilder::new("test.ChildService/Rpc1", request_id)
        .gateway_entry(now)
        .slo(1_000_000)
        .deadline(now + 1_000_000)
        .build();
    let mut request = Request::new(Input1 {});
    request.set_masa_context(&ctx);
    request
}

#[derive(Clone, Default)]
struct RecordingSvc {
    executed: Arc<AtomicBool>,
}

#[tonic::async_trait]
impl ChildService for RecordingSvc {
    async fn rpc1(&self, _req: Request<Input1>) -> Result<Response<Output1>, Status> {
        self.executed.store(true, Ordering::SeqCst);
        Ok(Response::new(Output1 {}))
    }

    async fn rpc2(&self, _req: Request<Input2>) -> Result<Response<Output2>, Status> {
        Ok(Response::new(Output2 {}))
    }
}

// ── Modules ─────────────────────────────────────────────────────────────

/// Rejects one request id before the handler is first polled.
#[derive(Debug)]
struct RejectBlocked;

impl Layer for RejectBlocked {
    type Server = ();
    type Child = ();
    const NAME: &'static str = "RejectBlocked";
    type Wire = ();

    fn new(
        _method: &CowGrpcMethod,
        _server: &(),
        _ctx: &mut Context,
        _wire: &WireIn<'_>,
        _ext: &mut Extensions,
    ) -> Self {
        Self
    }

    fn before_poll<Ret>(
        &self,
        ctx: &Context,
        _ext: &mut Extensions,
    ) -> Result<(), Result<Response<Ret>, Status>> {
        if ctx.request_id() == BLOCKED_REQUEST_ID {
            return Err(Err(Status::resource_exhausted("rejected by custom stack")));
        }
        Ok(())
    }
}

/// Marks every successful response so the client can see the stack ran.
#[derive(Debug)]
struct StampResponse;

impl Layer for StampResponse {
    type Server = ();
    type Child = ();
    const NAME: &'static str = "StampResponse";
    type Wire = ();

    fn new(
        _method: &CowGrpcMethod,
        _server: &(),
        _ctx: &mut Context,
        _wire: &WireIn<'_>,
        _ext: &mut Extensions,
    ) -> Self {
        Self
    }

    fn finalize<Ret>(
        &self,
        _ctx: &mut Context,
        result: &mut Result<Response<Ret>, Status>,
        _wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        if let Ok(response) = result {
            response
                .metadata_mut()
                .insert(STAMP_HEADER, MetadataValue::from_static("stamped"));
        }
    }
}

type TestStack = policy_stack![RejectBlocked, StampResponse];

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn custom_stack_runs_on_served_requests() {
    let svc = RecordingSvc::default();
    let executed = svc.executed.clone();
    let addr = unused_local_addr();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ChildServiceServer::<_, PolicyHooks<TestStack>>::with_custom_context(svc))
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = ChildServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    let response = client.rpc1(request_with_context(1)).await.unwrap();
    assert_eq!(
        response
            .metadata()
            .get(STAMP_HEADER)
            .map(|v| v.to_str().unwrap()),
        Some("stamped"),
        "finalize of the custom stack did not run"
    );
    assert!(executed.swap(false, Ordering::SeqCst));

    let error = client
        .rpc1(request_with_context(BLOCKED_REQUEST_ID))
        .await
        .expect_err("custom stack should reject the blocked request");
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        !executed.load(Ordering::SeqCst),
        "handler must not run when before_poll rejects"
    );

    server.abort();
}

#[cfg(feature = "stack_custom")]
#[test]
fn default_hooks_are_the_agent_stack() {
    use std::any::TypeId;

    assert_eq!(
        TypeId::of::<masa::DefaultHooks>(),
        TypeId::of::<PolicyHooks<masa_policy::AgentStack>>()
    );
}

/// Generated stubs default to `masa::DefaultHooks`, so under `stack_custom` a
/// plain `ChildServiceServer::new` serves through the agent stack.
#[cfg(feature = "stack_custom")]
#[tokio::test(flavor = "current_thread")]
async fn generated_stubs_serve_through_the_agent_stack() {
    let svc = RecordingSvc::default();
    let executed = svc.executed.clone();
    let addr = unused_local_addr();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ChildServiceServer::new(svc))
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = ChildServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();
    let response = client.rpc1(request_with_context(1)).await.unwrap();

    assert!(executed.load(Ordering::SeqCst));
    assert!(
        response.get_masa_context().is_some(),
        "PolicyHooks<AgentStack> should attach the response context"
    );

    server.abort();
}
