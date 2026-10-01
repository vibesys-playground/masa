// The custom run queue on a real served gRPC path.
//
// `sched_custom` swaps the queue Tokio's current-thread runtime polls tasks
// from for `rpcstack_sched::custom::Queue`. These tests check that the swap
// took effect in this build and that generated stubs still serve through it.
// Priority ordering through the selected queue is covered by
// `serve_behavior.rs`, which runs under the same features; `custom::Queue`
// starts as a copy of the default priority heap, so that test holds until the
// queue is edited on purpose.

#![cfg(all(feature = "sched_slo", feature = "sched_custom"))]

use std::any::TypeId;
use std::net::TcpListener;
use std::time::Duration;

use masa::MasaRequestExt;
use masa_core::time_now;
use masa_integration_tests::pb::{
    child_service_client::ChildServiceClient,
    child_service_server::{ChildService, ChildServiceServer},
    Input1, Input2, Output1, Output2,
};
use masa_policy::ContextBuilder;
use rpcstack_sched::{custom, SchedFlavor, SelectedQueue};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[test]
fn selected_queue_is_the_custom_queue() {
    assert_eq!(
        TypeId::of::<SelectedQueue<()>>(),
        TypeId::of::<custom::Queue<()>>()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_reports_the_custom_queue_flavor() {
    assert_eq!(tokio::runtime::get_sched_flavor(), SchedFlavor::Prio);
}

#[derive(Clone)]
struct EchoSvc;

#[tonic::async_trait]
impl ChildService for EchoSvc {
    async fn rpc1(&self, _req: Request<Input1>) -> Result<Response<Output1>, Status> {
        Ok(Response::new(Output1 {}))
    }

    async fn rpc2(&self, _req: Request<Input2>) -> Result<Response<Output2>, Status> {
        Ok(Response::new(Output2 {}))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn generated_stubs_serve_through_the_custom_queue() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral test port")
        .local_addr()
        .expect("read ephemeral test port");
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ChildServiceServer::new(EchoSvc))
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = ChildServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();
    let now = time_now();
    let ctx = ContextBuilder::new("test.ChildService/Rpc1", 1)
        .gateway_entry(now)
        .slo(1_000_000)
        .deadline(now + 1_000_000)
        .build();
    let mut request = Request::new(Input1 {});
    request.set_masa_context(&ctx);

    client.rpc1(request).await.unwrap();

    server.abort();
}
