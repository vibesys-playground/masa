// Rajomon in a toy stack built from the generic framework alone: a stack of
// one module, run as tonic hooks by `rpcstack-tonic`, with no Masa crate in
// the build. Every hop is driven in-process through the public hook API.
//
// Rajomon's state is process-wide, so the tests share it and take a lock; each
// uses its own method names so the downstream price tables do not overlap.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard, Once};

use rajomon::{RajomonModule, RajomonWire, RAJOMON_STATE};
use rpcstack::{policy_stack, WireIn, WireOut, HEADER_NAME};
use rpcstack_tonic::ResponseExt;
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

type Toy = policy_stack![RajomonModule];
type Server = <rpcstack_tonic::PolicyHooks<Toy> as Hooks>::ServerContext;
type Parent = <rpcstack_tonic::PolicyHooks<Toy> as Hooks>::ParentContext;
type Child = <rpcstack_tonic::PolicyHooks<Toy> as Hooks>::ChildContext;

static LOCK: Mutex<()> = Mutex::new(());

/// Serializes the tests and fixes the parameters: always propagate the price,
/// so a response's price section is deterministic. Must run before Rajomon
/// reads its parameters.
fn setup(own_price: u64) -> MutexGuard<'static, ()> {
    static PARAMS: Once = Once::new();
    PARAMS.call_once(|| {
        let path = std::env::temp_dir().join(format!("rajomon_toy_{}.json", std::process::id()));
        std::fs::write(&path, r#"{"rajomon": {"price_freq": 1}}"#).unwrap();
        std::env::set_var(rajomon::PARAMS_PATH_ENV, &path);
    });
    let guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    RAJOMON_STATE.own_price.store(own_price, Ordering::Relaxed);
    guard
}

/// Runs `body` as a task: Rajomon reads the polling task's queue latency in
/// `before_poll`, which exists only inside a task. The body never yields, so
/// the price-update worker the first request starts cannot tick during it.
fn in_task(body: impl FnOnce() + Send + 'static) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::spawn(async move { body() }).await.unwrap();
    });
}

fn request_with_tokens(tokens: u64) -> http::Request<()> {
    let mut wire = WireOut::new();
    wire.put::<RajomonModule>(&RajomonWire::request(tokens))
        .unwrap();
    let mut request = http::Request::new(());
    request
        .headers_mut()
        .insert(HEADER_NAME, wire.header_value().parse().unwrap());
    request
}

fn begin(server: &Arc<Server>, method: &'static str, req: &http::Request<()>) -> Parent {
    Parent::begin(GrpcMethod::new("toy.Service", method), req, server.clone())
}

fn new_server() -> Arc<Server> {
    Arc::new(Server::new("toy.Service"))
}

fn rejection_reason(result: Result<(), Result<Response<()>, Status>>) -> String {
    let status = result.unwrap_err().unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    status.message().to_string()
}

#[test]
fn inbound_gate_rejects_a_request_whose_tokens_do_not_cover_the_price() {
    in_task(|| {
        let _guard = setup(30);
        let server = new_server();

        let poor = begin(&server, "GateRpc", &request_with_tokens(10));
        let message = rejection_reason(poor.before_poll::<()>());
        assert!(message.contains("RajomonAdmissionRej"), "{message}");

        let funded = begin(&server, "GateRpc", &request_with_tokens(30));
        assert!(funded.before_poll::<()>().is_ok());
    });
}

#[test]
fn a_request_without_rajomon_data_gets_the_default_budget() {
    in_task(|| {
        let _guard = setup(30);
        let server = new_server();
        let parent = begin(&server, "DefaultRpc", &http::Request::new(()));
        assert!(parent.before_poll::<()>().is_ok());
    });
}

#[test]
fn child_request_carries_the_budget_left_after_this_hop_s_own_price() {
    in_task(|| {
        let _guard = setup(30);
        let server = new_server();
        let parent = begin(&server, "ForwardRpc", &request_with_tokens(100));
        parent.before_poll::<()>().unwrap();

        let mut request = Request::new(());
        let mut child = Child::new(GrpcMethod::new("toy.Service", "ForwardChild"), &request);
        parent
            .before_child_rpc(
                GrpcMethod::new("toy.Service", "ForwardChild"),
                &mut request,
                &mut child,
            )
            .unwrap();

        let wire = WireIn::from_metadata(request.metadata()).unwrap();
        let forwarded = wire.get::<RajomonModule>().unwrap().unwrap();
        assert_eq!(forwarded, RajomonWire::request(70));
    });
}

#[test]
fn child_rpc_is_rejected_when_the_remaining_budget_cannot_cover_its_price() {
    in_task(|| {
        let _guard = setup(30);
        let child_method = CowGrpcMethod::new("toy.Service", "PricyChild");
        RAJOMON_STATE.downstream_prices.insert(
            (CowGrpcMethod::new("toy.Service", "BudgetRpc"), child_method),
            80,
        );

        let server = new_server();
        let parent = begin(&server, "BudgetRpc", &request_with_tokens(100));
        parent.before_poll::<()>().unwrap();

        let mut request = Request::new(());
        let mut child = Child::new(GrpcMethod::new("toy.Service", "PricyChild"), &request);
        let status = parent
            .before_child_rpc(
                GrpcMethod::new("toy.Service", "PricyChild"),
                &mut request,
                &mut child,
            )
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(status.message().contains("RajomonChildBudgetRej"));
    });
}

#[test]
fn price_in_a_child_response_is_learned_and_raises_this_hop_s_price() {
    in_task(|| {
        let _guard = setup(0);
        let server = new_server();
        let parent = begin(&server, "LearnRpc", &request_with_tokens(100));
        parent.before_poll::<()>().unwrap();

        let mut request = Request::new(());
        let mut child = Child::new(GrpcMethod::new("toy.Service", "LearnChild"), &request);
        parent
            .before_child_rpc(
                GrpcMethod::new("toy.Service", "LearnChild"),
                &mut request,
                &mut child,
            )
            .unwrap();

        let mut reply = Response::new(());
        reply.set_wire::<RajomonModule>(&RajomonWire::response(100, 13));
        parent
            .after_child_rpc(
                GrpcMethod::new("toy.Service", "LearnChild"),
                &mut Ok(reply),
                child,
            )
            .unwrap();

        let learned = CowGrpcMethod::new("toy.Service", "LearnChild");
        assert_eq!(RAJOMON_STATE.child_price(&learned), 13);

        // The learned price now counts against later requests to this method.
        let poor = begin(&server, "LearnRpc", &request_with_tokens(12));
        let message = rejection_reason(poor.before_poll::<()>());
        assert!(message.contains("RajomonAdmissionRej"), "{message}");
        let funded = begin(&server, "LearnRpc", &request_with_tokens(13));
        assert!(funded.before_poll::<()>().is_ok());
    });
}

#[test]
fn response_echoes_the_request_budget_and_advertises_the_price() {
    in_task(|| {
        let _guard = setup(7);
        let server = new_server();
        let parent = begin(&server, "AdvertiseRpc", &request_with_tokens(100));
        parent.before_poll::<()>().unwrap();

        let mut result = Ok(Response::new(()));
        parent.finalize_before_serialization(&mut result);

        let wire = result.unwrap().get_wire::<RajomonModule>().unwrap();
        assert_eq!(wire, RajomonWire::response(100, 7));
    });
}
