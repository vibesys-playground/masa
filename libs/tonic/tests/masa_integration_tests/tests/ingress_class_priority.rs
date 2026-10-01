// A policy module that decides its own service's task priority at ingress.
//
// `ClassModule` is an example, not a shipped policy. Requests carry a service
// class in the module's own wire section, and each service queues the task that
// serves a request by the class: a gold request is treated as due 20 s earlier
// than the sender's priority says, a silver one 10 s. The sender, which knows
// nothing of classes, writes the usual deadline-ordered priority in the budget
// section, so the two orders disagree.
//
// The test serves generated stubs over real HTTP/2 on Tokio's priority queue and
// checks the order in which the handlers start. Three requests wait in the
// connection while the server is busy, and Hyper queues their tasks together,
// so the order of the first polls is the order the tasks were queued in, which
// is what the ingress decision sets. Before ingress hooks the only way to get
// this order was for the sender to write the receiver's priority itself.

#![cfg(all(feature = "sched_slo", not(feature = "sched_custom")))]

use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use masa_core::{peek_priority, Context, PriorityHint};
use masa_integration_tests::pb::{
    child_service_client::ChildServiceClient,
    child_service_server::{ChildService, ChildServiceServer},
    Input1, Input2, Output1, Output2,
};
use masa_policy::{
    header_string_with_wire, policy_stack, BudgetModule, Extensions, Ingress, Module, PolicyHooks,
    Requires, WireIn, WireOut,
};
use serde::{Deserialize, Serialize};
use tonic::masa::{ClientHooks, Hooks, Meta, ParentHooks, ServerHooks};
use tonic::transport::Server;
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

const SECOND_US: u64 = 1_000_000;
const BLOCKER: u64 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Class {
    Gold,
    Silver,
    Bronze,
}

impl Class {
    /// How much earlier than the sender says a request of this class is due.
    fn bonus_us(self) -> u64 {
        match self {
            Class::Gold => 20 * SECOND_US,
            Class::Silver => 10 * SECOND_US,
            Class::Bronze => 0,
        }
    }

    fn request_id(self) -> u64 {
        match self {
            Class::Gold => 1,
            Class::Silver => 2,
            Class::Bronze => 3,
        }
    }

    fn of(request_id: u64) -> Class {
        [Class::Gold, Class::Silver, Class::Bronze]
            .into_iter()
            .find(|class| class.request_id() == request_id)
            .expect("a request of a class")
    }
}

#[derive(Debug)]
struct ClassModule {
    class: Class,
}

impl ClassModule {
    fn class(wire: &WireIn<'_>) -> Class {
        wire.get::<Self>()
            .unwrap_or_else(|err| panic!("{err}"))
            .unwrap_or(Class::Bronze)
    }
}

impl Module for ClassModule {
    type Server = ();
    const NAME: &'static str = "class";
    type Wire = Class;

    fn requires(requires: &mut Requires) {
        requires.module::<BudgetModule>();
    }

    /// The task's priority is the sender's, moved earlier by the class bonus.
    /// The sender's priority is read from the budget section without decoding
    /// anything else in it.
    fn ingress(wire: &WireIn<'_>, ingress: &mut Ingress) {
        let budget = wire
            .get_encoded::<BudgetModule>()
            .expect("the request carries a budget section");
        let sender = peek_priority(budget).expect("a valid budget section");
        let class = Self::class(wire);
        ingress.propose(Meta::new(sender.value().saturating_sub(class.bonus_us())));
    }

    fn new(
        _method: &CowGrpcMethod,
        _server: &(),
        wire: &WireIn<'_>,
        _ext: &mut Extensions,
    ) -> Self {
        Self {
            class: Self::class(wire),
        }
    }

    /// Nothing is forwarded by the framework, so inheritance is explicit.
    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _child: &mut masa_policy::ChildState,
        _request: &mut Request<T>,
        child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        child_wire
            .put::<Self>(&self.class)
            .unwrap_or_else(|err| panic!("{err}"));
        Ok(())
    }
}

type ClassStack = policy_stack![BudgetModule, ClassModule];
type PlainStack = policy_stack![BudgetModule];

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral test port");
    listener.local_addr().expect("read ephemeral test port")
}

/// Records the order in which handlers start; the blocker keeps the server's
/// only thread busy so that the other requests queue behind it.
#[derive(Clone, Default)]
struct OrderSvc {
    started: Arc<Mutex<Vec<Class>>>,
}

#[tonic::async_trait]
impl ChildService for OrderSvc {
    async fn rpc1(&self, request: Request<Input1>) -> Result<Response<Output1>, Status> {
        let id = masa_policy::get_masa_context_from_metadata(request.metadata())
            .expect("a budget section")
            .request_id();
        if id == BLOCKER {
            std::thread::sleep(Duration::from_millis(400));
        } else {
            self.started.lock().unwrap().push(Class::of(id));
        }
        Ok(Response::new(Output1 {}))
    }

    async fn rpc2(&self, _req: Request<Input2>) -> Result<Response<Output2>, Status> {
        Ok(Response::new(Output2 {}))
    }
}

/// A request of `class`. The sender's priority is its deadline, so a request
/// that is due later is queued later, whatever its class.
fn request(request_id: u64, class: Class, due_in_s: u64) -> Request<Input1> {
    let now = masa_core::time_now();
    let deadline = now + due_in_s * SECOND_US;
    let ctx = Context::new(
        "test.ChildService/Rpc1",
        request_id,
        due_in_s * SECOND_US,
        now,
        deadline,
        PriorityHint::new(deadline),
    );
    let mut request = Request::new(Input1 {});
    request.metadata_mut().insert(
        "ctx",
        header_string_with_wire::<ClassModule>(&ctx, &class)
            .parse()
            .unwrap(),
    );
    request
}

/// Serve `svc` with hooks `H` on a thread of its own, so that blocking its
/// runtime does not stop the client.
fn serve<H: Hooks>(svc: OrderSvc) -> SocketAddr {
    let addr = unused_local_addr();
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                Server::builder()
                    .add_service(ChildServiceServer::<_, H>::with_custom_context(svc))
                    .serve(addr)
                    .await
                    .unwrap();
            });
    });
    addr
}

/// The order in which the handlers started when a silver, a bronze and a gold
/// request arrived, in that order, while the server was busy.
async fn start_order<H: Hooks>() -> Vec<Class> {
    let svc = OrderSvc::default();
    let started = svc.started.clone();
    let addr = serve::<H>(svc);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = ChildServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    // Due later means queued later by the sender's priority: bronze, silver,
    // gold. By class the order is the opposite.
    let blocker = {
        let mut client = client.clone();
        tokio::spawn(async move {
            client
                .rpc1(request(BLOCKER, Class::Bronze, 30))
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut waiting = Vec::new();
    for (class, due_in_s) in [(Class::Silver, 25), (Class::Bronze, 20), (Class::Gold, 30)] {
        let mut client = client.clone();
        waiting.push(tokio::spawn(async move {
            client
                .rpc1(request(class.request_id(), class, due_in_s))
                .await
                .unwrap()
        }));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    blocker.await.unwrap();
    for call in waiting {
        call.await.unwrap();
    }
    let order = started.lock().unwrap().clone();
    order
}

#[tokio::test(flavor = "current_thread")]
async fn without_an_ingress_module_the_senders_priority_orders_the_first_poll() {
    assert_eq!(
        start_order::<PolicyHooks<PlainStack>>().await,
        [Class::Bronze, Class::Silver, Class::Gold]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn the_ingress_decision_orders_the_first_poll_against_the_senders_priority() {
    assert_eq!(
        start_order::<PolicyHooks<ClassStack>>().await,
        [Class::Gold, Class::Silver, Class::Bronze]
    );
}

#[test]
fn ingress_reads_the_class_and_the_senders_priority_and_the_class_module_wins() {
    let mut headers = http::HeaderMap::new();
    let ctx = Context::new(
        "api",
        1,
        0,
        0,
        100 * SECOND_US,
        PriorityHint::new(100 * SECOND_US),
    );
    headers.insert(
        "ctx",
        header_string_with_wire::<ClassModule>(&ctx, &Class::Gold)
            .parse()
            .unwrap(),
    );

    assert_eq!(
        <PolicyHooks<ClassStack> as Hooks>::ingress(&headers),
        Some(Meta::new(80 * SECOND_US))
    );
    assert_eq!(
        <PolicyHooks<PlainStack> as Hooks>::ingress(&headers),
        Some(Meta::new(100 * SECOND_US))
    );
}

#[test]
fn children_inherit_the_class() {
    type Parent = <PolicyHooks<ClassStack> as Hooks>::ParentContext;
    type Child = <PolicyHooks<ClassStack> as Hooks>::ChildContext;
    type ServerCtx = <PolicyHooks<ClassStack> as Hooks>::ServerContext;

    let ctx = Context::new(
        "api",
        1,
        0,
        0,
        100 * SECOND_US,
        PriorityHint::new(100 * SECOND_US),
    );
    let mut inbound = http::Request::new(());
    inbound.headers_mut().insert(
        "ctx",
        header_string_with_wire::<ClassModule>(&ctx, &Class::Silver)
            .parse()
            .unwrap(),
    );
    let method = GrpcMethod::new("test.ChildService", "Rpc1");
    let parent = Parent::begin(
        method,
        &inbound,
        Arc::new(ServerCtx::new("test.ChildService")),
    );

    let mut outbound = Request::new(());
    let mut child = Child::new(method, &outbound);
    parent
        .before_child_rpc(method, &mut outbound, &mut child)
        .unwrap();

    let wire = WireIn::from_metadata(outbound.metadata()).unwrap();
    assert_eq!(wire.get::<ClassModule>().unwrap(), Some(Class::Silver));
}
