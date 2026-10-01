//! Server-free harness for the round-2 probe samples: drives `PolicyHooks<S>`
//! through virtual hops with virtual time (`masa_core::test_clock`).

use std::sync::Arc;
use std::task::Poll;

use masa_core::{test_clock, time_now, Context};
pub use masa_policy::{
    get_wire_from_metadata, peek, policy_stack, BudgetInfo, BudgetModule, ChildDeadline,
    ChildOutcome, ChildPriority, ChildState, ContextBuilder, Extensions, Module, ModuleStack,
    Outcome, PolicyHooks, Requires, WireIn, WireOut,
};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{GrpcMethod, Request, Response, Status};

pub type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
pub type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
pub type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

pub const MS: u64 = 1_000;

pub fn reset_clock() {
    test_clock::set_now_us(test_clock::START_US);
}
pub fn set_clock(t: u64) {
    test_clock::set_now_us(t);
}
pub fn advance_ms(ms: u64) {
    test_clock::advance_us(ms * MS);
}
pub fn now() -> u64 {
    time_now()
}

/// A root request's budget context: SLO `slo_ms`, deadline = now + SLO, default priority.
pub fn root_ctx(slo_ms: u64) -> Context {
    let n = now();
    ContextBuilder::new("probe-api", 1)
        .slo(slo_ms * MS)
        .gateway_entry(n)
        .deadline(n + slo_ms * MS)
        .build()
}

/// What a root client does to send `ctx` plus extra module sections.
pub fn root_http(ctx: &Context, extra: impl FnOnce(&mut WireOut)) -> http::Request<()> {
    let mut w = WireOut::new();
    w.put::<BudgetModule>(ctx).unwrap();
    extra(&mut w);
    let mut req = Request::new(());
    w.install(req.metadata_mut());
    to_http(&req)
}

pub fn to_http<T>(req: &Request<T>) -> http::Request<()> {
    let mut h = http::Request::new(());
    *h.headers_mut() = req.metadata().clone().into_headers();
    h
}

/// One service (server-side hook state shared by all requests it serves).
pub struct Svc<S: ModuleStack> {
    name: &'static str,
    server: Arc<Server<S>>,
}

impl<S: ModuleStack> Svc<S> {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            server: Arc::new(Server::<S>::new(name)),
        }
    }
    pub fn accept(&self, method: &'static str, req: &http::Request<()>) -> Hop<S> {
        Hop {
            parent: Parent::<S>::begin(
                GrpcMethod::new(self.name, method),
                req,
                self.server.clone(),
            ),
            service: self.name,
        }
    }
}

/// One in-flight request at a service.
pub struct Hop<S: ModuleStack> {
    pub parent: Parent<S>,
    service: &'static str,
}

/// An issued child RPC (the hooks have run; not yet answered).
pub struct Out<S: ModuleStack> {
    pub req: Request<()>,
    child: Child<S>,
    method: GrpcMethod,
}

impl<S: ModuleStack> Out<S> {
    pub fn http(&self) -> http::Request<()> {
        to_http(&self.req)
    }
    /// The budget section the child carries.
    pub fn budget(&self) -> Context {
        peek::<BudgetModule>(self.http().headers())
            .unwrap()
            .expect("child carries a budget section")
    }
    /// Module `M`'s section as the child receives it.
    pub fn wire<M: Module>(&self) -> Option<M::Wire> {
        peek::<M>(self.http().headers()).unwrap()
    }
    pub fn state(&self) -> &ChildState {
        self.child.state()
    }
}

pub type Reply = Result<Response<()>, Status>;

impl<S: ModuleStack> Hop<S> {
    pub fn before_poll(&self) -> Result<(), Status> {
        match self.parent.before_poll::<()>() {
            Ok(()) => Ok(()),
            Err(Err(s)) => Err(s),
            Err(Ok(_)) => panic!("early Ok response unsupported in probe"),
        }
    }
    pub fn after_poll(&self, ready: bool) -> Result<(), Status> {
        let p: Poll<Reply> = if ready {
            Poll::Ready(Ok(Response::new(())))
        } else {
            Poll::Pending
        };
        match self.parent.after_poll::<()>(&p) {
            Ok(()) => Ok(()),
            Err(Err(s)) => Err(s),
            Err(Ok(_)) => panic!("early Ok response unsupported in probe"),
        }
    }
    pub fn child(&self, method: &'static str) -> Result<Out<S>, Status> {
        let m = GrpcMethod::new(self.service, method);
        let mut req = Request::new(());
        let mut child = Child::<S>::new(m, &req);
        // On rejection the stack has already delivered `after_child_rpc`
        // (Rejected) to every module that ran; the caller owes nothing.
        self.parent.before_child_rpc(m, &mut req, &mut child)?;
        Ok(Out {
            req,
            child,
            method: m,
        })
    }
    /// Deliver a child's reply to the hooks (`after_child_rpc`).
    pub fn answer(&self, out: Out<S>, mut reply: Reply) -> Result<Reply, Status> {
        self.parent
            .after_child_rpc(out.method, &mut reply, out.child)?;
        Ok(reply)
    }
    pub fn finalize(&self, mut result: Reply) -> Reply {
        self.parent.finalize_before_serialization(&mut result);
        result
    }
}

/// Module `M`'s section of a reply (success or error status).
pub fn wire_of_reply<M: Module>(r: &Reply) -> Option<M::Wire> {
    let md = match r {
        Ok(resp) => resp.metadata(),
        Err(s) => s.metadata(),
    };
    get_wire_from_metadata::<M>(md)
}
