// Masa's budget section next to other modules' wire data: setting the budget
// keeps their data and vice versa, and decoding the budget ignores their
// sections. The framework's own wire behavior is tested in `rpcstack-tonic`.

use masa_core::{time_now, Context};
use masa_policy::ContextBuilder;
use masa_policy::{Extensions, MasaRequestExt, Module, WireIn, WireOut, MASA_CONTEXT_HEADER};
use serde::{Deserialize, Serialize};
use tonic::{CowGrpcMethod, Request};

fn root_context() -> Context {
    let now = time_now();
    ContextBuilder::new("wire-api", 1)
        .slo(1_000_000)
        .gateway_entry(now)
        .deadline(now + 1_000_000)
        .build()
}

/// The request a sender would produce: the context plus the given wire data.
fn sent(wire: impl FnOnce(&mut WireOut)) -> Request<()> {
    let mut request = Request::new(());
    let mut out = WireOut::new();
    wire(&mut out);
    out.install(request.metadata_mut());
    request.set_masa_context(&root_context());
    request
}

// ── Two modules that carry wire data ────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AlphaWire {
    n: u64,
}

#[derive(Debug)]
struct Alpha;

impl Module for Alpha {
    type Server = ();
    const NAME: &'static str = "alpha";
    type Wire = AlphaWire;

    fn new(_m: &CowGrpcMethod, _s: &(), _wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BetaWire {
    label: String,
    zero: Option<u64>,
}

#[derive(Debug)]
struct Beta;

impl Module for Beta {
    type Server = ();
    const NAME: &'static str = "beta";
    type Wire = BetaWire;

    fn new(_m: &CowGrpcMethod, _s: &(), _wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self
    }
}

// ── Budget and other sections ───────────────────────────────────────────

#[test]
fn setting_the_context_keeps_wire_data_and_vice_versa() {
    let mut request = Request::new(());
    request.set_masa_context(&root_context());
    request.set_wire::<Alpha>(&AlphaWire { n: 5 });
    request.set_wire::<Beta>(&BetaWire {
        label: "b".into(),
        zero: None,
    });
    request.set_masa_context(&root_context());
    assert_eq!(request.get_wire::<Alpha>(), Some(AlphaWire { n: 5 }));
    assert_eq!(request.get_wire::<Beta>().unwrap().label, "b");
    assert_eq!(request.get_masa_context().unwrap().request_id(), 1);
}

#[test]
fn header_without_wire_data_is_the_budget_section_alone() {
    let ctx = root_context();
    let mut request = Request::new(());
    request.set_masa_context(&ctx);
    let value = request.metadata().get(MASA_CONTEXT_HEADER).unwrap();
    assert_eq!(value.to_str().unwrap(), ctx.to_header_string());
}

#[test]
fn context_decoding_ignores_other_sections() {
    let root = sent(|out| out.put::<Alpha>(&AlphaWire { n: 9 }).unwrap());
    let value = root
        .metadata()
        .get(MASA_CONTEXT_HEADER)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(value.contains('.'));
    assert_eq!(Context::from_header_string(value).request_id(), 1);
}

// ── Rajomon ─────────────────────────────────────────────────────────────

#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
mod rajomon {
    use std::sync::Arc;

    use super::*;
    use masa_policy::modules::RajomonModule;
    use masa_policy::{policy_stack, MasaResponseExt, ModuleStack, PolicyHooks, RajomonWire};
    use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
    use tonic::{GrpcMethod, Response};

    type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
    type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
    type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

    /// What the receiving hop's transport hands to `ParentHooks::begin`.
    fn arrives(request: &Request<()>) -> http::Request<()> {
        let value = request.metadata().get(MASA_CONTEXT_HEADER).unwrap();
        http::Request::builder()
            .header(MASA_CONTEXT_HEADER, value.to_str().unwrap())
            .body(())
            .unwrap()
    }

    fn begin<S: ModuleStack>(server: &Arc<Server<S>>, request: &Request<()>) -> Parent<S> {
        Parent::<S>::begin(
            GrpcMethod::new("wire.Service", "Hop"),
            &arrives(request),
            server.clone(),
        )
    }

    fn call_child<S: ModuleStack>(parent: &Parent<S>) -> Request<()> {
        let method = GrpcMethod::new("wire.Service", "Child");
        let mut request = Request::new(());
        let mut child = Child::<S>::new(method, &request);
        parent
            .before_child_rpc(method, &mut request, &mut child)
            .unwrap();
        request
    }

    fn respond<S: ModuleStack>(parent: &Parent<S>) -> Response<()> {
        let mut result = Ok(Response::new(()));
        parent.finalize_before_serialization(&mut result);
        result.unwrap()
    }

    type Stack = policy_stack![RajomonModule];

    fn tokens_sent_to_child(root: &Request<()>) -> Option<RajomonWire> {
        let server = Arc::new(Server::<Stack>::new("wire.Service"));
        let parent = begin::<Stack>(&server, root);
        call_child::<Stack>(&parent).get_wire::<RajomonModule>()
    }

    #[test]
    fn tokens_travel_down_the_call_chain() {
        let root = sent(|out| out.put::<RajomonModule>(&RajomonWire::request(40)).unwrap());
        let server = Arc::new(Server::<Stack>::new("wire.Service"));

        let parent = begin::<Stack>(&server, &root);
        let to_child = call_child::<Stack>(&parent);
        assert_eq!(
            to_child.get_wire::<RajomonModule>(),
            Some(RajomonWire::request(40))
        );

        let child = begin::<Stack>(&server, &to_child);
        let reply = respond::<Stack>(&child);
        // The response may also carry a propagated price; the echo is the
        // token budget.
        assert_eq!(
            reply.get_wire::<RajomonModule>().map(|wire| wire.tokens),
            Some(40)
        );
    }

    #[test]
    fn zero_tokens_survive_the_round_trip_as_zero() {
        let root = sent(|out| out.put::<RajomonModule>(&RajomonWire::request(0)).unwrap());
        assert_eq!(
            root.get_wire::<RajomonModule>(),
            Some(RajomonWire::request(0))
        );
        assert_eq!(tokens_sent_to_child(&root), Some(RajomonWire::request(0)));
    }

    #[test]
    fn a_request_without_rajomon_data_is_given_the_default_budget() {
        let root = sent(|_| {});
        assert_eq!(root.get_wire::<RajomonModule>(), None);
        assert_eq!(tokens_sent_to_child(&root), Some(RajomonWire::request(100)));
    }

    fn response_with(wire: RajomonWire) -> Response<()> {
        let mut response = Response::new(()).with_masa_context(&root_context());
        response.set_wire::<RajomonModule>(&wire);
        response
    }

    #[test]
    fn a_propagated_price_round_trips_including_zero() {
        for price in [0, 13] {
            let response = response_with(RajomonWire::response(7, price));
            assert_eq!(
                response.get_wire::<RajomonModule>(),
                Some(RajomonWire::response(7, price))
            );
        }
    }

    #[test]
    fn no_price_is_not_written_and_reads_as_absent() {
        let response = response_with(RajomonWire::request(7));
        let value = response
            .metadata()
            .get(MASA_CONTEXT_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        // {"tokens":7}, exactly what a request carries.
        assert!(value.ends_with(".rajomon:eyJ0b2tlbnMiOjd9"), "{value}");
        assert_eq!(response.get_wire::<RajomonModule>().unwrap().price, None);
    }
}
