// Module-owned wire data, exercised end to end through the public hook API:
// an inbound header is read by `ParentHooks::begin`, modules write the child
// request's wire data in `before_child_rpc`, and the child's own `begin` reads
// it back from the request the parent produced.

use std::sync::Arc;

use masa_core::{time_now, Context};
use masa_policy::ContextBuilder;
use masa_policy::{
    peek, policy_stack, ChildState, Extensions, MasaRequestExt, MasaResponseExt, Module,
    ModuleStack, Outcome, PolicyHooks, WireIn, WireOut, MASA_CONTEXT_HEADER,
};
use serde::{Deserialize, Serialize};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

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

// ── Two modules with wire data ──────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AlphaWire {
    n: u64,
}

/// Forwards `n + 1` to children and echoes what it received in the response.
#[derive(Debug)]
struct Alpha {
    inbound: Option<AlphaWire>,
}

impl Module for Alpha {
    type Server = ();
    const NAME: &'static str = "alpha";
    type Wire = AlphaWire;

    fn new(_m: &CowGrpcMethod, _s: &(), wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self {
            inbound: wire.get::<Self>().unwrap(),
        }
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _child: &mut ChildState,
        _request: &mut Request<T>,
        child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        if let Some(wire) = &self.inbound {
            child_wire
                .put::<Self>(&AlphaWire { n: wire.n + 1 })
                .unwrap();
        }
        Ok(())
    }

    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        if let Some(inbound) = &self.inbound {
            wire.put::<Self>(inbound).unwrap();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BetaWire {
    label: String,
    zero: Option<u64>,
}

/// Forwards a longer label and the same `zero` to children, and echoes what it
/// received in the response.
#[derive(Debug)]
struct Beta {
    inbound: Option<BetaWire>,
}

impl Module for Beta {
    type Server = ();
    const NAME: &'static str = "beta";
    type Wire = BetaWire;

    fn new(_m: &CowGrpcMethod, _s: &(), wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self {
            inbound: wire.get::<Self>().unwrap(),
        }
    }

    fn before_child_rpc<T>(
        &self,
        _child_method: &CowGrpcMethod,
        _child: &mut ChildState,
        _request: &mut Request<T>,
        child_wire: &mut WireOut,
        _ext: &mut Extensions,
    ) -> Result<(), Status> {
        if let Some(wire) = &self.inbound {
            child_wire
                .put::<Self>(&BetaWire {
                    label: format!("{}/child", wire.label),
                    zero: wire.zero,
                })
                .unwrap();
        }
        Ok(())
    }

    fn finalize<Ret>(
        &self,
        _result: &mut Result<Response<Ret>, Status>,
        _outcome: Outcome<'_>,
        wire: &mut WireOut,
        _ext: &Extensions,
    ) {
        if let Some(inbound) = &self.inbound {
            wire.put::<Self>(inbound).unwrap();
        }
    }
}

/// Has wire data but never writes any: the framework must not carry it to the
/// child on its behalf.
#[derive(Debug)]
struct Silent;

impl Module for Silent {
    type Server = ();
    const NAME: &'static str = "silent";
    type Wire = u8;

    fn new(_m: &CowGrpcMethod, _s: &(), _wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self
    }
}

type Coexisting = policy_stack![Alpha, Silent, Beta];

#[test]
fn two_modules_wire_values_coexist_without_collision() {
    let server = Arc::new(Server::<Coexisting>::new("wire.Service"));
    let root = sent(|out| {
        out.put::<Alpha>(&AlphaWire { n: 1 }).unwrap();
        out.put::<Beta>(&BetaWire {
            label: "root".into(),
            zero: Some(0),
        })
        .unwrap();
    });

    let parent = begin::<Coexisting>(&server, &root);
    let to_child = call_child::<Coexisting>(&parent);
    assert_eq!(to_child.get_wire::<Alpha>(), Some(AlphaWire { n: 2 }));
    assert_eq!(
        to_child.get_wire::<Beta>(),
        Some(BetaWire {
            label: "root/child".into(),
            zero: Some(0),
        })
    );

    let child = begin::<Coexisting>(&server, &to_child);
    let reply = respond::<Coexisting>(&child);
    assert_eq!(reply.get_wire::<Alpha>(), Some(AlphaWire { n: 2 }));
    assert_eq!(
        reply.get_wire::<Beta>(),
        Some(BetaWire {
            label: "root/child".into(),
            zero: Some(0),
        })
    );
}

#[test]
fn nothing_is_carried_to_the_child_unless_a_module_writes_it() {
    let server = Arc::new(Server::<Coexisting>::new("wire.Service"));
    let root = sent(|out| {
        out.put::<Silent>(&7).unwrap();
        out.put::<Alpha>(&AlphaWire { n: 1 }).unwrap();
    });
    let parent = begin::<Coexisting>(&server, &root);
    assert_eq!(root.get_wire::<Silent>(), Some(7));

    let to_child = call_child::<Coexisting>(&parent);
    assert_eq!(to_child.get_wire::<Silent>(), None);
    assert_eq!(to_child.get_wire::<Beta>(), None);
    assert_eq!(to_child.get_wire::<Alpha>(), Some(AlphaWire { n: 2 }));
}

#[test]
fn absent_data_is_reported_as_absent() {
    let server = Arc::new(Server::<Coexisting>::new("wire.Service"));
    let parent = begin::<Coexisting>(&server, &sent(|_| {}));
    let to_child = call_child::<Coexisting>(&parent);
    assert_eq!(to_child.get_wire::<Alpha>(), None);
    assert_eq!(to_child.get_wire::<Beta>(), None);
}

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

#[test]
fn peek_decodes_one_section_without_the_context() {
    let root = sent(|out| {
        out.put::<Alpha>(&AlphaWire { n: 3 }).unwrap();
        out.put::<Beta>(&BetaWire {
            label: "b".into(),
            zero: Some(0),
        })
        .unwrap();
    });
    let headers = arrives(&root).into_parts().0.headers;
    assert_eq!(peek::<Alpha>(&headers).unwrap(), Some(AlphaWire { n: 3 }));
    assert_eq!(peek::<Silent>(&headers).unwrap(), None);

    // A corrupt section of another module is irrelevant to `peek`.
    let mut corrupt = http::HeaderMap::new();
    corrupt.insert(
        MASA_CONTEXT_HEADER,
        "!!.alpha:eyJuIjo0fQ==.beta:!!".parse().unwrap(),
    );
    assert_eq!(peek::<Alpha>(&corrupt).unwrap(), Some(AlphaWire { n: 4 }));
    assert!(peek::<Beta>(&corrupt).is_err());
}

// ── Name uniqueness ─────────────────────────────────────────────────────

#[derive(Debug)]
struct AlphaTwin;

impl Module for AlphaTwin {
    type Server = ();
    const NAME: &'static str = "alpha";
    type Wire = u8;

    fn new(_m: &CowGrpcMethod, _s: &(), _wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self
    }
}

#[test]
#[should_panic(expected = "share the name `alpha`")]
fn duplicate_wire_names_are_rejected() {
    let _ = Server::<policy_stack![Alpha, AlphaTwin]>::new("wire.Service");
}

#[derive(Debug)]
struct BadName;

impl Module for BadName {
    type Server = ();
    const NAME: &'static str = "has.dot";
    type Wire = u8;

    fn new(_m: &CowGrpcMethod, _s: &(), _wire: &WireIn<'_>, _ext: &mut Extensions) -> Self {
        Self
    }
}

#[test]
#[should_panic(expected = "must be non-empty and contain only")]
fn wire_names_that_break_the_envelope_are_rejected() {
    let _ = Server::<policy_stack![BadName]>::new("wire.Service");
}

// ── Rajomon ─────────────────────────────────────────────────────────────

#[cfg(all(feature = "ac_rajomon", not(feature = "ac_pred")))]
mod rajomon {
    use super::*;
    use masa_policy::modules::RajomonModule;
    use masa_policy::RajomonWire;

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
