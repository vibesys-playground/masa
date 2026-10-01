// Module-owned wire data, exercised end to end through the public hook API:
// an inbound header is read by `ParentHooks::begin`, modules write the child
// request's wire data in `before_child_rpc`, and the child's own `begin` reads
// it back from the request the parent produced.

use std::sync::Arc;

use rpcstack::{
    peek, policy_stack, ChildState, Extensions, Module, ModuleStack, Outcome, WireIn, WireOut,
    HEADER_NAME,
};
use rpcstack_tonic::{PolicyHooks, RequestExt, ResponseExt};
use serde::{Deserialize, Serialize};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{CowGrpcMethod, GrpcMethod, Request, Response, Status};

type Server<S> = <PolicyHooks<S> as Hooks>::ServerContext;
type Parent<S> = <PolicyHooks<S> as Hooks>::ParentContext;
type Child<S> = <PolicyHooks<S> as Hooks>::ChildContext;

/// The request a sender would produce, carrying the given wire data.
fn sent(wire: impl FnOnce(&mut WireOut)) -> Request<()> {
    let mut request = Request::new(());
    let mut out = WireOut::new();
    wire(&mut out);
    out.install(request.metadata_mut());
    request
}

/// What the receiving hop's transport hands to `ParentHooks::begin`.
fn arrives(request: &Request<()>) -> http::Request<()> {
    let mut arrived = http::Request::new(());
    if let Some(value) = request.metadata().get(HEADER_NAME) {
        arrived
            .headers_mut()
            .insert(HEADER_NAME, value.to_str().unwrap().parse().unwrap());
    }
    arrived
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
        HEADER_NAME,
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
