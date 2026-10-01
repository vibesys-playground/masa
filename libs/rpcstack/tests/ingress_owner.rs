// The ingress decision has exactly one owner: a stack with two modules that
// set `Module::OWNS_INGRESS` is misconfigured, and says so when the server
// state is built. Stacks with one owner or none are fine.

use std::panic::catch_unwind;

use rpcstack::{build_server, policy_stack, Extensions, Module, WireIn};
use tonic::CowGrpcMethod;

macro_rules! module {
    ($ty:ident, $name:literal, $owns:literal) => {
        #[derive(Debug)]
        struct $ty;

        impl Module for $ty {
            type Server = ();
            const NAME: &'static str = $name;
            type Wire = ();
            const OWNS_INGRESS: bool = $owns;

            fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
                Self
            }
        }
    };
}

module!(OwnerA, "owner-a", true);
module!(OwnerB, "owner-b", true);
module!(Plain, "plain", false);

#[test]
fn two_owners_are_rejected_when_the_server_state_is_built() {
    let payload = catch_unwind(|| build_server::<policy_stack![OwnerA, Plain, OwnerB]>("svc"))
        .expect_err("two modules own the ingress decision");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .expect("the panic carries a message");
    assert!(
        message.contains("OwnerA") && message.contains("OwnerB"),
        "{message}"
    );
    assert!(message.contains("ingress"), "{message}");
}

#[test]
fn one_owner_or_none_is_accepted() {
    assert!(build_server::<policy_stack![Plain, OwnerA]>("svc").is_ok());
    assert!(build_server::<policy_stack![Plain]>("svc").is_ok());
    assert!(build_server::<policy_stack![]>("svc").is_ok());
}
