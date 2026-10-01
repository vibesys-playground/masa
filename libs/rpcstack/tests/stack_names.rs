// `Module::NAME` identifies a module in outcomes, proposals and wire sections,
// so a stack must not hold two modules of the same name, whether or not they
// have wire data. The empty module `()` fills disabled slots and is exempt.

use std::panic::catch_unwind;

use rpcstack::{build_server, policy_stack, Extensions, Module, WireIn};
use tonic::CowGrpcMethod;

macro_rules! module {
    ($ty:ident, $name:literal, $wire:ty) => {
        #[derive(Debug)]
        struct $ty;

        impl Module for $ty {
            type Server = ();
            const NAME: &'static str = $name;
            type Wire = $wire;

            fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _e: &mut Extensions) -> Self {
                Self
            }
        }
    };
}

module!(First, "shared", ());
module!(Second, "shared", ());
module!(WithWire, "shared", u8);
module!(Other, "other", ());
module!(BadName, "has.dot", u8);
module!(UnwiredBadName, "has.dot", ());

fn panic_message<S: rpcstack::ModuleStack>() -> String {
    let payload = catch_unwind(|| build_server::<S>("svc")).expect_err("the stack is invalid");
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .expect("the panic carries a message")
}

#[test]
fn two_modules_without_wire_data_cannot_share_a_name() {
    let message = panic_message::<policy_stack![First, Other, Second]>();
    assert!(message.contains("share the name `shared`"), "{message}");
    assert!(
        message.contains("First") && message.contains("Second"),
        "{message}"
    );
}

#[test]
fn a_module_with_wire_data_cannot_share_a_name_with_one_without() {
    let message = panic_message::<policy_stack![First, WithWire]>();
    assert!(
        message.contains("First") && message.contains("WithWire"),
        "{message}"
    );
}

#[test]
fn the_same_module_twice_is_a_duplicate() {
    let message = panic_message::<policy_stack![First, First]>();
    assert!(message.contains("share the name `shared`"), "{message}");
}

#[test]
fn distinct_names_and_repeated_empty_slots_are_accepted() {
    assert!(build_server::<policy_stack![(), First, (), Other, ()]>("svc").is_ok());
}

#[test]
fn only_a_module_with_wire_data_needs_a_name_that_fits_a_section() {
    let message = panic_message::<policy_stack![BadName]>();
    assert!(
        message.contains("must be non-empty and contain only"),
        "{message}"
    );
    assert!(build_server::<policy_stack![UnwiredBadName]>("svc").is_ok());
}
