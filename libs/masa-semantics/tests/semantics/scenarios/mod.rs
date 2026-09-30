//! Pure-semantics scenarios. Each one states the rule it pins and is written
//! only against the harness API.

/// Declare a scenario: a function over a virtual world, plus a test that runs
/// it against the hooks under test. Doc comments and `cfg` attributes carry
/// over to the test.
macro_rules! scenario {
    ($(#[$meta:meta])* fn $name:ident($w:ident) $body:block) => {
        $(#[$meta])*
        mod $name {
            #[allow(unused_imports)]
            use super::*;

            fn scenario<H: $crate::harness::Hooks>($w: &mut $crate::harness::World<H>) $body

            #[test]
            fn run() {
                $crate::harness::run::<$crate::harness::Under, _>(
                    scenario::<$crate::harness::Under>,
                )
            }
        }
    };
}

mod priority;

#[cfg(feature = "abort_slo")]
mod abort_slo;
