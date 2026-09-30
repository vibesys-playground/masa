//! Built-in run queues.
//!
//! These read a [`Meta`](crate::Meta) as a priority and treat
//! [`default_policy::INFRA`](crate::default_policy::INFRA) specially, so they
//! embody policy. The runtime never depends on them directly; it goes through
//! [`RunQueue`](crate::RunQueue).

pub mod fifo;
pub(crate) mod keyed;
pub mod prio_heap;
pub mod tailclipper;
