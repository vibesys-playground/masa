//! Generic server implementation.
//!
//! This module contains the low level components to build a gRPC server. It
//! provides a codec agnostic gRPC server handler.
//!
//! The items in this module are generally designed to be used by some codegen
//! tool that will provide the user some custom way to implement the server that
//! will implement the proper gRPC service. Thusly, they are a bit hard to use
//! by hand.

mod grpc;
mod service;

pub use self::grpc::Grpc;
pub use self::service::{
    ClientStreamingService, ServerStreamingService, StreamingService, UnaryService,
};

/// A trait to provide a static reference to the service's
/// name. This is used for routing service's within the router.
pub trait NamedService {
    /// The `Service-Name` as described [here].
    ///
    /// [here]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests
    const NAME: &'static str;

    /// How a server decides the scheduling `Meta` of the task serving each
    /// HTTP/2 stream, before it is queued; see
    /// [`Hooks::ingress`](crate::masa::Hooks::ingress). Generated services
    /// supply their hooks type's function. A service that wraps another
    /// forwards it; the default is no decision.
    const INGRESS: Option<crate::masa::IngressFn> = None;
}
