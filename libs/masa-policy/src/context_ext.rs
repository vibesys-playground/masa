//! Masa context extension traits and helpers.

use masa_core::Context;

use crate::module::{BudgetModule, Module};
use crate::wire::WireOut;
use rpcstack_tonic::{RequestExt, ResponseExt, StatusExt};
use tonic::{Request, Response, Status};

pub use rpcstack_tonic::{
    get_method_name_override_from_headers, get_method_name_override_from_metadata,
    get_service_name_override_from_headers, get_service_name_override_from_metadata,
    get_wire_from_metadata, set_method_name_override_in_headers,
    set_service_name_override_in_headers, set_wire_in_metadata,
};

/// Internal header key for MASA context.
pub const MASA_CONTEXT_HEADER: &str = masa_core::MASA_CONTEXT_HEADER;

/// Get the MASA context (the budget module's wire data) from metadata.
pub fn get_masa_context_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<Context> {
    get_wire_from_metadata::<BudgetModule>(metadata)
}

/// Set the MASA context in metadata, as the budget module's wire data.
///
/// Module wire data already attached to `metadata` is kept, so the order of
/// attaching the context and the wire data does not matter.
pub fn set_masa_context_in_metadata(metadata: &mut tonic::metadata::MetadataMap, ctx: &Context) {
    set_wire_in_metadata::<BudgetModule>(metadata, ctx);
}

/// The `ctx` header value for `ctx` carrying module `M`'s wire data, for
/// building inbound requests by hand (tests, tools).
pub fn header_string_with_wire<M: Module>(ctx: &Context, data: &M::Wire) -> String {
    let mut out = WireOut::new();
    out.put::<BudgetModule>(ctx)
        .and_then(|()| out.put::<M>(data))
        .unwrap_or_else(|err| panic!("{err}"));
    out.header_value()
}

pub use masa_core::{read_context, read_context_from_headers, read_priority_from_headers};

/// Extension trait for `Request<T>` to set the method name override header.
pub trait MasaRequestExt<T> {
    /// Set the method name override header on this request.
    fn set_method_name_override(&mut self, method_name: &str) -> Result<(), Status>;

    /// Get the method name override header from this request.
    fn get_method_name_override(&self) -> Option<&str>;

    /// Set the service name override header on this request.
    fn set_service_name_override(&mut self, service_name: &str) -> Result<(), Status>;

    /// Get the service name override header from this request.
    fn get_service_name_override(&self) -> Option<&str>;

    /// Set the MASA context for this request.
    fn set_masa_context(&mut self, ctx: &Context);

    /// Set the MASA context for this request (builder style).
    fn with_masa_context(self, ctx: &Context) -> Self;

    /// Get the MASA context from this request.
    fn get_masa_context(&self) -> Option<Context>;

    /// Set module `M`'s wire data. The MASA context must be attached first.
    fn set_wire<M: Module>(&mut self, data: &M::Wire);

    /// Get module `M`'s wire data, if present.
    fn get_wire<M: Module>(&self) -> Option<M::Wire>;
}

impl<T> MasaRequestExt<T> for Request<T> {
    fn set_method_name_override(&mut self, method_name: &str) -> Result<(), Status> {
        RequestExt::set_method_name_override(self, method_name)
    }

    fn get_method_name_override(&self) -> Option<&str> {
        RequestExt::get_method_name_override(self)
    }

    fn set_service_name_override(&mut self, service_name: &str) -> Result<(), Status> {
        RequestExt::set_service_name_override(self, service_name)
    }

    fn get_service_name_override(&self) -> Option<&str> {
        RequestExt::get_service_name_override(self)
    }

    fn set_masa_context(&mut self, ctx: &Context) {
        set_masa_context_in_metadata(self.metadata_mut(), ctx);
    }

    fn with_masa_context(mut self, ctx: &Context) -> Self {
        set_masa_context_in_metadata(self.metadata_mut(), ctx);
        self
    }

    fn get_masa_context(&self) -> Option<Context> {
        get_masa_context_from_metadata(self.metadata())
    }

    fn set_wire<M: Module>(&mut self, data: &M::Wire) {
        RequestExt::set_wire::<M>(self, data);
    }

    fn get_wire<M: Module>(&self) -> Option<M::Wire> {
        RequestExt::get_wire::<M>(self)
    }
}

/// Extension trait for `Response<T>` to manage MASA context.
pub trait MasaResponseExt<T> {
    /// Set the MASA context for this response.
    fn set_masa_context(&mut self, ctx: &Context);

    /// Set the MASA context for this response (builder style).
    fn with_masa_context(self, ctx: &Context) -> Self;

    /// Get the MASA context from this response.
    fn get_masa_context(&self) -> Option<Context>;

    /// Get module `M`'s wire data, if present.
    fn get_wire<M: Module>(&self) -> Option<M::Wire>;

    /// Set module `M`'s wire data. The MASA context must be attached first.
    fn set_wire<M: Module>(&mut self, data: &M::Wire);
}

impl<T> MasaResponseExt<T> for Response<T> {
    fn set_masa_context(&mut self, ctx: &Context) {
        set_masa_context_in_metadata(self.metadata_mut(), ctx);
    }

    fn with_masa_context(mut self, ctx: &Context) -> Self {
        set_masa_context_in_metadata(self.metadata_mut(), ctx);
        self
    }

    fn get_masa_context(&self) -> Option<Context> {
        get_masa_context_from_metadata(self.metadata())
    }

    fn get_wire<M: Module>(&self) -> Option<M::Wire> {
        ResponseExt::get_wire::<M>(self)
    }

    fn set_wire<M: Module>(&mut self, data: &M::Wire) {
        ResponseExt::set_wire::<M>(self, data);
    }
}

/// Extension trait for `Status` to manage MASA context.
pub trait MasaStatusExt {
    /// Set the MASA context for this status.
    fn set_masa_context(&mut self, ctx: &Context);

    /// Set the MASA context for this status (builder style).
    fn with_masa_context(self, ctx: &Context) -> Self;

    /// Get the MASA context from this status.
    fn get_masa_context(&self) -> Option<Context>;

    /// Get module `M`'s wire data, if present.
    fn get_wire<M: Module>(&self) -> Option<M::Wire>;

    /// Set module `M`'s wire data. The MASA context must be attached first.
    fn set_wire<M: Module>(&mut self, data: &M::Wire);
}

impl MasaStatusExt for Status {
    fn set_masa_context(&mut self, ctx: &Context) {
        set_masa_context_in_metadata(self.metadata_mut(), ctx);
    }

    fn with_masa_context(mut self, ctx: &Context) -> Self {
        set_masa_context_in_metadata(self.metadata_mut(), ctx);
        self
    }

    fn get_masa_context(&self) -> Option<Context> {
        get_masa_context_from_metadata(self.metadata())
    }

    fn get_wire<M: Module>(&self) -> Option<M::Wire> {
        StatusExt::get_wire::<M>(self)
    }

    fn set_wire<M: Module>(&mut self, data: &M::Wire) {
        StatusExt::set_wire::<M>(self, data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContextBuilder;

    #[test]
    fn request_extension_round_trips_context() {
        let ctx = ContextBuilder::new("test.Service", 7)
            .slo(100)
            .gateway_entry(10)
            .deadline(110)
            .build();
        let mut request = Request::new(());

        request.set_masa_context(&ctx);

        assert_eq!(
            request
                .get_masa_context()
                .expect("missing context")
                .deadline(),
            ctx.deadline()
        );
    }

    #[test]
    fn response_extension_round_trips_context() {
        let ctx = ContextBuilder::new("test.Service", 8)
            .slo(100)
            .gateway_entry(10)
            .deadline(110)
            .build();
        let response = Response::new(()).with_masa_context(&ctx);

        assert_eq!(
            response
                .get_masa_context()
                .expect("missing context")
                .request_id(),
            ctx.request_id()
        );
    }
}
