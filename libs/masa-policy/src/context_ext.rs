//! Masa context extension traits and helpers.

use masa_core::Context;

use crate::module::{BudgetModule, Module};
use crate::wire::{self, WireOut};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Request, Response, Status};

/// Internal header key for MASA context.
pub const MASA_CONTEXT_HEADER: &str = masa_core::MASA_CONTEXT_HEADER;

const METHOD_NAME_OVERRIDE_HEADER: &str = "x-masa-method-name";
const SERVICE_NAME_OVERRIDE_HEADER: &str = "x-masa-service-name";

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

/// Get module `M`'s wire data from metadata, if present. Panics on malformed
/// wire data, like a malformed context.
pub fn get_wire_from_metadata<M: Module>(
    metadata: &tonic::metadata::MetadataMap,
) -> Option<M::Wire> {
    wire::get_from_metadata::<M>(metadata).unwrap_or_else(|err| panic!("{err}"))
}

/// Set module `M`'s wire data in metadata that already carries a context,
/// keeping other modules' data.
pub fn set_wire_in_metadata<M: Module>(
    metadata: &mut tonic::metadata::MetadataMap,
    data: &M::Wire,
) {
    wire::set_in_metadata::<M>(metadata, data).unwrap_or_else(|err| panic!("{err}"));
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

fn get_ascii_metadata<'a>(
    metadata: &'a tonic::metadata::MetadataMap,
    key: &str,
) -> Option<&'a str> {
    metadata.get(key).and_then(|value| value.to_str().ok())
}

fn get_ascii_header<'a>(headers: &'a http::HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key).and_then(|value| value.to_str().ok())
}

/// Get the logical method-name override from metadata.
pub fn get_method_name_override_from_metadata(
    metadata: &tonic::metadata::MetadataMap,
) -> Option<&str> {
    get_ascii_metadata(metadata, METHOD_NAME_OVERRIDE_HEADER)
}

/// Get the logical service-name override from metadata.
pub fn get_service_name_override_from_metadata(
    metadata: &tonic::metadata::MetadataMap,
) -> Option<&str> {
    get_ascii_metadata(metadata, SERVICE_NAME_OVERRIDE_HEADER)
}

/// Get the logical method-name override from HTTP headers.
pub fn get_method_name_override_from_headers(headers: &http::HeaderMap) -> Option<&str> {
    get_ascii_header(headers, METHOD_NAME_OVERRIDE_HEADER)
}

/// Get the logical service-name override from HTTP headers.
pub fn get_service_name_override_from_headers(headers: &http::HeaderMap) -> Option<&str> {
    get_ascii_header(headers, SERVICE_NAME_OVERRIDE_HEADER)
}

/// Set the logical method-name override in HTTP headers.
pub fn set_method_name_override_in_headers(
    headers: &mut http::HeaderMap,
    method_name: &str,
) -> Result<(), http::header::InvalidHeaderValue> {
    let value = http::HeaderValue::from_str(method_name)?;
    headers.insert(METHOD_NAME_OVERRIDE_HEADER, value);
    Ok(())
}

/// Set the logical service-name override in HTTP headers.
pub fn set_service_name_override_in_headers(
    headers: &mut http::HeaderMap,
    service_name: &str,
) -> Result<(), http::header::InvalidHeaderValue> {
    let value = http::HeaderValue::from_str(service_name)?;
    headers.insert(SERVICE_NAME_OVERRIDE_HEADER, value);
    Ok(())
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
        let value = MetadataValue::<Ascii>::try_from(method_name).map_err(|e| {
            Status::internal(format!(
                "Failed to create metadata value for method name override: {:?}",
                e
            ))
        })?;
        self.metadata_mut()
            .insert(METHOD_NAME_OVERRIDE_HEADER, value);
        Ok(())
    }

    fn get_method_name_override(&self) -> Option<&str> {
        get_method_name_override_from_metadata(self.metadata())
    }

    fn set_service_name_override(&mut self, service_name: &str) -> Result<(), Status> {
        let value = MetadataValue::<Ascii>::try_from(service_name).map_err(|e| {
            Status::internal(format!(
                "Failed to create metadata value for service name override: {:?}",
                e
            ))
        })?;
        self.metadata_mut()
            .insert(SERVICE_NAME_OVERRIDE_HEADER, value);
        Ok(())
    }

    fn get_service_name_override(&self) -> Option<&str> {
        get_service_name_override_from_metadata(self.metadata())
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
        set_wire_in_metadata::<M>(self.metadata_mut(), data);
    }

    fn get_wire<M: Module>(&self) -> Option<M::Wire> {
        get_wire_from_metadata::<M>(self.metadata())
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
        get_wire_from_metadata::<M>(self.metadata())
    }

    fn set_wire<M: Module>(&mut self, data: &M::Wire) {
        set_wire_in_metadata::<M>(self.metadata_mut(), data);
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
        get_wire_from_metadata::<M>(self.metadata())
    }

    fn set_wire<M: Module>(&mut self, data: &M::Wire) {
        set_wire_in_metadata::<M>(self.metadata_mut(), data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContextBuilder;

    #[test]
    fn method_override_helpers_round_trip_metadata_and_headers() {
        let mut request = Request::new(());

        request.set_method_name_override("test_method").unwrap();
        request.set_service_name_override("test.Service").unwrap();

        assert_eq!(request.get_method_name_override(), Some("test_method"));
        assert_eq!(request.get_service_name_override(), Some("test.Service"));
        assert_eq!(
            get_method_name_override_from_metadata(request.metadata()),
            Some("test_method")
        );
        assert_eq!(
            get_service_name_override_from_metadata(request.metadata()),
            Some("test.Service")
        );

        let mut headers = http::HeaderMap::new();
        set_method_name_override_in_headers(&mut headers, "test_method").unwrap();
        set_service_name_override_in_headers(&mut headers, "test.Service").unwrap();

        assert_eq!(
            get_method_name_override_from_headers(&headers),
            Some("test_method")
        );
        assert_eq!(
            get_service_name_override_from_headers(&headers),
            Some("test.Service")
        );
    }

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
