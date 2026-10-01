//! Helpers for reading and writing module wire data and method-name overrides
//! on tonic requests, responses and statuses.

use rpcstack::{wire, Module};
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};

/// Header carrying the logical method name of a call, when it differs from the
/// one in the request path.
pub const METHOD_NAME_OVERRIDE_HEADER: &str = "x-rpcstack-method-name";

/// Header carrying the logical service name of a call, when it differs from
/// the one in the request path.
pub const SERVICE_NAME_OVERRIDE_HEADER: &str = "x-rpcstack-service-name";

/// Get module `M`'s wire data from metadata, if present. Panics on malformed
/// wire data, like a malformed header.
pub fn get_wire_from_metadata<M: Module>(metadata: &MetadataMap) -> Option<M::Wire> {
    wire::get_from_metadata::<M>(metadata).unwrap_or_else(|err| panic!("{err}"))
}

/// Set module `M`'s wire data in metadata, keeping other modules' data.
pub fn set_wire_in_metadata<M: Module>(metadata: &mut MetadataMap, data: &M::Wire) {
    wire::set_in_metadata::<M>(metadata, data).unwrap_or_else(|err| panic!("{err}"));
}

fn get_ascii_metadata<'a>(metadata: &'a MetadataMap, key: &str) -> Option<&'a str> {
    metadata.get(key).and_then(|value| value.to_str().ok())
}

fn get_ascii_header<'a>(headers: &'a http::HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key).and_then(|value| value.to_str().ok())
}

/// Get the logical method-name override from metadata.
pub fn get_method_name_override_from_metadata(metadata: &MetadataMap) -> Option<&str> {
    get_ascii_metadata(metadata, METHOD_NAME_OVERRIDE_HEADER)
}

/// Get the logical service-name override from metadata.
pub fn get_service_name_override_from_metadata(metadata: &MetadataMap) -> Option<&str> {
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

/// Extension trait for `Request<T>`: method-name overrides and module wire data.
pub trait RequestExt<T> {
    /// Set the method name override header on this request.
    fn set_method_name_override(&mut self, method_name: &str) -> Result<(), Status>;

    /// Get the method name override header from this request.
    fn get_method_name_override(&self) -> Option<&str>;

    /// Set the service name override header on this request.
    fn set_service_name_override(&mut self, service_name: &str) -> Result<(), Status>;

    /// Get the service name override header from this request.
    fn get_service_name_override(&self) -> Option<&str>;

    /// Set module `M`'s wire data, keeping other modules' data.
    fn set_wire<M: Module>(&mut self, data: &M::Wire);

    /// Get module `M`'s wire data, if present.
    fn get_wire<M: Module>(&self) -> Option<M::Wire>;
}

impl<T> RequestExt<T> for Request<T> {
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

    fn set_wire<M: Module>(&mut self, data: &M::Wire) {
        set_wire_in_metadata::<M>(self.metadata_mut(), data);
    }

    fn get_wire<M: Module>(&self) -> Option<M::Wire> {
        get_wire_from_metadata::<M>(self.metadata())
    }
}

/// Extension trait for `Response<T>`: module wire data.
pub trait ResponseExt<T> {
    /// Get module `M`'s wire data, if present.
    fn get_wire<M: Module>(&self) -> Option<M::Wire>;

    /// Set module `M`'s wire data, keeping other modules' data.
    fn set_wire<M: Module>(&mut self, data: &M::Wire);
}

impl<T> ResponseExt<T> for Response<T> {
    fn get_wire<M: Module>(&self) -> Option<M::Wire> {
        get_wire_from_metadata::<M>(self.metadata())
    }

    fn set_wire<M: Module>(&mut self, data: &M::Wire) {
        set_wire_in_metadata::<M>(self.metadata_mut(), data);
    }
}

/// Extension trait for `Status`: module wire data.
pub trait StatusExt {
    /// Get module `M`'s wire data, if present.
    fn get_wire<M: Module>(&self) -> Option<M::Wire>;

    /// Set module `M`'s wire data, keeping other modules' data.
    fn set_wire<M: Module>(&mut self, data: &M::Wire);
}

impl StatusExt for Status {
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
}
