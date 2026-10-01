//! Dispatch of an HTTP/2 stream to its service's ingress decision.

use std::sync::Arc;

use hyper::rt::{StreamIngress, StreamVerdict};
use hyper::Body;
use tokio::task::meta_for_unannotated_spawn;

use crate::masa::{Ingress, ServiceIngress};

/// The ingress decisions of a server's services, found by the service name in
/// the request path (`/package.Service/Method`).
///
/// A request for a service without a decision (one that supplies none, or an
/// unknown path) is spawned with the runtime's default for unannotated spawns,
/// as it is without a table.
pub(crate) struct IngressTable {
    // A server has a handful of services, so a scan beats hashing.
    services: Vec<(&'static str, Arc<dyn ServiceIngress>)>,
}

impl IngressTable {
    pub(crate) fn new(services: Vec<(&'static str, Arc<dyn ServiceIngress>)>) -> Self {
        Self { services }
    }

    fn service_named(&self, path: &str) -> Option<&dyn ServiceIngress> {
        let name = path.strip_prefix('/')?.split('/').next()?;
        self.services
            .iter()
            .find(|(service, _)| *service == name)
            .map(|(_, handler)| &**handler)
    }
}

impl StreamIngress for IngressTable {
    fn ingress(&self, req: &http::Request<Body>) -> StreamVerdict {
        let path = req.uri().path();
        match self
            .service_named(path)
            .and_then(|service| service.ingress(path, req.headers()))
        {
            Some(Ingress::Admit(meta)) => StreamVerdict::Spawn(meta),
            Some(Ingress::Reject(status)) => StreamVerdict::Reject(status.to_http().into_parts().0),
            None => StreamVerdict::Spawn(meta_for_unannotated_spawn(None)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Code, Status};

    struct Fixed(Option<fn() -> Ingress>);

    impl ServiceIngress for Fixed {
        fn ingress(&self, _path: &str, _headers: &http::HeaderMap) -> Option<Ingress> {
            self.0.map(|decide| decide())
        }
    }

    fn table() -> IngressTable {
        IngressTable::new(vec![
            (
                "pkg.Admits",
                Arc::new(Fixed(Some(|| Ingress::Admit(crate::masa::Meta::new(7))))),
            ),
            (
                "pkg.Rejects",
                Arc::new(Fixed(Some(|| {
                    Ingress::Reject(Status::resource_exhausted("full"))
                }))),
            ),
            ("pkg.Undecided", Arc::new(Fixed(None))),
        ])
    }

    fn verdict(path: &str) -> StreamVerdict {
        let req = http::Request::builder()
            .uri(path)
            .body(Body::empty())
            .unwrap();
        table().ingress(&req)
    }

    #[test]
    fn a_path_is_dispatched_to_the_service_it_names() {
        assert!(matches!(
            verdict("/pkg.Admits/Rpc"),
            StreamVerdict::Spawn(meta) if meta == crate::masa::Meta::new(7)
        ));
        match verdict("/pkg.Rejects/Rpc") {
            StreamVerdict::Reject(head) => {
                assert_eq!(head.status, 200);
                assert_eq!(
                    head.headers.get("grpc-status").unwrap(),
                    &(Code::ResourceExhausted as i32).to_string()
                );
                assert_eq!(
                    head.headers.get("content-type").unwrap(),
                    "application/grpc"
                );
            }
            StreamVerdict::Spawn(_) => panic!("the stream was spawned"),
        }
    }

    #[test]
    fn an_undecided_or_unknown_path_gets_the_default() {
        for path in [
            "/pkg.Undecided/Rpc",
            "/pkg.Unknown/Rpc",
            "/",
            "",
            "no-slash",
        ] {
            assert!(
                matches!(
                    verdict(path),
                    StreamVerdict::Spawn(meta) if meta == meta_for_unannotated_spawn(None)
                ),
                "{path}"
            );
        }
    }

    #[test]
    fn a_service_name_must_match_exactly() {
        assert!(matches!(
            verdict("/pkg.Admit/Rpc"),
            StreamVerdict::Spawn(meta) if meta == meta_for_unannotated_spawn(None)
        ));
    }
}
