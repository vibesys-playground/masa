//! What the estimation module exchanges with other hops, and what it shares
//! with later modules of the same request.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::registry::MethodId;

/// Identifies the root (ingress) RPC method. Transported over the wire as a
/// (service, method) pair so that method identity is stable across replicas.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RootMethod {
    pub service: String,
    pub method: String,
}

/// Estimation's data on the wire, in the `estimation` section.
///
/// A request carries [`EstimationRequestWire`] down the call tree. Estimation
/// decides what a request without this section means: the sender runs no
/// estimation, so the request is treated as arriving at ingress (hop count 0,
/// this method as the root). A section whose hop count is 0 means the same
/// thing; the two differ only on the wire.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EstimationWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<EstimationRequestWire>,
}

impl EstimationWire {
    /// The wire data of a request sent `hop_count` hops below ingress.
    pub fn request(hop_count: u8, root_method: Option<RootMethod>) -> Self {
        Self {
            request: Some(EstimationRequestWire {
                hop_count,
                root_method,
            }),
        }
    }
}

/// The request half of [`EstimationWire`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EstimationRequestWire {
    /// Hops between ingress and the receiver. Ingress is 0, a real value.
    pub hop_count: u8,
    /// The ingress RPC method, set by the ingress hop and passed on unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_method: Option<RootMethod>,
}

/// Facts about one request that estimation publishes in
/// [`Extensions`](crate::Extensions) when the request begins, for modules
/// later in the stack. A module that reads it must come after estimation; a
/// stack that puts it earlier fails at construction, because the module's
/// server requires [`PublishesEstimationInfo`].
#[derive(Debug, Clone)]
pub struct EstimationInfo {
    pub(crate) hop_count: u8,
    pub(crate) root_method: Option<Arc<RootMethod>>,
    pub(crate) root_method_id: Option<MethodId>,
}

impl EstimationInfo {
    /// Hops between ingress and this service; 0 at ingress.
    pub fn hop_count(&self) -> u8 {
        self.hop_count
    }

    /// Whether this hop is the ingress of its request tree.
    pub fn is_ingress(&self) -> bool {
        self.hop_count == 0
    }

    /// The ingress RPC method, if known.
    pub fn root_method(&self) -> Option<&RootMethod> {
        self.root_method.as_deref()
    }

    /// The registry id of [`root_method`](Self::root_method).
    pub fn root_method_id(&self) -> Option<MethodId> {
        self.root_method_id
    }
}

#[cfg(all(test, feature = "ac_pred"))]
impl EstimationInfo {
    /// An ingress hop whose root method is not known.
    pub(crate) fn ingress() -> Self {
        Self {
            hop_count: 0,
            root_method: None,
            root_method_id: None,
        }
    }
}

/// Server-level marker that estimation publishes [`EstimationInfo`] for every
/// request. Modules that need the info require it in their
/// [`LayerServer::new`](crate::LayerServer::new).
#[derive(Debug, Clone, Copy)]
pub struct PublishesEstimationInfo;
