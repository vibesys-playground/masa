//! What the estimation module exchanges with other hops, and what it shares
//! with later modules of the same request.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::state::RequestMetadataTracker;
use crate::registry::MethodId;

// The wire types below are encoded as JSON arrays rather than objects:
// estimation's sections are built and parsed on every RPC, and dropping the
// field names shrinks each section to a third of its size. The array layouts
// are the `*Compact` types.

/// Identifies the root (ingress) RPC method. Transported over the wire as a
/// (service, method) pair so that method identity is stable across replicas.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "RootMethodCompact", into = "RootMethodCompact")]
pub struct RootMethod {
    pub service: String,
    pub method: String,
}

#[derive(Serialize, Deserialize)]
struct RootMethodCompact(String, String);

impl From<RootMethod> for RootMethodCompact {
    fn from(root: RootMethod) -> Self {
        Self(root.service, root.method)
    }
}

impl From<RootMethodCompact> for RootMethod {
    fn from(root: RootMethodCompact) -> Self {
        Self {
            service: root.0,
            method: root.1,
        }
    }
}

/// Estimation's data on the wire, in the `estimation` section.
///
/// A request carries [`EstimationRequestWire`] down the call tree and a
/// response carries [`EstimationResponseWire`] back up; each message has only
/// its own half.
///
/// Estimation decides what a missing section means. On a request, the sender
/// runs no estimation, so the request is treated as arriving at ingress (hop
/// count 0, this method as the root); a section whose hop count is 0 means the
/// same thing, and the two differ only on the wire. On a response, the child
/// reported nothing, so it contributes nothing to its parent's totals.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EstimationWire {
    #[serde(rename = "q", default, skip_serializing_if = "Option::is_none")]
    pub request: Option<EstimationRequestWire>,
    #[serde(rename = "r", default, skip_serializing_if = "Option::is_none")]
    pub response: Option<EstimationResponseWire>,
}

impl EstimationWire {
    /// The wire data of a request sent `hop_count` hops below ingress.
    pub fn request(hop_count: u8, root_method: Option<RootMethod>) -> Self {
        Self {
            request: Some(EstimationRequestWire {
                hop_count,
                root_method,
            }),
            response: None,
        }
    }

    /// The wire data of a response reporting `response`.
    pub fn response(response: EstimationResponseWire) -> Self {
        Self {
            request: None,
            response: Some(response),
        }
    }
}

/// The request half of [`EstimationWire`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "RequestCompact", into = "RequestCompact")]
pub struct EstimationRequestWire {
    /// Hops between ingress and the receiver. Ingress is 0, a real value.
    pub hop_count: u8,
    /// The ingress RPC method, set by the ingress hop and passed on unchanged.
    pub root_method: Option<RootMethod>,
}

#[derive(Serialize, Deserialize)]
struct RequestCompact(u8, Option<RootMethod>);

impl From<EstimationRequestWire> for RequestCompact {
    fn from(request: EstimationRequestWire) -> Self {
        Self(request.hop_count, request.root_method)
    }
}

impl From<RequestCompact> for EstimationRequestWire {
    fn from(request: RequestCompact) -> Self {
        Self {
            hop_count: request.0,
            root_method: request.1,
        }
    }
}

/// The response half of [`EstimationWire`]: what a hop and everything below it
/// spent on the request.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(from = "ResponseCompact", into = "ResponseCompact")]
pub struct EstimationResponseWire {
    /// CPU time this hop spent polling the handler.
    pub compute_time_us: u64,
    /// CPU time of this hop and its whole subtree.
    pub accumulated_compute_us: u64,
    /// This hop's CPU utilization when it responded.
    pub utilization: f32,
    /// The highest utilization at this hop or any descendant.
    pub max_downstream_util: f32,
    /// Number of early returns in the subtree (this hop + all children).
    pub early_return_count: u32,
    /// Whether any hop in the subtree tripped its local deadline under
    /// `signal_slack`. Saturated at 1 so a single user-facing request never
    /// counts as several events, however many hops it traversed; consumers
    /// treat it as a boolean (`> 0`).
    pub deadline_signal_count: u32,
}

#[derive(Serialize, Deserialize)]
struct ResponseCompact(u64, u64, f32, f32, u32, u32);

impl From<EstimationResponseWire> for ResponseCompact {
    fn from(response: EstimationResponseWire) -> Self {
        Self(
            response.compute_time_us,
            response.accumulated_compute_us,
            response.utilization,
            response.max_downstream_util,
            response.early_return_count,
            response.deadline_signal_count,
        )
    }
}

impl From<ResponseCompact> for EstimationResponseWire {
    fn from(response: ResponseCompact) -> Self {
        Self {
            compute_time_us: response.0,
            accumulated_compute_us: response.1,
            utilization: response.2,
            max_downstream_util: response.3,
            early_return_count: response.4,
            deadline_signal_count: response.5,
        }
    }
}

/// Facts about one request that estimation publishes in
/// [`Extensions`](crate::Extensions) when the request begins, for modules
/// later in the stack. A module that reads it declares
/// [`EstimationLayer`](super::EstimationLayer) in
/// [`Layer::requires`](crate::Layer::requires), so a stack that puts it earlier
/// fails at construction.
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
    /// A hop `hop_count` hops below ingress with the given root, for tests.
    pub(crate) fn for_test(hop_count: u8, root_method_id: Option<MethodId>) -> Self {
        Self {
            hop_count,
            root_method: None,
            root_method_id,
        }
    }
}

/// What estimation has recorded so far about the request's subtree: whether
/// any child returned early and whether any hop, this one or below, tripped
/// its local deadline under `signal_slack`.
///
/// Estimation keeps the underlying tally in [`Extensions`](crate::Extensions)
/// and updates it as polls and child responses arrive, so a snapshot taken in
/// any hook reflects what has happened up to then, and a snapshot taken in a
/// `finalize` is complete whichever module finalizes first. It does not
/// include this hop's own rejection: that is the [`Outcome`](crate::Outcome)
/// and the result `finalize` receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubtreeHealth {
    /// A child returned early, or reported an early return below it.
    pub early_return: bool,
    /// This hop or a descendant tripped its local deadline under
    /// `signal_slack`.
    pub deadline_signal: bool,
}

impl SubtreeHealth {
    /// A snapshot of the request's subtree. Panics if estimation is missing; a
    /// module that declares [`EstimationLayer`](super::EstimationLayer) in
    /// [`Layer::requires`](crate::Layer::requires) cannot hit that.
    pub fn of(ext: &crate::Extensions) -> Self {
        ext.get::<RequestMetadataTracker>()
            .unwrap_or_else(|| {
                panic!(
                    "estimation's tally is missing; put `EstimationLayer` before the module that \
                     reads `SubtreeHealth`"
                )
            })
            .subtree_health()
    }

    /// Whether the subtree saw an early return or a deadline signal.
    pub fn any(self) -> bool {
        self.early_return || self.deadline_signal
    }
}
