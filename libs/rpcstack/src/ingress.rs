//! The ingress decision: what happens to a request before the task that serves
//! it is queued, decided from the inbound wire and the modules' server state.

use rpcstack_sched::Meta;
use smallvec::SmallVec;
use tonic::Status;

use crate::extensions::Proposal;
use crate::module::Rejection;
use crate::wire::WireOut;

/// What modules decide about a request in [`Module::ingress`](crate::Module::ingress),
/// before the task that serves it is queued.
///
/// Two decisions, both following the pattern of the other decision points:
///
/// - The task's [`Meta`]: modules [`propose`](Self::propose) it, the framework
///   records each proposal in the order made with the proposing module's name,
///   and the one module that owns the decision
///   ([`Module::OWNS_INGRESS`](crate::Module::OWNS_INGRESS)) settles it with its
///   own rule in [`Module::resolve_ingress`](crate::Module::resolve_ingress).
///   The framework has no rule and gives no meaning to the `Meta`.
/// - Whether the request is turned away: a module [`reject`](Self::reject)s it
///   with a status. The framework records which module did and does not run the
///   modules after it, as it does when a `before_poll` returns an error. A
///   module that rejects can also report to the sender, with sections it puts
///   in [`wire_mut`](Self::wire_mut).
///
/// A request turned away here has no per-request state, so no module sees it
/// again: no `after_poll`, no `finalize`. A module that wants statistics on
/// rejected requests counts them in `ingress`.
///
/// This holds only these things, stored inline, so recording a proposal costs
/// no allocation and a request that is admitted allocates nothing: ingress runs
/// for every request stream on the connection's hot path.
#[derive(Debug, Default)]
pub struct Ingress {
    proposals: SmallVec<[Proposal<Meta>; 2]>,
    rejection: Option<Rejection>,
    wire: WireOut,
    /// The module whose hook is running, set by the framework.
    module: &'static str,
}

impl Ingress {
    /// Propose `meta` as the task's scheduling metadata, attributed to the
    /// module whose hook is running.
    #[inline]
    pub fn propose(&mut self, meta: Meta) {
        self.proposals.push(Proposal {
            by: self.module,
            value: meta,
        });
    }

    /// Turn the request away with `status`, attributed to the module whose
    /// hook is running. The modules after it do not run their `ingress`. A
    /// second rejection in the same hook does not replace the first.
    pub fn reject(&mut self, status: Status) {
        if self.rejection.is_none() {
            self.rejection = Some(Rejection {
                by: self.module,
                status,
            });
        }
    }

    /// The wire sections that become the rejection's response metadata; put
    /// this module's own with `wire_mut().put::<Self>(..)`. Ignored for a
    /// request that is admitted.
    pub fn wire_mut(&mut self) -> &mut WireOut {
        &mut self.wire
    }

    /// The proposals made so far, in the order they were made.
    pub fn proposals(&self) -> &[Proposal<Meta>] {
        &self.proposals
    }

    /// The rejection, if a module made one.
    pub fn rejection(&self) -> Option<&Rejection> {
        self.rejection.as_ref()
    }

    /// The wire sections modules put for a rejection.
    pub fn wire(&self) -> &WireOut {
        &self.wire
    }

    #[inline]
    pub(crate) fn set_module(&mut self, module: &'static str) {
        self.module = module;
    }
}
