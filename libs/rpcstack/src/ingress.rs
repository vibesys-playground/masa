//! The ingress decision: the scheduling metadata of the task that serves a
//! request, decided before the task is queued.

use smallvec::SmallVec;
use tonic::masa::Meta;

use crate::extensions::Proposal;

/// The [`Meta`] proposals modules make for the task that will serve a request,
/// in [`Module::ingress`](crate::Module::ingress).
///
/// The same pattern as the other decision points: modules propose, the framework
/// records each proposal in the order made with the proposing module's name, and
/// the one module that owns the decision
/// ([`Module::OWNS_INGRESS`](crate::Module::OWNS_INGRESS)) settles it with its
/// own rule in [`Module::resolve_ingress`](crate::Module::resolve_ingress). The
/// framework has no rule and gives no meaning to the `Meta`.
///
/// Unlike [`Extensions`](crate::Extensions) this holds nothing but `Meta`
/// proposals, stored inline, so recording one costs no allocation: ingress runs
/// for every request stream on the connection's hot path.
#[derive(Debug, Default)]
pub struct Ingress {
    proposals: SmallVec<[Proposal<Meta>; 2]>,
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

    /// The proposals made so far, in the order they were made.
    pub fn proposals(&self) -> &[Proposal<Meta>] {
        &self.proposals
    }

    #[inline]
    pub(crate) fn set_module(&mut self, module: &'static str) {
        self.module = module;
    }
}
