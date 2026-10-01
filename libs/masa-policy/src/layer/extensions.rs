//! Typed maps through which the modules of a stack share data.

use std::any::{Any, TypeId};
use std::ops::{Deref, DerefMut};

use smallvec::SmallVec;

/// A map holding at most one value of each type, through which the modules of
/// a stack share data without the framework having a field for it (the same
/// idea as `http::Extensions`).
///
/// The framework creates one empty map when a request begins and hands it to
/// every hook of that request: mutably to [`Layer::new`](super::Layer::new),
/// [`Layer::before_poll`](super::Layer::before_poll),
/// [`Layer::before_child_rpc`](super::Layer::before_child_rpc) and
/// [`Layer::seal_child_rpc`](super::Layer::seal_child_rpc), shared to the
/// other hooks. It never inserts, reads or interprets a value. The key is the
/// value's type, so a module that wants its data private should define a
/// private type for it; data meant for other modules is published by making
/// its type public.
///
/// A value inserted by a module in `new` is visible to modules later in the
/// stack during `new`, and to every module in later hooks. Values are looked
/// up by a linear scan: a request holds a handful of them, which is cheaper
/// than hashing.
#[derive(Debug, Default)]
pub struct Extensions {
    values: SmallVec<[(TypeId, Box<dyn Any + Send + Sync>); 4]>,
    /// The module whose hook is running, set by the framework before it calls
    /// a hook that takes the map mutably; empty outside a stack.
    module: &'static str,
}

impl Extensions {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }

    fn position<T: Any>(&self) -> Option<usize> {
        let id = TypeId::of::<T>();
        self.values.iter().position(|(key, _)| *key == id)
    }

    /// Store `value`, returning the value of the same type it replaces.
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) -> Option<T> {
        match self.position::<T>() {
            Some(at) => {
                let old = std::mem::replace(&mut self.values[at].1, Box::new(value));
                old.downcast().ok().map(|old| *old)
            }
            None => {
                self.values.push((TypeId::of::<T>(), Box::new(value)));
                None
            }
        }
    }

    /// The stored value of type `T`.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        let at = self.position::<T>()?;
        self.values[at].1.downcast_ref()
    }

    /// The stored value of type `T`, mutably.
    pub fn get_mut<T: Any + Send + Sync>(&mut self) -> Option<&mut T> {
        let at = self.position::<T>()?;
        self.values[at].1.downcast_mut()
    }

    /// Remove and return the stored value of type `T`.
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<T> {
        let at = self.position::<T>()?;
        self.values
            .swap_remove(at)
            .1
            .downcast()
            .ok()
            .map(|old| *old)
    }
}

/// A module's proposal for a decision point, with the module that made it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Proposal<T> {
    /// [`Layer::NAME`](super::Layer::NAME) of the proposing module, recorded
    /// by the framework.
    pub by: &'static str,
    pub value: T,
}

/// The proposals made for one decision, in the order they were made. A
/// decision usually has one or two, which are stored inline.
#[derive(Debug)]
pub struct Proposals<T>(SmallVec<[Proposal<T>; 2]>);

impl<T> Deref for Proposals<T> {
    type Target = [Proposal<T>];

    fn deref(&self) -> &[Proposal<T>] {
        &self.0
    }
}

impl<T> IntoIterator for Proposals<T> {
    type Item = Proposal<T>;
    type IntoIter = smallvec::IntoIter<[Proposal<T>; 2]>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// The proposals made so far for decision type `T`, and whether the owner has
/// resolved them.
#[derive(Debug)]
struct Decision<T> {
    proposals: Proposals<T>,
    resolved_by: Option<&'static str>,
}

/// A module proposed a value after the module that owns the decision had
/// resolved it, so the proposal could not count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionClosed {
    decision: &'static str,
    proposer: &'static str,
    resolver: &'static str,
}

impl DecisionClosed {
    /// Type name of the decision.
    pub fn decision(&self) -> &'static str {
        self.decision
    }

    /// Module that made the late proposal.
    pub fn proposer(&self) -> &'static str {
        self.proposer
    }

    /// Module that had resolved the decision.
    pub fn resolver(&self) -> &'static str {
        self.resolver
    }
}

impl std::fmt::Display for DecisionClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "module `{}` proposed a `{}` after module `{}` had resolved it; proposals must be \
             made before the owner's `seal_child_rpc`, which runs in reverse stack order, so a \
             proposer must come after its owner in the stack",
            self.proposer, self.decision, self.resolver
        )
    }
}

impl std::error::Error for DecisionClosed {}

impl From<DecisionClosed> for tonic::Status {
    fn from(closed: DecisionClosed) -> Self {
        tonic::Status::internal(closed.to_string())
    }
}

/// Typed decision points: several modules contribute to one decision, and the
/// module that owns it settles it by its own rule.
///
/// The decision type `T` is any type; the owner defines it, usually as a public
/// newtype, and documents how it combines proposals. Modules submit values
/// with [`propose`](Self::propose) while the request or child RPC is being set
/// up; the framework only records them, in the order they were made and with the
/// proposing module's name. The owner calls [`resolve`](Self::resolve) once,
/// typically in [`Layer::seal_child_rpc`](super::Layer::seal_child_rpc), which
/// runs after every `before_child_rpc`, and applies whatever rule it likes:
/// the last proposal, the smallest, a refusal if any module vetoed. The
/// framework has no rule of its own. Resolving closes the decision, so a proposal
/// that comes too late is an error rather than a silent no-op.
impl Extensions {
    /// Submit `value` for decision type `T`, attributed to the module whose
    /// hook is running. Fails if the owner already resolved `T`.
    pub fn propose<T: Any + Send + Sync>(&mut self, value: T) -> Result<(), DecisionClosed> {
        let by = self.module;
        match self.get_mut::<Decision<T>>() {
            Some(Decision {
                resolved_by: Some(resolver),
                ..
            }) => Err(DecisionClosed {
                decision: std::any::type_name::<T>(),
                proposer: by,
                resolver,
            }),
            Some(decision) => {
                decision.proposals.0.push(Proposal { by, value });
                Ok(())
            }
            None => {
                self.insert(Decision {
                    proposals: Proposals(smallvec::smallvec![Proposal { by, value }]),
                    resolved_by: None,
                });
                Ok(())
            }
        }
    }

    /// The proposals made so far for `T`, in the order they were made. A module
    /// that runs after another can see what the earlier one proposed.
    pub fn proposals<T: Any + Send + Sync>(&self) -> &[Proposal<T>] {
        self.get::<Decision<T>>()
            .map_or(&[], |decision| &decision.proposals)
    }

    /// Take the proposals for `T` and close the decision: later proposals fail.
    /// Called by the owner of `T`; the proposals come back in the order they
    /// were made.
    pub fn resolve<T: Any + Send + Sync>(&mut self) -> Proposals<T> {
        let by = self.module;
        if let Some(decision) = self.get_mut::<Decision<T>>() {
            decision.resolved_by = Some(by);
            return Proposals(std::mem::take(&mut decision.proposals.0));
        }
        self.insert(Decision::<T> {
            proposals: Proposals(SmallVec::new()),
            resolved_by: Some(by),
        });
        Proposals(SmallVec::new())
    }

    /// Record the module whose hook the framework is about to call.
    pub(crate) fn set_module(&mut self, module: &'static str) {
        self.module = module;
    }
}

/// The state of one child RPC: an [`Extensions`] map that lives from the
/// child's `before_child_rpc` to its `after_child_rpc`, or to its rejection.
///
/// Each child RPC has its own map, so what modules store here (the proposals
/// for this child's deadline and priority, a latency tracker) belongs to that
/// child alone, however many children are in flight. It is keyed by type
/// like [`Extensions`], so a module finds its state, and a module that wants
/// to read another's, by naming the type, never by position in the stack.
#[derive(Debug, Default)]
pub struct ChildState(Extensions);

impl ChildState {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Deref for ChildState {
    type Target = Extensions;

    fn deref(&self) -> &Extensions {
        &self.0
    }
}

impl DerefMut for ChildState {
    fn deref_mut(&mut self) -> &mut Extensions {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_value_per_type() {
        let mut ext = Extensions::new();
        assert_eq!(ext.insert(1u32), None);
        assert_eq!(ext.insert("a"), None);
        assert_eq!(ext.insert(2u32), Some(1));
        assert_eq!(ext.get::<u32>(), Some(&2));
        assert_eq!(ext.get::<&str>(), Some(&"a"));
        assert_eq!(ext.get::<u64>(), None);
    }

    #[test]
    fn zero_is_a_value() {
        let mut ext = Extensions::new();
        ext.insert(0u64);
        assert_eq!(ext.get::<u64>(), Some(&0));
    }

    #[test]
    fn get_mut_and_remove() {
        let mut ext = Extensions::new();
        ext.insert(vec![1]);
        ext.get_mut::<Vec<i32>>().unwrap().push(2);
        assert_eq!(ext.remove::<Vec<i32>>(), Some(vec![1, 2]));
        assert_eq!(ext.get::<Vec<i32>>(), None);
    }
}
