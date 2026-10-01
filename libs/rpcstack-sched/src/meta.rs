use std::cmp::Ordering;

/// Per-task scheduling metadata.
///
/// The runtime stores one `Meta` in every task header and passes it to the run
/// queue; it never looks inside. Queues decide what the number means. The
/// built-in queues read it as a priority where a smaller value runs first.
///
/// The `Ord` implementation is reversed for that reason: the smaller value
/// compares as greater, so a max-heap (`BinaryHeap`) pops it first.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Meta(u64);

impl Meta {
    /// Create a `Meta` from its raw value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw value.
    pub const fn value(&self) -> u64 {
        self.0
    }
}

impl PartialOrd for Meta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Meta {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::Meta;
    use std::collections::BinaryHeap;

    #[test]
    fn smaller_values_sort_first() {
        assert_eq!(
            Meta::new(1).partial_cmp(&Meta::new(2)),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(
            Meta::new(2).partial_cmp(&Meta::new(1)),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn binary_heap_pops_smallest_value_first() {
        let mut heap = BinaryHeap::new();
        heap.push(Meta::new(500_000));
        heap.push(Meta::new(250_000));
        heap.push(Meta::new(0));

        assert_eq!(heap.pop(), Some(Meta::new(0)));
        assert_eq!(heap.pop(), Some(Meta::new(250_000)));
        assert_eq!(heap.pop(), Some(Meta::new(500_000)));
    }
}
