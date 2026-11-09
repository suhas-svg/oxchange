use std::collections::{HashSet, VecDeque};

use domain::CommandId;

/// Default size of the duplicate-command memory (spec: "the last 1,000,000 command IDs").
pub const DEFAULT_DEDUP_CAPACITY: usize = 1_000_000;

/// Remembers the most recent command IDs, in log order, so a command the
/// outbox published twice is applied once.
///
/// Insertion order equals log order, so two engines fed the same log hold the
/// same window. That keeps replay deterministic; the window is saved in every
/// snapshot for the same reason.
#[derive(Debug, Clone)]
pub struct DedupWindow {
    capacity: usize,
    order: VecDeque<CommandId>,
    seen: HashSet<CommandId>,
}

impl DedupWindow {
    /// Allocates the full window up front, so the matcher never pauses to
    /// rehash a large set while trading (about 40 MB at the default capacity).
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "dedup capacity must be positive");
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity + 1),
            seen: HashSet::with_capacity(capacity + 1),
        }
    }

    /// Records `id` and returns true if it is new; returns false for a repeat.
    pub fn insert(&mut self, id: CommandId) -> bool {
        if !self.seen.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.seen.remove(&oldest);
        }
        true
    }

    pub fn contains(&self, id: &CommandId) -> bool {
        self.seen.contains(id)
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The remembered IDs, oldest first.
    pub fn ids(&self) -> impl ExactSizeIterator<Item = &CommandId> {
        self.order.iter()
    }

    /// Rebuilds a window from saved IDs (oldest first). If there are more than
    /// `capacity`, only the newest are kept, exactly as if they had been
    /// inserted one by one.
    pub fn from_ids(capacity: usize, ids: impl IntoIterator<Item = CommandId>) -> Self {
        let mut w = Self::new(capacity);
        for id in ids {
            w.insert(id);
        }
        w
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> CommandId {
        CommandId::from_u128(n)
    }

    #[test]
    fn repeats_are_detected() {
        let mut w = DedupWindow::new(10);
        assert!(w.insert(id(1)));
        assert!(!w.insert(id(1)));
        assert_eq!(w.len(), 1);
    }

    #[test]
    fn oldest_is_forgotten_at_capacity() {
        let mut w = DedupWindow::new(3);
        for n in 1..=4 {
            assert!(w.insert(id(n)));
        }
        assert!(!w.contains(&id(1)));
        assert!(w.contains(&id(2)) && w.contains(&id(4)));
        assert_eq!(
            w.ids().copied().collect::<Vec<_>>(),
            vec![id(2), id(3), id(4)]
        );
    }

    #[test]
    fn rebuild_matches_incremental() {
        let mut w = DedupWindow::new(3);
        for n in 1..=5 {
            w.insert(id(n));
        }
        let r = DedupWindow::from_ids(3, w.ids().copied());
        assert_eq!(r.ids().collect::<Vec<_>>(), w.ids().collect::<Vec<_>>());
    }
}
