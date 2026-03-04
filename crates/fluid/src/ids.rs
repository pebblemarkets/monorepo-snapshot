use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use pebble_trading::{InternalAccountId, InternalOrderId};

pub trait InternalId: Copy + Eq + Hash {
    fn from_u64(value: u64) -> Self;
    fn as_u64(self) -> u64;
}

impl InternalId for InternalOrderId {
    fn from_u64(value: u64) -> Self {
        Self(value)
    }

    fn as_u64(self) -> u64 {
        self.0
    }
}

impl InternalId for InternalAccountId {
    fn from_u64(value: u64) -> Self {
        Self(value)
    }

    fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug)]
pub struct StringInterner<I: InternalId> {
    to_int: HashMap<Arc<str>, I>,
    to_ext: Vec<Option<Arc<str>>>,
    free_list: Vec<u64>,
    marker: PhantomData<I>,
}

impl<I: InternalId> Default for StringInterner<I> {
    fn default() -> Self {
        Self {
            to_int: HashMap::new(),
            to_ext: Vec::new(),
            free_list: Vec::new(),
            marker: PhantomData,
        }
    }
}

impl<I: InternalId> StringInterner<I> {
    pub fn intern(&mut self, ext: &str) -> I {
        if let Some(existing) = self.lookup(ext) {
            return existing;
        }

        let ext_arc: Arc<str> = Arc::from(ext);
        let id = if let Some(slot) = self.free_list.pop() {
            let slot_usize = usize::try_from(slot).unwrap_or(usize::MAX);
            if slot_usize >= self.to_ext.len() {
                let new_id = I::from_u64(u64::try_from(self.to_ext.len()).unwrap_or(u64::MAX));
                self.to_ext.push(Some(ext_arc.clone()));
                new_id
            } else {
                self.to_ext[slot_usize] = Some(ext_arc.clone());
                I::from_u64(slot)
            }
        } else {
            let raw = u64::try_from(self.to_ext.len()).unwrap_or(u64::MAX);
            let id = I::from_u64(raw);
            self.to_ext.push(Some(ext_arc.clone()));
            id
        };

        self.to_int.insert(ext_arc, id);
        id
    }

    pub fn lookup(&self, ext: &str) -> Option<I> {
        self.to_int.get(ext).copied()
    }

    pub fn release(&mut self, int: I) {
        let slot = int.as_u64();
        let Ok(slot_usize) = usize::try_from(slot) else {
            return;
        };
        let Some(cell) = self.to_ext.get_mut(slot_usize) else {
            return;
        };
        let Some(ext) = cell.take() else {
            return;
        };
        let _ = self.to_int.remove(ext.as_ref());
        self.free_list.push(slot);
    }

    pub fn to_external(&self, int: I) -> Option<&str> {
        let slot = usize::try_from(int.as_u64()).ok()?;
        self.to_ext.get(slot)?.as_deref()
    }

    pub fn contains(&self, ext: &str) -> bool {
        self.to_int.contains_key(ext)
    }

    pub fn len(&self) -> usize {
        self.to_int.len()
    }

    pub fn is_empty(&self) -> bool {
        self.to_int.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_order_interner_roundtrip() {
        let mut interner = StringInterner::<InternalOrderId>::default();

        let id1 = interner.intern("o1");
        let id2 = interner.intern("o2");

        assert_eq!(interner.lookup("o1"), Some(id1));
        assert_eq!(interner.lookup("o2"), Some(id2));
        assert_eq!(interner.to_external(id1), Some("o1"));
        assert_eq!(interner.to_external(id2), Some("o2"));
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_order_interner_release_reuses_slot() {
        let mut interner = StringInterner::<InternalOrderId>::default();

        let id1 = interner.intern("o1");
        let _ = interner.intern("o2");

        interner.release(id1);
        assert_eq!(interner.lookup("o1"), None);
        assert_eq!(interner.to_external(id1), None);

        let id3 = interner.intern("o3");
        assert_eq!(id3, id1);
        assert_eq!(interner.lookup("o3"), Some(id1));
        assert_eq!(interner.to_external(id1), Some("o3"));
    }

    #[test]
    fn test_release_unknown_or_double_release_is_noop() {
        let mut interner = StringInterner::<InternalOrderId>::default();
        interner.release(InternalOrderId(99));
        let id = interner.intern("o1");
        interner.release(id);
        interner.release(id);
        assert!(interner.is_empty());
    }

    #[test]
    fn test_account_interner_stable_lookup() {
        let mut interner = StringInterner::<InternalAccountId>::default();
        let id = interner.intern("alice");
        assert_eq!(interner.lookup("alice"), Some(id));
        assert!(interner.contains("alice"));
        assert_eq!(interner.to_external(id), Some("alice"));
    }

    #[test]
    fn test_order_interner_slot_reuse_under_high_churn() {
        let mut interner = StringInterner::<InternalOrderId>::default();

        let mut original = Vec::new();
        for idx in 0..1_024 {
            original.push(interner.intern(&format!("order-{idx}")));
        }
        assert_eq!(interner.len(), 1_024);

        for idx in (0..1_024).step_by(2) {
            interner.release(original[idx]);
        }
        assert_eq!(interner.len(), 512);

        let mut reused_slots = HashSet::new();
        for idx in 0..512 {
            let id = interner.intern(&format!("replacement-{idx}"));
            reused_slots.insert(id.0);
        }

        assert_eq!(interner.len(), 1_024);
        assert_eq!(reused_slots.len(), 512);
        assert!(reused_slots.iter().all(|slot| *slot < 1_024));
        for idx in (0..1_024).step_by(2) {
            assert!(interner.lookup(&format!("order-{idx}")).is_none());
        }
        for idx in 0..512 {
            assert!(interner.lookup(&format!("replacement-{idx}")).is_some());
        }
    }
}
