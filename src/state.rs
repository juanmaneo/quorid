use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::command::{Command, Outcome};
use crate::error::{Error, Result};
use crate::id::{FIRST_ID, Id, IdRange, Namespace};

/// The replicated state machine: the next free ID of every namespace.
///
/// [`IdState::apply`] is deterministic, so replicas that apply the same
/// commands in the same order end up identical. Rejected commands (e.g.
/// exhaustion) leave the state untouched.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IdState {
    next: BTreeMap<Namespace, Id>,
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    /// Next free ID per namespace, as decimal strings.
    next: BTreeMap<Namespace, String>,
}

const SNAPSHOT_VERSION: u32 = 1;

impl IdState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, command: &Command) -> Result<Outcome> {
        match command {
            Command::Reserve { namespace, count } => {
                self.reserve(namespace, *count).map(Outcome::Reserved)
            }
        }
    }

    fn reserve(&mut self, namespace: &Namespace, count: u64) -> Result<IdRange> {
        if count == 0 {
            return Err(Error::InvalidRequest("count must be at least 1".into()));
        }
        let start = self.next_id(namespace);
        let end = start
            .checked_add(count as u128)
            .ok_or_else(|| Error::Exhausted(namespace.clone()))?;
        self.next.insert(namespace.clone(), end);
        Ok(IdRange::new(start, end))
    }

    /// The ID the next reservation in `namespace` would start at.
    pub fn next_id(&self, namespace: &Namespace) -> Id {
        self.next.get(namespace).copied().unwrap_or(FIRST_ID)
    }

    pub fn namespaces(&self) -> impl Iterator<Item = (&Namespace, Id)> {
        self.next.iter().map(|(ns, &id)| (ns, id))
    }

    /// Serializes the whole state (human-readable JSON).
    pub fn snapshot(&self) -> Vec<u8> {
        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            next: self
                .next
                .iter()
                .map(|(ns, id)| (ns.clone(), id.to_string()))
                .collect(),
        };
        serde_json::to_vec_pretty(&snapshot).expect("snapshots always serialize")
    }

    pub fn restore(bytes: &[u8]) -> Result<Self> {
        let corrupt = |msg: String| Error::Storage(format!("corrupt snapshot: {msg}"));
        let snapshot: Snapshot =
            serde_json::from_slice(bytes).map_err(|e| corrupt(e.to_string()))?;
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(corrupt(format!("unsupported version {}", snapshot.version)));
        }
        let next = snapshot
            .next
            .into_iter()
            .map(|(ns, id)| match id.parse::<Id>() {
                Ok(id) if id >= FIRST_ID => Ok((ns, id)),
                _ => Err(corrupt(format!("bad next id {id:?} for {ns}"))),
            })
            .collect::<Result<_>>()?;
        Ok(IdState { next })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(s: &str) -> Namespace {
        Namespace::new(s).unwrap()
    }

    fn reserve(state: &mut IdState, namespace: &str, count: u64) -> Result<IdRange> {
        let Outcome::Reserved(r) = state.apply(&Command::Reserve {
            namespace: ns(namespace),
            count,
        })?;
        Ok(r)
    }

    #[test]
    fn reservations_are_contiguous_and_per_namespace() {
        let mut s = IdState::new();
        assert_eq!(reserve(&mut s, "a", 10).unwrap(), IdRange::new(1, 11));
        assert_eq!(reserve(&mut s, "a", 5).unwrap(), IdRange::new(11, 16));
        assert_eq!(reserve(&mut s, "b", 1).unwrap(), IdRange::new(1, 2));
        assert_eq!(s.next_id(&ns("a")), 16);
        assert_eq!(s.namespaces().count(), 2);
    }

    #[test]
    fn rejects_zero_and_exhaustion_without_mutating() {
        let mut s = IdState::new();
        assert_eq!(
            reserve(&mut s, "a", 0).unwrap_err().to_string(),
            "invalid request: count must be at least 1"
        );
        s.next.insert(ns("a"), u128::MAX - 2);
        assert_eq!(
            reserve(&mut s, "a", 3).unwrap_err().to_string(),
            "namespace a is exhausted: not enough IDs left in the 128-bit space"
        );
        assert_eq!(s.next_id(&ns("a")), u128::MAX - 2);
        assert_eq!(
            reserve(&mut s, "a", 2).unwrap(),
            IdRange::new(u128::MAX - 2, u128::MAX)
        );
    }

    #[test]
    fn snapshot_roundtrip() {
        let mut s = IdState::new();
        reserve(&mut s, "users", 1000).unwrap();
        s.next.insert(ns("big"), u128::MAX);
        let restored = IdState::restore(&s.snapshot()).unwrap();
        assert_eq!(restored, s);
        assert!(IdState::restore(b"{}").is_err());
        assert!(IdState::restore(br#"{"version":1,"next":{"a":"0"}}"#).is_err());
        assert!(IdState::restore(br#"{"version":9,"next":{}}"#).is_err());
    }
}
