use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A 128-bit identifier, unique within its [`Namespace`].
pub type Id = u128;

/// The first ID ever handed out in a namespace. `0` is never allocated so it
/// can be used as a "no ID" sentinel.
pub const FIRST_ID: Id = 1;

/// Name of an independent ID sequence, e.g. `users` or `orders.v2`.
///
/// 1 to 64 characters of `[a-z0-9_.-]`, starting with `[a-z0-9]`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Namespace(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid namespace {0:?}: expected 1-64 characters of [a-z0-9_.-] starting with [a-z0-9]")]
pub struct InvalidNamespace(pub String);

impl Namespace {
    pub const MAX_LEN: usize = 64;

    pub fn new(name: impl Into<String>) -> Result<Self, InvalidNamespace> {
        let name = name.into();
        let mut chars = name.chars();
        let valid = name.len() <= Self::MAX_LEN
            && chars
                .next()
                .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            && chars.all(|c| {
                c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '.' | '-')
            });
        if valid {
            Ok(Namespace(name))
        } else {
            Err(InvalidNamespace(name))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Namespace {
    type Error = InvalidNamespace;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Namespace::new(s)
    }
}

impl From<Namespace> for String {
    fn from(ns: Namespace) -> Self {
        ns.0
    }
}

impl FromStr for Namespace {
    type Err = InvalidNamespace;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Namespace::new(s)
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A half-open range of IDs: `start..end`.
///
/// Bounds are serialized as decimal strings because JSON numbers cannot
/// faithfully carry 128-bit integers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdRange {
    #[serde(with = "u128_string")]
    pub start: Id,
    #[serde(with = "u128_string")]
    pub end: Id,
}

impl IdRange {
    pub fn new(start: Id, end: Id) -> Self {
        assert!(start <= end, "invalid range {start}..{end}");
        IdRange { start, end }
    }

    pub fn len(&self) -> u128 {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    pub fn contains(&self, id: Id) -> bool {
        (self.start..self.end).contains(&id)
    }

    /// Splits off up to `n` IDs from the front of the range.
    pub fn take_front(&mut self, n: u128) -> IdRange {
        let taken = IdRange::new(self.start, self.start + n.min(self.len()));
        self.start = taken.end;
        taken
    }

    pub fn iter(&self) -> std::ops::Range<Id> {
        self.start..self.end
    }
}

impl IntoIterator for IdRange {
    type Item = Id;
    type IntoIter = std::ops::Range<Id>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(crate) mod u128_string {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
        String::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_validation() {
        for ok in ["users", "a", "0", "orders.v2", "a-b_c.d", &"x".repeat(64)] {
            assert!(Namespace::new(ok).is_ok(), "{ok:?} should be valid");
        }
        for bad in [
            "",
            "Users",
            "_x",
            ".x",
            "-x",
            "a b",
            "a/b",
            "é",
            &"x".repeat(65),
        ] {
            assert!(Namespace::new(bad).is_err(), "{bad:?} should be invalid");
        }
    }

    #[test]
    fn namespace_serde_validates() {
        let ns: Namespace = serde_json::from_str("\"users\"").unwrap();
        assert_eq!(ns.as_str(), "users");
        assert!(serde_json::from_str::<Namespace>("\"NOPE\"").is_err());
    }

    #[test]
    fn namespace_parses_from_str() {
        let ns: Namespace = "orders.v2".parse().unwrap();
        assert_eq!(ns.to_string(), "orders.v2");
        assert_eq!(
            "NOPE".parse::<Namespace>(),
            Err(InvalidNamespace("NOPE".into()))
        );
    }

    #[test]
    fn range_contains_and_iterates() {
        let r = IdRange::new(10, 13);
        assert!(!r.contains(9));
        assert!(r.contains(10));
        assert!(r.contains(12));
        assert!(!r.contains(13));
        assert_eq!(r.into_iter().collect::<Vec<_>>(), [10, 11, 12]);
    }

    #[test]
    fn range_take_front() {
        let mut r = IdRange::new(10, 15);
        assert_eq!(r.take_front(3), IdRange::new(10, 13));
        assert_eq!(r.take_front(10), IdRange::new(13, 15));
        assert!(r.is_empty());
        assert!(r.take_front(1).is_empty());
    }

    #[test]
    fn range_serializes_as_strings() {
        let r = IdRange::new(1, u128::MAX);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(
            json,
            format!("{{\"start\":\"1\",\"end\":\"{}\"}}", u128::MAX)
        );
        assert_eq!(serde_json::from_str::<IdRange>(&json).unwrap(), r);
        // Bare JSON numbers are refused, not silently rounded.
        assert!(serde_json::from_str::<IdRange>(r#"{"start":1,"end":2}"#).is_err());
        assert!(serde_json::from_str::<IdRange>(r#"{"start":"x","end":"2"}"#).is_err());
    }
}
