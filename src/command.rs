use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::id::{IdRange, Namespace};

/// A write to the replicated ID state. Backends replicate these and apply
/// them, in the same order on every replica, with [`crate::IdState::apply`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Command {
    /// Reserve `count` consecutive IDs in `namespace`.
    Reserve { namespace: Namespace, count: u64 },
}

/// Result of applying a [`Command`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Outcome {
    Reserved(IdRange),
}

impl Command {
    /// Wire/log representation, for backends that replicate raw bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("commands always serialize")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| Error::Storage(format!("corrupt command: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_roundtrip() {
        let cmd = Command::Reserve {
            namespace: Namespace::new("users").unwrap(),
            count: 1000,
        };
        assert_eq!(
            String::from_utf8(cmd.to_bytes()).unwrap(),
            r#"{"op":"reserve","namespace":"users","count":1000}"#
        );
        assert_eq!(Command::from_bytes(&cmd.to_bytes()).unwrap(), cmd);
        assert!(Command::from_bytes(br#"{"op":"reserve","namespace":"BAD","count":1}"#).is_err());
    }
}
