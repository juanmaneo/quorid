use crate::id::{InvalidNamespace, Namespace};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    InvalidNamespace(#[from] InvalidNamespace),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("namespace {0} is exhausted: not enough IDs left in the 128-bit space")]
    Exhausted(Namespace),

    /// The backend cannot accept writes on this node. `leader` is a hint
    /// (typically an address) of where to retry, when known.
    #[error("this node is not the leader")]
    NotLeader { leader: Option<String> },

    #[error("backend unavailable: {0}")]
    Unavailable(String),

    #[error("storage error: {0}")]
    Storage(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Storage(e.to_string())
    }
}

/// A blocking storage task panicked or was cancelled.
impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Error::Storage(format!("storage task failed: {e}"))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_errors_are_storage_errors() {
        let err = Error::from(std::io::Error::other("disk on fire"));
        assert_eq!(err.to_string(), "storage error: disk on fire");
    }

    #[tokio::test]
    async fn failed_tasks_are_storage_errors() {
        let join_error = tokio::spawn(async { panic!("injected task panic") })
            .await
            .unwrap_err();
        let err = Error::from(join_error);
        assert!(
            matches!(&err, Error::Storage(msg) if msg.starts_with("storage task failed")),
            "{err}"
        );
    }
}
