use std::sync::Mutex;

use super::{Backend, BackendStatus, Role};
use crate::command::{Command, Outcome};
use crate::error::Result;
use crate::state::IdState;

/// Volatile single-process backend. Restarting it restarts every namespace
/// at [`FIRST_ID`](crate::FIRST_ID), so only use it for tests and demos.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    state: Mutex<IdState>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_state(state: IdState) -> Self {
        MemoryBackend {
            state: Mutex::new(state),
        }
    }

    pub fn state(&self) -> IdState {
        self.state.lock().unwrap().clone()
    }
}

impl Backend for MemoryBackend {
    async fn execute(&self, command: Command) -> Result<Outcome> {
        self.state.lock().unwrap().apply(&command)
    }

    fn status(&self) -> BackendStatus {
        BackendStatus {
            kind: "memory",
            role: Role::Standalone,
            durable: false,
            node_id: None,
            leader: None,
            members: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{IdRange, Namespace};

    #[tokio::test]
    async fn resumes_from_given_state() {
        let users = Namespace::new("users").unwrap();
        let mut state = IdState::new();
        state
            .apply(&Command::Reserve {
                namespace: users.clone(),
                count: 41,
            })
            .unwrap();

        let backend = MemoryBackend::with_state(state.clone());
        assert_eq!(backend.state(), state);
        assert_eq!(
            backend
                .execute(Command::Reserve {
                    namespace: users.clone(),
                    count: 2,
                })
                .await
                .unwrap(),
            Outcome::Reserved(IdRange::new(42, 44))
        );
        assert_eq!(backend.state().next_id(&users), 44);
        assert!(!backend.status().durable);
    }
}
