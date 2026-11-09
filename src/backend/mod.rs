//! Pluggable storage/consensus backends.
//!
//! A backend owns one or more replicas of [`IdState`](crate::IdState) and
//! applies [`Command`]s to them. Implementations shipped here:
//!
//! * [`MemoryBackend`]: single process, nothing persisted (tests, demos).
//! * [`FileBackend`]: single node, every reservation fsync'd to disk.
//! * [`RaftBackend`]: replicated and durable, on the Raft implementation of
//!   [LumenDB](https://github.com/juanmaneo/LumenDB).
//!
//! A replicated backend implements the same trait: propose the command to the
//! log, and once the entry is committed apply it to the local
//! [`IdState`](crate::IdState) with
//! [`IdState::apply`](crate::IdState::apply) and resolve the caller's future
//! with the outcome. [`RaftBackend`] is the reference for that.

mod file;
mod memory;
mod raft;

use std::future::Future;

use serde::Serialize;

use crate::command::{Command, Outcome};
use crate::error::Result;

pub use file::FileBackend;
pub use memory::MemoryBackend;
pub use raft::{Peer, RaftBackend, RaftConfig};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Single node, no replication.
    Standalone,
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone, Debug, Serialize)]
pub struct BackendStatus {
    /// Short backend name, e.g. `memory`, `file`, `raft`.
    pub kind: &'static str,
    pub role: Role,
    /// Whether acknowledged reservations survive a process crash.
    pub durable: bool,
    pub node_id: Option<String>,
    /// Where writes should go when this node is not the leader.
    pub leader: Option<String>,
}

/// A linearizable executor of [`Command`]s over the replicated ID state.
///
/// Contract every implementation must uphold:
///
/// 1. **Apply through [`IdState`](crate::IdState).** All replicas apply the
///    same commands in the same order with `IdState::apply`, so every backend
///    has identical semantics.
/// 2. **Acknowledged means durable.** When `execute` returns `Ok`, the command
///    is applied, will survive the failures the backend claims to tolerate,
///    and is visible to every later `execute` on any node.
/// 3. **Failures may or may not have been applied.** On `Err` (timeouts,
///    leader changes, ...) the reservation may still have taken effect. That is
///    safe for ID allocation: at worst a range is skipped, never handed out
///    twice. Backends therefore need no client-level deduplication.
pub trait Backend: Send + Sync + 'static {
    fn execute(&self, command: Command) -> impl Future<Output = Result<Outcome>> + Send;

    fn status(&self) -> BackendStatus;
}
