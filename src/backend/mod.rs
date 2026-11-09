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
//! with the outcome. [`RaftBackend`] is the reference for that. A node that
//! cannot take the write itself either fails with
//! [`Error::NotLeader`](crate::Error::NotLeader) or forwards the command to the
//! node that can, in which case it must serve forwarded commands with
//! [`Backend::execute_here`].
//!
//! A backend whose cluster can change implements [`Backend::add_voter`] and
//! [`Backend::remove_voter`]; the others refuse them.

mod file;
mod forward;
mod memory;
mod raft;

use std::future::Future;

use serde::Serialize;

use crate::command::{Command, Outcome};
use crate::error::{Error, Result};

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
    /// Who is in the cluster, for the backends that have one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<Members>,
}

/// The membership of a cluster, as one node sees it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Members {
    /// The nodes that vote, ascending: the last configuration this node holds,
    /// committed or not.
    pub voters: Vec<u64>,
    /// The nodes the leader brings up to date before they vote (empty on the
    /// others).
    pub learners: Vec<u64>,
    /// A change of the voters is under way: it is not committed yet.
    pub config_pending: bool,
}

fn no_members() -> Error {
    Error::InvalidRequest("this backend has no cluster to change".into())
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

    /// Like [`execute`](Backend::execute), but never hands the command to
    /// another node: one that cannot apply it itself fails with
    /// [`Error::NotLeader`](crate::Error::NotLeader). It serves the commands
    /// that another node already forwarded, which must not be forwarded again.
    /// Backends that never forward need not override it.
    fn execute_here(&self, command: Command) -> impl Future<Output = Result<Outcome>> + Send {
        self.execute(command)
    }

    /// Makes `peer` a voter of the cluster, once it has caught up, and returns
    /// the membership then. The node has to be running already, and to know
    /// where to reach the cluster.
    fn add_voter(&self, peer: Peer) -> impl Future<Output = Result<Members>> + Send {
        let _ = peer;
        std::future::ready(Err(no_members()))
    }

    /// Stops node `id` from voting, and returns the membership then.
    fn remove_voter(&self, id: u64) -> impl Future<Output = Result<Members>> + Send {
        let _ = id;
        std::future::ready(Err(no_members()))
    }

    fn status(&self) -> BackendStatus;
}
