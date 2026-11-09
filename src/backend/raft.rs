//! Replicated backend on the Raft implementation of
//! [LumenDB](https://github.com/juanmaneo/LumenDB) (`raft` crate).
//!
//! Every node keeps the whole [`IdState`] in memory and the Raft log, plus
//! snapshots of the state, in its data directory. A command is proposed to the
//! leader's log; once a majority has it durably, every node applies it with
//! [`IdState::apply`], and the leader answers the caller with the outcome. So
//! the replicas stay identical, and after a restart the state is rebuilt from
//! the latest snapshot and the log behind it.
//!
//! **Only the leader takes writes.** A follower answers [`Error::NotLeader`],
//! with the leader's client address as the hint if it was given with
//! [`Peer::client_addr`].
//!
//! **A caller is answered for its own entry only.** After a leader change, a
//! different command can end up at the log index a caller was told about, so
//! the index cannot identify the entry. Each proposal carries the node, a
//! per-run identifier and a sequence number instead, and `apply` completes
//! only the call that is waiting for exactly that. Anything else (entries of
//! other nodes, of an earlier run of this process, or of calls that have
//! timed out) is applied to the state and nothing more, so at worst a range
//! is skipped, never handed out twice.
//!
//! **Failures.** `execute` fails with [`Error::Unavailable`] when the command
//! did not commit within [`RaftConfig::timeout`] (a partitioned leader, for
//! example). The entry may still commit later: that is the third rule of the
//! [`Backend`] contract. A storage error stops the Raft server for good, and
//! every later call says why.
//!
//! The cluster is static: its voters are fixed on the command line. The
//! Raft transport has no authentication or encryption, so run it on a trusted
//! network.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lumen_raft::{
    Config, FileStorage, LogEntry, NetOptions, Node, NodeId, NodeState, Server, ServerOptions,
    Snapshot, StateMachine, Transport,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::{Backend, BackendStatus, Role};
use crate::command::{Command, Outcome};
use crate::error::{Error, Result};
use crate::state::IdState;

/// Another node of the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    pub id: NodeId,
    /// Where its Raft transport listens.
    pub raft_addr: SocketAddr,
    /// Where its clients should send writes, handed out as the leader hint
    /// when this node leads. Free-form, typically `host:port`.
    pub client_addr: Option<String>,
}

/// `ID=RAFT_ADDR` or `ID=RAFT_ADDR@CLIENT_ADDR`, as `--peer` takes it.
impl FromStr for Peer {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, String> {
        let (id, addrs) = s
            .split_once('=')
            .ok_or_else(|| format!("expected ID=RAFT_ADDR[@CLIENT_ADDR], got {s:?}"))?;
        let (raft_addr, client_addr) = match addrs.split_once('@') {
            Some((raft, client)) => (raft, Some(client.to_string())),
            None => (addrs, None),
        };
        Ok(Peer {
            id: id.parse().map_err(|_| format!("invalid node ID {id:?}"))?,
            raft_addr: raft_addr
                .parse()
                .map_err(|e| format!("invalid Raft address {raft_addr:?}: {e}"))?,
            client_addr,
        })
    }
}

#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// This node's ID: unique and non-zero in the cluster, and one of `voters`.
    pub node_id: NodeId,
    /// Where the Raft transport listens.
    pub listen: SocketAddr,
    /// The IDs of all voters, the same on every node. This node must be one.
    pub voters: Vec<NodeId>,
    /// The other nodes (this node may be listed too, it is harmless).
    pub peers: Vec<Peer>,
    /// How long `execute` waits for a command to commit and be applied.
    pub timeout: Duration,
    /// Real time per logical tick: election timeouts and heartbeats are
    /// counted in ticks, and a committed entry is applied on the next tick.
    pub tick: Duration,
    /// Election timeout range, in ticks.
    pub election_ticks: (u64, u64),
    /// Snapshot the state and drop the log up to it once this many entries
    /// have been applied since the last snapshot (0: never, the log grows).
    pub snapshot_every: u64,
}

impl RaftConfig {
    pub fn new(node_id: NodeId, listen: SocketAddr, voters: Vec<NodeId>) -> Self {
        RaftConfig {
            node_id,
            listen,
            voters,
            peers: Vec::new(),
            timeout: Duration::from_secs(5),
            tick: Duration::from_millis(20),
            election_ticks: (10, 20),
            snapshot_every: 10_000,
        }
    }
}

/// What is proposed to the log: a command and who is waiting for it.
#[derive(Serialize, Deserialize)]
struct Request {
    node: NodeId,
    /// Identifies one run of the proposing process, so that a request of an
    /// earlier run, which has the same node and may have the same `seq`,
    /// cannot complete a call of this one.
    run: u64,
    seq: u64,
    command: Command,
}

/// What the Raft thread and the callers share.
struct Shared {
    node_id: NodeId,
    run: u64,
    next_seq: AtomicU64,
    state: Mutex<IdState>,
    /// Calls waiting for their entry to be applied, by sequence number.
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Outcome>>>>,
    /// Client addresses of the peers that have one.
    clients: Mutex<HashMap<NodeId, String>>,
}

impl Shared {
    fn new(node_id: NodeId) -> Self {
        Shared {
            node_id,
            // Random keys, distinct for every call and every process.
            run: RandomState::new().hash_one(node_id),
            next_seq: AtomicU64::new(0),
            state: Mutex::new(IdState::new()),
            pending: Mutex::new(HashMap::new()),
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// Applies a committed entry. Every node does this the same way, whatever
    /// the outcome: a command that [`IdState`] rejects changes nothing.
    fn apply(&self, index: u64, data: &[u8]) {
        let Ok(request) = serde_json::from_slice::<Request>(data) else {
            // Bad on every node alike, so skipping it keeps them identical,
            // where failing would stop the whole cluster.
            tracing::warn!(index, "skipping a log entry that is not a quorid request");
            return;
        };
        let outcome = self.state.lock().unwrap().apply(&request.command);
        if request.node == self.node_id
            && request.run == self.run
            && let Some(waiter) = self.pending.lock().unwrap().remove(&request.seq)
        {
            // The caller may have given up by now.
            let _ = waiter.send(outcome);
        }
    }

    fn client_addr(&self, node: NodeId) -> Option<String> {
        self.clients.lock().unwrap().get(&node).cloned()
    }
}

/// The Raft state machine: the [`IdState`] in `Shared`.
struct App(Arc<Shared>);

impl StateMachine for App {
    fn apply(&mut self, entry: &LogEntry) -> std::result::Result<(), String> {
        self.0.apply(entry.index, &entry.data);
        Ok(())
    }

    fn restore(&mut self, snapshot: &Snapshot) -> std::result::Result<(), String> {
        let state = IdState::restore(&snapshot.data).map_err(|e| e.to_string())?;
        *self.0.state.lock().unwrap() = state;
        Ok(())
    }

    fn snapshot(&mut self) -> std::result::Result<Vec<u8>, String> {
        Ok(self.0.state.lock().unwrap().snapshot())
    }
}

/// Takes a call out of `pending` however it ends, including when the future
/// is dropped because the client went away.
struct Waiting<'a> {
    shared: &'a Shared,
    seq: u64,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.shared.pending.lock().unwrap().remove(&self.seq);
    }
}

/// A node of a Raft cluster; see the module docs.
pub struct RaftBackend {
    server: Arc<Server>,
    shared: Arc<Shared>,
    timeout: Duration,
}

fn not_a_voter() -> Error {
    Error::InvalidRequest("this node is not one of the voters, check --node-id and --voters".into())
}

/// Raft errors that say nothing about leadership.
fn convert(e: lumen_raft::Error) -> Error {
    match e {
        lumen_raft::Error::Storage(message) => Error::Storage(message),
        lumen_raft::Error::NotConfigured => not_a_voter(),
        other => Error::Unavailable(format!("raft: {other}")),
    }
}

impl RaftBackend {
    /// Opens (or creates) the node whose Raft log and snapshots live in `dir`
    /// and starts its Raft server. The node first finds or becomes the leader
    /// in the background: until then writes fail with [`Error::NotLeader`].
    pub fn open(dir: impl AsRef<Path>, config: RaftConfig) -> Result<Self> {
        // Raft accepts an empty list and then waits for a configuration to
        // reach its log, which never happens here: the voters are static, and
        // a node that waits forever does not fail visibly.
        if config.voters.is_empty() {
            return Err(not_a_voter());
        }
        let storage = FileStorage::open(dir.as_ref()).map_err(convert)?;
        let node = Node::new(
            Config {
                election_timeout_min: config.election_ticks.0,
                election_timeout_max: config.election_ticks.1,
                ..Config::cluster(config.node_id, config.voters)
            },
            storage,
        )
        .map_err(convert)?;
        let options = ServerOptions {
            tick: config.tick,
            snapshot_every: config.snapshot_every,
            net: NetOptions::default(),
        };
        let transport = Transport::bind(config.listen, options.net.clone()).map_err(|e| {
            Error::Unavailable(format!("cannot listen on {} for Raft: {e}", config.listen))
        })?;
        let shared = Arc::new(Shared::new(config.node_id));
        let server = Server::start(node, transport, App(shared.clone()), options);

        let backend = RaftBackend {
            server: Arc::new(server),
            shared,
            timeout: config.timeout,
        };
        for peer in config.peers {
            backend.set_peer(peer);
        }
        tracing::info!(
            node = config.node_id,
            raft = %backend.local_addr(),
            "raft node started"
        );
        Ok(backend)
    }

    /// Where the Raft transport listens (differs from the configured address
    /// for port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    /// Says where a peer lives, or moves it. Peers that are not in the
    /// configuration yet can be told about before they start.
    pub fn set_peer(&self, peer: Peer) {
        self.server.set_peer(peer.id, peer.raft_addr);
        let mut clients = self.shared.clients.lock().unwrap();
        match peer.client_addr {
            Some(addr) => clients.insert(peer.id, addr),
            None => clients.remove(&peer.id),
        };
    }

    /// A copy of this node's replica of the state, which lags the leader by
    /// the entries it has not applied yet.
    pub fn state(&self) -> IdState {
        self.shared.state.lock().unwrap().clone()
    }

    /// Stops the Raft server: later calls fail with [`Error::Unavailable`].
    /// Also done on drop.
    pub fn shutdown(&self) {
        self.server.shutdown();
    }

    /// An error of the Raft server, as the caller of `execute` sees it.
    fn error(&self, e: lumen_raft::Error) -> Error {
        match e {
            lumen_raft::Error::NotLeader(leader) => Error::NotLeader {
                leader: leader.and_then(|node| self.shared.client_addr(node)),
            },
            lumen_raft::Error::Stopped => Error::Unavailable(match self.server.failure() {
                Some(why) => format!("raft server stopped: {why}"),
                None => "raft server is shut down".into(),
            }),
            other => convert(other),
        }
    }
}

impl Backend for RaftBackend {
    async fn execute(&self, command: Command) -> Result<Outcome> {
        let seq = self.shared.next_seq.fetch_add(1, Ordering::Relaxed);
        let request = Request {
            node: self.shared.node_id,
            run: self.shared.run,
            seq,
            command,
        };
        let data = serde_json::to_vec(&request).expect("requests always serialize");

        // Registered before proposing: the entry can be applied at once.
        let (waiter, outcome) = oneshot::channel();
        self.shared.pending.lock().unwrap().insert(seq, waiter);
        let _waiting = Waiting {
            shared: &self.shared,
            seq,
        };

        // The proposal writes the entry to disk and waits for the sync.
        let server = self.server.clone();
        tokio::task::spawn_blocking(move || server.propose(data))
            .await
            .map_err(Error::from)
            .and_then(|proposed| proposed.map_err(|e| self.error(e)))?;

        // A dropped waiter (the node was replaced as leader, say) never
        // answers, which is a timeout like any other.
        match tokio::time::timeout(self.timeout, outcome).await {
            Ok(Ok(result)) => result,
            _ => Err(Error::Unavailable(format!(
                "the reservation did not commit within {:?}",
                self.timeout
            ))),
        }
    }

    fn status(&self) -> BackendStatus {
        let node = self.server.node();
        let role = match node.role() {
            NodeState::Leader => Role::Leader,
            NodeState::Follower => Role::Follower,
            NodeState::PreCandidate | NodeState::Candidate => Role::Candidate,
        };
        BackendStatus {
            kind: "raft",
            role,
            durable: true,
            node_id: Some(node.id().to_string()),
            leader: node
                .leader()
                .filter(|&leader| leader != node.id())
                .and_then(|leader| self.shared.client_addr(leader)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Instant;

    use lumen_raft::Storage;

    use super::*;
    use crate::id::{IdRange, Namespace};

    fn users() -> Namespace {
        Namespace::new("users").unwrap()
    }

    fn reserve(count: u64) -> Command {
        Command::Reserve {
            namespace: users(),
            count,
        }
    }

    fn any_local() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    /// Fast ticks and slow elections, so that a busy machine does not cause
    /// any.
    fn config(node_id: NodeId, voters: Vec<NodeId>) -> RaftConfig {
        let mut config = RaftConfig::new(node_id, any_local(), voters);
        config.tick = Duration::from_millis(4);
        config.election_ticks = (50, 100);
        config.timeout = Duration::from_secs(20);
        config
    }

    async fn wait_until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
        let start = Instant::now();
        loop {
            if let Some(value) = f() {
                return value;
            }
            assert!(
                start.elapsed() < Duration::from_secs(20),
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    async fn lease(backend: &RaftBackend, count: u64) -> IdRange {
        match backend.execute(reserve(count)).await.unwrap() {
            Outcome::Reserved(range) => range,
        }
    }

    async fn open_single(dir: &Path, snapshot_every: u64) -> RaftBackend {
        let mut config = config(1, vec![1]);
        config.snapshot_every = snapshot_every;
        let backend = RaftBackend::open(dir, config).unwrap();
        wait_until("the leader", || {
            (backend.status().role == Role::Leader).then_some(())
        })
        .await;
        backend
    }

    fn open_error(dir: &Path, config: RaftConfig) -> String {
        RaftBackend::open(dir, config)
            .err()
            .expect("open must fail")
            .to_string()
    }

    fn peer(id: NodeId, backend: &RaftBackend) -> Peer {
        Peer {
            id,
            raft_addr: backend.local_addr(),
            client_addr: Some(format!("node{id}:7070")),
        }
    }

    /// Nodes `1..=size` of one cluster, on ports picked by the system. A
    /// stopped node is `None`; its directory stays for a restart.
    struct Cluster {
        dir: tempfile::TempDir,
        size: u64,
        nodes: Vec<Option<RaftBackend>>,
    }

    impl Cluster {
        fn start(size: u64) -> Cluster {
            let mut cluster = Cluster {
                dir: tempfile::tempdir().unwrap(),
                size,
                nodes: Vec::new(),
            };
            for id in 1..=size {
                cluster.nodes.push(Some(cluster.open(id)));
            }
            cluster.connect();
            cluster
        }

        fn open(&self, id: NodeId) -> RaftBackend {
            let dir = self.dir.path().join(format!("node{id}"));
            RaftBackend::open(dir, config(id, (1..=self.size).collect())).unwrap()
        }

        /// Every running node learns where every other running node is.
        fn connect(&self) {
            for (i, node) in self.nodes.iter().enumerate() {
                for (j, other) in self.nodes.iter().enumerate() {
                    if let (Some(node), Some(other)) = (node, other)
                        && i != j
                    {
                        node.set_peer(peer(j as NodeId + 1, other));
                    }
                }
            }
        }

        fn node(&self, id: NodeId) -> &RaftBackend {
            self.nodes[id as usize - 1].as_ref().unwrap()
        }

        fn running(&self) -> impl Iterator<Item = (NodeId, &RaftBackend)> {
            (1..)
                .zip(&self.nodes)
                .filter_map(|(id, node)| Some((id, node.as_ref()?)))
        }

        fn stop(&mut self, id: NodeId) {
            self.nodes[id as usize - 1] = None;
        }

        fn restart(&mut self, id: NodeId) {
            self.nodes[id as usize - 1] = Some(self.open(id));
            self.connect();
        }

        /// The one running node that leads.
        async fn leader(&self) -> NodeId {
            wait_until("a single leader", || {
                let leaders: Vec<NodeId> = (self.running())
                    .filter(|(_, node)| node.status().role == Role::Leader)
                    .map(|(id, _)| id)
                    .collect();
                (leaders.len() == 1).then(|| leaders[0])
            })
            .await
        }

        /// Waits until every running node has applied up to `next` in `users`.
        async fn converge(&self, next: u128) {
            wait_until("the replicas to converge", || {
                (self
                    .running()
                    .all(|(_, node)| node.state().next_id(&users()) == next))
                .then_some(())
            })
            .await;
        }
    }

    #[tokio::test]
    async fn single_node_leases_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let backend = open_single(dir.path(), 0).await;
            let status = backend.status();
            assert_eq!(status.kind, "raft");
            assert_eq!(status.role, Role::Leader);
            assert!(status.durable);
            assert_eq!(status.node_id.as_deref(), Some("1"));
            assert_eq!(status.leader, None);

            assert_eq!(lease(&backend, 100).await, IdRange::new(1, 101));
            assert_eq!(lease(&backend, 10).await, IdRange::new(101, 111));
            assert_eq!(backend.state().next_id(&users()), 111);
            assert!(backend.shared.pending.lock().unwrap().is_empty());
        }
        // The log is replayed, with no snapshot to start from.
        let backend = open_single(dir.path(), 0).await;
        assert_eq!(lease(&backend, 5).await, IdRange::new(111, 116));
    }

    #[tokio::test]
    async fn rejected_commands_come_back_and_change_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_single(dir.path(), 0).await;
        let err = backend.execute(reserve(0)).await.unwrap_err();
        assert_eq!(err.to_string(), "invalid request: count must be at least 1");
        assert_eq!(lease(&backend, 3).await, IdRange::new(1, 4));
    }

    #[tokio::test]
    async fn snapshots_compact_the_log_and_restore_the_state() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots = || {
            fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|entry| entry.unwrap().file_name().into_string().ok())
                .filter(|name| name.starts_with("raft-snap-"))
                .count()
        };
        {
            let backend = open_single(dir.path(), 2).await;
            for _ in 0..6 {
                lease(&backend, 10).await;
            }
            wait_until("a snapshot", || (snapshots() > 0).then_some(())).await;
        }
        let backend = open_single(dir.path(), 2).await;
        assert_eq!(lease(&backend, 10).await, IdRange::new(61, 71));
    }

    #[tokio::test]
    async fn cluster_replicates_and_fails_over() {
        let mut cluster = Cluster::start(3);
        let first = cluster.leader().await;
        assert_eq!(lease(cluster.node(first), 100).await, IdRange::new(1, 101));
        cluster.converge(101).await;

        for (id, node) in cluster.running() {
            let status = node.status();
            if id == first {
                assert_eq!((status.role, status.leader), (Role::Leader, None));
            } else {
                // A follower sends clients to the leader.
                let hint = Some(format!("node{first}:7070"));
                assert_eq!((status.role, status.leader), (Role::Follower, hint.clone()));
                let err = node.execute(reserve(1)).await.unwrap_err();
                assert!(matches!(err, Error::NotLeader { leader } if leader == hint));
            }
        }

        cluster.stop(first);
        let second = wait_until("a new leader", || {
            cluster
                .running()
                .find(|(_, node)| node.status().role == Role::Leader)
                .map(|(id, _)| id)
        })
        .await;
        assert_ne!(first, second);
        assert_eq!(
            lease(cluster.node(second), 50).await,
            IdRange::new(101, 151)
        );

        // The old leader catches up with what it missed.
        cluster.restart(first);
        cluster.converge(151).await;
        assert_eq!(cluster.leader().await, cluster.leader().await);
    }

    #[tokio::test]
    async fn a_leader_without_a_quorum_times_out() {
        let mut cluster = Cluster::start(3);
        let leader = cluster.leader().await;
        for id in (1..=3).filter(|&id| id != leader) {
            cluster.stop(id);
        }

        // It keeps leading until it hears of a higher term, but cannot commit.
        let backend = cluster.nodes[leader as usize - 1].as_mut().unwrap();
        backend.timeout = Duration::from_millis(100);
        let err = backend.execute(reserve(10)).await.unwrap_err();
        assert!(
            matches!(&err, Error::Unavailable(why) if why.contains("did not commit within 100ms")),
            "{err}"
        );
        assert!(backend.shared.pending.lock().unwrap().is_empty());
        assert_eq!(backend.state().next_id(&users()), 1);
    }

    #[tokio::test]
    async fn a_node_that_hears_nobody_is_a_candidate_without_a_leader() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RaftBackend::open(dir.path(), config(1, vec![1, 2, 3])).unwrap();
        wait_until("a candidate", || {
            (backend.status().role == Role::Candidate).then_some(())
        })
        .await;
        assert_eq!(backend.status().leader, None);
        let err = backend.execute(reserve(1)).await.unwrap_err();
        assert!(matches!(err, Error::NotLeader { leader: None }), "{err}");
    }

    #[tokio::test]
    async fn leader_hints_come_from_the_peers_client_addresses() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RaftBackend::open(dir.path(), config(1, vec![1])).unwrap();
        let hint = |leader| match backend.error(lumen_raft::Error::NotLeader(leader)) {
            Error::NotLeader { leader } => leader,
            other => panic!("{other}"),
        };
        let mut other = peer(7, &backend);
        assert_eq!(hint(Some(7)), None);

        backend.set_peer(other.clone());
        assert_eq!(hint(Some(7)), Some("node7:7070".to_string()));
        assert_eq!(hint(None), None);

        other.client_addr = None;
        backend.set_peer(other);
        assert_eq!(hint(Some(7)), None);
    }

    #[test]
    fn raft_errors_become_quorid_errors() {
        use lumen_raft::Error as Raft;
        assert!(matches!(convert(Raft::Storage("disk".into())), Error::Storage(m) if m == "disk"));
        assert!(matches!(
            convert(Raft::NotConfigured),
            Error::InvalidRequest(_)
        ));
        let err = convert(Raft::Timeout).to_string();
        assert_eq!(err, "backend unavailable: raft: timed out");
    }

    #[tokio::test]
    async fn a_shut_down_server_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_single(dir.path(), 0).await;
        backend.shutdown();
        let err = backend.execute(reserve(1)).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "backend unavailable: raft server is shut down"
        );
    }

    #[tokio::test]
    async fn a_failed_server_says_why() {
        let dir = tempfile::tempdir().unwrap();
        // A snapshot that the state machine cannot read.
        let storage = FileStorage::open(dir.path()).unwrap();
        storage
            .install_snapshot(Arc::new(Snapshot {
                index: 1,
                term: 1,
                voters: vec![1],
                data: b"garbage".to_vec(),
            }))
            .unwrap();
        drop(storage);

        let backend = RaftBackend::open(dir.path(), config(1, vec![1])).unwrap();
        wait_until("the server to fail", || backend.server.failure()).await;
        let err = backend.execute(reserve(1)).await.unwrap_err().to_string();
        assert!(
            err.starts_with("backend unavailable: raft server stopped: state machine: ")
                && err.contains("corrupt snapshot"),
            "{err}"
        );
    }

    #[test]
    fn startup_problems_are_reported() {
        let dir = tempfile::tempdir().unwrap();

        for voters in [vec![], vec![2, 3]] {
            let err = open_error(dir.path(), config(1, voters));
            assert!(
                err.starts_with("invalid request: this node is not one of the voters"),
                "{err}"
            );
        }

        // The data directory is exclusive.
        let first = RaftBackend::open(dir.path(), config(1, vec![1])).unwrap();
        let err = open_error(dir.path(), config(1, vec![1]));
        assert!(
            err.starts_with("storage error: ") && err.contains("is locked"),
            "{err}"
        );
        drop(first);

        // So is the Raft port.
        let taken = std::net::TcpListener::bind(any_local()).unwrap();
        let mut busy = config(1, vec![1]);
        busy.listen = taken.local_addr().unwrap();
        let err = open_error(dir.path(), busy);
        assert!(
            err.starts_with("backend unavailable: cannot listen on 127.0.0.1:"),
            "{err}"
        );

        // A Raft state that does not decode.
        fs::write(dir.path().join("raft.state"), b"garbage").unwrap();
        let err = open_error(dir.path(), config(1, vec![1]));
        assert!(
            err.starts_with("storage error: corrupt raft.state"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn configured_peers_are_known_from_the_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config(1, vec![1]);
        config.peers = vec![Peer {
            id: 2,
            raft_addr: "127.0.0.1:9".parse().unwrap(),
            client_addr: Some("node2:7070".into()),
        }];
        let backend = RaftBackend::open(dir.path(), config).unwrap();
        assert_eq!(backend.shared.client_addr(2).as_deref(), Some("node2:7070"));
    }

    /// A request as the log holds it.
    fn entry(index: u64, node: NodeId, run: u64, seq: u64, count: u64) -> LogEntry {
        let request = Request {
            node,
            run,
            seq,
            command: reserve(count),
        };
        LogEntry::new(index, 1, serde_json::to_vec(&request).unwrap())
    }

    /// An app that has a call waiting for `seq`.
    fn waiting_app(seq: u64) -> (App, Arc<Shared>, oneshot::Receiver<Result<Outcome>>) {
        let shared = Arc::new(Shared::new(1));
        let (waiter, outcome) = oneshot::channel();
        shared.pending.lock().unwrap().insert(seq, waiter);
        (App(shared.clone()), shared, outcome)
    }

    #[test]
    fn only_the_call_that_proposed_an_entry_is_answered() {
        let (mut app, shared, mut outcome) = waiting_app(7);

        // The same sequence number from another node, and from an earlier
        // run of this one: applied, but nobody is answered.
        app.apply(&entry(1, 2, shared.run, 7, 10)).unwrap();
        app.apply(&entry(2, 1, shared.run.wrapping_add(1), 7, 10))
            .unwrap();
        assert!(outcome.try_recv().is_err());
        assert_eq!(shared.state.lock().unwrap().next_id(&users()), 21);

        // Its own: answered with the range it was applied at.
        app.apply(&entry(3, 1, shared.run, 7, 5)).unwrap();
        assert_eq!(
            outcome.try_recv().unwrap().unwrap(),
            Outcome::Reserved(IdRange::new(21, 26))
        );
        assert!(shared.pending.lock().unwrap().is_empty());

        // A call that gave up (or never existed) is not an error.
        app.apply(&entry(4, 1, shared.run, 7, 5)).unwrap();
        assert_eq!(shared.state.lock().unwrap().next_id(&users()), 31);
    }

    #[test]
    fn rejected_entries_are_answered_and_change_nothing() {
        let (mut app, shared, mut outcome) = waiting_app(0);
        app.apply(&entry(1, 1, shared.run, 0, 0)).unwrap();
        let err = outcome.try_recv().unwrap().unwrap_err();
        assert_eq!(err.to_string(), "invalid request: count must be at least 1");
        assert_eq!(shared.state.lock().unwrap().next_id(&users()), 1);
    }

    #[test]
    fn entries_that_are_not_requests_are_skipped() {
        let (mut app, shared, mut outcome) = waiting_app(0);
        app.apply(&LogEntry::new(1, 1, b"not json".to_vec()))
            .unwrap();
        assert!(outcome.try_recv().is_err());
        assert_eq!(shared.state.lock().unwrap().next_id(&users()), 1);
    }

    #[test]
    fn snapshots_roundtrip_through_the_state_machine() {
        let (mut app, shared, _outcome) = waiting_app(0);
        app.apply(&entry(1, 2, 0, 0, 40)).unwrap();
        let data = app.snapshot().unwrap();

        let (mut other, other_shared, _outcome) = waiting_app(0);
        let snapshot = Snapshot {
            index: 1,
            term: 1,
            voters: vec![1],
            data,
        };
        other.restore(&snapshot).unwrap();
        assert_eq!(
            *other_shared.state.lock().unwrap(),
            *shared.state.lock().unwrap()
        );

        let garbage = Snapshot {
            data: b"garbage".to_vec(),
            ..snapshot
        };
        let err = other.restore(&garbage).unwrap_err();
        assert!(err.starts_with("storage error: corrupt snapshot"), "{err}");
        // A snapshot that does not decode leaves the state alone.
        assert_eq!(other_shared.state.lock().unwrap().next_id(&users()), 41);
    }

    #[test]
    fn peers_parse_from_the_command_line() {
        assert_eq!(
            "2=10.0.0.2:7171".parse::<Peer>(),
            Ok(Peer {
                id: 2,
                raft_addr: "10.0.0.2:7171".parse().unwrap(),
                client_addr: None,
            })
        );
        assert_eq!(
            "3=[::1]:7171@node3:7070".parse::<Peer>(),
            Ok(Peer {
                id: 3,
                raft_addr: "[::1]:7171".parse().unwrap(),
                client_addr: Some("node3:7070".into()),
            })
        );
        for (input, error) in [
            ("2", "expected ID=RAFT_ADDR[@CLIENT_ADDR], got \"2\""),
            ("x=10.0.0.2:7171", "invalid node ID \"x\""),
            (
                "2=nope",
                "invalid Raft address \"nope\": invalid socket address syntax",
            ),
        ] {
            assert_eq!(input.parse::<Peer>().unwrap_err(), error);
        }
    }

    #[test]
    fn the_run_differs_between_nodes_and_openings() {
        assert_ne!(Shared::new(1).run, Shared::new(1).run);
        assert_ne!(Shared::new(1).run, Shared::new(2).run);
    }
}
