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
//! **Only the leader takes writes**, but any node takes requests. A follower
//! that knows the leader's client address ([`Peer::client_addr`]) forwards the
//! command to it over HTTP and returns the leader's answer. Otherwise, and for
//! a command that was forwarded already ([`Backend::execute_here`]), it
//! answers [`Error::NotLeader`], with that address as the hint if it has one.
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
//! **Membership.** The voters on the command line only start a cluster. After
//! that the cluster changes through [`Backend::add_voter`] and
//! [`Backend::remove_voter`], on the leader, and the configuration lives in the
//! Raft log: a node that restarts takes the voters from there, whatever its
//! command line says. A node joins by starting with [`RaftConfig::join`] and
//! no voters, and being added. Raft itself replicates the voters' ids but not
//! where they live, so the address book travels in the log too, one entry per
//! node that is added, and in the snapshots.
//!
//! The Raft transport has no authentication or encryption, so run it on a
//! trusted network.

use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, RandomState};
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use lumen_raft::{
    Config, FileStorage, Index, LogEntry, NetOptions, Node, NodeId, NodeState, Server,
    ServerOptions, Snapshot, StateMachine, Transport,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::{Backend, BackendStatus, Members, Role, forward};
use crate::command::{Command, Outcome};
use crate::error::{Error, Result};
use crate::state::IdState;

/// Another node of the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    /// The IDs of all the voters of a new cluster, the same on every node.
    /// This node must be one. Once the cluster has a configuration in its
    /// log, that one counts instead.
    pub voters: Vec<NodeId>,
    /// Start with no voters, as a node that is going to be added to a running
    /// cluster ([`Backend::add_voter`]). It needs `peers` to reach the cluster
    /// and takes no `voters`. A node that was added restarts the same way.
    pub join: bool,
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
            join: false,
            peers: Vec::new(),
            timeout: Duration::from_secs(5),
            tick: Duration::from_millis(20),
            election_ticks: (10, 20),
            snapshot_every: 10_000,
        }
    }
}

/// What is proposed to the log.
///
/// Untagged, so that the entries of a log that only held requests, which are
/// plain [`Request`]s, still decode.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Entry {
    Request(Request),
    /// Where a node that was added lives. Every node learns it when the entry
    /// is applied.
    Peer {
        peer: Peer,
    },
}

/// A command and who is waiting for it.
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
    /// Where every node known so far lives: the address book of the cluster.
    peers: Mutex<BTreeMap<NodeId, Peer>>,
    /// The Raft server, which has to hear about the peers, once it is started.
    server: OnceLock<Weak<Server>>,
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
            peers: Mutex::new(BTreeMap::new()),
            server: OnceLock::new(),
        }
    }

    /// Applies a committed entry. Every node does this the same way, whatever
    /// the outcome: a command that [`IdState`] rejects changes nothing.
    fn apply(&self, index: u64, data: &[u8]) {
        let request = match serde_json::from_slice::<Entry>(data) {
            Ok(Entry::Request(request)) => request,
            Ok(Entry::Peer { peer }) => return self.learn(peer),
            Err(_) => {
                // Bad on every node alike, so skipping it keeps them
                // identical, where failing would stop the whole cluster.
                tracing::warn!(index, "skipping a log entry that is not a quorid entry");
                return;
            }
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

    /// Remembers where `peer` lives, and tells the Raft server if it is not
    /// told yet. A server that is told again drops what it queued for the
    /// peer, so only a change is passed on.
    fn learn(&self, peer: Peer) {
        let mut peers = self.peers.lock().unwrap();
        if peers.get(&peer.id) == Some(&peer) {
            return;
        }
        if let Some(server) = self.server.get().and_then(Weak::upgrade) {
            server.set_peer(peer.id, peer.raft_addr);
        }
        peers.insert(peer.id, peer);
    }

    /// Gives the server the address book, and from now on every change to it.
    /// What was learned before the server existed (the command line, and the
    /// log when it replays) is in the book already.
    fn connect(&self, server: &Arc<Server>) {
        let peers = self.peers.lock().unwrap();
        self.server
            .set(Arc::downgrade(server))
            .expect("a server is connected once");
        for peer in peers.values() {
            server.set_peer(peer.id, peer.raft_addr);
        }
    }

    fn address_book(&self) -> Vec<Peer> {
        self.peers.lock().unwrap().values().cloned().collect()
    }

    fn client_addr(&self, node: NodeId) -> Option<String> {
        (self.peers.lock().unwrap().get(&node)).and_then(|peer| peer.client_addr.clone())
    }
}

/// What a snapshot holds next to the state of [`IdState`], which ignores it:
/// the address book. Snapshots from before it existed have none.
#[derive(Deserialize)]
struct SnapshotPeers {
    #[serde(default)]
    peers: Vec<Peer>,
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
        // It parsed as JSON for the state, so this can only find no peers.
        let known = serde_json::from_slice::<SnapshotPeers>(&snapshot.data)
            .map(|known| known.peers)
            .unwrap_or_default();
        // They add to what is known: the command line has others.
        for peer in known {
            self.0.learn(peer);
        }
        Ok(())
    }

    fn snapshot(&mut self) -> std::result::Result<Vec<u8>, String> {
        let state = self.0.state.lock().unwrap().snapshot();
        let mut snapshot: serde_json::Value =
            serde_json::from_slice(&state).expect("the state is saved as JSON");
        snapshot["peers"] =
            serde_json::to_value(self.0.address_book()).expect("peers always serialize");
        Ok(serde_json::to_vec_pretty(&snapshot).expect("snapshots always serialize"))
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

/// Seeds a node's election timeouts. Raft derives its generator from
/// `(seed ^ id) | 1`, so with one seed shared by every node, ids that differ
/// only in the lowest bit (2 and 3, 4 and 5...) draw identical timeouts and
/// split the vote again and again after a leader fails. Hashing the id
/// (SplitMix64) gives each node its own sequence.
fn election_seed(node_id: NodeId) -> u64 {
    let mut z = node_id.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Raft errors that say nothing about leadership.
fn convert(e: lumen_raft::Error) -> Error {
    match e {
        lumen_raft::Error::Storage(message) => Error::Storage(message),
        lumen_raft::Error::NotConfigured => not_a_voter(),
        lumen_raft::Error::BadConfig(_) => Error::InvalidRequest(e.to_string()),
        other => Error::Unavailable(format!("raft: {other}")),
    }
}

impl RaftBackend {
    /// Opens (or creates) the node whose Raft log and snapshots live in `dir`
    /// and starts its Raft server. The node first finds or becomes the leader
    /// in the background: until then writes fail with [`Error::NotLeader`].
    pub fn open(dir: impl AsRef<Path>, config: RaftConfig) -> Result<Self> {
        // Raft accepts an empty list and then waits for a configuration to
        // reach its log. Only a node that was told to join gets one, and a
        // node that waits forever by mistake does not fail visibly.
        match (config.join, config.voters.is_empty()) {
            (false, true) => return Err(not_a_voter()),
            (true, false) => {
                return Err(Error::InvalidRequest(
                    "a node that joins takes no voters: the cluster tells it who they are".into(),
                ));
            }
            _ => {}
        }
        let storage = FileStorage::open(dir.as_ref()).map_err(convert)?;
        let node = Node::new(
            Config {
                election_timeout_min: config.election_ticks.0,
                election_timeout_max: config.election_ticks.1,
                seed: election_seed(config.node_id),
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
        // The log, when it replays, tells newer things than the command line.
        let shared = Arc::new(Shared::new(config.node_id));
        for peer in config.peers {
            shared.learn(peer);
        }
        let server = Arc::new(Server::start(node, transport, App(shared.clone()), options));
        shared.connect(&server);

        let backend = RaftBackend {
            server,
            shared,
            timeout: config.timeout,
        };
        let raft = backend.local_addr();
        tracing::info!(node = config.node_id, %raft, "raft node started");
        Ok(backend)
    }

    /// Where the Raft transport listens (differs from the configured address
    /// for port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    /// Says where a peer lives, or moves it, to this node alone. Peers that
    /// are not in the configuration yet can be told about before they start.
    /// The nodes that are added to the cluster are known to all of it already:
    /// that goes through the log.
    pub fn set_peer(&self, peer: Peer) {
        self.shared.learn(peer);
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

/// How long a membership change that the leader refuses for now waits before
/// it tries again.
const RETRY: Duration = Duration::from_millis(10);

/// Whether the leader refuses a membership change for now but will accept it:
/// right after an election it has committed nothing yet, the change before
/// has to commit first, and a node that was just added needs time to catch up.
fn transient(e: &lumen_raft::Error) -> bool {
    use lumen_raft::Error::{BadConfig, NotReady};
    matches!(
        e,
        NotReady
            | BadConfig(
                "the learner has not caught up" | "the previous change is not committed yet"
            )
    )
}

/// Makes `attempt` until the leader accepts it, or `deadline` has passed.
fn retry<T>(
    deadline: Instant,
    mut attempt: impl FnMut() -> lumen_raft::Result<T>,
) -> lumen_raft::Result<T> {
    loop {
        match attempt() {
            Err(e) if transient(&e) => {
                if Instant::now() >= deadline {
                    return Err(lumen_raft::Error::Timeout);
                }
                std::thread::sleep(RETRY);
            }
            result => return result,
        }
    }
}

fn left(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// Brings `peer` into the cluster as a voter. The leader first has to be able
/// to reach it, so everyone learns where it lives through the log; then it
/// replicates to it as a learner, which becomes a voter once it has caught up.
fn add(server: &Server, peer: Peer, deadline: Instant) -> lumen_raft::Result<()> {
    let id = peer.id;
    let entry = serde_json::to_vec(&Entry::Peer { peer }).expect("entries always serialize");
    let added = server
        .propose(entry)
        .and_then(|index| server.wait_applied(index, left(deadline)))
        .and_then(|()| server.node().add_learner(id))
        .and_then(|()| retry(deadline, || server.node().add_voter(id)))
        .and_then(|index| server.wait_applied(index, left(deadline)));
    if added.is_err() {
        // A node that did not make it must not be fed forever. It is not a
        // learner anymore if it made it as far as being a voter.
        let _ = server.node().remove_learner(id);
    }
    added
}

fn remove(server: &Server, id: NodeId, deadline: Instant) -> lumen_raft::Result<()> {
    retry(deadline, || server.node().remove_voter(id))
        .and_then(|index: Index| server.wait_applied(index, left(deadline)))
}

impl RaftBackend {
    fn members(&self) -> Members {
        let node = self.server.node();
        Members {
            voters: node.voters(),
            learners: node.learners(),
            config_pending: node.config_pending(),
        }
    }

    /// Runs a membership change, which waits for the cluster, on a thread of
    /// its own, and returns the membership after it.
    async fn change(
        &self,
        change: impl FnOnce(&Server, Instant) -> lumen_raft::Result<()> + Send + 'static,
    ) -> Result<Members> {
        let server = self.server.clone();
        let deadline = Instant::now() + self.timeout;
        tokio::task::spawn_blocking(move || change(&server, deadline))
            .await
            .map_err(Error::from)
            .and_then(|changed| changed.map_err(|e| self.error(e)))
            .map(|()| self.members())
    }
}

impl RaftBackend {
    /// Appends `command` to the log, if this node leads, and waits for the
    /// outcome. A follower with the leader's client address hands the command
    /// to the leader instead, if `forward` allows it.
    async fn run(&self, command: Command, forward: bool) -> Result<Outcome> {
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
        let proposed = tokio::task::spawn_blocking(move || server.propose(data))
            .await
            .map_err(Error::from)
            .and_then(|proposed| proposed.map_err(|e| self.error(e)));
        // The hint is where the leader's clients go, which is where to forward.
        if let Err(Error::NotLeader { leader: Some(addr) }) = &proposed
            && forward
        {
            return forward::execute(addr, &request.command, self.timeout).await;
        }
        proposed?;

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
}

impl Backend for RaftBackend {
    async fn execute(&self, command: Command) -> Result<Outcome> {
        self.run(command, true).await
    }

    async fn execute_here(&self, command: Command) -> Result<Outcome> {
        self.run(command, false).await
    }

    async fn add_voter(&self, peer: Peer) -> Result<Members> {
        // Refused before anything is logged: the entry would move a voter.
        if self.server.node().voters().contains(&peer.id) {
            return Err(Error::InvalidRequest(format!(
                "node {} is already a voter",
                peer.id
            )));
        }
        self.change(move |server, deadline| add(server, peer, deadline))
            .await
    }

    async fn remove_voter(&self, id: u64) -> Result<Members> {
        self.change(move |server, deadline| remove(server, id, deadline))
            .await
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
            members: Some(self.members()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Instant;

    use lumen_raft::Storage;

    use super::*;
    use crate::allocator::{Allocator, AllocatorConfig};
    use crate::codec::Format;
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

    /// A node that waits to be added to a cluster.
    fn joiner(node_id: NodeId) -> RaftConfig {
        let mut config = config(node_id, Vec::new());
        config.join = true;
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
        nodes: Vec<Option<Arc<RaftBackend>>>,
    }

    impl Cluster {
        fn start(size: u64) -> Cluster {
            let mut cluster = Cluster {
                dir: tempfile::tempdir().unwrap(),
                size,
                nodes: Vec::new(),
            };
            for id in 1..=size {
                cluster.nodes.push(Some(Arc::new(cluster.open(id))));
            }
            cluster.connect();
            cluster
        }

        /// A node past `size` is one that was added: it never has voters
        /// of its own.
        fn open(&self, id: NodeId) -> RaftBackend {
            let dir = self.dir.path().join(format!("node{id}"));
            let config = if id > self.size {
                joiner(id)
            } else {
                config(id, (1..=self.size).collect())
            };
            RaftBackend::open(dir, config).unwrap()
        }

        /// Starts the next node as one that waits to be added. It knows where
        /// the others are, but they do not know where it is: that has to
        /// reach them through the log.
        fn join(&mut self) -> Peer {
            let id = self.nodes.len() as NodeId + 1;
            let node = self.open(id);
            for (other, running) in self.running() {
                node.set_peer(peer(other, running));
            }
            let newcomer = peer(id, &node);
            self.nodes.push(Some(Arc::new(node)));
            newcomer
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
            self.nodes[id as usize - 1].as_deref().unwrap()
        }

        fn running(&self) -> impl Iterator<Item = (NodeId, &RaftBackend)> {
            (1..)
                .zip(&self.nodes)
                .filter_map(|(id, node)| Some((id, node.as_deref()?)))
        }

        fn stop(&mut self, id: NodeId) {
            self.nodes[id as usize - 1] = None;
        }

        fn restart(&mut self, id: NodeId) {
            self.nodes[id as usize - 1] = Some(Arc::new(self.open(id)));
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
                // A follower tells where the leader's clients go.
                let hint = Some(format!("node{first}:7070"));
                assert_eq!((status.role, status.leader), (Role::Follower, hint.clone()));
                let err = node.execute_here(reserve(1)).await.unwrap_err();
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

    /// An HTTP server in front of `backend`, like the one `quorid serve`
    /// runs, and the task that runs it.
    async fn serve(
        backend: Arc<RaftBackend>,
    ) -> (String, tokio::task::JoinHandle<std::io::Result<()>>) {
        let allocator = Allocator::new(backend, AllocatorConfig::with_block_size(100));
        let app = crate::server::router(Arc::new(allocator), Format::Decimal);
        let listener = tokio::net::TcpListener::bind(any_local()).await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(std::future::IntoFuture::into_future(axum::serve(
            listener, app,
        )));
        (addr, task)
    }

    #[tokio::test]
    async fn followers_forward_writes_to_the_leader() {
        let cluster = Cluster::start(3);
        let leader = cluster.leader().await;
        let follower_id = (1..=3).find(|&id| id != leader).unwrap();
        let bystander = (1..=3)
            .find(|&id| id != leader && id != follower_id)
            .unwrap();

        // Every node has an HTTP server, and every node knows where they are.
        let mut fronts = HashMap::new();
        for (id, _) in cluster.running() {
            fronts.insert(
                id,
                serve(cluster.nodes[id as usize - 1].clone().unwrap()).await,
            );
        }
        let tell = |node: NodeId, about: NodeId, client: Option<&str>| {
            cluster.node(node).set_peer(Peer {
                id: about,
                raft_addr: cluster.node(about).local_addr(),
                client_addr: client.map(str::to_string),
            });
        };
        for node in 1..=3 {
            for about in (1..=3).filter(|&about| about != node) {
                tell(node, about, Some(&fronts[&about].0));
            }
        }
        let leader_front = fronts[&leader].0.clone();
        let follower = cluster.node(follower_id);

        // The follower has the leader reserve the block, and sees the same
        // outcomes: blocks, and refusals.
        assert_eq!(lease(follower, 10).await, IdRange::new(1, 11));
        let err = follower.execute(reserve(0)).await.unwrap_err();
        assert_eq!(err.to_string(), "invalid request: count must be at least 1");
        assert_eq!(lease(cluster.node(leader), 5).await, IdRange::new(11, 16));
        cluster.converge(16).await;

        // What was forwarded is not forwarded again.
        let err = follower.execute_here(reserve(1)).await.unwrap_err();
        assert_eq!(
            format!("{err:?}"),
            format!("NotLeader {{ leader: Some({leader_front:?}) }}")
        );

        // A follower that was sent to a node that is not the leader gets
        // pointed to the leader, and stops there.
        tell(follower_id, leader, Some(&fronts[&bystander].0));
        let err = follower.execute(reserve(1)).await.unwrap_err();
        assert_eq!(
            format!("{err:?}"),
            format!("NotLeader {{ leader: Some({leader_front:?}) }}")
        );
        assert_eq!(cluster.node(bystander).state().next_id(&users()), 16);

        // Without a client address for the leader there is nowhere to forward.
        tell(follower_id, leader, None);
        let err = follower.execute(reserve(1)).await.unwrap_err();
        assert_eq!(format!("{err:?}"), "NotLeader { leader: None }");

        // And a leader whose server is down cannot be reached.
        tell(follower_id, leader, Some(&leader_front));
        let (_, server) = fronts.remove(&leader).unwrap();
        server.abort();
        let _ = server.await;
        let err = follower.execute(reserve(1)).await.unwrap_err();
        assert!(
            err.to_string().starts_with(&format!(
                "backend unavailable: cannot reach the leader at {leader_front}"
            )),
            "{err}"
        );
        assert_eq!(cluster.node(leader).state().next_id(&users()), 16);
    }

    fn members(voters: &[NodeId]) -> Members {
        Members {
            voters: voters.to_vec(),
            learners: Vec::new(),
            config_pending: false,
        }
    }

    #[tokio::test]
    async fn a_node_joins_a_running_cluster_and_keeps_its_place() {
        let mut cluster = Cluster::start(3);
        let first = cluster.leader().await;
        assert_eq!(lease(cluster.node(first), 100).await, IdRange::new(1, 101));
        cluster.converge(101).await;

        let newcomer = cluster.join();
        let added = cluster.node(first).add_voter(newcomer.clone()).await;
        assert_eq!(added.unwrap(), members(&[1, 2, 3, 4]));

        // Everyone learned where it lives from the log: the others were never
        // told, and neither was the transport of any of them.
        for (id, node) in cluster.running().filter(|&(id, _)| id != 4) {
            wait_until("the address of the new node", || {
                let known = node.server.transport().peers().get(&4).copied();
                (known == Some(newcomer.raft_addr)).then_some(())
            })
            .await;
            assert_eq!(
                node.shared.client_addr(4).as_deref(),
                Some("node4:7070"),
                "{id}"
            );
        }
        // It caught up, and knows the voters.
        cluster.converge(101).await;
        wait_until("the voters", || {
            (cluster.node(4).status().members == Some(members(&[1, 2, 3, 4]))).then_some(())
        })
        .await;

        // It counts: three of the four still make a majority.
        cluster.stop(first);
        let second = wait_until("a new leader", || {
            (cluster.running())
                .find(|(_, node)| node.status().role == Role::Leader)
                .map(|(id, _)| id)
        })
        .await;
        assert_eq!(
            lease(cluster.node(second), 50).await,
            IdRange::new(101, 151)
        );
        cluster.converge(151).await;

        // And a restart takes the voters from the log, not from the command line.
        cluster.stop(4);
        cluster.restart(4);
        wait_until("the voters", || {
            (cluster.node(4).status().members == Some(members(&[1, 2, 3, 4]))).then_some(())
        })
        .await;
        cluster.converge(151).await;
    }

    #[tokio::test]
    async fn voters_can_be_removed_the_leader_included() {
        let cluster = Cluster::start(3);
        let first = cluster.leader().await;
        let others: Vec<NodeId> = (1..=3).filter(|&id| id != first).collect();
        let (second, third) = (others[0], others[1]);
        assert_eq!(lease(cluster.node(first), 10).await, IdRange::new(1, 11));

        // Only the leader changes the cluster, and only what is in it.
        let err = cluster.node(second).remove_voter(third).await.unwrap_err();
        assert_eq!(
            format!("{err:?}"),
            format!("NotLeader {{ leader: Some(\"node{first}:7070\") }}")
        );
        let err = cluster.node(first).remove_voter(9).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid request: membership change refused: not a voter"
        );
        let err = cluster
            .node(first)
            .add_voter(peer(third, cluster.node(third)))
            .await;
        assert_eq!(
            err.unwrap_err().to_string(),
            format!("invalid request: node {third} is already a voter")
        );

        let mut left = vec![first, third];
        left.sort();
        let removed = cluster.node(first).remove_voter(second).await;
        assert_eq!(removed.unwrap(), members(&left));
        assert_eq!(lease(cluster.node(first), 10).await, IdRange::new(11, 21));

        // The leader leaves: the one that is left takes over.
        let removed = cluster.node(first).remove_voter(first).await;
        assert_eq!(removed.unwrap(), members(&[third]));
        wait_until("the last voter to lead", || {
            (cluster.node(third).status().role == Role::Leader).then_some(())
        })
        .await;
        assert_eq!(lease(cluster.node(third), 5).await, IdRange::new(21, 26));
        let err = cluster.node(third).remove_voter(third).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid request: membership change refused: cannot remove the last voter"
        );
    }

    #[tokio::test]
    async fn a_node_that_cannot_be_reached_is_not_added() {
        let mut cluster = Cluster::start(3);
        let leader = cluster.leader().await;
        let backend = Arc::get_mut(cluster.nodes[leader as usize - 1].as_mut().unwrap()).unwrap();
        backend.timeout = Duration::from_millis(500);

        let nobody = Peer {
            id: 4,
            raft_addr: "127.0.0.1:9".parse().unwrap(),
            client_addr: None,
        };
        let err = backend.add_voter(nobody).await.unwrap_err();
        assert_eq!(err.to_string(), "backend unavailable: raft: timed out");

        // It is not left half added: not a voter, and no longer fed.
        assert_eq!(backend.members(), members(&[1, 2, 3]));
    }

    #[tokio::test]
    async fn a_cluster_without_a_leader_cannot_change() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RaftBackend::open(dir.path(), config(1, vec![1, 2, 3])).unwrap();
        let err = backend.remove_voter(2).await.unwrap_err();
        assert_eq!(format!("{err:?}"), "NotLeader { leader: None }");
        let nobody = peer(4, &backend);
        let err = backend.add_voter(nobody).await.unwrap_err();
        assert_eq!(format!("{err:?}"), "NotLeader { leader: None }");
    }

    #[tokio::test]
    async fn a_leader_without_a_quorum_times_out() {
        let mut cluster = Cluster::start(3);
        let leader = cluster.leader().await;
        for id in (1..=3).filter(|&id| id != leader) {
            cluster.stop(id);
        }

        // It keeps leading until it hears of a higher term, but cannot commit.
        let backend = Arc::get_mut(cluster.nodes[leader as usize - 1].as_mut().unwrap()).unwrap();
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
        assert_eq!(format!("{err:?}"), "NotLeader { leader: None }");
    }

    #[tokio::test]
    async fn leader_hints_come_from_the_peers_client_addresses() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RaftBackend::open(dir.path(), config(1, vec![1])).unwrap();
        let hint = |leader| format!("{:?}", backend.error(lumen_raft::Error::NotLeader(leader)));
        let unknown = "NotLeader { leader: None }";
        let mut other = peer(7, &backend);
        assert_eq!(hint(Some(7)), unknown);

        backend.set_peer(other.clone());
        assert_eq!(hint(Some(7)), r#"NotLeader { leader: Some("node7:7070") }"#);
        assert_eq!(hint(None), unknown);

        other.client_addr = None;
        backend.set_peer(other);
        assert_eq!(hint(Some(7)), unknown);
    }

    #[tokio::test]
    async fn other_raft_errors_are_not_about_leadership() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RaftBackend::open(dir.path(), config(1, vec![1])).unwrap();
        let err = backend.error(lumen_raft::Error::Storage("disk".into()));
        assert_eq!(err.to_string(), "storage error: disk");
    }

    #[test]
    fn every_node_draws_its_own_election_timeouts() {
        // This is how Raft turns the configured seed into its generator.
        let generators: std::collections::HashSet<u64> =
            (1..=1000).map(|id| (election_seed(id) ^ id) | 1).collect();
        assert_eq!(generators.len(), 1000);
    }

    #[test]
    fn raft_errors_become_quorid_errors() {
        use lumen_raft::Error as Raft;
        assert_eq!(
            convert(Raft::Storage("disk".into())).to_string(),
            "storage error: disk"
        );
        assert_eq!(
            convert(Raft::NotConfigured).to_string(),
            "invalid request: this node is not one of the voters, check --node-id and --voters"
        );
        let err = convert(Raft::Timeout).to_string();
        assert_eq!(err, "backend unavailable: raft: timed out");
        assert_eq!(
            convert(Raft::BadConfig("not a voter")).to_string(),
            "invalid request: membership change refused: not a voter"
        );
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

        // A node that joins has no voters to give.
        let mut joining = config(1, vec![1]);
        joining.join = true;
        let err = open_error(dir.path(), joining);
        assert_eq!(
            err,
            "invalid request: a node that joins takes no voters: the cluster tells it who they are"
        );

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

    fn node(id: NodeId, client: &str) -> Peer {
        Peer {
            id,
            raft_addr: format!("127.0.0.1:{}", 9000 + id).parse().unwrap(),
            client_addr: Some(client.into()),
        }
    }

    #[test]
    fn entries_tell_where_nodes_live() {
        let (mut app, shared, mut outcome) = waiting_app(0);
        let entry = |index, peer: &Peer| {
            let peer = peer.clone();
            LogEntry::new(index, 1, serde_json::to_vec(&Entry::Peer { peer }).unwrap())
        };

        let mut five = node(5, "node5:7070");
        app.apply(&entry(1, &five)).unwrap();
        assert_eq!(shared.client_addr(5).as_deref(), Some("node5:7070"));
        // Again changes nothing; a node that moved is where it says now.
        app.apply(&entry(2, &five)).unwrap();
        five.client_addr = Some("elsewhere:7070".into());
        app.apply(&entry(3, &five)).unwrap();
        assert_eq!(shared.address_book(), [five]);

        // That was no request.
        assert!(outcome.try_recv().is_err());
        assert_eq!(shared.state.lock().unwrap().next_id(&users()), 1);
    }

    #[test]
    fn snapshots_roundtrip_through_the_state_machine() {
        let (mut app, shared, _outcome) = waiting_app(0);
        app.apply(&entry(1, 2, 0, 0, 40)).unwrap();
        let (three, four) = (node(3, "node3:7070"), node(4, "node4:7070"));
        shared.learn(three.clone());
        let data = app.snapshot().unwrap();

        let (mut other, other_shared, _outcome) = waiting_app(0);
        // Something the command line said, which the snapshot does not know.
        other_shared.learn(four.clone());
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
        assert_eq!(other_shared.address_book(), [three, four]);

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
    fn snapshots_from_before_the_address_book_still_restore() {
        let (mut app, shared, _outcome) = waiting_app(0);
        let state = IdState::restore(br#"{"version":1,"next":{"users":"7"}}"#).unwrap();
        let old = Snapshot {
            index: 1,
            term: 1,
            voters: vec![1],
            data: state.snapshot(),
        };
        app.restore(&old).unwrap();
        assert_eq!(shared.state.lock().unwrap().next_id(&users()), 7);
        assert!(shared.address_book().is_empty());
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
