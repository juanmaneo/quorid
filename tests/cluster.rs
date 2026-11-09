//! End-to-end tests of a Raft cluster: real `quorid` processes talking over
//! real sockets, some of them killed.

mod common;

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::time::{Duration, Instant};

use common::{Server, http, http_body, http_with, split};
use serde_json::Value;

const DEADLINE: Duration = Duration::from_secs(30);

/// A free port, free until somebody else takes it.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn addrs() -> (String, String) {
    (
        format!("127.0.0.1:{}", free_port()),
        format!("127.0.0.1:{}", free_port()),
    )
}

/// Nodes `1..=size` of one cluster, each with the same flags after a restart,
/// and the nodes added to it later, which join instead of having voters.
struct Cluster {
    dir: tempfile::TempDir,
    /// How many nodes the cluster started with.
    size: usize,
    /// `(client address, Raft address)` of every node.
    addrs: Vec<(String, String)>,
    nodes: Vec<Option<Server>>,
}

impl Cluster {
    fn start(size: usize) -> Cluster {
        let mut cluster = Cluster {
            dir: tempfile::tempdir().unwrap(),
            size,
            addrs: (0..size).map(|_| addrs()).collect(),
            nodes: Vec::new(),
        };
        for id in 1..=size {
            let node = cluster.spawn(id);
            cluster.nodes.push(Some(node));
        }
        cluster
    }

    /// Starts one more node, which waits to be added to the cluster.
    fn start_joiner(&mut self) -> usize {
        self.addrs.push(addrs());
        let id = self.addrs.len();
        let node = self.spawn(id);
        self.nodes.push(Some(node));
        id
    }

    /// The JSON that adds node `id` through the API.
    fn describe(&self, id: usize) -> String {
        let (client, raft) = &self.addrs[id - 1];
        format!(r#"{{"id":{id},"raft_addr":"{raft}","client_addr":"{client}"}}"#)
    }

    fn spawn(&self, id: usize) -> Server {
        let (client, raft) = &self.addrs[id - 1];
        let voters = if id > self.size {
            vec!["--join".to_string()]
        } else {
            let voters = (1..=self.size).map(|id| id.to_string());
            vec!["--voters".to_string(), voters.collect::<Vec<_>>().join(",")]
        };
        let data = self.dir.path().join(format!("node{id}"));
        let mut args = vec![
            "--backend".to_string(),
            "raft".into(),
            "--node-id".into(),
            id.to_string(),
            "--raft-listen".into(),
            raft.clone(),
            "--data-dir".into(),
            data.to_str().unwrap().into(),
            "--block-size".into(),
            "10".into(),
            "--format".into(),
            "decimal".into(),
        ];
        args.extend(voters);
        for (i, (client, raft)) in self.addrs.iter().enumerate() {
            if i + 1 != id {
                args.push("--peer".into());
                args.push(format!("{}={raft}@{client}", i + 1));
            }
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        Server::start_on(client, &args)
    }

    fn client_addr(&self, id: usize) -> &str {
        &self.addrs[id - 1].0
    }

    fn kill(&mut self, id: usize) {
        self.nodes[id - 1].take().expect("the node is down").kill();
    }

    fn restart(&mut self, id: usize) {
        assert!(self.nodes[id - 1].is_none(), "the node is running");
        self.nodes[id - 1] = Some(self.spawn(id));
    }

    fn running(&self) -> impl Iterator<Item = usize> + '_ {
        (1..=self.nodes.len()).filter(|&id| self.nodes[id - 1].is_some())
    }

    /// The running node that leads, once there is exactly one.
    fn wait_for_leader(&self) -> usize {
        let start = Instant::now();
        loop {
            let leaders: Vec<usize> = self
                .running()
                .filter(|&id| status(self.client_addr(id))["backend"]["role"] == "leader")
                .collect();
            if let [leader] = leaders[..] {
                return leader;
            }
            assert!(start.elapsed() < DEADLINE, "no single leader: {leaders:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Waits until every running node but `leader` follows `leader`.
    fn wait_for_followers(&self, leader: usize) {
        let start = Instant::now();
        loop {
            let following = self.running().filter(|&id| id != leader).all(|id| {
                status(self.client_addr(id))["backend"]["leader"] == self.client_addr(leader)
            });
            if following {
                return;
            }
            assert!(
                start.elapsed() < DEADLINE,
                "the nodes do not follow {leader}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn status(addr: &str) -> Value {
    let response = http(addr, "GET", "/v1/status");
    let (code, body) = split(&response);
    assert_eq!(code, 200, "{response}");
    serde_json::from_str(body).unwrap()
}

/// IDs from `addr`, or the error it answered.
fn allocate(addr: &str, count: u64) -> Result<Vec<u128>, (u16, Value)> {
    let response = http(addr, "POST", &format!("/v1/ns/users/ids?count={count}"));
    let (code, body) = split(&response);
    let body: Value = serde_json::from_str(body).unwrap();
    if code != 200 {
        return Err((code, body));
    }
    Ok(body["ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().parse().unwrap())
        .collect())
}

/// Asks `addr` for a block of IDs the way a follower asks the leader: with
/// the header that forbids passing the request on again.
fn block_without_forwarding(addr: &str) -> (u16, Value) {
    let response = http_with(
        addr,
        "POST",
        "/v1/ns/users/blocks?size=1",
        &[("x-quorid-forwarded", "1")],
    );
    let (code, body) = split(&response);
    (code, serde_json::from_str(body).unwrap())
}

/// The IDs of every successful call, to check that no ID is handed out twice
/// and that later calls get greater ones.
#[derive(Default)]
struct Issued {
    all: BTreeSet<u128>,
}

impl Issued {
    /// Records the IDs, none of which may have been issued before.
    fn add(&mut self, ids: &[u128]) {
        assert!(!ids.is_empty());
        for id in ids {
            assert!(self.all.insert(*id), "ID {id} was issued twice");
        }
    }

    /// Whether every ID is greater than all the earlier ones.
    fn after(&self, ids: &[u128]) -> bool {
        let last = self.all.last().copied().unwrap_or(0);
        ids.iter().all(|&id| id > last)
    }
}

#[test]
fn a_cluster_survives_losing_its_leader() {
    let mut cluster = Cluster::start(3);
    let mut issued = Issued::default();

    let leader = cluster.wait_for_leader();
    cluster.wait_for_followers(leader);
    let ids = allocate(cluster.client_addr(leader), 3).unwrap();
    assert_eq!(ids, [1, 2, 3]);
    issued.add(&ids);

    // A follower gets its blocks from the leader, so it serves too.
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    let ids = allocate(cluster.client_addr(follower), 1).unwrap();
    assert!(issued.after(&ids), "{ids:?}");
    issued.add(&ids);
    // The leader is the only one to take a block directly: a follower that is
    // asked on behalf of another one sends it to the leader instead.
    let (code, body) = block_without_forwarding(cluster.client_addr(follower));
    assert_eq!(code, 421);
    assert_eq!(body["error"], "not_leader");
    assert_eq!(body["leader"], cluster.client_addr(leader));
    let (code, _) = block_without_forwarding(cluster.client_addr(leader));
    assert_eq!(code, 200);

    // The leader dies, with the rest of its first block unused.
    cluster.kill(leader);
    let second = cluster.wait_for_leader();
    assert_ne!(second, leader);
    let ids = allocate(cluster.client_addr(second), 15).unwrap();
    assert!(issued.after(&ids), "{ids:?}");
    issued.add(&ids);

    // It comes back as a follower: it serves through the new leader, and
    // never from the block it held when it died.
    cluster.restart(leader);
    cluster.wait_for_followers(second);
    let ids = allocate(cluster.client_addr(second), 15).unwrap();
    assert!(issued.after(&ids), "{ids:?}");
    issued.add(&ids);
    let ids = allocate(cluster.client_addr(leader), 1).unwrap();
    assert!(issued.after(&ids), "{ids:?}");
    issued.add(&ids);
}

#[test]
fn a_cluster_keeps_its_state_when_every_node_restarts() {
    let mut cluster = Cluster::start(3);
    let mut issued = Issued::default();

    let leader = cluster.wait_for_leader();
    for _ in 0..3 {
        let ids = allocate(cluster.client_addr(leader), 25).unwrap();
        assert!(issued.after(&ids), "{ids:?}");
        issued.add(&ids);
    }

    for id in 1..=3 {
        cluster.kill(id);
    }
    for id in 1..=3 {
        cluster.restart(id);
    }

    let leader = cluster.wait_for_leader();
    cluster.wait_for_followers(leader);
    let ids = allocate(cluster.client_addr(leader), 25).unwrap();
    assert!(issued.after(&ids), "{ids:?}");
    issued.add(&ids);
    assert_eq!(issued.all.len(), 100);
}

#[test]
fn a_node_that_lost_its_majority_does_not_serve() {
    let mut cluster = Cluster::start(3);
    let leader = cluster.wait_for_leader();
    cluster.wait_for_followers(leader);
    allocate(cluster.client_addr(leader), 1).unwrap();

    let others: Vec<usize> = (1..=3).filter(|&id| id != leader).collect();
    for id in others {
        cluster.kill(id);
    }
    // The first block is mostly unused, so IDs are still served from memory,
    // but a new block cannot be committed without a majority.
    let ids = allocate(cluster.client_addr(leader), 5).unwrap();
    assert_eq!(ids, [2, 3, 4, 5, 6]);
    let (code, _) = allocate(cluster.client_addr(leader), 20).unwrap_err();
    assert_eq!(code, 503);
}

/// The ids of the voters that `addr` knows of.
fn voters(addr: &str) -> Vec<u64> {
    status(addr)["backend"]["members"]["voters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_u64().unwrap())
        .collect()
}

/// Calls the cluster API of `addr`, and returns what it answered.
fn change(addr: &str, method: &str, path: &str, body: &str) -> (u16, Value) {
    let response = http_body(addr, method, path, body);
    let (code, body) = split(&response);
    (code, serde_json::from_str(body).unwrap())
}

fn wait_for_voters(addr: &str, wanted: &[u64]) {
    let start = Instant::now();
    while voters(addr) != wanted {
        assert!(
            start.elapsed() < DEADLINE,
            "the voters of {addr} are not {wanted:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_cluster_takes_in_a_node_and_lets_one_go() {
    let mut cluster = Cluster::start(3);
    let mut issued = Issued::default();

    let leader = cluster.wait_for_leader();
    cluster.wait_for_followers(leader);
    let ids = allocate(cluster.client_addr(leader), 3).unwrap();
    issued.add(&ids);

    // A fourth node starts, and waits to be told it is one of the cluster.
    let joiner = cluster.start_joiner();
    assert_eq!(joiner, 4);
    let node = cluster.describe(joiner);
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    let (code, body) = change(
        cluster.client_addr(follower),
        "POST",
        "/v1/cluster/voters",
        &node,
    );
    assert_eq!(code, 421);
    assert_eq!(body["leader"], cluster.client_addr(leader));

    let (code, body) = change(
        cluster.client_addr(leader),
        "POST",
        "/v1/cluster/voters",
        &node,
    );
    assert_eq!(
        (code, &body["voters"]),
        (200, &serde_json::json!([1, 2, 3, 4]))
    );
    let (code, body) = change(
        cluster.client_addr(leader),
        "POST",
        "/v1/cluster/voters",
        &node,
    );
    assert_eq!(code, 400);
    assert_eq!(
        body["message"],
        "invalid request: node 4 is already a voter"
    );

    // It follows, serves through the leader, and still does after a restart.
    wait_for_voters(cluster.client_addr(joiner), &[1, 2, 3, 4]);
    cluster.wait_for_followers(leader);
    let ids = allocate(cluster.client_addr(joiner), 5).unwrap();
    assert!(issued.after(&ids), "{ids:?}");
    issued.add(&ids);
    cluster.kill(joiner);
    cluster.restart(joiner);
    wait_for_voters(cluster.client_addr(joiner), &[1, 2, 3, 4]);
    cluster.wait_for_followers(leader);
    let ids = allocate(cluster.client_addr(joiner), 5).unwrap();
    assert!(issued.after(&ids), "{ids:?}");
    issued.add(&ids);

    // Three of the four are a majority: the cluster outlives its leader,
    // which is then taken out so that the rest does not wait for it.
    cluster.kill(leader);
    let second = cluster.wait_for_leader();
    let ids = allocate(cluster.client_addr(second), 15).unwrap();
    assert!(issued.after(&ids), "{ids:?}");
    issued.add(&ids);
    let path = format!("/v1/cluster/voters/{leader}");
    let (code, body) = change(cluster.client_addr(second), "DELETE", &path, "");
    let rest: Vec<u64> = (1..=4).filter(|&id| id != leader as u64).collect();
    assert_eq!((code, &body["voters"]), (200, &serde_json::json!(rest)));
    let (code, _) = change(cluster.client_addr(second), "DELETE", &path, "");
    assert_eq!(code, 400);

    for id in cluster.running().collect::<Vec<_>>() {
        wait_for_voters(cluster.client_addr(id), &rest);
        // Each node hands out what is left of its own block, so the IDs are
        // unique but not in order across nodes.
        issued.add(&allocate(cluster.client_addr(id), 1).unwrap());
    }
}
