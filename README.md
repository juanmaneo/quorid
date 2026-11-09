# quorid

[![CI](https://github.com/juanmaneo/quorid/actions/workflows/ci.yml/badge.svg)](https://github.com/juanmaneo/quorid/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Quorum + ID**: globally unique **128-bit IDs** served by a replicated state
machine with a **pluggable consensus backend**, rendered in compact text
encodings (ASCII or emoji).

```text
$ curl -XPOST 'localhost:7070/v1/ns/users/ids?count=3'
{"namespace":"users","format":"mania","ids":["🤬","🤣","🤗"]}

$ curl -XPOST 'localhost:7070/v1/ns/users/ids?count=2&format=base62'
{"namespace":"users","format":"base62","ids":["4","5"]}
```

## How it works

```text
                        Clients
                           |
                 POST /v1/ns/users/ids
                           |
               +-----------+-----------+
               |                       |
               v                       v
     +-------------------+   +-------------------+
     | quorid server A   |   | quorid server B   |
     +-------------------+   +-------------------+
     | Allocator:        |   | Allocator:        |
     | blocks in memory  |   | blocks in memory  |
     +---------+---------+   +---------+---------+
               |                       |
               +-----------+-----------+
                           |
                 Reserve(users, 1000)
                           |
                           v
                   +---------------+
                   | Backend trait |
                   +-------+-------+
                           |
        +------------------+------------------+
        |                  |                  |
        v                  v                  v
+---------------+  +---------------+  +---------------+
| MemoryBackend |  | FileBackend   |  | RaftBackend   |
| (volatile)    |  | (durable)     |  | (replicated)  |
+-------+-------+  +-------+-------+  +-------+-------+
        |                  |                  |
        +------------------+------------------+
                           |
                           v
        +-------------------------------------+
        | IdState: next free ID per namespace |
        +-------------------------------------+
```

* **State machine** (`IdState`): for each namespace, the next free ID. The only
  write is `Command::Reserve { namespace, count }`, which returns the range
  `start..start+count`. It is deterministic, so every replica agrees.
* **Backend** (`trait Backend`): replicates and applies commands
  linearizably. Swap it without touching anything else.
* **Allocator**: the segment (Hi/Lo) pattern. Each server leases blocks of
  `--block-size` IDs and serves them from memory, prefetching the next block
  in the background. Consensus is paid once per block, not once per ID.

### Guarantees

| property                         |                                                           |
|----------------------------------|-----------------------------------------------------------|
| unique per namespace             | yes, never `0`                                            |
| increasing                       | per server and namespace; roughly ordered across servers  |
| gap-free                         | **no**: leased but unused IDs are skipped after a restart |
| retries / timeouts               | safe: worst case a range is skipped, never reused         |

## Encodings

All formats are reversible bijections with `u128`. `padded=true` gives a
fixed width, which sorts lexicographically like the number for ASCII formats.

| format    | alphabet                          | max chars | sortable |
|-----------|-----------------------------------|----------:|:--------:|
| `decimal` | `0-9`                             | 39        | yes      |
| `base32`  | Crockford (case-insensitive, I/L→1, O→0) | 26 | yes      |
| `base62`  | `0-9A-Za-z`                       | 22        | yes      |
| `fruity`  | 16 fruits, i.e. hex               | 32        | no       |
| `mania`   | 95 emoji (default)                | 20        | no       |

`fruity` and `mania` reuse the alphabets of
[mania-id](https://github.com/juanmaneo/mania-id), turned from random strings
into reversible encodings. Custom alphabets: `AlphabetCodec::new(name, "...")`.

```text
$ quorid encode 340282366920938463463374607431768211455
🤫🤬🍹🎻🥝🦏🍈🍐🤩😤🥪🍷🍫🍎🥪🍎🤧🚒🤬🍭
$ quorid decode 7n42DGM5Tflk9n8mt7Fhc7 -f base62
 decimal  340282366920938463463374607431768211455
  base32  7ZZZZZZZZZZZZZZZZZZZZZZZZZ
  ...
```

## Running

```sh
cargo run -- serve                        # file backend in ./quorid-data, port 7070
cargo run -- serve --backend memory       # volatile, for demos
cargo run -- serve --block-size 10000 --format base32 --listen 0.0.0.0:7070
```

Every flag has an env var: `QUORID_LISTEN`, `QUORID_BACKEND`,
`QUORID_DATA_DIR`, `QUORID_BLOCK_SIZE`, `QUORID_FORMAT`, and for the `raft`
backend `QUORID_NODE_ID`, `QUORID_VOTERS`, `QUORID_JOIN`, `QUORID_RAFT_LISTEN`,
`QUORID_PEERS`, `QUORID_TIMEOUT`. Logs via `RUST_LOG`.

### Running a cluster

The `raft` backend replicates the state over [LumenDB](https://github.com/juanmaneo/LumenDB)'s
`raft` crate. Start one node per machine, with the same `--voters` everywhere
and the other nodes as `--peer ID=RAFT_ADDR[@CLIENT_ADDR]`:

```sh
# on 10.0.0.1 (and likewise on the others, with their own --node-id)
quorid serve --backend raft --node-id 1 --voters 1,2,3 \
  --listen 0.0.0.0:7070 --raft-listen 0.0.0.0:7171 \
  --peer 2=10.0.0.2:7171@10.0.0.2:7070 \
  --peer 3=10.0.0.3:7171@10.0.0.3:7070
```

* The Raft log and snapshots live in `<data-dir>/raft`. A single node
  (`--node-id 1 --voters 1`) is a valid cluster of one.
* **Any node serves IDs, only the leader reserves blocks.** A follower that
  needs a new block asks the leader for it over HTTP, at the leader's
  `CLIENT_ADDR` (so give every peer one), and hands out IDs from it. Those
  requests carry an `x-quorid-forwarded` header, and a node that receives one
  never forwards it again: it serves the block or answers `421 not_leader`.
  That is also what a follower answers when it cannot forward, because it has
  no leader yet or does not know the leader's `CLIENT_ADDR`; the `leader` hint
  of the 421 (also in `/v1/status`) tells clients where to retry. The `--peer`
  list may include the node itself.
* `--voters` only starts a cluster. Afterwards the voters are changed through
  the API (below) and live in the Raft log, which a restarted node trusts over
  its command line.
* A block is committed after a majority has it on disk, so reserving one costs
  a few ticks (20 ms each by default). The segment allocator pays that once
  per `--block-size` IDs, not per ID.
* The Raft transport has no authentication or encryption: use a trusted
  network.
* A node that cannot commit (it lost its majority, say) answers 503 after a
  timeout. The block may still be committed later, which only skips IDs.
* `--timeout` is that timeout, in seconds (5 by default). It also bounds a
  follower's request to the leader and a change of the cluster, so give every
  node the same value.

#### Changing the cluster

Start the new node first, as a node that waits to be told it is part of a
cluster (`--join` instead of `--voters`), and with `--peer` for the nodes it
has to reach:

```sh
# on 10.0.0.4
quorid serve --backend raft --node-id 4 --join \
  --listen 0.0.0.0:7070 --raft-listen 0.0.0.0:7171 \
  --peer 1=10.0.0.1:7171@10.0.0.1:7070 \
  --peer 2=10.0.0.2:7171@10.0.0.2:7070 \
  --peer 3=10.0.0.3:7171@10.0.0.3:7070

# then, on the leader, say where it lives (the addresses the others reach it at)
curl -X POST http://10.0.0.1:7070/v1/cluster/voters \
  -d '{"id":4,"raft_addr":"10.0.0.4:7171","client_addr":"10.0.0.4:7070"}'

# and later, to take a node out (a dead one included)
curl -X DELETE http://10.0.0.1:7070/v1/cluster/voters/3
```

Both answer with the new membership once the change is committed, and
`/v1/status` shows it under `backend.members` (`voters`, `learners`, and
`config_pending` while a change is under way).

* Only the leader changes the cluster. Any other node answers `421
  not_leader` with the leader as the hint: these calls are not forwarded.
* The leader gives every node where the new one lives through the log (and the
  snapshots), so they need no `--peer` for it. A node that is added keeps
  `--join` when it restarts, and its voters come from its log.
* The new node first receives the log as a learner, which does not count
  toward any majority, and becomes a voter once it has caught up. If it does
  not within `--timeout` (it is not running, say, or its state is big and the
  link slow: raise the timeout of the leader), the call fails with 503 and the
  membership is as it was. One change is made at a time.
* The leader may remove itself: it steps down once that is committed.
* A change needs a majority, so this does not bring back a cluster that lost
  one.
* Do not give a node the ID of one that was removed, unless its data directory
  is empty.

## HTTP API

IDs are always JSON **strings**: JSON numbers can't carry 128 bits.

| method | path                        | query / body                                |
|--------|-----------------------------|---------------------------------------------|
| POST   | `/v1/ns/{namespace}/ids`    | `count` (1..=10000), `format`, `padded`     |
| POST   | `/v1/ns/{namespace}/blocks` | `size` (default: block size)                |
| GET    | `/v1/decode/{value}`        | `format`                                    |
| GET    | `/v1/status`                |                                             |
| GET    | `/healthz`                  |                                             |
| POST   | `/v1/cluster/voters`        | JSON body: `id`, `raft_addr`, `client_addr` |
| DELETE | `/v1/cluster/voters/{id}`   |                                             |

`/blocks` hands a raw range (`{"start":"101","end":"1000101",...}`, half-open)
to clients that want to allocate locally themselves. On a replicated backend a
follower forwards it to the leader, unless the request has the
`x-quorid-forwarded` header (see above). The `/v1/cluster/voters` calls
change the cluster of a replicated backend (see "Changing the cluster"); the
others answer 400 `invalid_request`.

Errors are `{"error": "...", "message": "..."}` with status 400 (bad
namespace/count/ID), 409 (namespace exhausted), 421 (`not_leader`, with a
`leader` hint), 503 (unavailable), 500 (storage).

Namespaces: 1–64 chars of `[a-z0-9_.-]`, starting with `[a-z0-9]`.

## Backends

| backend  | durable | replicated | notes                                       |
|----------|:-------:|:----------:|---------------------------------------------|
| `memory` | no      | no         | tests and demos                             |
| `file`   | yes     | no         | atomic write + fsync per block, `LOCK` file |
| `raft`   | yes     | yes        | LumenDB's `raft` crate; followers forward   |

The `raft` backend depends on LumenDB through git (`Cargo.lock` pins the
commit), which `cargo publish` to crates.io does not accept.

### Writing a backend

```rust
pub trait Backend: Send + Sync + 'static {
    fn execute(&self, command: Command) -> impl Future<Output = Result<Outcome>> + Send;
    fn execute_here(&self, command: Command) -> impl Future<Output = Result<Outcome>> + Send; // defaults to execute
    fn add_voter(&self, peer: Peer) -> impl Future<Output = Result<Members>> + Send; // defaults to a refusal
    fn remove_voter(&self, id: u64) -> impl Future<Output = Result<Members>> + Send; // likewise
    fn status(&self) -> BackendStatus;
}
```

`RaftBackend` (`src/backend/raft.rs`) is the reference for a replicated one:
propose the command → when the entry commits, apply it with `IdState::apply`
→ resolve the waiting caller. Followers return `Error::NotLeader { leader }`
or forward the command to the leader (`execute`); `execute_here` is what
serves a command that was already forwarded, and must never forward it again.
Snapshots are `IdState::snapshot()` / `IdState::restore()`.
Match a caller to its own entry with a unique tag (node, run, sequence) rather
than the log index: after a leader change another command can sit at that
index, and the wrong caller would get its range. Read the contract on
`Backend` in `src/backend/mod.rs`, notably: a failed `execute` may still have
been applied, and that's fine for IDs.

## Development

```sh
cargo test
cargo clippy --all-targets
cargo coverage               # tests + coverage report, fails below 100%
```

Automation follows the [cargo-xtask](https://github.com/matklad/cargo-xtask)
pattern: tasks live in the dependency-free `xtask` crate, and
`.cargo/config.toml` exposes them as cargo subcommands.

* `cargo coverage [--fail-under <PCT>] [--fail-under-regions <PCT>] [--html]`
  — run the tests under `cargo-llvm-cov` (fallback: `cargo-tarpaulin`) and
  print the report. It exits non-zero below the line (`--fail-under`) or
  region (`--fail-under-regions`, llvm-cov only) percentage; the alias sets
  both to `100`. `--html` also writes an HTML report under `target/`, and any
  other flags pass through to the tool, e.g.
  `cargo coverage --show-missing-lines`.

Line, region and function coverage are kept at 100%. `tests/cli.rs` runs the
real binary, so `main.rs` is measured too. Error paths are tested by making
the OS fail for real, e.g. a directory where a file is expected.

CI (`.github/workflows/ci.yml`) runs the same three checks on every push and
pull request: format + clippy, tests on Linux and macOS, and the coverage gate.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
