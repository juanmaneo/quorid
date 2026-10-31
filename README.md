# quorid

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

```mermaid
flowchart TD
    C[Clients] -->|POST /v1/ns/users/ids| S1[quorid server A]
    C --> S2[quorid server B]
    S1 --> A1[Allocator: blocks in memory]
    S2 --> A2[Allocator: blocks in memory]
    A1 -->|Reserve users, 1000| BK[Backend trait]
    A2 -->|Reserve users, 1000| BK
    BK --> M[MemoryBackend]
    BK --> F[FileBackend]
    BK --> R[Raft backend - planned]
    R --> SM[IdState: next free id per namespace]
    F --> SM
    M --> SM
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
`QUORID_DATA_DIR`, `QUORID_BLOCK_SIZE`, `QUORID_FORMAT`. Logs via `RUST_LOG`.

## HTTP API

IDs are always JSON **strings**: JSON numbers can't carry 128 bits.

| method | path                        | query                                   |
|--------|-----------------------------|-----------------------------------------|
| POST   | `/v1/ns/{namespace}/ids`    | `count` (1..=10000), `format`, `padded` |
| POST   | `/v1/ns/{namespace}/blocks` | `size` (default: block size)            |
| GET    | `/v1/decode/{value}`        | `format`                                |
| GET    | `/v1/status`                |                                         |
| GET    | `/healthz`                  |                                         |

`/blocks` hands a raw range (`{"start":"101","end":"1000101",...}`, half-open)
to clients that want to allocate locally themselves.

Errors are `{"error": "...", "message": "..."}` with status 400 (bad
namespace/count/ID), 409 (namespace exhausted), 421 (`not_leader`, with a
`leader` hint), 503 (unavailable), 500 (storage).

Namespaces: 1–64 chars of `[a-z0-9_.-]`, starting with `[a-z0-9]`.

## Backends

| backend  | durable | replicated | notes                                       |
|----------|:-------:|:----------:|---------------------------------------------|
| `memory` | no      | no         | tests and demos                             |
| `file`   | yes     | no         | atomic write + fsync per block, `LOCK` file |
| Raft     | yes     | yes        | planned, e.g. on Lumen's `raft` crate       |

### Writing a backend

```rust
pub trait Backend: Send + Sync + 'static {
    fn execute(&self, command: Command) -> impl Future<Output = Result<Outcome>> + Send;
    fn status(&self) -> BackendStatus;
}
```

A Raft backend: `command.to_bytes()` → propose → when the entry commits, decode
it with `Command::from_bytes`, apply with `IdState::apply`, resolve the waiting
caller. Followers return `Error::NotLeader { leader }` (or forward). Snapshots
are `IdState::snapshot()` / `IdState::restore()`. Read the contract on
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
