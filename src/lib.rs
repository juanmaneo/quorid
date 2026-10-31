//! # quorid
//!
//! Globally unique 128-bit IDs handed out by a replicated state machine.
//!
//! The design is the classic *segment* (Hi/Lo) allocator:
//!
//! * A [`Backend`] replicates a tiny state machine ([`IdState`]): for every
//!   [`Namespace`] it stores the next free ID. The only write is
//!   [`Command::Reserve`], which hands out a contiguous [`IdRange`].
//! * Each server runs an [`Allocator`] that leases *blocks* of IDs from the
//!   backend and serves individual IDs from memory, so consensus is paid once
//!   per block, not once per ID.
//! * IDs are rendered with a pluggable [`Codec`] (base32, base62, emoji, ...).
//!
//! Guarantees: IDs are unique per namespace and never `0`. They are increasing
//! per server, roughly ordered across servers, and *not* gap-free (unused
//! leased IDs are dropped on restart).
//!
//! Backends are swappable: anything that can linearizably apply
//! [`Command`]s to an [`IdState`] can implement [`Backend`] (an in-memory map,
//! a fsync'd file, or a Raft cluster).

pub mod allocator;
pub mod backend;
pub mod codec;
pub mod command;
pub mod error;
pub mod id;
pub mod server;
pub mod state;

pub use allocator::{Allocator, AllocatorConfig};
pub use backend::{Backend, BackendStatus, FileBackend, MemoryBackend, Role};
pub use codec::{AlphabetCodec, Codec, DecodeError, Format};
pub use command::{Command, Outcome};
pub use error::{Error, Result};
pub use id::{FIRST_ID, Id, IdRange, InvalidNamespace, Namespace};
pub use state::IdState;
