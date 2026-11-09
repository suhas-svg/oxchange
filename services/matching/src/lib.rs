//! Oxchange matching engine service.
//!
//! This crate holds the partition [`Engine`] (all order books of one Kafka
//! partition, the input offset, and the duplicate-command memory) and its
//! [`SnapshotStore`]. The Kafka reader/writer threads and the gRPC `GetBook`
//! server come next and wrap this logic without changing it.
//!
//! Recovery (spec: "Matching Engine → Recovery"):
//! 1. Load the newest snapshot with a valid checksum ([`recover`]).
//! 2. Reread commands from [`Engine::next_offset`].
//! 3. Republish the resulting events; they have identical IDs and seqs
//!    because the book is deterministic.
//! 4. Consumers ignore everything they already processed.
//!
//! The Kafka layer must check that the topic still holds `next_offset` before
//! replaying. If retention has already deleted it, replay would silently skip
//! commands, so the engine must halt instead.

pub mod codec;
mod dedup;
mod engine;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod pipeline;
pub mod snapshot;

use std::io;
use std::path::PathBuf;

pub use dedup::{DEFAULT_DEDUP_CAPACITY, DedupWindow};
pub use engine::{Engine, EngineError, EngineState, FromStateError, ProcessOutcome};
pub use snapshot::{Skipped, SnapshotStore};

/// What [`recover`] found.
#[derive(Debug)]
pub struct Recovery {
    pub engine: Engine,
    /// The snapshot the engine was rebuilt from, or `None` for a fresh start.
    pub snapshot: Option<PathBuf>,
    /// Newer snapshots that were unusable (corrupt, truncated, wrong partition).
    pub skipped: Vec<Skipped>,
}

#[derive(Debug, thiserror::Error)]
pub enum RecoverError {
    #[error("reading snapshots: {0}")]
    Io(#[from] io::Error),
    /// The checksum passed but the contents describe an impossible book. That
    /// points at a bug, not disk damage, so recovery stops rather than guess.
    #[error("snapshot {path} is valid on disk but cannot be restored: {source}")]
    Restore {
        path: PathBuf,
        #[source]
        source: FromStateError,
    },
}

/// Rebuilds the engine for `store`'s partition from its newest usable snapshot,
/// or starts fresh at offset 0 if there is none. Replay from
/// [`Engine::next_offset`] afterwards.
pub fn recover(
    store: &SnapshotStore,
    partition: u32,
    dedup_capacity: usize,
) -> Result<Recovery, RecoverError> {
    let (loaded, skipped) = store.load_latest()?;
    let Some(loaded) = loaded else {
        return Ok(Recovery {
            engine: Engine::new(partition, dedup_capacity),
            snapshot: None,
            skipped,
        });
    };
    let engine = Engine::from_state(loaded.state, dedup_capacity).map_err(|source| {
        RecoverError::Restore {
            path: loaded.path.clone(),
            source,
        }
    })?;
    Ok(Recovery {
        engine,
        snapshot: Some(loaded.path),
        skipped,
    })
}
