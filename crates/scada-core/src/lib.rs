//! `scada-core`: the pure kernel of the SCADA engine (VS-001).
//!
//! The crate contains the static configuration model and its validation, the
//! in-memory real-time database (RTDB), the normalized kernel operations and the
//! deterministic transition function [`Kernel::apply`].
//!
//! It performs no I/O, reads no clock, spawns no thread and uses no randomness.
//! It depends on nothing but `blake3` (pure-Rust build) for the state digest.
//! See `docs/contract.md` and `docs/adr/0002-kernel-state.md`.
#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic
    )
)]

mod config;
mod digest;
mod kernel;
mod types;

pub use config::{
    ChannelSpec, ConfigError, ConfigSpec, CoreLimits, SourceMeta, SourceSpec, TagMeta, TagSpec, ValidatedConfig,
};
pub use kernel::{
    AppliedKind, ApplyError, IgnoredReason, Kernel, LastBatchSnapshot, LinkState, LogicalSnapshot, Observation,
    ObservationBatch, Operation, SequencedInput, SourceSnapshot, SourceStatus, TagSnapshot, TransitionOutcome,
};
pub use types::{BatchMode, ChannelId, Continuity, Quality, SourceId, TagId, UncertainReason, Value, ValueType};

/// Version of the canonical encoding used by [`Kernel::state_digest`].
pub const STATE_DIGEST_VERSION: u16 = 1;
/// Version of the canonical encoding used by [`ValidatedConfig::fingerprint`].
pub const CONFIG_FINGERPRINT_VERSION: u16 = 1;
