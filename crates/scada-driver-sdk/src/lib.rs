//! VS-007 (ADR 0015): common SDK for **out-of-process** MoScada drivers.
//!
//! A driver is an independent executable. It talks to the kernel only through the
//! versioned wire contract `scada.driver.v1` (minor up to 1.2) over the kernel's Unix
//! socket; it shares no code with the kernel besides the protocol crate (no dependency on
//! `scada-core`, `scada-runtime` or `scada-journal`) and never runs inside it.
//!
//! [`SourcePublisher`] owns one logical session for one configured `source_id`:
//! handshake and minor negotiation, `source_epoch` and `source_sequence`, the choice between
//! `FullState` and `Delta`, the bounded window of batches not yet confirmed, ACK and durable
//! receipt handling, retry of the same sequence after a typed refusal, reconnection with
//! ordered replay (contract §5, §5.1, §10.2), heartbeats (minor 1.1) and diagnostics. Its
//! state is explicit:
//!
//! `Disconnected → Connecting → Handshaking → AwaitingFullState → Streaming`, with
//! `Backpressured` (typed refusal, retry later) and `Recovering` (journal fault, non
//! durable receipt, rejected batch) as side states. Every transition is recorded with its
//! reason.
//!
//! What the SDK does **not** do: acquisition (protocol drivers), product mappings, value
//! timestamps (a driver that has no device timestamp sends none), any claim of continuity
//! the source cannot prove.
#![forbid(unsafe_code)]

pub mod epoch;
pub mod frames;
pub mod latency;
pub mod publisher;

pub use latency::LatencyHistogram;
pub use publisher::{Backoff, Counters, LinkState, Publish, PublisherConfig, SdkError, SourcePublisher, Transition};

/// Value of one channel (the two types the RTDB accepts).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Value {
    Bool(bool),
    I64(i64),
}

impl Value {
    pub fn channel_type(&self) -> ChannelType {
        match self {
            Value::Bool(_) => ChannelType::Bool,
            Value::I64(_) => ChannelType::I64,
        }
    }
}

/// Configured type of a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelType {
    Bool,
    I64,
}

impl ChannelType {
    pub fn name(self) -> &'static str {
        match self {
            ChannelType::Bool => "bool",
            ChannelType::I64 => "i64",
        }
    }
}
