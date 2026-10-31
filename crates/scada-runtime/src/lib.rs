//! Kernel host for VS-001: loads and validates the static configuration, accepts driver
//! connections on a Unix domain socket, converts protocol messages into normalized core
//! operations and feeds them, one at a time, to the single-writer sequencer thread that
//! owns the [`scada_core::Kernel`].
//!
//! Tokio is used only here, at the I/O edge. The sequencer runs on a plain OS thread.
#![forbid(unsafe_code)]

pub mod budget;
pub mod capture;
pub mod config;
pub mod convert;
pub mod diag;
pub mod sequencer;
pub mod server;
