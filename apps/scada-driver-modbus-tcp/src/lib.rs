//! VS-007 (ADR 0015): read-only Modbus TCP driver for MoScada.
//!
//! Separate process: reads devices with FC01..FC04 through `scada-modbus`, maps the points
//! of a versioned product profile (`catalog`) to the channels of a MoScada source
//! (`config`), and feeds the kernel through `scada-driver-sdk`. It never writes to a
//! device, never opens a listening socket and never runs inside the kernel.
#![forbid(unsafe_code)]

pub mod catalog;
pub mod config;
pub mod device;
pub mod status;
