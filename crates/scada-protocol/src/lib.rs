//! IPC types and framing for the kernel <-> driver protocol (`scada.driver.v1`), the
//! read-only test introspection (`scada.introspect.v1`) and, since VS-005, the local alarm
//! control plane (`scada.alarm.v1`).
//!
//! Frame = `u32` big-endian payload length + protobuf payload. A length of zero or above
//! the configured maximum is rejected *before* any buffer is reserved.
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

pub mod frame;

/// Generated code for `scada.driver.v1`.
#[allow(clippy::all, clippy::pedantic, missing_debug_implementations)]
pub mod driver_v1 {
    include!(concat!(env!("OUT_DIR"), "/scada.driver.v1.rs"));
}

/// Generated code for `scada.introspect.v1`.
#[allow(clippy::all, clippy::pedantic, missing_debug_implementations)]
pub mod introspect_v1 {
    include!(concat!(env!("OUT_DIR"), "/scada.introspect.v1.rs"));
}

/// VS-005: generated code for `scada.alarm.v1` (alarm control plane).
#[allow(clippy::all, clippy::pedantic, missing_debug_implementations)]
pub mod alarm_v1 {
    include!(concat!(env!("OUT_DIR"), "/scada.alarm.v1.rs"));
}

pub use prost::Message;

/// Protocol major version implemented by this crate.
pub const PROTOCOL_MAJOR: u32 = 1;
/// Highest protocol minor version implemented by this crate (1.1 since VS-004).
pub const PROTOCOL_MINOR: u32 = 1;
/// VS-004: first minor version of major 1 with `DriverMessage.heartbeat` (ADR 0011).
pub const MINOR_HEARTBEAT: u32 = 1;
/// Length of `kernel_instance_id` in bytes.
pub const KERNEL_INSTANCE_ID_LEN: usize = 16;
/// Maximum length of `Hello.driver_id` in bytes.
pub const MAX_DRIVER_ID_BYTES: usize = 64;
