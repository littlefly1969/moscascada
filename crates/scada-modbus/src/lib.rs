//! VS-007 (ADR 0015): read-only Modbus TCP for MoScada drivers.
//!
//! Layers, each usable alone:
//! * [`pdu`]: the four read functions of the Modbus Application Protocol V1.1b3
//!   (FC01 Read Coils, FC02 Read Discrete Inputs, FC03 Read Holding Registers, FC04 Read
//!   Input Registers): request encoding and strict response validation;
//! * [`mbap`]: Modbus TCP framing (MBAP header, Messaging Implementation Guide V1.0b) with an
//!   incremental, bounded decoder that tolerates TCP segmentation and coalescing;
//! * [`client`]: a blocking client, one outstanding request per connection;
//! * [`plan`]: groups the points of a product profile into bounded read requests inside
//!   declared readable blocks;
//! * [`scan`]: executes a plan and returns a register image with its timing;
//! * [`value`]: deterministic conversion of register images to the two value types the
//!   MoScada RTDB accepts (`bool`, `i64`).
//!
//! Write functions (FC05/06/15/16) do not exist in this crate on purpose (VS-007 §1).
//! Addresses are **PDU addresses, zero-based**: the "4xxxx/3xxxx" register labels of some
//! documentation are never added or subtracted here.
#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::indexing_slicing, clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod client;
pub mod mbap;
pub mod pdu;
pub mod plan;
pub mod scan;
pub mod value;

/// Default Modbus TCP port (IANA "mbap").
pub const DEFAULT_PORT: u16 = 502;
/// Maximum PDU size (function code + data), Application Protocol §4.1.
pub const MAX_PDU_BYTES: usize = 253;
/// Maximum quantity of bits for FC01/FC02 (§6.1, §6.2).
pub const MAX_READ_BITS: u16 = 2000;
/// Maximum quantity of registers for FC03/FC04 (§6.3, §6.4).
pub const MAX_READ_REGISTERS: u16 = 125;

/// Data area (and therefore read function) of a Modbus point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Area {
    /// FC01 Read Coils (1 bit each).
    Coil,
    /// FC02 Read Discrete Inputs (1 bit each).
    DiscreteInput,
    /// FC03 Read Holding Registers (16 bit each).
    HoldingRegister,
    /// FC04 Read Input Registers (16 bit each).
    InputRegister,
}

impl Area {
    pub const ALL: [Area; 4] = [Area::Coil, Area::DiscreteInput, Area::HoldingRegister, Area::InputRegister];

    /// Function code of the read request for this area.
    pub fn function_code(self) -> u8 {
        match self {
            Area::Coil => 0x01,
            Area::DiscreteInput => 0x02,
            Area::HoldingRegister => 0x03,
            Area::InputRegister => 0x04,
        }
    }

    pub fn from_function_code(fc: u8) -> Option<Area> {
        Area::ALL.into_iter().find(|a| a.function_code() == fc)
    }

    /// Whether items of this area are single bits.
    pub fn is_bit(self) -> bool {
        matches!(self, Area::Coil | Area::DiscreteInput)
    }

    /// Largest quantity of one read request in this area.
    pub fn max_quantity(self) -> u16 {
        if self.is_bit() { MAX_READ_BITS } else { MAX_READ_REGISTERS }
    }

    /// Stable configuration name.
    pub fn name(self) -> &'static str {
        match self {
            Area::Coil => "coil",
            Area::DiscreteInput => "discrete_input",
            Area::HoldingRegister => "holding_register",
            Area::InputRegister => "input_register",
        }
    }

    pub fn from_name(s: &str) -> Option<Area> {
        Area::ALL.into_iter().find(|a| a.name() == s)
    }
}

impl std::fmt::Display for Area {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}
