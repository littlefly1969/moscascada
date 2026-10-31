//! Strongly typed identifiers and values.

use std::fmt;

macro_rules! id_type {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u32);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id_type!(
    /// Configured, stable identity of a data source.
    SourceId
);
id_type!(
    /// Channel of a source; meaningful only together with its [`SourceId`].
    ChannelId
);
id_type!(
    /// Stable identity of an RTDB tag; the canonical ordering key.
    TagId
);

/// Static type of a tag.
///
/// Only `bool` and `i64` exist in VS-001. Physical quantities are scaled integers
/// (for example temperature in hundredths of °C) so that equality, ordering and the
/// digest stay exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueType {
    Bool,
    I64,
}

impl ValueType {
    pub(crate) fn code(self) -> u8 {
        match self {
            ValueType::Bool => 1,
            ValueType::I64 => 2,
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ValueType::Bool => "bool",
            ValueType::I64 => "i64",
        })
    }
}

/// A typed tag value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Value {
    Bool(bool),
    I64(i64),
}

impl Value {
    pub fn value_type(self) -> ValueType {
        match self {
            Value::Bool(_) => ValueType::Bool,
            Value::I64(_) => ValueType::I64,
        }
    }
}

/// Why a value is uncertain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UncertainReason {
    /// The link of the owning source is down or was replaced; the value is the last known one.
    SourceUnavailable,
}

/// Communication quality of a tag (not the physical state of the plant).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Quality {
    /// No valid value has ever been received in this kernel instance.
    Unknown,
    /// The value was received in a valid, applied observation on the active link.
    Good,
    /// The last known value is preserved but not currently confirmed.
    Uncertain(UncertainReason),
}

/// Kind of observation batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BatchMode {
    /// Every configured channel of the source exactly once.
    FullState,
    /// A subset of channels; needs an established baseline and contiguous sequence.
    Delta,
}

/// Continuity evidence attached to an applied batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Continuity {
    /// `source_sequence == last_accepted + 1` within the same epoch.
    Continuous,
    /// First full state of the epoch: earlier history is not proven continuous.
    BaselineEstablished,
    /// Full state after a gap: intermediate events were not recovered.
    Resynchronized,
}
