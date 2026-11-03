//! Execution of a read plan: all requests in order, fail on the first error.
//!
//! A polling scan is **not** an atomic snapshot of the device: each request is answered at a
//! different moment. [`ScanImage`] reports the duration between the first request and the
//! last response (`spread`), which is the window in which values of different requests may
//! be mutually inconsistent.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::client::{Client, ModbusError, Transport};
use crate::pdu::ReadData;
use crate::plan::{Location, Plan};
use crate::value::{DecodeError, Decoding, Value};

/// Results of one complete scan.
#[derive(Debug, Clone)]
pub struct ScanImage {
    /// One entry per plan request, same order.
    pub data: Vec<ReadData>,
    pub started: Instant,
    /// After the last response was validated.
    pub finished: Instant,
    /// Wall clock (CLOCK_REALTIME) sampled right after the last response: the **driver's
    /// acquisition time**, not a device timestamp. `None` if the clock is before 1970 or
    /// beyond 2262.
    pub acquired_at_utc_ns: Option<i64>,
}

impl ScanImage {
    /// First request sent -> last response validated.
    pub fn spread(&self) -> Duration {
        self.finished.saturating_duration_since(self.started)
    }

    /// Decode one point.
    pub fn decode(&self, loc: Location, d: &Decoding) -> Result<Value, DecodeError> {
        match self.data.get(loc.request).ok_or(DecodeError::Missing)? {
            ReadData::Bits(bits) => d.decode_bits(*bits.get(loc.offset).ok_or(DecodeError::Missing)?),
            ReadData::Registers(regs) => {
                let w = usize::from(d.raw.width());
                let end = loc.offset.checked_add(w).ok_or(DecodeError::Missing)?;
                d.decode_registers(regs.get(loc.offset..end).ok_or(DecodeError::Missing)?)
            }
        }
    }
}

/// Failure of a scan: which request failed and why.
#[derive(Debug)]
pub struct ScanError {
    pub request_index: usize,
    pub error: ModbusError,
    /// Requests completed before the failure (their data is discarded).
    pub completed: usize,
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request #{} failed after {} completed: {}", self.request_index, self.completed, self.error)
    }
}

impl std::error::Error for ScanError {}

/// UTC nanoseconds of the system clock, if representable in `[0, 2^63)`.
pub fn utc_now_ns() -> Option<i64> {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(d.as_nanos()).ok()
}

/// Run every request of `plan`, pausing `inter_request_delay` between two requests (rate
/// limit towards the device). Returns the complete image or the first error; a partial
/// image is never returned.
pub fn execute<S: Transport>(
    client: &mut Client<S>,
    plan: &Plan,
    inter_request_delay: Duration,
) -> Result<ScanImage, ScanError> {
    let started = Instant::now();
    let mut data = Vec::with_capacity(plan.requests.len());
    for (i, req) in plan.requests.iter().enumerate() {
        if i > 0 && !inter_request_delay.is_zero() {
            std::thread::sleep(inter_request_delay);
        }
        match client.read(req) {
            Ok(d) => data.push(d),
            Err(error) => return Err(ScanError { request_index: i, error, completed: i }),
        }
    }
    let finished = Instant::now();
    Ok(ScanImage { data, started, finished, acquired_at_utc_ns: utc_now_ns() })
}
