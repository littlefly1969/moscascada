//! `source_epoch`: non-zero, new for every start of the producer (contract §1).

use std::io::{self, Read};

/// A fresh random epoch from the kernel's CSPRNG (`/dev/urandom`). Never 0.
///
/// The epoch is not persisted: a driver process without a durable spool cannot claim to
/// have kept pending acquisitions across its own restart, so every start is a new epoch
/// and the kernel answers `full_state_required` (history continuity `Unknown`).
pub fn new_epoch() -> io::Result<u64> {
    let mut f = std::fs::File::open("/dev/urandom")?;
    loop {
        let mut b = [0u8; 8];
        f.read_exact(&mut b)?;
        let e = u64::from_le_bytes(b);
        if e != 0 {
            return Ok(e);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn epochs_are_non_zero_and_differ() {
        let a = super::new_epoch().unwrap();
        let b = super::new_epoch().unwrap();
        assert_ne!(a, 0);
        assert_ne!(a, b);
    }
}
