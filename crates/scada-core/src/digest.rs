//! Canonical, versioned, allocation-free encoding fed into BLAKE3.

/// Writes fixed-width big-endian fields into a BLAKE3 hasher. Variable-length data is
/// always length-prefixed so that the encoding is unambiguous.
pub(crate) struct CanonicalHasher {
    hasher: blake3::Hasher,
}

impl CanonicalHasher {
    pub(crate) fn new(domain: &[u8], version: u16) -> Self {
        let mut h = CanonicalHasher { hasher: blake3::Hasher::new() };
        h.hasher.update(domain);
        h.hasher.update(&[0]);
        h.hasher.update(&version.to_be_bytes());
        h
    }

    pub(crate) fn u8(&mut self, v: u8) {
        self.hasher.update(&[v]);
    }

    pub(crate) fn u32(&mut self, v: u32) {
        self.hasher.update(&v.to_be_bytes());
    }

    pub(crate) fn u64(&mut self, v: u64) {
        self.hasher.update(&v.to_be_bytes());
    }

    pub(crate) fn i64(&mut self, v: i64) {
        self.hasher.update(&v.to_be_bytes());
    }

    /// Collection length as u64 (usize width is platform dependent).
    pub(crate) fn len(&mut self, n: usize) {
        self.u64(n as u64);
    }

    pub(crate) fn bytes(&mut self, b: &[u8]) {
        self.len(b.len());
        self.hasher.update(b);
    }

    pub(crate) fn raw(&mut self, b: &[u8]) {
        self.hasher.update(b);
    }

    pub(crate) fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            None => self.u8(0),
            Some(x) => {
                self.u8(1);
                self.u64(x);
            }
        }
    }

    pub(crate) fn finish(&self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}
