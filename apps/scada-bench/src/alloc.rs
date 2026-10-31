//! Counting global allocator (ADR 0003): wraps the system allocator and counts calls and
//! live bytes. Benchmark binary only.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};

pub struct Counting;

pub static ALLOCS: AtomicU64 = AtomicU64::new(0);
pub static DEALLOCS: AtomicU64 = AtomicU64::new(0);
pub static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

// SAFETY: every method forwards to `System` with the same arguments; the counters are
// plain atomics and do not allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        LIVE_BYTES.fetch_add(l.size() as i64, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        DEALLOCS.fetch_add(1, Relaxed);
        LIVE_BYTES.fetch_sub(l.size() as i64, Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        LIVE_BYTES.fetch_add(l.size() as i64, Relaxed);
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        LIVE_BYTES.fetch_add(new as i64 - l.size() as i64, Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}
