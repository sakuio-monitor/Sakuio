//! Minimal global-allocator accounting for the runtime overview.
//! 为运行时概览提供进程内堆分配计数（不引入额外分配器依赖）。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

pub static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
pub static LIVE_OBJECTS: AtomicU64 = AtomicU64::new(0);
pub static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);

fn record_alloc(size: usize) {
    let size = size as u64;
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    TOTAL_BYTES.fetch_add(size, Ordering::Relaxed);
    LIVE_OBJECTS.fetch_add(1, Ordering::Relaxed);
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

fn record_dealloc(size: usize) {
    let size = size as u64;
    let _ = LIVE_BYTES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_sub(size))
    });
    let _ = LIVE_OBJECTS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_sub(1))
    });
}

pub struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_alloc(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_alloc(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        record_dealloc(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            let old_size = layout.size();
            if new_size >= old_size {
                let delta = (new_size - old_size) as u64;
                let live = LIVE_BYTES.fetch_add(delta, Ordering::Relaxed) + delta;
                TOTAL_BYTES.fetch_add(delta, Ordering::Relaxed);
                PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
            } else {
                let delta = (old_size - new_size) as u64;
                let _ = LIVE_BYTES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                    Some(live.saturating_sub(delta))
                });
            }
        }
        resized
    }
}
