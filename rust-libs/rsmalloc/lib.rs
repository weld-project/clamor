extern crate libc;

use std::alloc::{GlobalAlloc, Layout};
use std::io;
use std::io::Write;

mod rsmalloc;

pub struct Allocator;

impl Allocator {
    pub fn new() -> Allocator {
        Allocator {
        }
    }
}

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let alignment = layout.align();
        let res = rsmalloc::smalloc_byte_aligned(size as libc::size_t, alignment as libc::size_t);
        res
    }
    
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        rsmalloc::sfree(ptr)
    }
}
