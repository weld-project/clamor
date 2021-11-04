//! Interface to Clamor shared memory allocator for use in the Weld runtime.

extern crate libc;

#[link(name="smalloc")]
extern "C" {
    pub fn smalloc(nbytes: usize) -> *mut u8;
    pub fn smalloc_aligned(nbytes: usize) -> *mut u8;
    pub fn smalloc_byte_aligned(nbytes: usize, alignment: usize) -> *mut u8;
    pub fn srealloc(pointer: *mut libc::c_void, nbytes: usize) -> *mut u8;
    pub fn sfree(pointer: *mut u8);
    pub fn sfree_reuse(pointer: *mut u8);
}
