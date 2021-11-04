use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::Hasher;
use std::hash::BuildHasher;
use std::slice;

extern crate libc;
extern crate rand;
extern crate rsmalloc;

use rand::Rng;

#[global_allocator]
static GLOBAL: rsmalloc::Allocator = rsmalloc::Allocator;

///! A prototype parameter server, implemented as a simple hash map.

#[derive(Clone)]
pub struct KeyHasher
{
    hash: u64,
}

// We just want to return the key as the hash.
impl Hasher for KeyHasher {
    fn finish(&self)->u64 {
        self.hash
    }

    fn write(&mut self, val: &[u8]) {
        let (int_bytes, rest) = val.split_at(std::mem::size_of::<u64>());
        self.hash = u64::from_ne_bytes(int_bytes.try_into().unwrap())
    }
}

impl BuildHasher for KeyHasher {
    type Hasher = Self;
    fn build_hasher(&self) -> Self::Hasher {
        self.clone()
    }
}

#[no_mangle]
pub extern "C" fn ps_create(size: u64) -> *mut HashMap<u64, f64, KeyHasher> {
    println!("Constructing parameter server");
    let mut hm = HashMap::with_capacity_and_hasher((size as usize)*8, KeyHasher { hash: 0 });
    let mut rng = rand::thread_rng();

    println!("size: {}", size);
    for i in 0..size {
        if i % 100000 == 0 {
            println!("Inserting: {}", i);
        }
        hm.insert(i, rng.gen::<f64>());
    }

    Box::into_raw(Box::new(hm))
}

#[no_mangle]
pub unsafe extern "C" fn ps_insert(hm: *mut HashMap<u64, f64, KeyHasher>, k: u64, v: f64) {
    let hm = hm.as_mut().unwrap(); 
    hm.insert(k, v);
}

///! Updates weights in-place using aggregated gradients
#[no_mangle]
pub unsafe extern "C" fn ps_update(hm: *mut HashMap<u64, f64, KeyHasher>, update_idx: u64,
                                   grad: f64) {
    // let lr: f64 = 0.01; // learning rate
    
    /* let grads_slice = unsafe {
        assert!(!grads.is_null());
        slice::from_raw_parts(grads, len)
    }; */

    let hm = hm.as_mut().unwrap(); 

    // update gradient
    let mut val = *(hm.get(&update_idx).unwrap());
    val -= grad;
    hm.insert(update_idx, val); // TODO
}

///! Regularize the current weights in-place before adding gradient updates
#[no_mangle]
pub unsafe extern "C" fn ps_regularize(hm: *mut HashMap<u64, f64, KeyHasher>) {
    let lr: f64 = 0.01; // learning rate
    let lambda: f64 = 1.0;

    let hm = hm.as_mut().unwrap(); 

    for (k, v) in hm.iter_mut() {
        *v -= lr * lambda;
    }
}

#[no_mangle]
pub unsafe extern "C" fn ps_lookup(hm: *mut HashMap<u64, f64, KeyHasher>, idx: u64) -> f64 {
    let hm = hm.as_ref().unwrap();
    println!("Looking up idx: {}", idx);
    *(hm.get(&idx).unwrap())
}
