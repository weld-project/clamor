use std::collections::HashMap;
use std::hash::Hasher;
use std::hash::BuildHasher;
use std::convert::TryInto;

#[cfg(feature = "clamor")]
extern crate rsmalloc;

#[cfg(feature = "clamor")]
#[cfg_attr(feature = "clamor", global_allocator)] 
static GLOBAL: rsmalloc::Allocator = rsmalloc::Allocator;

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
pub extern "C" fn hm_create(size: u64) -> *mut HashMap<u64, u64, KeyHasher> {
    println!("Calling hashmap new");
    let mut hm = HashMap::with_capacity_and_hasher((size as usize)*8, KeyHasher { hash: 0 });
    println!("size: {}", size);
    for i in 0..size {
        if i % 100000 == 0 {
            println!("Inserting: {}", i);
        }
        hm.insert(i, i);
    }

    Box::into_raw(Box::new(hm))
}

#[no_mangle]
pub unsafe extern "C" fn hm_lookup(hm: *mut HashMap<u64, u64, KeyHasher>, idx: u64) -> u64 {
    let hm = hm.as_ref().unwrap(); 
    *(hm.get(&idx).unwrap())
}

#[no_mangle]
pub unsafe extern "C" fn hm_update(hm: *mut HashMap<u64, u64, KeyHasher>, idx: u64) {
    let mut hm = hm.as_mut().unwrap(); 
    hm.insert(idx, idx+1);
}
