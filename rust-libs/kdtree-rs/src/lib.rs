//! # kdtree
//!
//! K-dimensional tree for Rust (bucket point-region implementation)
//!
//! ## Usage
//!
//! ```
//! use kdtree::KdTree;
//! use kdtree::ErrorKind;
//! use kdtree::distance::squared_euclidean;
//!
//! let a: ([f64; 2], usize) = ([0f64, 0f64], 0);
//! let b: ([f64; 2], usize) = ([1f64, 1f64], 1);
//! let c: ([f64; 2], usize) = ([2f64, 2f64], 2);
//! let d: ([f64; 2], usize) = ([3f64, 3f64], 3);
//!
//! let dimensions = 2;
//! let mut kdtree = KdTree::new(dimensions);
//!
//! kdtree.add(&a.0, a.1).unwrap();
//! kdtree.add(&b.0, b.1).unwrap();
//! kdtree.add(&c.0, c.1).unwrap();
//! kdtree.add(&d.0, d.1).unwrap();
//!
//! assert_eq!(kdtree.size(), 4);
//! assert_eq!(
//!     kdtree.nearest(&a.0, 0, &squared_euclidean).unwrap(),
//!     vec![]
//! );
//! assert_eq!(
//!     kdtree.nearest(&a.0, 1, &squared_euclidean).unwrap(),
//!     vec![(0f64, &0)]
//! );
//! assert_eq!(
//!     kdtree.nearest(&a.0, 2, &squared_euclidean).unwrap(),
//!     vec![(0f64, &0), (2f64, &1)]
//! );
//! assert_eq!(
//!     kdtree.nearest(&a.0, 3, &squared_euclidean).unwrap(),
//!     vec![(0f64, &0), (2f64, &1), (8f64, &2)]
//! );
//! assert_eq!(
//!     kdtree.nearest(&a.0, 4, &squared_euclidean).unwrap(),
//!     vec![(0f64, &0), (2f64, &1), (8f64, &2), (18f64, &3)]
//! );
//! assert_eq!(
//!     kdtree.nearest(&a.0, 5, &squared_euclidean).unwrap(),
//!     vec![(0f64, &0), (2f64, &1), (8f64, &2), (18f64, &3)]
//! );
//! assert_eq!(
//!     kdtree.nearest(&b.0, 4, &squared_euclidean).unwrap(),
//!     vec![(0f64, &1), (2f64, &0), (2f64, &2), (8f64, &3)]
//! );
//! ```

extern crate num_traits;

#[cfg(feature = "serialize")]
#[cfg_attr(feature = "serialize", macro_use)]
extern crate serde_derive;
#[macro_use]
extern crate serde_big_array;
big_array! { BigArray; }

#[cfg(feature = "clamor")]
extern crate rsmalloc;

#[cfg(feature = "clamor")]
#[cfg_attr(feature = "clamor", global_allocator)] 
static GLOBAL: rsmalloc::Allocator = rsmalloc::Allocator;
    
extern crate rand;

use rand::{thread_rng, Rng, distributions::Alphanumeric};

pub mod distance;
mod heap_element;
pub mod kdtree;
mod util;
pub use crate::kdtree::ErrorKind;
pub use crate::kdtree::KdTree;

use distance::squared_euclidean;
use std::convert::From;

extern crate serde; 
extern crate serde_json;

use std::time::{Duration, Instant};

use std::io::{Read, Write};
use std::ffi::CStr;
use std::fs;
use std::fs::{File};
use std::path::Path;

// A struct that can be passed between C and Rust
#[repr(C)]
pub struct ResTuple {
    point_idx: usize,
    dist: f64,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct Payload {
    #[serde(with="BigArray")]
    arr: [u8; 2048],
    rating: i64
}

impl Payload {
    fn new() -> Payload {
        Payload {
            arr: [0;2048],
            rating: 0
        }
    }
}

// Conversion functions
impl From<(f64, usize)> for ResTuple {
    fn from(tup: (f64, usize)) -> ResTuple {
        ResTuple { point_idx: tup.1, dist: tup.0 }
    }
}

impl From<ResTuple> for (usize, f64) {
    fn from(tup: ResTuple) -> (usize, f64) {
        (tup.point_idx, tup.dist)
    }
}

fn initial_buffer_size(file: &File) -> usize {
    let bsize = file.metadata().map(|m| m.len() as usize + 1).unwrap_or(0);
    println!("Buffer size: {}", bsize);
    bsize
}

fn get_data_from_file(file: &mut File, ret: &mut [u8]) {
    file.read(ret).unwrap();
}

#[no_mangle]
pub extern "C" fn kdtree_create(size: u64) -> *mut KdTree<f64, usize, [f64; 3]> {
    println!("Calling kdtree new");

    let dimensions = 3;
    let mut kdtree = KdTree::new(dimensions);
    let npoints = size;
    let mut rng = thread_rng();

    for i in 0..npoints {
        let arr: [f64; 3] = rng.gen();
        kdtree.add(arr, i as usize).unwrap();
    }

    /*let ser_start = Instant::now();
    //let jsonval = serde_json::to_string(&kdtree).unwrap();
    let ser_tot = ser_start.elapsed();

    let deser_start = Instant::now();
    //let val: KdTree<f64, usize, [f64;3]> = serde_json::from_str(&jsonval).unwrap();
    let deser_tot = deser_start.elapsed();

    println!("serialize time: {}", ser_tot.as_millis());
    println!("deserialize time: {}", deser_tot.as_millis());
    println!("k-d tree size: {}", kdtree.size());
*/
    Box::into_raw(Box::new(kdtree))
}


#[no_mangle]
pub extern "C" fn kdtree_from_csv() -> *mut KdTree<f64, Payload, [f64; 2]> {
    println!("Calling kdtree new");

    let csvfile = "/home/ubuntu/clamor/data/kdtree/nodes.csv";
    let mut file = File::open(csvfile).expect("Could not open test data file");
    let cap = initial_buffer_size(&file);
    let mut data_vec = vec![0; cap];
    get_data_from_file(&mut file, &mut data_vec);
    let mut data = std::str::from_utf8(&data_vec[..data_vec.len() - 1]).unwrap().to_string();

    println!(">>>>>> Loaded data, constructing...");

    let dimensions = 2;
    let mut kdtree: KdTree<f64, Payload, [f64; 2]> = KdTree::new(dimensions);
    let mut rng = thread_rng();

    for (row, line) in data.lines().enumerate() {
        if ( row % 100 == 0 ) {
            println!("Row: {}", row);
        }
        let pts: Vec<&str> = line.split(',').collect();
        let arr: [f64; 2] = [pts[0].parse().unwrap(), pts[1].parse().unwrap()];
        let mut s: Payload = Payload::new();
        let rand_s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(2048)
            .map(char::from)
            .collect();
        s.arr.copy_from_slice(rand_s.as_bytes());
        s.rating = rand::thread_rng().gen::<i64>();
        kdtree.add(arr, s).unwrap();
    }

    println!(">>>>>> done.");
    
    /*let ser_start = Instant::now();
    let encoded: Vec<u8> = bincode::serialize(&kdtree).unwrap();
    println!("Encoded size: {}", encoded.len());
    let ser_tot = ser_start.elapsed();

    let deser_start = Instant::now();
    let mut decoded: KdTree<f64, Payload, [f64; 2]> = bincode::deserialize(&encoded[..]).unwrap();
    let deser_tot = deser_start.elapsed();

    println!("serialize time: {}", ser_tot.as_millis());
    println!("deserialize time: {}", deser_tot.as_millis());
    println!("k-d tree size: {}", kdtree.size());
     */
    Box::into_raw(Box::new(kdtree))
}

#[no_mangle]
pub unsafe extern "C" fn kdtree_lookup(kdtree: *mut KdTree<f64, Payload, [f64; 2]>, idx: &[f64; 2]) -> ResTuple {
    let kdtree = kdtree.as_ref().unwrap();
    println!("Idxs: {} {}", idx[0], idx[1]);
    let mut res =
        kdtree.nearest(idx, 10, &squared_euclidean).unwrap()
        .iter()
        .map(|x| x.1.rating)
        //.map(|x| rand::thread_rng().gen::<i64>())
        .collect::<Vec<i64>>();
    res.sort();
    println!("Result: {}", res[0]);
    (res[0] as f64, 1).into()
}
