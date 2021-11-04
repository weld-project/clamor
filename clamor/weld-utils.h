#ifndef WELD_UTILS_H
#define WELD_UTILS_H

#include "weld.h"

#include "dsm.grpc.pb.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

#include <string>
#include <utility>
#include <vector>

typedef struct perf_times {
  double compile;
  double request_driver;
  double request_peer;
  double compute;
} perf_times;

typedef struct shard {
  int64_t start_index;
  int64_t size;
} shard;

typedef struct weld_vector {
    void* data;
    int64_t length;
  
weld_vector() :
  data(),
    length(0)
  {}
  
weld_vector(void* s_data, int64_t s_length) :
  data(s_data),
    length(s_length)
  {}
} weld_vector;

template <typename T>
struct t_weld_vector {
    T *data;
    int64_t length;
};

template <typename T>
t_weld_vector<T> make_weld_vector(T *data, int64_t length) {
    struct t_weld_vector<T> vector;
    vector.data = data;
    vector.length = length;
    return vector;
}

/* Results must be returned as a tuple that includes the worker ID, for sorting. */
typedef struct ret_struct {
  int32_t worker_id;
  weld_vector* data;
} ret_struct;

namespace WeldUtils {
  /* Use this channel to send RPCs to scheduler. */
  extern std::unique_ptr<dsm::DSM::Stub> manager_stub;
  
  std::pair<void*, perf_times> run_query_weld(std::string prog, void *args, bool distribute, uint64_t npartitions);
}

typedef struct dispatch_input {
  weld_vector code_vec; /* strings are represented as a {i8*, size} vector */
  uint32_t worker_idx;
  weld_vector input_data; /* weld_vector that points to a struct of the inputs */
} dispatch_input;

typedef struct dispatch_all_input {
  weld_vector code_vec; /* strings are represented as a {i8*, size} vector */
  weld_vector input_data; /* weld_vector that is a list of input structs */
} dispatch_all_input;

typedef struct dispatch_output {
  weld_vector output_data;
} dispatch_output;

extern "C" void dispatch_all(void* input, void* output);
extern "C" void dispatch(void* input, void* output);

typedef struct lookup_input {
  weld_vector shards;
  uint64_t index;
} lookup_input;

/* The (shard, index in shard) tuple returned for a distributed Lookup. */
typedef struct lookup_index {
  int64_t shard_idx;
  int64_t element_idx;
} lookup_index;

extern "C" void lookup_idx(void* input, void* output);

/* typedef struct slice_input { */
/*   weld_vector shards; */
/*   uint64_t index; */
/*   uint64_t size; */
/* } shard_input; */

/* /\* List of shards and their sizes corresponding to slice. *\/ */
/* typedef struct slice_output { */
/*   weld_vector output_shards; */
/* } slice_output; */

// extern "C" void slice(void* input, void* output);

/* Data is simply partitioned into equal, contiguous regions right now to avoid page and data movement.
   This could also be reimplemented as e.g. a hash partitioner. */
typedef struct partition_input {
  uint64_t data_size;
  uint32_t nshards;
  uint64_t increment;
} partition_input;

extern "C" void partition_data(void* input, void* result);

typedef struct i32_i32 {
  int32_t x;
  int32_t y;
} i32_i32;

extern "C" void prefetch_i32(void* input, void* output);
extern "C" void prefetch_i32_i32_vec(void* input, void* output);
extern "C" void prefetch_i64(void* input, void* output);
extern "C" void prefetch_f64(void* input, void* output);

extern "C" void print_time(void* input, void* output);

#endif // WELD_UTILS_H
