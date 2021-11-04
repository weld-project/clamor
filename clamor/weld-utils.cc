#include "weld-utils.h"

#include <sys/time.h>

#include <grpc/grpc.h>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/sum.hpp>

#include <future>
#include <string>
#include <vector>

#include "debug.h"
#include "fault-handler.h"
#include "worker-server.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

#include <chrono>
#include <thread>

using namespace std;
using namespace std::chrono;
using namespace std::chrono_literals;

std::unique_ptr<dsm::DSM::Stub> WeldUtils::manager_stub;

steady_clock::time_point driver_time = steady_clock::now();

pair<void*, perf_times> WeldUtils::run_query_weld(string prog, void* args, bool distribute, uint64_t npartitions) {
    // Compile Weld module.
    weld_error_t e = weld_error_new();
    weld_conf_t conf = weld_conf_new();

    weld_conf_set(conf, "weld.llvm.optimization.level", "3");

    if ( distribute ) {
      //weld_conf_set(conf, "weld.compile.dumpCode", "true");
      //weld_conf_set(conf, "weld.optimization.sirOptimization", "false");
      //weld_conf_set(conf, "weld.optimization.passes", "loop-fusion,unroll-static-loop,infer-size,short-circuit-booleans,fix-iterate");
      //weld_conf_set(conf, "weld.compile.traceExecution", "true");
      
      weld_conf_set(conf, "weld.distribute", "true");
      weld_conf_set(conf, "weld.distribute.partitions", to_string(npartitions).c_str());
      weld_conf_set(conf, "weld.optimization.passes", "");
    } else {
      weld_conf_set(conf, "weld.distribute", "false");
      //weld_conf_set(conf, "weld.optimization.passes", "loop-fusion,unroll-static-loop,infer-size,short-circuit-booleans,fix-iterate");
      weld_conf_set(conf, "weld.optimization.passes", "inline-zip,inline-let,inline-apply,loop-fusion,unroll-static-loop,infer-size,algebraic,inline-literals,cse,short-circuit-booleans");
    }

    //std::this_thread::sleep_for(chrono::milliseconds(5000)); // For fault tolerance testing
    
    struct timeval tstart, tend, tdiff;
    gettimeofday(&tstart, 0);
    steady_clock::time_point begin = steady_clock::now();
    
    weld_module_t m = weld_module_compile(prog.c_str(), conf, e);
    weld_conf_free(conf);
    gettimeofday(&tend, 0);
    timersub(&tend, &tstart, &tdiff);
    steady_clock::time_point end = steady_clock::now();
    duration<double> secs = end - begin;
    double compile_time = secs.count();
    
    printf("Weld compile time: %ld.%06ld\n",
            (long) tdiff.tv_sec, (long) tdiff.tv_usec);

    if (weld_error_code(e)) {
      const char* err = weld_error_message(e);
      printf("Error message: %s\n", err);
      exit(1);
    }

    gettimeofday(&tstart, 0);
    begin = steady_clock::now();
    
    cout << "making args" << endl;
    weld_value_t weld_args = weld_value_new(args);
    cout << "got args" << endl;
    
    // Run the module and get the result.
    cout << "making conf" << endl;
    conf = weld_conf_new();
    weld_context_t context = weld_context_new(conf);
    SPDLOG_DEBUG("got conf, running...");

    weld_value_t result;
    try {
      result = weld_module_run(m, context, weld_args, e);
    } catch (...) {
      // Rethrow, but make sure to clean up first!
      SPDLOG_DEBUG("Exception caught in Weld, cleaning up...");
      weld_value_free(weld_args);
      weld_conf_free(conf);
      
      weld_error_free(e);
      weld_module_free(m);
      SPDLOG_DEBUG("Done with cleanup.");
      throw;
    }
    
    if (weld_error_code(e)) {
      // Weld error, not C++ exception
      const char *err = weld_error_message(e);
      printf("Error message: %s\n", err);
      exit(1);
    }

    SPDLOG_DEBUG("Getting result data");
    void* result_data = weld_value_data(result);

    SPDLOG_DEBUG("Ending timer.");
    gettimeofday(&tend, 0);
    end = steady_clock::now();
    secs = end - begin;
    double compute_time = secs.count();

    SPDLOG_DEBUG("Cleaning up...");
    
    // Free the values.
    // weld_value_free(result); /* Don't free result -- it should have been allocated using smalloc, and will be referenced by caller */
    weld_value_free(weld_args);
    weld_conf_free(conf);
    
    weld_error_free(e);
    weld_module_free(m);
    SPDLOG_DEBUG("Done with cleanup.");
    
    timersub(&tend, &tstart, &tdiff);
    fprintf(stderr, "Weld: %ld.%06ld\n",
            (long) tdiff.tv_sec, (long) tdiff.tv_usec);

    pair<double, double> request_times = FaultHandler::fh.get_request_times();
    SPDLOG_DEBUG("Time spent in compile: {:f}", compile_time);
    SPDLOG_DEBUG("Time spent in page requests: driver {:f} peer {:f}", 
		 request_times.first, request_times.second);
    SPDLOG_DEBUG("Time spent in compute: {:f}", compute_time - (request_times.first + request_times.second));

    perf_times times = {compile_time, request_times.first, request_times.second, compute_time-(request_times.first + request_times.second)};
    FaultHandler::fh.reset_request_time();
    
    /* Caller must know data size. smalloc knows allocated size, 
     * so new data will not be allocated over the result. */
    return make_pair(result_data, times);
}

/* shards is a weld_vector containing weld_vectors, in the order they should be iterated over. 
 * @return: an {i64, i64} struct indicating the shard and index within the shard in which the 
 *          element is located.
 */
void lookup_idx(void* input, void* output) {
  lookup_input* args = (lookup_input*)(input);
  weld_vector shards = args->shards;
  uint64_t index = args->index;
  printf("lookup index: %d\n", index);
  lookup_index* result = (lookup_index*)(output);
  
  int64_t nshards = shards.length;
  uint64_t cur_total = 0;

  /* if not found, error: index out of range */
  result->shard_idx = -1;
  result->element_idx = -1;

  for ( uint32_t i = 0; i < nshards; i++ ){
    int64_t next_size = (((weld_vector*)(shards.data))[i]).length;
    if ( cur_total + next_size > index ) {
      result->shard_idx = i;
      result->element_idx = index - cur_total;
      break;
    }

    cur_total += next_size;
  }
}

void partition_data(void* input, void* result) {
  partition_input* args = (partition_input*)(input);
  uint64_t data_size = (args->data_size);
  uint32_t nshards = (args->nshards);
  uint64_t increment = (args->increment);

  if ( nshards > data_size ) { nshards = data_size; }
  
  cout << "Shard request: " << data_size << " " << nshards << " " << increment << endl;
  shard* ret = (shard*)smalloc(sizeof(shard)*(nshards)); // TODO when to free this?
  for ( uint32_t i = 0; i < nshards; i++ ) {
    ret[i].start_index = 0;
    ret[i].size = 0;
  }

  // uint64_t rem = data_size;

  uint64_t rem = data_size % nshards;
  uint64_t shard_size = (data_size - rem) / nshards;
  for ( uint64_t i = 0; i < nshards; i++ ) {
    ret[i].size = shard_size;
  }
  
  uint32_t j = 0;
  while ( rem >= increment ) {
    ret[j].size += increment;
    rem -= increment;
    j = (j+1) % nshards;
  }

  ret[nshards - 1].size += rem; // put remainder in last shard to avoid ragged intermediate shards

  uint64_t total_size = 0;
  for ( uint32_t i = 0; i < nshards; i++ ) {
    ret[i].start_index = total_size;
    total_size += ret[i].size;
    cout << "Shard: " << ret[i].start_index << " " << "size: " << ret[i].size << endl;
  }

  weld_vector* vec_result = (weld_vector*)(result);
  (*vec_result).data = (void*)ret;
  (*vec_result).length = nshards;
}

void prefetch_i32(void* input, void* output) {
  weld_vector* input_vec = (weld_vector*)input;

  vector<future<int32_t>> futures;
  for ( int i = 0; i < input_vec->length; i++ ) {
    futures.push_back(async(launch::async,
                            [&input_vec] ( auto && idx ) {
                              int32_t x = ((int32_t*)input_vec)[idx];
			      return x;
                            }, i));
  }

  for ( auto & x : futures ) {
    x.get();
  }

  *(int32_t*)output = 0;
}

void prefetch_i32_i32_vec(void* input, void* output) {
  SPDLOG_DEBUG("Enter prefetch...");
  weld_vector* input_vec = (weld_vector*)input;

  vector<future<int32_t>> futures;
  for ( int i = 0; i < input_vec->length; i++ ) {
    futures.push_back(async(launch::async,
                            [&input_vec] ( auto && idx ) {
                              weld_vector x = ((weld_vector*)(input_vec->data))[idx];
			      int64_t len = x.length;
			      i32_i32* data = (i32_i32*)(x.data);
			      for ( int j = 0; j < len; j++ ) {
				volatile i32_i32 y = data[j];
			      }
			      return 0;
                            }, i));
  }

  for ( auto & x : futures ) {
    x.get();
  }

  SPDLOG_DEBUG("Exit prefetch...");
  *(int32_t*)output = 0;
}

void prefetch_i64(void* input, void* output) {
  weld_vector* input_vec = (weld_vector*)input;

  vector<future<int64_t>> futures;
  for ( int i = 0; i < input_vec->length; i++ ) {
    futures.push_back(async(launch::async,
                            [&input_vec] ( auto && idx ) {
                              int64_t x = ((int64_t*)input_vec)[idx];
			      return x;
                            }, i));
  }

  for ( auto & x : futures ) {
    x.get();
  }

  *(int64_t*)output = 0;
}

void prefetch_f64(void* input, void* output) {
  SPDLOG_DEBUG("Starting prefetch");
  weld_vector* input_vec = (weld_vector*)input;

  vector<future<double>> futures;
  for ( int i = 0; i < input_vec->length; i++ ) {
    futures.push_back(async(launch::async,
                            [&input_vec] ( auto && idx ) {
                              double x = ((double*)input_vec)[idx];
			      return x;
                            }, i));
  }

  for ( auto & x : futures ) {
    x.get();
  }

  SPDLOG_DEBUG("Finished prefetch");
  *(double*)output = 0;
}

void dispatch_all(void* input, void* output) {
  dispatch_all_input* args = (dispatch_all_input*)(input);
  weld_vector code_vec = args->code_vec;
  char* code = (char*)(code_vec.data);

  weld_vector input_data = args->input_data;
  uint64_t num_partitions = input_data.length;
  
  dispatch_output* res = (dispatch_output*)(output);

  grpc::ClientContext context;
  dsm::Task task;
  dsm::Result result;

  // send the intermediate computation times as well
  dsm::PerfTimes times;
  times.set_compile_time(0);
  auto request_times = FaultHandler::fh.get_request_times();
  times.set_request_time_driver(request_times.first);
  times.set_request_time_peer(request_times.second);

  auto old_driver_time = driver_time;
  driver_time = steady_clock::now();
  duration<double> secs = driver_time - old_driver_time;
  double compute_time = secs.count();
  times.set_compute_time(compute_time - (request_times.first + request_times.second));
  
  for ( uint64_t i = 0; i < num_partitions; i++ ) {
    void* addr = ((weld_vector*)(input_data.data))[i].data; // input_data is a vec[vec[{..}]] where the inner vectors each have a single element

    SPDLOG_DEBUG("data: {:x}", (uintptr_t)(addr));
    weld_vector* tmp = (weld_vector*)(addr);
    SPDLOG_DEBUG("Vector ptr: {:p}, length: {:d}", tmp->data, tmp->length);
    task.add_data_addrs((uintptr_t)(addr));
  }

  task.set_num_partitions(num_partitions);
  task.set_code(code);

  task.set_return_vectors(Worker::return_vectors);
  
  SPDLOG_DEBUG("code: {}", code);
  SPDLOG_DEBUG("partitions: {}", num_partitions);

  SPDLOG_DEBUG("Submitting task...");
  grpc::Status status = WeldUtils::manager_stub->SubmitTask(&context, task, &result);
  SPDLOG_DEBUG("Got response. {:d}", status.error_code());

  /* allocate space for the result */
  weld_vector* ret_data = (weld_vector*)(smalloc(sizeof(weld_vector)*num_partitions));
  (res->output_data).data = (void*)ret_data;
  (res->output_data).length = num_partitions;

  SPDLOG_DEBUG("Results: {:d}", result.results_list().size());
  for ( int i = 0; i < num_partitions; i++ ) {
    /* package result in a vector so we can pass pointer through instead of value */
    uintptr_t ptr = result.results_list(i);
    //SPDLOG_DEBUG("Result {:i}: {:d}", i, *(uint64_t*)(ptr));

    ret_data[i].data = (void*)(ptr);
    ret_data[i].length = 1;
    //SPDLOG_DEBUG("{:p} {:d}", ret_data[i].data, *(int64_t*)(ret_data[i].data));
  }

  SPDLOG_DEBUG("Returning response...");
}

void print_time(void* input, void* output) {
  struct timeval time;
  gettimeofday(&time, 0);
  double time_in_mill = 
    (time.tv_sec) * 1000 + (time.tv_usec) / 1000 ; // convert tv_sec & tv_usec to millisecond
  printf("Time: %.02f\n", time_in_mill);

  *(uint32_t*)output = 0;
}
