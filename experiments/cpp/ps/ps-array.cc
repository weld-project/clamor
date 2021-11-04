/* Main program for running workers + driver */

//#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
//#include <spdlog/spdlog.h>

#include <stdint.h>
#include <sys/time.h>
#include <sys/mman.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <future>
#include <iostream>
#include <random>
#include <vector>
#include <string>
#include <unordered_map>
#include <utility>

#include "clamor/dsm.grpc.pb.h"
#include "clamor/debug.h"
#include "clamor/lock.h"

#include "clamor/cluster.h"
#include "clamor/util.h"
#include "clamor/util-templates.cc"
#include "clamor/fault-handler.h"
#include "clamor/worker-server.h"
#include "clamor/weld-utils.h"

#include "omp.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;

constexpr uint64_t num_weights = 4000000000;
constexpr uint64_t num_locks = 10000000;
constexpr uint64_t weights_per_lock = num_weights / num_locks;
//constexpr int64_t num_weights = 1000000;
//constexpr int64_t num_items = 200000000;
//constexpr int64_t num_items = 80000000;

uint64_t lock_for_key( uint64_t key ) {
  uint64_t lock = key / weights_per_lock;
  return lock;
}

struct item {
  t_weld_vector<uint64_t> x; // sparse one hot encoding: indices of 1s
  double y; // label
};

struct gd_iter_args {
  t_weld_vector<t_weld_vector<item>> items;
  int64_t hm_addr;
  uint64_t weights_start;
  uint64_t weights_end;
};

struct gd_args {
  t_weld_vector<item> items;
  int64_t hm_addr;
  uint64_t weights_start;
  uint64_t weights_end;
};

// for sparse gradient return
struct grad_kv {
  uint64_t k;
  double grad;
};

struct gd_ret {
  t_weld_vector<grad_kv> grads;
  double loss;
};

struct load_pair {
  uint32_t idx;
  uint64_t row_start;
  uint64_t row_end;
};

struct load_iter_args {
  t_weld_vector<t_weld_vector<load_pair>> x;
};

struct load_args {
  t_weld_vector<load_pair> x;
};

// Modifies weights array in-place
void regularize(double* weights) {
  double lr = 0.01;
  double lambda = 1.0;
  double reg = lr*lambda;

  for ( uint64_t i = 0; i < num_weights; i++ ) {
    if ( weights[i] < 0 ) {
      weights[i] -= reg;
    } else {
      weights[i] += reg;
    }
  }
}

// assumes grad is already multiplied by learning rate.
void update(double* weights, uint64_t idx, double grad) {
  weights[idx] -= grad;
}

extern "C" int load_zipf_data(void* p_input, void* p_output) {
  t_weld_vector<load_pair>* pairs = (t_weld_vector<load_pair>*)p_input;
  string url_base = "http://weld-dsm-east.s3.amazonaws.com/uniform_data/uniform_";

  t_weld_vector<item>* res_vec = (t_weld_vector<item>*)p_output;
  uint64_t len = 0;
  for ( uint64_t i = 0; i < pairs->length; i++ ) {
    len += pairs->data[i].row_end - pairs->data[i].row_start;
  }

  int64_t repetitions = 25;
  len *= repetitions;
  
  res_vec->data = (item*)smalloc(sizeof(item)*len);
  res_vec->length = len;

  uint64_t offset = 0;
  for ( uint64_t fidx = 0; fidx < pairs->length; fidx++ ) {
    load_pair pair = pairs->data[fidx];
    std::ostringstream ss;
    ss << url_base << pair.idx << ".csv";
    string str_url = ss.str();
    
    SPDLOG_INFO("Download from url: {:s}", str_url);

    MemoryStruct chunk;
  
    FaultHandler::fh.download_to_chunk(&chunk, -1, -1, str_url);
    string str_data(chunk.memory);
    free(chunk.memory);

    vector<string> lines = Util::split(str_data, '\n');
    vector<vector<uint64_t>> split_lines;
    for ( uint64_t i = pair.row_start; i < pair.row_end; i++ ) {
      auto & line = lines.at(i);
      auto split_line_str = Util::split(line, ',');
      vector<uint64_t> int_line;
      for ( uint64_t j = 0; j < split_line_str.size(); j++ ) {
	auto & x = split_line_str.at(j);
	if ( (j % 2) == (i % 2) ) { // drop alternating values so we can learn something
	  int_line.push_back(atoi(x.c_str()) - 1);
	}
      }
      std::sort(int_line.begin(), int_line.end());
      split_lines.push_back(int_line);
    }

    for ( uint64_t i = 0; i < split_lines.size() * repetitions; i++ ) {
      if ( i % 50 == 0 ) printf("Loading %d\n", i);
      auto & data_vec = split_lines.at(i % split_lines.size());
      (res_vec->data)[i + offset].x.data = (uint64_t*)smalloc(sizeof(uint64_t)*data_vec.size());
      size_t ctr = 0;
      for ( uint64_t j = 0; j < data_vec.size(); j++ ) {
	uint64_t idx = data_vec.at(j);
	if ( idx < num_weights ) {
	  (res_vec->data)[i + offset].x.data[j] = idx;
	  ctr++;
	}
      }
      (res_vec->data)[i + offset].y = i % 2;
      (res_vec->data)[i + offset].x.length = ctr;
    }

    offset += (pair.row_end - pair.row_start) * repetitions;
  }
}

double logistic_loss(double xTw, double y) {
  double sigmoid = 1/(1+exp(-xTw));
  return -y*log(sigmoid) - (1-y)*log(1-sigmoid);
}

double aggregate(double* weights, dsm::Pages & pages, pthread_mutex_t* locks,
		 double lr, uint64_t npartitions) {
  printf("In aggregate\n");
  auto num_pages = pages.pages().size();
  char* res_arr = (char*)malloc(PG_SIZE * num_pages);
  printf("Data addr: %x\n", pages.start_addr());
  uint64_t j = 0;
  for ( auto & page : pages.pages() ) {
    memcpy((void*)(res_arr + j*PG_SIZE),
	   page.pagedata().data(), page.pagedata().size());
    j++;
  }

  char* offset_arr = res_arr + sizeof(memhdr)*2;
  gd_ret* ret = (gd_ret*)offset_arr;
  double task_loss = ret->loss;
  printf("loss %f\n", task_loss);
  t_weld_vector<grad_kv> kv = ret->grads;
  grad_kv* data = kv.data;
  uint64_t num_kvs = kv.length;
  uintptr_t offset = (uintptr_t)data - pages.start_addr();
  //printf("data addr %x, offset %x\n", (void*)data, offset);
  grad_kv* data_real = (grad_kv*)(res_arr + offset);

  for ( uint64_t j = 0; j < num_kvs; j++ ) {
    if ( j % 10000 == 0 ) printf("processing %ld of %ld\n", j, num_kvs);
    uint64_t key = data_real[j].k;
    double grad = data_real[j].grad / npartitions;
    Lock::lock(&(locks[lock_for_key(key)]));
    weights[key] -= lr*grad;
    Lock::unlock(&(locks[lock_for_key(key)]));
  }

  printf("done.\n");
  
  free(res_arr);

  printf("returning\n");
  return task_loss / npartitions;
}

// returns aggregated loss.
double aggregate_gradients(double* weights, Cluster::Driver & driver,
			   uint64_t npartitions, pthread_mutex_t* locks,
			   t_weld_vector<gd_ret> grads, double lr) {
  double loss = 0.0;
  printf("num grads: %d\n", grads.length);

  // get the raw result byte arrays instead of transparently indexing.
  vector<future<dsm::Pages>> page_futures;
  for ( uint64_t i = 0; i < npartitions; i++ ) {
    page_futures.push_back( async( launch::async,
				   [&driver] ( auto && i )  {
				     return driver.get_pages(i);
				   },
				   i)
			    );
  }

  printf("getting pages\n");
  vector<dsm::Pages> pages_vec;
  for ( auto & x : page_futures ) { pages_vec.push_back(x.get()); }

  double weights_tot = 0;
  for ( uint64_t i = 0 ; i < num_weights; i++ ) {
    weights[i] += 0.1;
    weights[i] -= 0.1;
    weights_tot += weights[i];
  }
  printf("%f\n", weights_tot);
  
  printf("got pages, launching aggregate\n");
  vector<future<double>> loss_futures;
  for ( uint64_t i = 0; i < npartitions; i++ ) {
    loss_futures.push_back( async( launch::async,
				   [&weights, &locks, &lr, &npartitions] ( auto && in_pages ) {
				     return aggregate(weights, in_pages, locks, lr, npartitions);
				   },
				   pages_vec.at(i))
			    );
  }

  for ( auto & x : loss_futures ) {
    loss += x.get();
  }

  return loss;
}

double eps = 0.0;
//double eps = 0.0;
uint64_t blocksize = num_weights / 1000;

// C UDF to compute gradient for data point.
extern "C" int compute_gradient(void* p_input, void* p_result) {
  printf("Entering compute gradient\n");
  gd_args* args = (gd_args*)(p_input);

  printf("Got args\n");
  int64_t hm_addr = args->hm_addr;
  double* hm = (double*)hm_addr;

  printf("Creating result struct\n");
  gd_ret* ret = (gd_ret*)p_result;
  ret->loss = 0.0;
  //ret->grads = *(t_weld_vector<grad_kv>*)smalloc(sizeof(weld_vector));

  SPDLOG_INFO("Range: {:d} {:d}", args->weights_start, args->weights_end);
  printf("Hash map addr: %p\n", args->hm_addr);
  printf("num things in vec: %ld\n", (args->items).length);
  
  // sum over all gradients for our training data
  t_weld_vector<item> items = (args->items);
  vector<double> xtws(items.length, 0.0);
  for ( uint64_t i = 0; i < items.length; i++ ) {
    item item = items.data[i];
    t_weld_vector<uint64_t> x = item.x;
    double y = item.y;
    
    // compute x^Tw using sparse coords
    double xTw = 0.0;
    
    for ( int j = 0; j < x.length; j++ ) {
      xTw += hm[x.data[j]]; // only retrieve relevant weight from parameter server
    }
    
    //xtws[i] = (1/(1+exp(xTw * -y)) - 1) * y;
    xtws[i] = 1/(1+exp(-xTw)) - y;
    //printf("%f %f %f\n", xTw, xtws[i], y);
    
    double loss = logistic_loss(xTw, y);
    ret->loss += loss / (double)(items.length);
  }

  // update weights in blocks
  ret->grads.data = (grad_kv*)smalloc(sizeof(grad_kv));
  ret->grads.length = 0;

  uint64_t updates_offset = 0;
  uint64_t n_nonzero = 0;
  //vector<double> grads(blocksize, 0.0);
  vector<uint64_t> ctrs(items.length, 0);
  for ( uint64_t weight_idx = args->weights_start; weight_idx < args->weights_end; weight_idx += blocksize ) { 
    unordered_map<uint64_t, double> grads;
    if ( weight_idx % 100000 == 0 ) SPDLOG_DEBUG("Updating weight {:d}", weight_idx);
    uint64_t offset = weight_idx;
    uint64_t block_end = offset + blocksize;
    
    for ( uint64_t i = 0; i < items.length; i++ ) {
      double grad_base = xtws[i];
      item item = items.data[i];
      t_weld_vector<uint64_t> x = item.x;

      while ( (x.data[ctrs[i]] < offset) and ctrs[i] < x.length ) { ctrs[i]++; }
      while ( x.data[ctrs[i]] < block_end and ctrs[i] < x.length ) {
	uint64_t data_w = x.data[ctrs[i]];
	auto it = grads.find(data_w - offset);
	if ( it == grads.end() ) {
	  grads.emplace(data_w - offset, grad_base);
	} else {
	  it->second += grad_base;
	}
	ctrs[i]++;
      }
    }

    n_nonzero += grads.size();

    SPDLOG_DEBUG("done update. updating grads: {:d}", grads.size());
    ret->grads.data = (grad_kv*)srealloc(ret->grads.data, sizeof(grad_kv)*(n_nonzero));
    ret->grads.length = n_nonzero;

    // create sparse vector
    for ( auto & x : grads ) {
      ret->grads.data[updates_offset].k = offset + x.first;
      ret->grads.data[updates_offset].grad = x.second / (double)(items.length);
      //SPDLOG_DEBUG("Gradient: {:f}, original {:f}, divided {:f}", ret->grads.data[updates_offset].grad,
      //	     grads.at(i), grads.at(i) / (double)(items.length));
      updates_offset++;
    }

    assert( updates_offset == n_nonzero );
    SPDLOG_DEBUG("done partition");
  }

  SPDLOG_INFO("Nonzero weights: {:d}", n_nonzero);
}

int main(int argc, char** argv) {
  Cluster::Role role;

  string weld_filename="";
  
  string ip = "0.0.0.0";
  string manager_ip = "0.0.0.0"; /* Manager and driver IP are the same */

  uint32_t manager_port = 40000; /* Port that manager listens on for page requests. */
  uint32_t worker_port = 50000; /* Port that worker listens on for cache invalidations. */
  uint32_t driver_port  = 70000; /* Port that driver listens on for cache invalidations. */

  uint32_t worker_range_start = 50000;

  uint32_t nprocs = 1; /* processes launched per worker node */
  vector<string> worker_ips;

  string driver = "driver";
  string manager = "manager";
  string worker = "worker";

  bool local = false;
  bool slow = false;
  bool speculate = false;

  bool uniform_distribution = false;

  int64_t npartitions = 64;
  
  int ch;
  while ((ch = getopt(argc, argv, "m:i:p:q:d:r:s:t:l:w:x:ucz")) != -1) {
    switch (ch) {
    case 'm':
      if ( optarg == driver ) {
        role = Cluster::Role::DRIVER;
      } else if ( optarg == manager ) {
        role = Cluster::Role::MANAGER;
      } else if ( optarg == worker ) {
        role = Cluster::Role::WORKER;
      } else {
        cout << "Error: Role not recognized: " << optarg << endl;
        exit(1);
      }
      break;
    case 'i':
      ip = optarg;
      break;
    case 'p':
      worker_port = atoi(optarg);
      break;
    case 'd':
      manager_ip = optarg;
      break;
    case 'r':
      manager_port = atoi(optarg);
      break;
    case 's':
      driver_port = atoi(optarg);
      break;
    case 't':
      nprocs = atoi(optarg); 
      break;
    case 'w':
      worker_ips = Util::split(optarg, ",");
      break;
    case 'x': // Partitions.
      npartitions = atoi(optarg);
      break;
    case 'u':
      uniform_distribution = true;
      break;
    case 'l': // Weld program to execute
      weld_filename = optarg;
      break;
    case 'c':
      local = true;
      break;
    case 'z':
      slow = true; // Adds delay to task execution, for testing speculation
      break;
    case '?':
    default:
      fprintf(stderr, "invalid options");
      exit(1);
    }
  }

  string driver_ip = manager_ip;

  for ( auto & x : worker_ips ) {
    cout << "Worker: " << x << endl;
  }

  switch ( role ) {
  case Cluster::Role::DRIVER:
    {
      Cluster::Driver driver(ip, driver_port,
                             manager_ip, manager_port,
                             worker_ips, nprocs, npartitions);
 
      printf("Threads: %d\n", omp_get_num_threads());
      omp_set_dynamic(0);
      omp_set_num_threads(30);
      printf("Threads: %d\n", omp_get_num_threads());
      
      /* Create weights array in shared memory. */
      double* weights = (double*)smalloc(sizeof(double)*num_weights);
      std::fill(weights, weights + num_weights, 0.0);
      if (mlock(weights, sizeof(double)*num_weights) != 0) {
        printf("Could not lock swap: %d\n", errno);
        exit(1);
      } // prevent swap  
      printf("Weights addr: %p\n", weights);

      // a little brute force, but it's ok
      pthread_mutex_t* locks = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t)*num_weights);
      for ( uint64_t i = 0; i < num_locks; i++ ) {
	Lock::init_default(&locks[i]);
      }
     
      struct gd_iter_args args = *(gd_iter_args*)malloc(sizeof(gd_iter_args));

      std::default_random_engine generator;
      std::uniform_int_distribution<uint64_t> distribution(0, num_weights-1);
      args.hm_addr = (int64_t)((void*)(weights));

      string load_data_file = "load_data.weld";
      string gradient_descent_file = "gradient_descent.weld";
      
      string load_query = Util::read(load_data_file); 
      string gd_query = Util::read(gradient_descent_file); 

      //string url_base = "http://weld-dsm-east.s3.amazonaws.com/zipf_data/zipf_";
      uint64_t rows_per_file = 3000000;
      uint64_t num_files = 32;
      uint64_t total_rows = rows_per_file * num_files;
      uint64_t rows_per_part = total_rows / npartitions;
      printf("Rows per partition: %ld\n", rows_per_part);
      
      t_weld_vector<t_weld_vector<load_pair>>* pairs = (t_weld_vector<t_weld_vector<load_pair>>*)smalloc(sizeof(weld_vector)); 
      pairs->length = npartitions;
      pairs->data = (t_weld_vector<load_pair>*)smalloc(sizeof(weld_vector)*npartitions);
      
      uint64_t idx = 0;
      uint64_t offset = 0;
      for ( uint64_t i = 0; i < npartitions; i++ ) {
	uint64_t rem = rows_per_part;
	std::vector<load_pair> pairs_vec;
	while ( rem > 0 ) {
	  uint64_t num = std::min(rows_per_file - offset, rem);
	  load_pair pair = {idx, offset, offset+num};
	  pairs_vec.push_back(pair);
	  rem -= num;
	  printf(">>>>>> Offsets %ld: %ld %ld %ld %ld %ld\n", i, idx, offset, offset+num, rem, rows_per_file - offset);
	  offset += num;
	  if ( (rows_per_file - offset) <= rem ) {
	    idx++;
	    offset = 0;
	  } 
	}
	pairs->data[i].length = pairs_vec.size();
	pairs->data[i].data = (load_pair*)smalloc(sizeof(load_pair)*pairs_vec.size());
	std::copy(pairs_vec.begin(), pairs_vec.end(), pairs->data[i].data);
      }
      
      void* loaded_items = driver.run_query(load_query, (void*)(pairs), nprocs*worker_ips.size(), false); // returns vec<vec<item>>
      args.items = *(t_weld_vector<t_weld_vector<item>>*)loaded_items;

      /* run distributed gradient descent! */
      
      uint64_t num_iters = 10;
      double lr = 1.0;

      for ( uint64_t i = 0; i < num_iters; i++ ) {
	struct timeval tstart, tend, tdiff;
	gettimeofday(&tstart, 0);
	printf("Iteration: %d\n", i);

	// regularize current weights
	//regularize(weights);
        
	//printf("Weight: %ld\n", j);//, args.hm_addr);
	args.weights_start = 0;
	args.weights_end = num_weights;
	
	// compute gradients
	void* res = driver.run_query(gd_query, (void*)(&args), nprocs * worker_ips.size(), /*spec_return=*/false, /*cache_none=*/true);
	t_weld_vector<gd_ret>* res_grads = (t_weld_vector<gd_ret>*)res;
	  
	// update gradients
	double loss = aggregate_gradients(weights, driver, npartitions, locks, *res_grads, lr);
	printf("Iteration %d, loss: %f\n", i, loss);
	printf("Weights: ");
	for ( int j = 0; j < 10; j++ ) {
	  printf("%f ", weights[j]);
	}
	printf("\n");

	gettimeofday(&tend, 0);
	timersub(&tend, &tstart, &tdiff);
	
	fprintf(stderr, "Iteration %ld: %ld.%06ld\n",
		i, (long) tdiff.tv_sec, (long) tdiff.tv_usec);
      }
    }
    break;
   case Cluster::Role::MANAGER:
    {
      Cluster::start_task_manager(ip, manager_port,
				  ip, driver_port,
				  worker_range_start,
				  worker_ips, nprocs, npartitions, local, speculate);
      return 0;
    }
    break;
  case Cluster::Role::WORKER:
    {
      Cluster::start_worker(ip, worker_port,
                            manager_ip, manager_port,
                            manager_ip, driver_port, slow);
      return 0;
    }
    break;
  }
}
