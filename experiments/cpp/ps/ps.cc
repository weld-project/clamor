/* Main program for running workers + driver */

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <stdint.h>
#include <sys/time.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <future>
#include <iostream>
#include <random>
#include <vector>
#include <string>
#include <utility>

#include "clamor_ps.h"

#include "clamor/debug.h"

#include "clamor/cluster.h"
#include "clamor/util.h"
#include "clamor/fault-handler.h"
#include "clamor/weld-utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;

constexpr int64_t NUM_WEIGHTS = 8000000000;
constexpr int64_t NUM_ITEMS = 80000000000;

struct item {
  t_weld_vector<uint64_t> x; // sparse one hot encoding: indices of 1s
  uint64_t y; // label
};

struct gd_iter_args {
  t_weld_vector<t_weld_vector<item>> items;
  int64_t hm_addr;
  uint64_t weight_idx;
};

struct gd_args {
  t_weld_vector<item> items;
  int64_t hm_addr;
  uint64_t weight_idx;
};

struct gd_ret {
  double grad;
  double loss;
};

struct load_iter_args {
  t_weld_vector<int64_t> x;
  int64_t num_items;
  uint64_t d;
};

struct load_args {
  int64_t num_items;
  uint64_t d;
};

// C UDF to load some data onto each worker
extern "C" int load_dummy_data(void* p_input, void* p_result) {
  std::default_random_engine generator;
  uniform_real_distribution<double> unif(0.0, 1.0);

  load_args* args = (load_args*)p_input;
  uint64_t n = args->num_items;
  uint64_t d = args->d;

  printf(">>>> Loading: %ld %ld\n", n, d);

  item* items = (item*)smalloc(sizeof(item)*n);
  for ( int i = 0; i < n; i++ ) {
    vector<uint64_t> idxs;
    double p = 0.1;
    for ( uint64_t j = 0; j < d; j++ ) {
      // just generate random sparse features for now
      if ( unif(generator) < p ) {
        idxs.push_back(j);
      }
    }

    uint64_t* data = (uint64_t*)smalloc(sizeof(uint64_t)*idxs.size());
    copy(idxs.begin(), idxs.end(), data);

    items[i].x = *(t_weld_vector<uint64_t>*)smalloc(sizeof(t_weld_vector<uint64_t>));
    items[i].x.data = data;
    items[i].x.length = idxs.size();

    // uniformly random labels
    items[i].y = 1;
    if ( unif(generator) < 0.5 ) {
      items[i].y = 0;
    }
  }

  *(t_weld_vector<item>*)p_result = *(t_weld_vector<item>*)smalloc(sizeof(t_weld_vector<item>));
  t_weld_vector<item>* res = (t_weld_vector<item>*)p_result;
  res->data = items;
  res->length = n;
}

double logistic_loss(double xTw, uint64_t y) {
  return log(1 + exp(-y*xTw));
}

// C UDF to compute gradient for data point.
extern "C" int compute_gradient(void* p_input, void* p_result) {
  gd_args* args = (gd_args*)(p_input);

  uint64_t hm_addr = args->hm_addr;
  HashMap<uint64_t, double, KeyHasher>* hm = (HashMap<uint64_t, double, KeyHasher>*)(hm_addr);

  *(gd_ret*)p_result = *(gd_ret*)smalloc(sizeof(gd_ret));
  gd_ret ret = *(gd_ret*)p_result;
    
  // sum over all gradients for our training data
  t_weld_vector<item> items = (args->items);
  for ( int i = 0; i < items.length; i++ ) {
    item item = items.data[i];
    t_weld_vector<uint64_t> x = item.x;
    uint64_t y = item.y;
      
    // compute x^Tw using sparse coords
    double xTw = 0.0;
    for ( int j = 0; j < x.length; j++ ) {
      xTw += ps_lookup(hm, x.data[j]); // only retrieve relevant weight from parameter server
    }
      
    xTw *= -y;
    double grad = xTw * exp(xTw * -y) * item.x.data[args->weight_idx]; // do we only do one weight at a time?

    double loss = logistic_loss(xTw, y);

    ret.grad += grad;
    ret.loss += loss;
  }
}

int main(int argc, char** argv) {
  Cluster::Role role;

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
  while ((ch = getopt(argc, argv, "m:i:p:q:d:r:s:t:w:x:ucz")) != -1) {
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
 
      /* Create hash map in shared memory. */
      printf("Start create...\n");
      struct timeval cstart, cend, cdiff;
      gettimeofday(&cstart, 0);
      HashMap<uint64_t, double, KeyHasher>* hm = ps_create(NUM_WEIGHTS);
      printf("...done.\n");
      gettimeofday(&cend, 0);
      timersub(&cend, &cstart, &cdiff);
      fprintf(stdout, "Hashmap create time: %ld.%06ld\n",
              (long) cdiff.tv_sec, (long) cdiff.tv_usec);
	
      printf("Hashmap addr: %p\n", hm);
      struct gd_iter_args args;

      std::default_random_engine generator;
      std::uniform_int_distribution<uint64_t> distribution(0, NUM_WEIGHTS-1);
      args.hm_addr = (uintptr_t)((void*)(hm));

      string load_data_file = "load_data.weld";
      string gradient_descent_file = "gradient_descent.weld";
      
      string load_query = Util::read(load_data_file); 
      string gd_query = Util::read(gradient_descent_file); 

      /* load the dummy data */
      struct load_iter_args load_args;
      load_args.x = *(t_weld_vector<int64_t>*)smalloc(sizeof(t_weld_vector<int64_t>));
      load_args.x.length = nprocs*worker_ips.size() * 10; // generate ~10 data shards per worker
      load_args.x.data = (int64_t*)smalloc(sizeof(int64_t) * load_args.x.length);
      for ( int i = 0; i < load_args.x.length; i++ ) {
	load_args.x.data[i] = i;
      }
      load_args.num_items = NUM_ITEMS;
      load_args.d = NUM_WEIGHTS;
      void* loaded_items = driver.run_query(load_query, (void*)(&load_args), nprocs*worker_ips.size()); // returns vec<vec<item>>

      /* run distributed gradient descent! */
      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);
      
      uint64_t num_iters = 100;
      for ( uint64_t i = 0; i < num_iters; i++ ) {
	printf("Iteration: %d\n", i);
        for ( uint64_t j = 0; j < NUM_WEIGHTS; j++ ) { // coordinate descent
	  args.weight_idx = j;
	  args.items = *(t_weld_vector<t_weld_vector<item>>*)loaded_items;
	  
          // compute gradients
          void* res = driver.run_query(gd_query, (void*)(&args), nprocs * worker_ips.size());
	  gd_ret res_grad = *(gd_ret*)res;
	  printf("Iter %ld, coordinate %ld, loss %f\n", i, j, res_grad.loss);
          
          // regularize current weights
          ps_regularize(hm);
          
          // update gradients
          ps_update(hm, j, res_grad.grad);
        }
      }
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);

      //printf("Result: %ld\n", ret_weld);
      
      fprintf(stderr, "Weld, %d worker(s): %ld.%06ld\n",
              nprocs, (long) tdiff.tv_sec, (long) tdiff.tv_usec);
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
