/* Main program for running workers + driver */

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
#include <spdlog/spdlog.h>

#include <stdint.h>
#include <sys/time.h>

#include <fstream>
#include <future>
#include <iostream>
#include <random>
#include <vector>
#include <string>
#include <utility>

#include "clamor_hashmap.h"

#include "clamor/debug.h"

#include "clamor/cluster.h"
#include "clamor/util.h"
#include "clamor/fault-handler.h"
#include "clamor/weld-utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;

constexpr int64_t NUM_ELEMENTS = 62500000;
constexpr int64_t NUM_INDEXES = 100000000;

struct input_args {
  t_weld_vector<t_weld_vector<uint64_t>> keys_shards;
  uint64_t hm_addr;
};

struct lookup_args {
  t_weld_vector<uint64_t> keys;
  uint64_t hm_addr;
};

// CUDF that each worker will call to look up into hash map.
extern "C" int r_lookup(void* p_input, void* p_result) {
  lookup_args* args = (lookup_args*)(p_input);

  uint64_t n = (args->keys).length;

  for ( uint64_t i = 0; i < n; i++ ) {
    uint64_t k = (args->keys).data[i];
    uint64_t hm_addr = args->hm_addr;
    HashMap<uint64_t, uint64_t, KeyHasher>* hm = (HashMap<uint64_t, uint64_t, KeyHasher>*)(hm_addr);
    
    uint64_t v = 0;
    if ( k < NUM_ELEMENTS ) {
      v = hm_lookup(hm, k);
    }
      
    *(int64_t*)p_result += v;
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
  string weld_filename = "";
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
    case 'l':
      weld_filename = optarg;
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
      HashMap<uint64_t, uint64_t, KeyHasher>* hm = hm_create(NUM_ELEMENTS);
      printf("...done.\n");
      gettimeofday(&cend, 0);
      timersub(&cend, &cstart, &cdiff);
      fprintf(stdout, "Hashmap create time: %ld.%06ld\n",
              (long) cdiff.tv_sec, (long) cdiff.tv_usec);
	
      printf("Hashmap addr: %p\n", hm);
      struct input_args args = *(input_args*)smalloc(sizeof(args));
      args.keys_shards = *(t_weld_vector<t_weld_vector<uint64_t>>*)smalloc(sizeof(weld_vector));
      args.keys_shards.data = (t_weld_vector<uint64_t>*)smalloc(sizeof(weld_vector)*npartitions);

      std::default_random_engine generator;
      std::uniform_int_distribution<uint64_t> distribution(0, NUM_ELEMENTS-1);
      ifstream in("zipf.txt", ifstream::in);

      uint64_t lookups_per_partition = NUM_INDEXES / npartitions;
      
      for ( uint64_t i = 0; i < npartitions; i++ ) {
	args.keys_shards.data[i].data = (uint64_t*)smalloc(sizeof(uint64_t)*lookups_per_partition);
	args.keys_shards.data[i].length = lookups_per_partition;
	for ( uint64_t j = 0; j < lookups_per_partition; j++ ) {
	  if (uniform_distribution) {
	    args.keys_shards.data[i].data[j] = distribution(generator);
	  } else {
	    uint64_t num;
	    in >> num;
	    if (num >= NUM_ELEMENTS) num = NUM_ELEMENTS-1;
	    args.keys_shards.data[i].data[j] = num;
	  }
	}
      }

      args.keys_shards.length = npartitions;
      args.hm_addr = (uintptr_t)((void*)(hm));
      
      string query = Util::read(weld_filename); // dispatch call will automatically be inserted during compilation
      
      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);
      
      void* res = driver.run_query(query, (void*)(&args), nprocs * worker_ips.size());
      int64_t ret_weld = *(int64_t*)(res);
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);

      printf("Result: %ld\n", ret_weld);
      
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
