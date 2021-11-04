/* Main program for running workers + driver */

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <stdint.h>
#include <sys/time.h>
#include <stdlib.h>
#include <time.h>

#include <future>
#include <iostream>
#include <random>
#include <vector>
#include <string>
#include <utility>

#include "clamor_kdtree.h"

#include "clamor/debug.h"

#include "clamor/weld.grpc.pb.h"

#include "clamor/cluster.h"
#include "clamor/util.h"
#include "clamor/fault-handler.h"
#include "clamor/weld-utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace weld;
using namespace std;


struct args {
  t_weld_vector<t_weld_vector<double>> lookup_idxs;
  uint64_t kdtree_addr;
};

struct lookup_args {
  uint64_t kdtree_addr;
  t_weld_vector<double> idx;
};

// CUDF that each worker will call to look up into k-d tree.
extern "C" int r_lookup(void* p_input, void* p_result) {
  lookup_args* args = (lookup_args*)(p_input);

  double idx[3];
  for (int i = 0; i < 3; i++) {
    idx[i] = (args->idx.data)[i];
  }

  uint64_t kdtree_addr = args->kdtree_addr;
  KdTree<double, uintptr_t, double[3]>* kdtree = (KdTree<double, uintptr_t, double[3]>*)(kdtree_addr);

  ResTuple tup = kdtree_lookup(kdtree, &idx);
  *(int64_t*)p_result = tup.dist;
}

int main(int argc, char** argv) {
  Cluster::Role role;

  string ip = "0.0.0.0";
  string manager_ip = "0.0.0.0"; /* Manager and driver IP are the same */

  uint32_t manager_port = 40000; /* Port that manager listens on for page requests. */
  uint32_t worker_port   = 50000; /* Port that worker listens on for cache invalidations. */
  uint32_t driver_port  = 70000; /* Port that driver listens on for cache invalidations. */

  uint32_t worker_range_start = 50000;

  uint32_t nprocs = 1; /* processes launched per worker node */
  string weld_filename = "kdtree.weld";
  vector<string> worker_ips;

  string driver = "driver";
  string manager = "manager";
  string worker = "worker";

  bool local = false;
  bool slow = false;

  int ch;
  while ((ch = getopt(argc, argv, "m:i:p:q:d:r:s:t:l:w:cz")) != -1) {
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
                             worker_ips, nprocs);

      uint64_t size = 12500000;
      //uint64_t size = 1250000;
      
      /* Create k-d tree in shared memory. */
      printf("Start create...\n");
      struct timeval cstart, cend, cdiff;
      gettimeofday(&cstart, 0);
      KdTree<double, uintptr_t, double[3]>* kdtree = kdtree_create(size);
      printf("...done.\n");
      gettimeofday(&cend, 0);
      timersub(&cend, &cstart, &cdiff);
      fprintf(stdout, "k-d tree create time: %ld.%06ld\n",
              (long) cdiff.tv_sec, (long) cdiff.tv_usec);
	
      printf("k-d tree addr: %p\n", kdtree);
      struct args args;
      uint64_t niters = 10000;
      uint64_t ndims = 3;
      srand(time(NULL));
      args.lookup_idxs.data = (t_weld_vector<double>*)smalloc(sizeof(t_weld_vector<double>)*niters);
      for ( uint64_t i = 0; i < niters; i++ ) {
	args.lookup_idxs.data[i].data = (double*)smalloc(sizeof(double)*ndims);
	for ( uint64_t j = 0; j < ndims; j++ ) {
	  args.lookup_idxs.data[i].data[j] = (double)(rand()) / RAND_MAX;
	}
	args.lookup_idxs.data[i].length = ndims;
      }
      args.lookup_idxs.length = niters;
      args.kdtree_addr = (uintptr_t)((void*)(kdtree));
      
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
				  worker_ips, nprocs, local);
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
