/* Main program for running workers + driver */

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <stdint.h>
#include <sys/time.h>

#include <future>
#include <iostream>
#include <vector>
#include <string>
#include <utility>

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

typedef struct lookups_args {
  weld_vector data;
  weld_vector indexes;
} lookups_args;

int main(int argc, char** argv) {
  Cluster::Role role;

  spdlog::default_logger()->set_level(spdlog::level::debug);
  SPDLOG_DEBUG("Running...");
  
  string ip = "0.0.0.0";
  string manager_ip = "0.0.0.0"; /* Manager and driver IP are the same */

  uint32_t manager_port = 40000; /* Port that manager listens on for page requests. */
  uint32_t worker_port   = 50000; /* Port that worker listens on for cache invalidations. */
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
  bool speculate = true;
  
  int ch;
  while ((ch = getopt(argc, argv, "m:i:p:q:d:r:s:t:l:w:cz")) != -1) {
    switch (ch) {
    case 'm': // Server role
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
    case 'i': // Local IP
      ip = optarg;
      break;
    case 'p': // Port on which workers are listening
      worker_port = atoi(optarg);
      break;
    case 'd': // Manager IP
      manager_ip = optarg;
      break;
    case 'r': // Port on which manager is listening
      manager_port = atoi(optarg);
      break;
    case 's': // Port on which driver is listening
      driver_port = atoi(optarg);
      break;
    case 't': // Processes per worker machine
      nprocs = atoi(optarg); 
      break;
    case 'l': // Weld program to execute
      weld_filename = optarg;
      break;
    case 'w': // List of worker IP addresses, comma-separated
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

      string url = "http://weld-dsm-2.s3-us-west-2.amazonaws.com/test-ints-sm";
      auto mapped_data = driver.map_url(url);
      char* data = mapped_data.first;
      //size_t size = mapped_data.second;
      size_t size = sizeof(int32_t)*1024*8;
      size_t nelements = size / sizeof(int32_t);

      string query = Util::read(weld_filename); // dispatch call will automatically be inserted during compilation

      int64_t* indexes = (int64_t*)smalloc(sizeof(int64_t)*nelements);
      for (int i = 0; i < size; i++) {
        indexes[i] = i;
      }
      
      lookups_args args;
      (args.data).data = (void*)data;
      (args.data).length = nelements;
      (args.indexes).data = (void*)indexes;
      (args.indexes).length = nelements;

      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);

      void* res = driver.run_query(query, (void*)(&args), nprocs * worker_ips.size());
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);
      fprintf(stderr, "Result: %ld\n", *(uint64_t*)(res));
      
      fprintf(stderr, "%d: %ld.%06ld\n",
              nprocs, (long) tdiff.tv_sec, (long) tdiff.tv_usec);

      /*
      gettimeofday(&tstart, 0);

      void* cached_res = driver.run_query(query, args, nprocs * worker_ips.size());
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);
      fprintf(stderr, "Result after caching: %ld\n", *(uint64_t*)(cached_res));
      
      fprintf(stderr, "%d cores available, cached: %ld.%06ld\n",
              nprocs, (long) tdiff.tv_sec, (long) tdiff.tv_usec);
      */
    }
    break;
   case Cluster::Role::MANAGER:
    {
      Cluster::start_task_manager(ip, manager_port,
				  ip, driver_port,
				  worker_range_start,
				  worker_ips, nprocs, local, speculate);
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
