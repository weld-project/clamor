#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>

#include <iostream>
#include <vector>
#include <string>

#include "clamor/cluster.h"
#include "clamor/curl-handle.h"
#include "clamor/weld-utils.h"

#include "../tpch_util/s3utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;
using namespace weld;

/* Shared address buffer */
char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

int main(int argc, char** argv) {
  Cluster::Role role;
  spdlog::default_logger()->set_level(spdlog::level::debug);
  SPDLOG_DEBUG("Running...");
  
  string ip = "0.0.0.0";
  string manager_ip = "0.0.0.0"; /* Manager and driver IP are the same */

  uint32_t manager_port = 40000; /* Port that manager listens on for page requests. */
  uint32_t worker_port = 50000; /* Port that worker listens on for cache invalidations. */
  uint32_t driver_port = 70000; /* Port that driver listens on for cache invalidations. */

  uint32_t worker_range_start = 50000;

  uint32_t nprocs = 1; /* processes launched per worker node */
  string weld_filename = "tpch_q6.weld";
  vector<string> worker_ips;

  string driver = "driver";
  string manager = "manager";
  string worker = "worker";

  bool local = false;
  bool slow = false;
  bool speculate = false;

  int npartitions = 64;
  
  int ch;
  while ((ch = getopt(argc, argv, "m:i:p:q:d:r:s:t:l:w:x:cz")) != -1) {
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
    case 'x': // Partitions.
      npartitions = atoi(optarg);
      break;
    case 'c': 
      local = true;
      break;
    case 'z':
      slow = true; // Adds delay to task execution, for testing speculation
      break;
    default:
      fprintf(stderr, "invalid options");
      exit(1);
    }
  }

  string driver_ip = manager_ip;

  switch ( role ) {
  case Cluster::Role::DRIVER:
    {
      Cluster::Driver driver(ip, driver_port,
                             manager_ip, manager_port,
                             worker_ips, nprocs, npartitions);

      //string lineitem_url = "http://weld-dsm-east.s3.amazonaws.com/lineitem";
      string lineitem_url = "/home/ubuntu/clamor/baselines/tpch_data/lineitem_large";
      auto mapped_data = driver.map_url(lineitem_url, /*local=*/true);

      weld_vector args_load;
      args_load.data = mapped_data.first;
      args_load.length = mapped_data.second / sizeof(Lineitem);

      string query_load = Util::read("load.weld");
      
      void* res_load = driver.run_query(
                              query_load, &args_load, nprocs * worker_ips.size());

      string query = Util::read("tpch_q6.weld");
      weld_vector args;
      args.data = mapped_data.first;
      args.length = mapped_data.second / sizeof(Lineitem);      

      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);

      void* res = driver.run_query(query, &args, nprocs * worker_ips.size());

      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);
      fprintf(stderr, "process time: %d: %ld.%06ld\n",
              nprocs*worker_ips.size(), (long) tdiff.tv_sec, (long) tdiff.tv_usec);
      printf("Result: %f\n", *(double*)res);
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
