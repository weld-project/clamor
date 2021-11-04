#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>

#include <algorithm>
#include <iostream>
#include <vector>
#include <string>

#include "clamor/cluster.h"
#include "clamor/weld-utils.h"
#include "../tpch_util/s3utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;

char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

struct output {
  int32_t elem1;
  int32_t elem2;
  float elem3;

  output(int32_t e1, int32_t e2, float e3)
      : elem1(e1), elem2(e2), elem3(e3) {}

  bool operator==(const output &other) {
    return elem1 == other.elem1 &&
           elem2 == other.elem2 &&
           static_cast<int32_t>(elem3) == static_cast<int32_t>(other.elem3);
  }

  bool operator<(const output &other) {
    return elem1 < other.elem1;
  }

  friend ostream& operator<<(ostream& os, const output& res);
};

ostream& operator<<(ostream& os, const output& res) {
  os << res.elem1 << "," << res.elem2 << ", " << res.elem3;
  return os;
}

struct weld_args {
  weld_vector suppliers;
  weld_vector revenue;
};

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
  string weld_filename;
  vector<string> worker_ips;

  string driver = "driver";
  string manager = "manager";
  string worker = "worker";

  bool local = false;
  bool slow = false;
  bool speculate = false;
  
  unsigned int seed = 1;
  int num_items = 4000;
  bool run_local = false;

  int64_t npartitions = 64;
  
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
    case 'e': // Seed for randomly populating data.
      seed = atoi(optarg);
      break;
    case 'n': // Number of items.
      num_items = atoi(optarg);
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
      auto mapped_lineitem = driver.map_url(lineitem_url, /*local=*/true);

      //string supplier_url = "http://weld-dsm-east.s3.amazonaws.com/supplier";
      string supplier_url = "/home/ubuntu/clamor/baselines/tpch_data/supplier_large";
      auto mapped_supplier = driver.map_url(supplier_url, /*local=*/true);

      weld_vector args_load;
      args_load.data = mapped_lineitem.first;
      args_load.length = mapped_lineitem.second / sizeof(Lineitem);

      
      string query_load = Util::read("load.weld");
      
      void* res_load = driver.run_query(
                              query_load, &args_load, nprocs * worker_ips.size());

      string query_p0 = Util::read("tpch_q15_phase0.weld");
      weld_vector args_p0;
      args_p0.data = mapped_supplier.first;
      args_p0.length = mapped_supplier.second / sizeof(Supplier);

      weld_vector* res_p0 = (weld_vector*)driver.run_query(
                              query_p0, &args_p0, nprocs * worker_ips.size());


      string query_p1 = Util::read("tpch_q15_phase1.weld");
     
      weld_vector args_p1;
      args_p1.data = mapped_lineitem.first;
      args_p1.length = mapped_lineitem.second / sizeof(Lineitem);

      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);
      weld_vector* res_p1 = (weld_vector*)driver.run_query(
                              query_p1, &args_p1, nprocs * worker_ips.size());

      weld_args args_p2;
      args_p2.suppliers = *res_p0;
      args_p2.revenue = *res_p1;

      string query_p2 = Util::read("tpch_q15_phase2.weld");

      weld_vector* res = (weld_vector*)driver.run_query(
                              query_p2, &args_p2, nprocs * worker_ips.size());

      /*
      // Sort.
      string query_p3 = Util::read("tpch_q15_phase3.weld");
      weld_vector* res = (weld_vector*)driver.run_query(
                              query_p3, res_p2, nprocs * worker_ips.size());
      */

      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);
      
      fprintf(stderr, "%d: %ld.%06ld\n",
              nprocs*worker_ips.size(), (long) tdiff.tv_sec, (long) tdiff.tv_usec);  
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
