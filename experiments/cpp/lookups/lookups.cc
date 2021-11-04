#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>

#include <iostream>
#include <vector>
#include <string>

#include "clamor/cluster.h"
#include "clamor/weld-utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using std::string;
using std::cout;
using std::endl;
using std::vector;

struct weld_args {
  weld_vector data;
  weld_vector indexes;
};

char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

constexpr int64_t data_length = 1250000000; // 10 GB of int32_t
constexpr int64_t index_length = 1000;
//constexpr int64_t data_max = 1000;

void load_data(int32_t *data, int64_t *indexes) {
  for (int32_t i = 0; i < data_length; ++i) {
    data[i] = i;
  }
  for (int32_t i = 0; i < index_length; ++i) {
    indexes[i] = static_cast<int64_t>(rand() % data_length);
  }
}

int64_t run_query_local(int32_t *data, int64_t *indexes) {
  int64_t sum = 0;
  for (int i = 0; i < index_length; ++i) {
    sum += static_cast<int64_t>(data[indexes[i]] * 2);
  }
  return sum;
}

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
  string weld_filename = "lookups.weld";
  vector<string> worker_ips;

  string driver = "driver";
  string manager = "manager";
  string worker = "worker";

  bool local = false;
  bool slow = false;
  bool speculate = true;
  
  unsigned int seed = 1;

  int ch;
  while ((ch = getopt(argc, argv, "m:i:p:q:d:r:s:t:l:w:e:cz")) != -1) {
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
    case 'e': // Seed for randomly populating data and indexes.
      seed = atoi(optarg);
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
                             worker_ips, nprocs);

      int32_t* data = (int32_t *)smalloc_aligned(sizeof(int32_t) * data_length);
      int64_t* indexes = (int64_t *)smalloc_aligned(sizeof(int64_t) * index_length);
     
      srand(seed);
      load_data(data, indexes);

      weld_args args;
      args.data = weld_vector((void*)data, data_length);
      args.indexes = weld_vector((void*)indexes, index_length);

      int64_t local_result = run_query_local(data, indexes);

      string query = Util::read(weld_filename);

      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);

      void* res = driver.run_query(query, &args, nprocs * worker_ips.size());
      
      fprintf(stdout, "Local result: %ld\n", local_result);
 
      gettimeofday(&tend, 0);
      double time_in_mill = 
	(tend.tv_sec) * 1000 + (tend.tv_usec) / 1000 ; // convert tv_sec & tv_usec to millisecond
      printf("End time: %.02f\n", time_in_mill);

      timersub(&tend, &tstart, &tdiff);
      fprintf(stdout, "Result: %ld\n", *(int64_t*)(res));
      
      fprintf(stdout, "%d: %ld.%06ld\n",
              nprocs, (long) tdiff.tv_sec, (long) tdiff.tv_usec);  
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
