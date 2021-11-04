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

#include "clamor_regex.h"

#include "clamor/debug.h"

#include "clamor/cluster.h"
#include "clamor/util.h"
#include "clamor/fault-handler.h"
#include "clamor/weld-utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;

/* Shared address buffer */
char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

struct args {
  t_weld_vector<t_weld_vector<char>> haystacks;
  uint64_t re_addr;
};

struct match_args {
  t_weld_vector<char> haystack;
  uint64_t re_addr;
};

// CUDF that each worker will call to match on regex.
extern "C" int match(void* p_input, void* p_result) {
  match_args* args = (match_args*)(p_input);
  t_weld_vector<char> haystack = args->haystack;
  int64_t re_addr = args->re_addr;
  Regex<DenseDFA<Vec<uintptr_t>, uintptr_t>>* re =
    (Regex<DenseDFA<Vec<uintptr_t>, uintptr_t>>*)(re_addr);

  printf("Regex address: %p\n", re_addr);
  printf("Data: %s\n", haystack.data);
  uint64_t nmatches = regex_match(re, (const char*)(haystack.data));
  printf("Matched: %lu\n", nmatches);
  *(int64_t*)p_result = nmatches;
}

int main(int argc, char** argv) {
  Cluster::Role role;

  string ip = "0.0.0.0";
  string manager_ip = "0.0.0.0"; /* Manager and driver IP are the same */

  uint32_t manager_port = 40000; /* Port that manager listens on for page requests. */
  uint32_t worker_port  = 50000; /* Port that worker listens on for cache invalidations. */
  uint32_t driver_port  = 70000; /* Port that driver listens on for cache invalidations. */

  uint32_t worker_range_start = 50000;

  uint32_t nprocs = 1; /* processes launched per worker node */
  string weld_filename = "regex.weld";
  vector<string> worker_ips;

  string driver = "driver";
  string manager = "manager";
  string worker = "worker";

  bool local = false;
  bool slow = false;
  bool speculate = false;

  int64_t npartitions = 64;

  int ch;
  while ((ch = getopt(argc, argv, "m:i:p:q:d:r:s:t:l:w:x:cz")) != -1) {
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

      uint32_t nelements = 625000000;
      const char* haystack_s = "testemail@testemail.com";
      char* haystack = (char*)smalloc(strlen(haystack_s) + 1);
      strcpy(haystack, haystack_s);
      
      /* Create regex in shared memory. */
      printf("Start compile...\n");
      struct timeval cstart, cend, cdiff;
      gettimeofday(&cstart, 0);
      Regex<DenseDFA<Vec<uintptr_t>, uintptr_t>> *re = regex_create("(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])"); /* Uses the regex library compiled with smalloc. */
      printf("...done.\n");
      gettimeofday(&cend, 0);
      timersub(&cend, &cstart, &cdiff);
      fprintf(stderr, "Regex compile time: %ld.%06ld\n",
              (long) cdiff.tv_sec, (long) cdiff.tv_usec);

      size_t end = 0;

      /* Load the data into shared memory. */
      struct args args;
      args.haystacks.data = (t_weld_vector<char>*)smalloc(sizeof(t_weld_vector<char>)*nelements);
      args.haystacks.length = nelements;

      for ( int i = 0; i < nelements; i++ ) {
	args.haystacks.data[i].data = haystack;
	args.haystacks.data[i].length = strlen(haystack);
      }
	
      printf("Regex addr: %p\n", re);
      args.re_addr = (uintptr_t)((void*)(re));
      
      string query = Util::read("regex-load.weld"); // dispatch call will automatically be inserted during compilation
      
      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);
      
      void* res = driver.run_query(query, (void*)(&args), nprocs * worker_ips.size());
      int32_t ret_weld = *(int32_t*)(res);
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);

      printf("Result: %d\n", ret_weld);
      
      fprintf(stderr, "Weld, %d worker(s): %ld.%06ld\n",
              nprocs, (long) tdiff.tv_sec, (long) tdiff.tv_usec);

      query = Util::read(weld_filename); // dispatch call will automatically be inserted during compilation

      struct timeval tstart2, tend2, tdiff2;
      gettimeofday(&tstart2, 0);
      
      res = driver.run_query(query, (void*)(&args), nprocs * worker_ips.size());
      ret_weld = *(int64_t*)(res);
      
      gettimeofday(&tend2, 0);
      timersub(&tend2, &tstart2, &tdiff2);

      printf("Result: %ld\n", ret_weld);
      
      fprintf(stderr, "Weld (cached), %d worker(s): %ld.%06ld\n",
              nprocs, (long) tdiff2.tv_sec, (long) tdiff2.tv_usec);
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
