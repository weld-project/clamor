/* Main program for running workers + driver */

#include <stdint.h>
#include <sys/time.h>

#include <future>
#include <iostream>
#include <vector>
#include <string>
#include <utility>

#include "clamor/weld.grpc.pb.h"

#include "clamor/cluster.h"
#include "clamor/curl-handle.h"
#include "clamor/util.h"
#include "clamor/fault_handler.h"
#include "clamor/weld-utils.h"

#include "smalloc/smalloc.h"

using namespace weld;
using namespace std;

/* Shared address buffer */
char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

struct node;

struct node {
  int64_t value;
  node* next;
};

/* Traverse the list and add the value of the final element. */
extern "C" void traverse_list(int64_t *x, int64_t* list_head, int64_t *result) {
  node* cur = (node*)(*list_head);
  while ( cur->next != NULL ) {
    cur = cur->next;
  }
  *result = cur->value;
}

node* build_list(int32_t nnodes) {
  node* head = (node*)smalloc(sizeof(node));
  node* cur = head;

  for ( int32_t i = 0; i < nnodes-1; i++ ) {
    node* next = (node*)smalloc(sizeof(node));
    cur->next = next;
    cur->value = i;
    cur = cur->next;
  }
  cur->value = nnodes;
  cur->next = NULL;

  return head;
}

struct args {
  weld_vector data;
  int64_t list_head;
};

int main(int argc, char** argv) {
  Cluster::Role role;

  string ip = "0.0.0.0";
  string manager_ip = "0.0.0.0"; /* Manager and driver IP are the same */

  uint32_t manager_port = 40000; /* Port that manager listens on for page requests. */
  uint32_t cache_port   = 50000; /* Port that worker listens on for cache invalidations. */
  uint32_t weld_port    = 60000; /* Port that worker listens on for Weld tasks. */
  uint32_t driver_port  = 70000; /* Port that driver listens on for cache invalidations. */

  uint32_t cache_range_start = 50000;
  uint32_t weld_range_start  = 60000;

  uint32_t nprocs = 1; /* processes launched per worker node */
  string weld_filename = "";
  vector<string> worker_ips;

  string driver = "driver";
  string manager = "manager";
  string worker = "worker";
  
  int ch;
  while ((ch = getopt(argc, argv, "m:i:p:q:d:r:s:t:l:w:")) != -1) {
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
      cache_port = atoi(optarg);
      break;
    case 'q':
      weld_port = atoi(optarg);
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
                             weld_range_start,
                             worker_ips, nprocs);
      
      string url = "http://weld-dsm-2.s3-us-west-2.amazonaws.com/test-ints";
      auto mapped_data = driver.map_url(url);
      char* data = mapped_data.first;
      size_t size = mapped_data.second;
      //size_t size = 100000;

      node* list_head = build_list(100);
      printf("list head: %lu\n", (int64_t)list_head);
      
      string query = Util::read(weld_filename); // dispatch call will automatically be inserted during compilation

      args* input_args = (args*)smalloc(sizeof(args));
      input_args->data.data = (void*)data;
      input_args->data.length = size / sizeof(int32_t);
      input_args->list_head = (int64_t)(list_head);
      
      struct timeval time = driver.run_query(query, input_args, nprocs * worker_ips.size());
      fprintf(stderr, "%d: %ld.%06ld\n",
              nprocs, (long) time.tv_sec, (long) time.tv_usec);
    }
    break;
  case Cluster::Role::MANAGER:
    {
      Cluster::start_memory_manager(ip, manager_port,
                                    ip, driver_port,
                                    cache_range_start,
                                    worker_ips, nprocs);
      return 0;
    }
    break;
  case Cluster::Role::WORKER:
    {
      Cluster::start_worker(ip, weld_port, cache_port,
                            manager_ip, manager_port,
                            manager_ip, driver_port);
      return 0;
    }
    break;
  }
}