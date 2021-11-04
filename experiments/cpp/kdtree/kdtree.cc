/* Main program for running workers + driver */

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
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

#include "clamor/cluster.h"
#include "clamor/util.h"
#include "clamor/fault-handler.h"
#include "clamor/weld-utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;

struct byte_range {
  int64_t start;
  int64_t end;
};

extern "C" int load_data(void* p_input, void* p_output) {
  string url = "http://weld-dsm-east.s3.amazonaws.com/kdtree-nodes/queries-ca-restaurants-full.csv";
  byte_range* bytes = (byte_range*)p_input;
  SPDLOG_DEBUG("Download from url: {:s}", url);

  MemoryStruct chunk;
  
  FaultHandler::fh.download_to_chunk(&chunk, bytes->start, bytes->end, url);
  string str_data(chunk.memory);
  free(chunk.memory);

  vector<string> lines = Util::split(str_data, '\n');
  cout << "got lines: " << lines.size() << endl;

  t_weld_vector<t_weld_vector<double>>* res_vec = (t_weld_vector<t_weld_vector<double>>*)p_output;
  res_vec->data = (t_weld_vector<double>*)smalloc(sizeof(t_weld_vector<double>)*lines.size());

  uint64_t ndims = 2;
  for (uint64_t j = 0; j < lines.size(); j++) {
    res_vec->data[j].data = (double*)smalloc(sizeof(double)*ndims);
    res_vec->data[j].length = ndims;
  }

  res_vec->length = 0;
  for (uint64_t j = 0; j < lines.size(); j++) {
    auto line = lines.at(j);
    auto vals = Util::split(line, ',');
    if ( vals.size() != 2 ) continue;
    res_vec->data[j].data[0] = atof(vals.at(0).c_str());
    res_vec->data[j].data[1] = atof(vals.at(1).c_str());
    res_vec->length++;
  }
}

struct args {
  t_weld_vector<t_weld_vector<t_weld_vector<double>>> lookup_idxs;
  uint64_t kdtree_addr;
};

struct lookup_args {
  uint64_t kdtree_addr;
  t_weld_vector<t_weld_vector<double>> idxs;
};

// CUDF that each worker will call to look up into k-d tree.
extern "C" int r_lookup(void* p_input, void* p_result) {
  lookup_args* args = (lookup_args*)(p_input);
  uint64_t kdtree_addr = args->kdtree_addr;
  KdTree<double, Payload, double[2]>* kdtree = (KdTree<double, Payload, double[2]>*)(kdtree_addr);

  for ( uint64_t i = 0; i < args->idxs.length; i++ ) {
    const double idx[2] = {args->idxs.data[i].data[0],
			   args->idxs.data[i].data[1]};    
    ResTuple tup = kdtree_lookup(kdtree, &idx);
    *(double*)p_result += tup.dist;
  }
  
  // iterate twice
  for ( uint64_t i = 0; i < args->idxs.length; i++ ) {
    const double idx[2] = {args->idxs.data[i].data[0],
			   args->idxs.data[i].data[1]};    
    ResTuple tup = kdtree_lookup(kdtree, &idx);
  }
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

      /* Create k-d tree in shared memory. */
      printf("Start create...\n");
      struct timeval cstart, cend, cdiff;
      gettimeofday(&cstart, 0);
      KdTree<double, Payload, double[2]>* kdtree = kdtree_from_csv();
      printf("...done.\n");
      gettimeofday(&cend, 0);
      timersub(&cend, &cstart, &cdiff);
      fprintf(stdout, "k-d tree create time: %ld.%06ld\n",
              (long) cdiff.tv_sec, (long) cdiff.tv_usec);
      printf("k-d tree addr: %p\n", kdtree);
      struct args args;
      uint64_t ndims = 2;

      string load_data_file = "load_data.weld";
      string load_query = Util::read(load_data_file); 
      string url = "http://weld-dsm-east.s3.amazonaws.com/kdtree-nodes/queries-ca-restaurants-full.csv";

      uint64_t bytes_per_line = 8*2 + 2;
      CurlHandle handle;
      double bytes = handle.get_filesize_only(url);

      t_weld_vector<byte_range>* urls = (t_weld_vector<byte_range>*)smalloc(sizeof(weld_vector));
      urls->data = (byte_range*)smalloc(sizeof(byte_range)*npartitions);
      urls->length = npartitions;
      uint64_t bytes_per_partition = bytes / (npartitions * bytes_per_line);
      printf("bytes %f bytes per line %ld per partition %ld\n", bytes, bytes_per_line, bytes_per_partition);
      for ( int32_t i = 0; i < npartitions; i++ ) {
	urls->data[i].start = i*bytes_per_partition;
	urls->data[i].end = urls->data[i].start + bytes_per_partition;
	printf("Start: %d end %d\n", urls->data[i].start, urls->data[i].end);
      }

      void* loaded_items = driver.run_query(load_query, (void*)(urls), nprocs*worker_ips.size()); // returns vec<vec<item>>
      args.lookup_idxs = *(t_weld_vector<t_weld_vector<t_weld_vector<double>>>*)loaded_items;

      printf("Done creating queries.\n");
      args.kdtree_addr = (uintptr_t)((void*)(kdtree));

      string query = Util::read(weld_filename);
      
      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);
      
      printf("Running weld query...\n");
      void* res = driver.run_query(query, (void*)(&args), nprocs * worker_ips.size());
      double ret_weld = *(double*)(res);
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);

      printf("Result: %f\n", ret_weld);
      
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
