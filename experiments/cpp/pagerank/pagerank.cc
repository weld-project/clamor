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

/* Shared address buffer */
char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

template <typename T>
struct t_weld_vector {
    T *data;
    int64_t length;
};

template <typename T>
t_weld_vector<T> make_weld_vector(T *data, int64_t length) {
    struct t_weld_vector<T> vector;
    vector.data = data;
    vector.length = length;
    return vector;
}

typedef struct load_args {
  t_weld_vector<uint64_t> input_ptrs;
} load_args;

typedef struct parse_args {
  uint64_t start_addr;
} parse_args;

typedef struct parse_output {
  t_weld_vector<t_weld_vector<int32_t>> outlinks;
} parse_output;

typedef struct pagerank_args {
  t_weld_vector<t_weld_vector<int32_t>> in;
  t_weld_vector<int32_t> ranks;
} pagerank_args;

extern "C" int load_outlinks(void* input, void* output) {
  parse_args* args = (parse_args*)(input);
  uint64_t start_addr = args->start_addr;

  const char* inptr = (const char*)(start_addr);
  auto lines = Util::readlines_from_char(inptr); // Reads to the end of the CSV file.
  vector<vector<string>> split_lines;
  for ( auto line : lines ) {
    split_lines.push_back(Util::split(line, ','));
  }

  parse_output* out = (parse_output*)(output);
  (out->outlinks).data = (t_weld_vector<int32_t>*)smalloc(sizeof(t_weld_vector<int32_t>)*lines.size());
  (out->outlinks).length = lines.size();

  for ( uint64_t i = 0; i < split_lines.size(); i++ ) {
    auto & outlinks = split_lines.at(i);
    (out->outlinks).data[i].data = (int32_t*)smalloc(sizeof(int32_t)*outlinks.size());
    (out->outlinks).data[i].length = outlinks.size();
    for ( uint64_t j = 0; j < outlinks.size(); j++ ) {
      (out->outlinks).data[i].data[j] = atoi(outlinks.at(j).c_str());
    }
  }
}

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
      worker_ips = Util::split(optarg, ','); 
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

      string url1 = "http://weld-dsm-east.s3.amazonaws.com/pagerank-0.05-0.csv";
      string url2 = "http://weld-dsm-east.s3.amazonaws.com/pagerank-0.05-1.csv";
      vector<string> urls;
      urls.push_back(url1);
      urls.push_back(url2);
      urls.push_back(url1);
      urls.push_back(url2);
      uint64_t nurls = urls.size();

      load_args args;
      args.input_ptrs.data = (uint64_t*)smalloc(sizeof(uint64_t)*nurls);
      args.input_ptrs.length = nurls;
      
      string query = Util::read("pagerank-load.weld"); // dispatch call will automatically be inserted during compilation
      for ( uint64_t i = 0; i < nurls; i++ ) {
        auto mapped = driver.map_url(urls[i]);
	args.input_ptrs.data[i] = (uint64_t)(mapped.first); // no length specified, load function will read to EOF
      }
      
      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);
      
      void* res = driver.run_query(query, (void*)(&args), nprocs * worker_ips.size());
      t_weld_vector<t_weld_vector<int32_t>>* vec_res =
	(t_weld_vector<t_weld_vector<int32_t>>*)res;
      for ( uint64_t i = 0; i < (vec_res->data)[0].length; i++ ) {
	printf("%d, ", (vec_res->data)[0].data[i]);
      }
      printf("\n");
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);
      fprintf(stderr, "Result: %ld\n", *(uint64_t*)(res));
      
      fprintf(stderr, "%d: %ld.%06ld\n",
              nprocs, (long) tdiff.tv_sec, (long) tdiff.tv_usec);

      int32_t n = 1000;
      int32_t iters = 2;
      
      pagerank_args pargs;
      pargs.in = *vec_res;
      pargs.ranks.data = (int32_t*)smalloc(sizeof(int32_t)*n);
      for ( int32_t i = 0; i < n; i++ ) pargs.ranks.data[i] = 1;
      pargs.ranks.length = n;

      string query2 = Util::read("pagerank.weld");
      void* res2;
      for ( int i = 0; i < iters; i++ ) {
        res2 = driver.run_query(query2, (void*)(&pargs), nprocs * worker_ips.size());
        pargs.ranks = *(t_weld_vector<int32_t>*)(res2);
      }
      
      t_weld_vector<int32_t>* ranks = (t_weld_vector<int32_t>*) res2;
      for ( int32_t i = 0; i < n; i++ ) {
	printf("%d, ", (ranks->data)[i]);
      }
      printf("\n");
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
