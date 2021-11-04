/* Main program for running workers + driver */

#include <stdint.h>
#include <sys/time.h>

#include <future>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <utility>

#include "argmin.h"

#include "clamor/debug.h"

#include "clamor/cluster.h"
#include "clamor/util.h"
#include "clamor/fault-handler.h"
#include "clamor/weld-utils.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;

struct args {
  t_weld_vector<t_weld_vector<double>> data_vec;
  t_weld_vector<t_weld_vector<double>> means;
  int64_t k;
  int64_t iters;
};

/** Generates input data.
 *
 * @param num_items the number of line items.
 * @param d the dimension of each point.
 * @param k the number of clusters.
 * @return the generated sequence of dummy inputs.
 */
double** generate_data(int num_items, int d, int k) {
  double** data = (double**)smalloc(sizeof(double*)*num_items);
  for ( uint64_t i = 0; i < num_items; i++ ) {
    data[i] = (double*)smalloc(sizeof(double)*d);
  }
  
  int n_per_cluster = num_items/k;
  for ( uint64_t i = 0; i < k; i++ ) {
    for ( uint64_t j = 0; j < n_per_cluster; j++ ) {
      for ( uint64_t t = 0; t < d; t++ ) {
	data[i*n_per_cluster + j][t] = (double)j;
      }
    }
  }

  return data;
}

/** Generates input data.
 *
 * @param num_items the number of line items.
 * @param d the dimension of each point.
 * @param k the number of clusters.
 * @return the generated sequence of dummy inputs.
 */
double** generate_means(int d, int k) {
  default_random_engine generator;
  uniform_real_distribution<double> unif(0.0, 100.0);

  double** means = (double**)malloc(sizeof(double*)*k);
  for ( uint64_t i = 0; i < k; i++ ) {
    means[i] = (double*)malloc(sizeof(double)*d);
  }
  
  for ( uint64_t i = 0; i < k; i++ ) {
    for ( uint64_t t = 0; t < d; t++ ) {
      means[i][t] = (double)(unif(generator));
    }
  }

  return means;
}

void free_generated_data(double** d, int k) {
  for ( uint32_t i = 0; i < k; i++ ){
    free(d[i]);
  }

  free(d);
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
  string weld_filename = "kmeans.weld";
  vector<string> worker_ips;

  string driver = "driver";
  string manager = "manager";
  string worker = "worker";

  bool local = false;
  bool slow = false;
  bool speculate = false;

  uint64_t npartitions = 64;

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

      string url = "http://weld-dsm-east.s3.amazonaws.com/kmeans-100gb";
      auto mapped_data = driver.map_url(url);
      double* data = (double*)(mapped_data.first);
      size_t size = mapped_data.second;

      SPDLOG_DEBUG("Got data size: {:d}", size);
      
      //uint64_t nitems = 6250000000 / 5; //100000;
      uint64_t ndoubles = 12500000000;
      int d = 10;
      int k = 100;
      int niters = 10;

      uint64_t nitems = ndoubles / d;
      fprintf(stderr, "Items: %ld\n", nitems);
      
      struct args args;
      args.data_vec.data = (t_weld_vector<double>*)smalloc(sizeof(t_weld_vector<double>)*nitems);
      args.data_vec.length = nitems;
      for ( uint64_t i = 0; i < nitems; i++ ) {
	args.data_vec.data[i] = make_weld_vector(&(data[i*d]), d);
      }

      // these aren't data values, they're just randomly-initialized means
      double** means = generate_means(d, k);
      args.means.data = (t_weld_vector<double>*)smalloc(sizeof(t_weld_vector<double>)*k);
      args.means.length = k;
      for ( uint64_t i = 0; i < k; i++ ) {
	args.means.data[i] = make_weld_vector(means[i], d);
      }

      args.k = k;
      args.iters = niters;
      
      string query = Util::read("kmeans-load.weld"); // dispatch call will automatically be inserted during compilation

      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);

      void* res = driver.run_query(query, (void*)(&args), nprocs * worker_ips.size());
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);

      fprintf(stderr, "Weld, %d worker(s): %ld.%06ld\n",
              nprocs, (long) tdiff.tv_sec, (long) tdiff.tv_usec);

      struct timeval tstart2, tend2, tdiff2;
      gettimeofday(&tstart2, 0);

      string query2 = Util::read(weld_filename); // dispatch call will automatically be inserted during compilation
      void* res2 = driver.run_query(query2, (void*)(&args), nprocs * worker_ips.size());
      t_weld_vector<t_weld_vector<double>>* ret_weld2 = (t_weld_vector<t_weld_vector<double>>*)(res2);
      printf("Length: %d\n", ret_weld2->length);
      
      gettimeofday(&tend2, 0);
      timersub(&tend2, &tstart2, &tdiff2);

      for ( uint64_t i = 0; i < k; i++ ) {
	for ( uint64_t j = 0; j < d; j++ ) {
	  printf("%f ", ret_weld2->data[i].data[j]);
	}
	printf("\n");
      }

      fprintf(stderr, "Weld, cached, %d worker(s): %ld.%06ld\n",
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
