/* Main program for running workers + driver */

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
  double** means = (double**)malloc(sizeof(double*)*k);
  for ( uint64_t i = 0; i < k; i++ ) {
    means[i] = (double*)malloc(sizeof(double)*d);
  }
  
  for ( uint64_t i = 0; i < k; i++ ) {
    for ( uint64_t t = 0; t < d; t++ ) {
      means[i][t] = (double)i;
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
			     cache_range_start,
                             worker_ips, nprocs);
      int nitems = 100;
      int k = 1000;
      int d = 10;
      int niters = 1;

      double** data = generate_data(nitems, d, k);
      struct args args;
      args.data_vec.data = (t_weld_vector<double>*)smalloc(sizeof(t_weld_vector<double>)*nitems);
      args.data_vec.length = nitems;
      for ( uint64_t i = 0; i < nitems; i++ ) {
	args.data_vec.data[i] = make_weld_vector(data[i], d);
      }

      double** means = generate_means(d, k);
      args.means.data = (t_weld_vector<double>*)smalloc(sizeof(t_weld_vector<double>)*k);
      args.means.length = k;
      for ( uint64_t i = 0; i < k; i++ ) {
	args.means.data[i] = make_weld_vector(means[i], d);
      }

      args.k = k;
      args.iters = niters;
      
      string query = Util::read(weld_filename); // dispatch call will automatically be inserted during compilation

      struct timeval tstart, tend, tdiff;
      gettimeofday(&tstart, 0);

      void* res = driver.run_query(query, (void*)(&args), nprocs * worker_ips.size());
      t_weld_vector<t_weld_vector<double>>* ret_weld = (t_weld_vector<t_weld_vector<double>>*)(res);
      printf("Length: %d\n", ret_weld->length);
      
      gettimeofday(&tend, 0);
      timersub(&tend, &tstart, &tdiff);

      for ( uint64_t i = 0; i < k; i++ ) {
	for ( uint64_t j = 0; j < d; j++ ) {
	  printf("%f ", ret_weld->data[i].data[j]);
	}
	printf("\n");
      }

      fprintf(stderr, "Weld, %d worker(s): %ld.%06ld\n",
              nprocs, (long) tdiff.tv_sec, (long) tdiff.tv_usec);
    }
    break;
  case Cluster::Role::MANAGER:
    {
      Cluster::start_task_manager(ip, manager_port,
				  ip, driver_port,
				  weld_range_start,
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
