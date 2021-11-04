#include <algorithm>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <assert.h>
#include <sys/time.h>

#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#include "../../experiments/cpp/tpch_util/s3utils.h"
#include "../../experiments/cpp/tpch_util/s3utils-templates.cpp"

using namespace std;

double eps = 0.001;

bool fuzzy_eq(double a, double b) {
  return fabs(a-b) < eps;
}

typedef struct kv {
  double revenue;
  int32_t suppkey;
  char pad;
} kv;

int main(int argc, char** argv) {
  int sf = 100;
  size_t num_items = LINE_ITEM_PER_SF * sf;
  size_t num_supps = SUPPS_PER_SF * sf;
  
  MPI_Init(NULL, NULL);
  int rank, nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  printf("nprocs %d, rank %d\n", nprocs, rank);

  size_t items_per_proc = num_items / nprocs;
  size_t supps_per_proc = num_supps / nprocs;

  Supplier* supps;
  assert(supps = (Supplier*)malloc(sizeof(Supplier)*num_supps));
  Lineitem* items;
  assert(items = (Lineitem*)malloc(sizeof(Lineitem)*items_per_proc));
  
  load_from_file<Lineitem>(items_per_proc * rank, items, items_per_proc, "../tpch_data/lineitem_large");
  if ( rank == 0 ) load_from_file<Supplier>(0, supps, num_supps, "../tpch_data/supplier_large");
  printf("Loaded: %d\n", items[0].orderkey);

  struct timeval start, end, diff;
  gettimeofday(&start, 0);

  MPI_Barrier(MPI_COMM_WORLD);

  // build revenues view, locally
  std::unordered_map<int, double> revenues;
  for ( long i = 0; i < items_per_proc; i++ ) {
    if ( items[i].shipdate >= 19960101 && items[i].shipdate < 19960401 ) {
      double price = items[i].extendedprice * ( 1 - items[i].discount );
      revenues[items[i].suppkey] += price;
    }
  }

  // merge revenue views
  long rsize = revenues.size();
  kv* local_revenues = (kv*)malloc(sizeof(kv)*rsize);
  int idx = 0;
  for ( auto & x : revenues ) {
    local_revenues[idx].suppkey = x.first;
    local_revenues[idx].revenue = x.second;
    idx++;
  }

  // get size of local revenues
  int* rev_sizes;
  if ( rank == 0 ) { 
    rev_sizes = (int*)malloc(sizeof(int)*nprocs);
  }
  
  MPI_Gather(&rsize, 1, MPI_INT, rev_sizes, 1, MPI_INT, 0, MPI_COMM_WORLD);

  if ( rank == 0 ) {
    printf("procs: %d\n", nprocs);
    for ( int i = 0; i < nprocs; i++ ) {
      printf("size: %ld\n", rev_sizes[i]);
    }
  }

  int max_rev_size = 0;
  int tot_rev_size = 0;
  if ( rank == 0 ) {
    for ( int i = 0; i < nprocs; i++ ) {
      if ( rev_sizes[i] > max_rev_size ) max_rev_size = rev_sizes[i];
      tot_rev_size += rev_sizes[i];
    }
  }

  MPI_Bcast(&max_rev_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
  printf("%d %ld\n", rank, max_rev_size);

  kv* rev_buf;
  if ( rank == 0 ) {
    rev_buf = (kv*)malloc(sizeof(kv) * tot_rev_size);
  }

  int* displs = (int*)malloc(sizeof(int) * nprocs);
  int disp = 0;
  if ( rank == 0 ) {
    for ( int i = 0; i < nprocs; i++ ) {
      displs[i] = disp;
      disp += rev_sizes[i];
    }
  }
    
  MPI_Gatherv(local_revenues, revenues.size(), MPI_LONG,
	      rev_buf, rev_sizes, displs, MPI_LONG, 0, MPI_COMM_WORLD);

  // merge into single revenue map
  double global_max_rev = 0;
  std::vector<int> top_supps;
  int* top_supps_arr;
  if ( rank == 0 ) {
    std::unordered_map<int, double> global_revenues;
    for ( int i = 0; i < tot_rev_size; i++ ) {
      global_revenues[rev_buf[i].suppkey] += rev_buf[i].revenue;
    }
    
    for ( auto x : global_revenues ) {
      if ( x.second > global_max_rev ) {
	global_max_rev = x.second;
      }
    }

    printf("%f\n", global_max_rev);
    for ( auto x : global_revenues ) {
      if ( fuzzy_eq( x.second, global_max_rev ) ) {
	top_supps.push_back(x.first);
      }
    }

    printf("%ld\n", top_supps.size());

    // join on suppkey
    std::vector<int> top_supp_data;
    for ( int i = 0; i < num_supps; i++ ) {
      top_supp_data.push_back(global_revenues[supps[i].suppkey]);
      /*int x;
      try {
	x = global_revenues.at(supps[i].suppkey);
	top_supp_data.push_back(x);
      } catch ( std::out_of_range ) {
	continue;
	}*/
    }

    std::sort(top_supp_data.begin(), top_supp_data.end());
  }

  if ( rank == 0 ) {
    gettimeofday(&end, 0);

    timersub(&end, &start, &diff);
    printf("TPCH q15: %ld.%06ld\n",
	   (long) diff.tv_sec, (long) diff.tv_usec);
  }

  MPI_Finalize();
}
