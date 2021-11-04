#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <assert.h>
#include <sys/time.h>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#include "../../experiments/cpp/tpch_util/s3utils.h"
#include "../../experiments/cpp/tpch_util/s3utils-templates.cpp"

using namespace std;

typedef struct kv {
  double price;
  int32_t partkey;
  char pad;
} kv;

int main(int argc, char** argv) {
  printf("%d\n", sizeof(kv));
  int sf = 100;
  size_t num_items = LINE_ITEM_PER_SF * sf;
  size_t num_parts = PARTS_PER_SF * sf;
  
  MPI_Init(NULL, NULL);
  int rank, nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  printf("nprocs %d, rank %d\n", nprocs, rank);

  size_t items_per_proc = num_items / nprocs;
  size_t parts_per_proc = num_parts / nprocs;

  Part* parts;
  assert(parts = (Part*)malloc(sizeof(Part)*parts_per_proc));
  Lineitem* items;
  assert(items = (Lineitem*)malloc(sizeof(Lineitem)*items_per_proc));

  struct timeval start, end, diff;

  MPI_Barrier(MPI_COMM_WORLD);

  load_from_file<Part>(parts_per_proc * rank, parts, parts_per_proc, "../tpch_data/part_large");
  load_from_file<Lineitem>(items_per_proc * rank, items, items_per_proc, "../tpch_data/lineitem_large");
  printf("Loaded: %d\n", items[0].orderkey);

  // build items table
  kv* filtered_buffer;
  int num_filtered_items;
  std::vector<kv> filtered_items;
  for ( long i = 0; i < items_per_proc; i++ ) {
    if ( items[i].shipdate >= 19950901 && items[i].shipdate < 19951001 ) {
      filtered_items.push_back({items[i].extendedprice * (1-items[i].discount), items[i].partkey});
    }
  }

  printf("%d %d\n", filtered_items.size(), filtered_items[0].partkey);
  num_filtered_items = filtered_items.size();
  filtered_buffer = &filtered_items[0];
  
  int* lineitem_sizes;
  if ( rank == 0 ) {
    lineitem_sizes = (int*)malloc(sizeof(int)*nprocs);
  }

  MPI_Gather(&num_filtered_items, 1, MPI_INT, lineitem_sizes, 1, MPI_INT, 0, MPI_COMM_WORLD);

  if ( rank == 0 ) {
    printf("procs: %d\n", nprocs);
    for ( int i = 0; i < nprocs; i++ ) {
      printf("rank: %d size: %ld\n", rank, lineitem_sizes[i]);
    }
  }

  size_t tot_size = 0;
  if ( rank == 0 ) {
    for ( int i = 0; i < nprocs; i++ ) {
      tot_size += lineitem_sizes[i];
    }
  }
  printf("tot size: %d\n", tot_size);
  
  kv* buffer;
  if ( rank == 0 ) {
    buffer = (kv*)malloc(sizeof(kv) * tot_size);
    printf("Created buffer\n");
  }

  printf("here1\n");
  int* displs;
  int disp = 0;
  if ( rank == 0 ) {
    displs = (int*)malloc(sizeof(int) * nprocs);
    for ( int i = 0; i < nprocs; i++ ) {
      displs[i] = disp;
      disp += lineitem_sizes[i];
      printf("%d\n", displs[i]);
    }
    printf("Created displs\n");
    printf("%d %f\n", filtered_buffer[0].partkey, filtered_buffer[0].price);
  }

  printf("%d: here\n", rank);

  // merge into single vector of filtered items
  MPI_Gatherv(filtered_buffer, num_filtered_items, MPI_LONG,
	      buffer, lineitem_sizes, displs, MPI_LONG, 0, MPI_COMM_WORLD);

  if ( rank == 0 ) {
    printf("Done gather\n");
  }
  
  // broadcast merged vector
  size_t buf_size = *(size_t*)malloc(sizeof(size_t));
  if ( rank == 0 ) {
    buf_size = tot_size;
  }
  MPI_Bcast(&buf_size, 1, MPI_LONG, 0, MPI_COMM_WORLD);

  if ( rank != 0 ) {
    printf("Buf size: %d\n", buf_size);
    buffer = (kv*)malloc(sizeof(kv) * buf_size); 
  }

  printf("%ld\n", buf_size);
  MPI_Bcast(buffer, buf_size, MPI_DOUBLE_INT, 0, MPI_COMM_WORLD);

  std::unordered_map<int, double> items_table;
  for ( int i = 0; i < buf_size; i++ ) {
    items_table.emplace(buffer[i].partkey, buffer[i].price);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  
  if ( rank == 0 ) {
    gettimeofday(&start, 0);
  }

  double* sums = (double*)malloc(2*sizeof(double)); /* 0 = no promo, 1 = promo */

  for ( long i = 0; i < parts_per_proc; i++ ) {
    double price = items_table[parts[i].partkey];
    sums[parts[i].promo_str] += price; // defaults to 0 if not found
  }
  
  double p0_sum, p1_sum;
  MPI_Reduce(&sums[0], &p0_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&sums[1], &p1_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  
  printf("Reduced\n");

  if ( rank == 0 ) {
    gettimeofday(&end, 0);

    printf("%f %f %f\n", p0_sum, p1_sum, p1_sum/(p0_sum + p1_sum));

    timersub(&end, &start, &diff);
    printf("TPCH q14: %ld.%06ld\n",
	   (long) diff.tv_sec, (long) diff.tv_usec);
  }

  MPI_Finalize();
}
