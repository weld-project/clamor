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
  int32_t orderkey;
  int32_t orderpriority;
} kv;

int main(int argc, char** argv) {
  int sf = 100;
  size_t num_items = LINE_ITEM_PER_SF * sf;
  size_t num_orders = ORDERS_PER_SF * sf;
  
  MPI_Init(NULL, NULL);
  int rank, nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  printf("nprocs %d, rank %d\n", nprocs, rank);

  size_t items_per_proc = num_items / nprocs;
  size_t orders_per_proc = num_orders / nprocs;

  Lineitem* items;
  assert(items = (Lineitem*)malloc(sizeof(Lineitem)*items_per_proc));
  Order* orders;
  assert(orders = (Order*)malloc(sizeof(Order)*num_orders));

  struct timeval start, end, diff;

  MPI_Barrier(MPI_COMM_WORLD);

  load_from_file<Lineitem>(items_per_proc * rank, items, items_per_proc, "../tpch_data/lineitem_large");
  load_from_file<Order>(orders_per_proc * rank, orders, orders_per_proc, "../tpch_data/order_large");
  printf("Loaded: %d\n", items[0].orderkey);
    
  if ( rank == 0 ) {
    gettimeofday(&start, 0);
  }

  // build orders table
  kv* filtered_buffer;
  int num_filtered_orders;
  std::vector<kv> filtered_orders;
  for ( long i = 0; i < num_orders; i++ ) {
    if ( orders[i].orderdate >= 19930701 && orders[i].orderdate < 19931001 ) {
      filtered_orders.push_back({orders[i].orderkey, orders[i].orderpriority});
    }
  }

  printf("%d\n", filtered_orders[0].orderkey);
    
  num_filtered_orders = filtered_orders.size();
  filtered_buffer = &filtered_orders[0];

  int* order_sizes;
  if ( rank == 0 ) {
    order_sizes = (int*)malloc(sizeof(int)*nprocs);
  }

  MPI_Gather(&num_filtered_orders, 1, MPI_INT, order_sizes, 1, MPI_INT, 0, MPI_COMM_WORLD);

  if ( rank == 0 ) {
    printf("procs: %d\n", nprocs);
    for ( int i = 0; i < nprocs; i++ ) {
      printf("rank: %d size: %ld\n", rank, order_sizes[i]);
    }
  }

  int tot_size = 0;
  if ( rank == 0 ) {
    for ( int i = 0; i < nprocs; i++ ) {
      tot_size += order_sizes[i];
    }
  }
  printf("tot size: %d\n", tot_size);

  kv* buffer;
  if ( rank == 0 ) {
    printf("Buf size: %d\n", tot_size);
    buffer = (kv*)malloc(sizeof(kv) * tot_size); 
  }
  
  int* displs = (int*)malloc(sizeof(int) * nprocs);
  int disp = 0;
  if ( rank == 0 ) {
    for ( int i = 0; i < nprocs; i++ ) {
      displs[i] = disp;
      disp += order_sizes[i];
      printf("%d\n", displs[i]);
    }
    printf("Created displs\n");
    printf("%d %f\n", filtered_buffer[0].orderkey, filtered_buffer[0].orderpriority);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  struct timeval gather_start, gather_end, gdiff;
  // merge into single vector of filtered items
  MPI_Gatherv(filtered_buffer, num_filtered_orders, MPI_LONG,
	      buffer, order_sizes, displs, MPI_LONG, 0, MPI_COMM_WORLD);

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

  MPI_Bcast(buffer, buf_size, MPI_LONG, 0, MPI_COMM_WORLD);

  std::unordered_map<int, int> order_table;
  for ( int i = 0; i < buf_size; i++ ) {
    order_table.emplace(buffer[i].orderkey, buffer[i].orderpriority);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  
  if ( rank == 0 ) {
    gettimeofday(&gather_start, 0);
  }
  int num_priorities = 5;
  int counts[5] = {0, 0, 0, 0, 0};
  for ( long i = 0; i < items_per_proc; i++ ) {
    if ( items[i].commitdate < items[i].receiptdate ) {
      int priority = order_table[items[i].orderkey];
      if ( priority > 0 ) {
	counts[priority-1] += 1;
      }
    }
  }
  if ( rank == 0 ) {
    gettimeofday(&gather_end, 0);
  }
  
  int p0_sum, p1_sum, p2_sum, p3_sum, p4_sum;
  MPI_Reduce(&counts[0], &p0_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&counts[1], &p1_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&counts[2], &p2_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&counts[3], &p3_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&counts[4], &p4_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  
  printf("Reduced\n");

  if ( rank == 0 ) {
    gettimeofday(&end, 0);

    printf("%d %d %d %d %d\n", p0_sum, p1_sum, p2_sum, p3_sum, p4_sum);

    timersub(&end, &start, &diff);
    timersub(&gather_end, &gather_start, &gdiff);
    printf("TPCH q4: %ld.%06ld\n",
	   (long) diff.tv_sec, (long) diff.tv_usec);
    printf("Gather: %ld.%06ld\n",
	   (long) gdiff.tv_sec, (long) gdiff.tv_usec);
  }
  
  MPI_Finalize();
}
