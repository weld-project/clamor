#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <assert.h>
#include <sys/time.h>

#include <fstream>
#include <iostream>
#include <string>

#include "../../experiments/cpp/tpch_util/s3utils.h"
#include "../../experiments/cpp/tpch_util/s3utils-templates.cpp"

using namespace std;

int PASS_THRESH = 19980901;


int main(int argc, char** argv) {
  int sf = 100;
  size_t num_items = LINE_ITEM_PER_SF * sf;

  MPI_Init(NULL, NULL);
  int rank, nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  printf("nprocs %d, rank %d\n", nprocs, rank);
  
  size_t items_per_proc = num_items / nprocs;

  Lineitem* items;
  assert(items = (Lineitem*)malloc(sizeof(Lineitem)*items_per_proc));

  int return_idxs = 6;

  int64_t* elem1 = (int64_t*)malloc(sizeof(int64_t)*return_idxs);
  double* elem2 = (double*)malloc(sizeof(double)*return_idxs);
  double* elem3 = (double*)malloc(sizeof(double)*return_idxs);
  double* elem4 = (double*)malloc(sizeof(double)*return_idxs);
  double* elem5 = (double*)malloc(sizeof(double)*return_idxs);
  int64_t* elem6 = (int64_t*)malloc(sizeof(int64_t)*return_idxs);

  int64_t elem1_sum[return_idxs] = {0};
  double elem2_sum[return_idxs] = {0};
  double elem3_sum[return_idxs] = {0};
  double elem4_sum[return_idxs] = {0};
  double elem5_sum[return_idxs] = {0};
  int64_t elem6_sum[return_idxs] = {0};
  
  struct timeval start, end, diff;

  MPI_Barrier(MPI_COMM_WORLD);

  load_from_file<Lineitem>(items_per_proc * rank, items, items_per_proc, "../tpch_data/lineitem_large");

  printf("Loaded: %d\n", items[0].orderkey);

  if ( rank == 0 ) {
    gettimeofday(&start, 0);
  }

  for ( long i = 0; i < items_per_proc; i++ ) {
    if ( items[i].shipdate <= PASS_THRESH ) {
      double sum_disc_price = items[i].extendedprice * (1-items[i].discount);
      int idx = items[i].returnflag + items[i].linestatus * 2;
      elem1[idx] += items[i].quantity;
      elem2[idx] += items[i].extendedprice;
      elem3[idx] += sum_disc_price;
      elem4[idx] += sum_disc_price * (1.0 * items[i].tax);
      elem5[idx] += items[i].discount;
      elem6[idx] += 1;
    }
  }

  MPI_Reduce(elem1, &elem1_sum, return_idxs, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(elem2, &elem2_sum, return_idxs, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(elem3, &elem3_sum, return_idxs, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(elem4, &elem4_sum, return_idxs, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(elem5, &elem5_sum, return_idxs, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(elem6, &elem6_sum, return_idxs, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  printf("Reduced\n");

  if ( rank == 0 ) {
    gettimeofday(&end, 0);

    printf("%ld %f %f %f %f %ld\n", elem1_sum[0], elem2_sum[0],
	   elem3_sum[0], elem4_sum[0], elem5_sum[0], elem6_sum[0]);

    timersub(&end, &start, &diff);
    printf("TPCH q1: %ld.%06ld\n",
	   (long) diff.tv_sec, (long) diff.tv_usec);
  }
  
  MPI_Finalize();
}
