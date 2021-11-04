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

int PASS_LOWER = 19940101;
int PASS_UPPER = 19950101;

/*bool fuzzy_eq(double a, double b) {
  return fabs(a-b) < eps;
}

struct output {
  int32_t elem1;
  double elem2;
  double elem3;
  double elem4;
  double elem5;
  int32_t elem6;

  bool operator==(const output &other) {
    return elem1 == other.elem1 &&
      fuzzy_eq(elem2, other.elem2) && 
      fuzzy_eq(elem3, other.elem3) && 
      fuzzy_eq(elem4, other.elem4) && 
      fuzzy_eq(elem5, other.elem5) && 
      elem6 == other.elem6;
  }
  friend ostream& operator<<(ostream& os, const output& res);
  };*/

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

  double elem1 = *(double*)malloc(sizeof(double));

  double elem1_sum;
  
  struct timeval start, end, diff;

  MPI_Barrier(MPI_COMM_WORLD);

  load_from_file<Lineitem>(items_per_proc * rank, items, items_per_proc, "../tpch_data/lineitem_large");

  printf("Loaded: %d\n", items[0].orderkey);

  if ( rank == 0 ) {
    gettimeofday(&start, 0);
  }

  for ( long i = 0; i < items_per_proc; i++ ) {
    if ( items[i].shipdate >= PASS_LOWER
	 && items[i].shipdate < PASS_UPPER
	 && items[i].discount >= 0.05
	 && items[i].discount <= 0.07
	 && items[i].quantity < 24 ) {
      double sum_disc_price = items[i].extendedprice * (1-items[i].discount);
      elem1 += sum_disc_price;
    }
  }

  MPI_Reduce(&elem1, &elem1_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  printf("Reduced\n");

  if ( rank == 0 ) {
    gettimeofday(&end, 0);

    printf("%f\n", elem1_sum);

    timersub(&end, &start, &diff);
    printf("TPCH q6: %ld.%06ld\n",
	   (long) diff.tv_sec, (long) diff.tv_usec);
  }
  
  MPI_Finalize();
}
