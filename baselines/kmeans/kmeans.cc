/*
 * https://github.com/rexdwyer/MPI-K-means-clustering
 */

#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <assert.h>
#include <sys/time.h>

#include "curl-handle.h"

using namespace std;

// Creates an array of random doubles. Each number has a value from 0 - 1
double* create_rand_nums(const size_t num_elements, int seed) {
  srand(seed);
  double *rand_nums = (double *)malloc(sizeof(double) * num_elements);
  assert(rand_nums != NULL);
  for (int i = 0; i < num_elements; i++) {
    rand_nums[i] = (rand() / (double)RAND_MAX);
  }
  return rand_nums;
}

double* load_data() {
  // download input data
  std::string url = "http://weld-dsm-east.s3.amazonaws.com/kmeans-10gb";
  CurlHandle handle;
  MemoryStruct chunk;
  handle.download_data(&chunk, -1, -1, url);

  return (double*)(chunk.memory);
}

double* download_data(long byte_start, long byte_end) {
  // download input data
  std::string url = "http://weld-dsm-east.s3.amazonaws.com/kmeans-10gb";
  CurlHandle handle;
  MemoryStruct chunk;
  handle.download_data(&chunk, byte_start, byte_end, url);

  return (double*)(chunk.memory);
}

// Distance**2 between d-vectors pointed to by v1, v2.
double distance2(const double *v1, const double *v2, const int d) {
  double dist = 0.0;
  for (int i=0; i<d; i++) {
    double diff = v1[i] - v2[i];
    dist += diff * diff;
  }
  return dist;
}

// Assign a site to the correct cluster by computing its distances to
// each cluster centroid.
int assign_site(const double* site, double* centroids,
		const int k, const int d) {
  int best_cluster = 0;
  double best_dist = distance2(site, centroids, d);
  double* centroid = centroids + d;
  for (int c = 1; c < k; c++, centroid += d) {
    double dist = distance2(site, centroid, d);
    if (dist < best_dist) {
      best_cluster = c;
      best_dist = dist;
    }
  }
  return best_cluster;
}


// Add a site (vector) into a sum of sites (vector).
void add_site(const double * site, double * sum, const int d) {
  for (int i=0; i<d; i++) {
    sum[i] += site[i];
  }
}

// Print the centroids one per line.
void print_centroids(double * centroids, const int k, const int d) {
  double *p = centroids;
  printf("Centroids:\n");
  for (int i = 0; i<k; i++) {
    for (int j = 0; j<d; j++, p++) {
      printf("%f ", *p);
    }
    printf("\n");
  }
}

double* load_data_from_file() {
  printf("Loading data...\n");
  ifstream file("kmeans-10gb", ios::in | ios::binary | ios::ate);
  if (!file) {
    printf("Error opening file\n");
    exit(1);
  }
  
  file.seekg(0, std::ios::end); 
  size_t size = file.tellg();
  printf("Got filesize: %ld\n", size);
  file.seekg(0, std::ios::beg);
  char* buffer = (char*)malloc(size);
  printf("Allocated buffer: %p\n", buffer);
  file.read(buffer, size);
  printf("Read file\n");
  file.close();  

  return (double*)buffer;
}

int main(int argc, char** argv) {
  srand(31359);

  size_t ndoubles = 12500000000 / 64;
  int k = 100;
  int d = 10;
  int niters = 10;
  
  size_t num_items = ndoubles / d;

  // Initial MPI and find process rank and number of processes.
  MPI_Init(NULL, NULL);
  int rank, nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  size_t sites_per_proc = num_items / nprocs;

  printf("k: %d, sites: %ld\n", k, sites_per_proc);
  
  //
  // Data structures in all processes.
  //
  // The sites assigned to this process.
  double* sites;  
  assert(sites = (double*)malloc(sites_per_proc * d * sizeof(double)));
  // The sum of sites assigned to each cluster by this process.
  // k vectors of d elements.
  double* sums;
  assert(sums = (double*)malloc(k * d * sizeof(double)));
  // The number of sites assigned to each cluster by this process. k integers.
  int* counts;
  assert(counts = (int*)malloc(k * sizeof(int)));
  // The current centroids against which sites are being compared.
  // These are shipped to the process by the root process.
  double* centroids;
  assert(centroids = (double*)malloc(k * d * sizeof(double)));
  // The cluster assignments for each site.
  int* labels;
  assert(labels = (int*)malloc(sites_per_proc * sizeof(int)));

  //long byte_start = sites_per_proc * rank;
  //long byte_end = byte_start + sites_per_proc;
  //printf("Rank %d downloading %ld, %ld\n", rank, byte_start, byte_end);
  //sites = download_data(byte_start, byte_end);

  sites = create_rand_nums(sites_per_proc * d, rank);
  
  //
  // Data structures maintained only in root process.
  //
  // All the sites for all the processes.
  // site_per_proc * nprocs vectors of d doubles.
  double* all_sites = NULL;
  // Sum of sites assigned to each cluster by all processes.
  double* grand_sums = NULL;
  // Number of sites assigned to each cluster by all processes.
  int* grand_counts = NULL;
  // Result of program: a cluster label for each site.
  int* all_labels;

  struct timeval start, end, diff;
  if (rank == 0) {
    // Take the first k sites as the initial cluster centroids.
    for (int i = 0; i < k * d; i++) {
      centroids[i] = sites[i]; 
    }
    print_centroids(centroids, k, d);
    assert(grand_sums = (double*)malloc(k * d * sizeof(double)));
    printf("Malloced sums\n");
    assert(grand_counts = (int*)malloc(k * sizeof(int)));
    printf("Malloced counts\n");
    assert(all_labels = (int*)malloc(nprocs * sites_per_proc * sizeof(int)));
    printf("Malloced labels, scattering\n");
  }

  if ( rank == 0 ) {
    gettimeofday(&start, 0);
  }

  // Root sends each process its share of sites.
  // MPI_Scatter(all_sites, d*sites_per_proc, MPI_DOUBLE, sites,
  //           d*sites_per_proc, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD);

  printf("Done scatter\n");
  double norm = 1.0;  // Will tell us if centroids have moved.
  
  struct timeval istart, iend, idiff;
  for ( int iter = 0; iter < niters; iter++ ) {
    if ( rank == 0 ) {
      gettimeofday(&istart, 0);
    }

    printf("Before broadcast\n");
    // Broadcast the current cluster centroids to all processes.
    MPI_Bcast(centroids, k*d, MPI_DOUBLE,0, MPI_COMM_WORLD);

    printf("After broadcast\n");
    // Each process reinitializes its cluster accumulators.
    for (int i = 0; i < k*d; i++) sums[i] = 0.0;
    for (int i = 0; i < k; i++) counts[i] = 0;

    printf("Reinitialized accumulators\n");
    // Find the closest centroid to each site and assign to cluster.
    //double* site = sites;
    for (size_t i = 0; i < sites_per_proc; i++) {
      int cluster = assign_site(&sites[i*d], centroids, k, d);
      // Record the assignment of the site to the cluster.
      counts[cluster]++;
      add_site(&sites[i*d], &sums[cluster*d], d);
    }

    printf("Assigned sites\n");

    // Gather and sum at root all cluster sums for individual processes.
    MPI_Reduce(sums, grand_sums, k * d, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(counts, grand_counts, k, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    printf("Reduced\n");
    if (rank == 0) {
      // Root process computes new centroids by dividing sums per cluster
      // by count per cluster.
      for (int i = 0; i<k; i++) {
	for (int j = 0; j<d; j++) {
	  int dij = d*i + j;
	  grand_sums[dij] /= grand_counts[i];
	}
      }
      // Have the centroids changed much?
      //norm = distance2(grand_sums, centroids, d*k);
      //printf("norm: %f\n",norm);
      // Copy new centroids from grand_sums into centroids.
      for (int i=0; i<k*d; i++) {
	centroids[i] = grand_sums[i];
      }
      print_centroids(centroids,k,d);
    }
    printf("Done centroids\n");
    
    // Broadcast the norm.  All processes will use this in the loop test.
    MPI_Bcast(&norm, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    if ( rank == 0 ) {
      gettimeofday(&iend, 0);
      
      timersub(&iend, &istart, &idiff);
      printf("Iteration %d: %ld.%06ld\n", iter,
	     (long) idiff.tv_sec, (long) idiff.tv_usec);
    }

  }

  // Now centroids are fixed, so compute a final label for each site.
  //double* site = sites;
  //for (int i = 0; i < sites_per_proc; i++, site += d) {
  //  labels[i] = assign_site(site, centroids, k, d);
  //}

  // Gather all labels into root process.
  //MPI_Gather(labels, sites_per_proc, MPI_INT,
  //	     all_labels, sites_per_proc, MPI_INT, 0, MPI_COMM_WORLD);

  // Root can print out all sites and labels.
  /*if ((rank == 0) && 1) {
    double* site = all_sites; 
    for (int i = 0;
	 i < nprocs * sites_per_proc;
	 i++, site += d) {
      for (int j = 0; j < d; j++) printf("%f ", site[j]);
      printf("%4d\n", all_labels[i]);
    }
    }*/
      
  if ( rank == 0 ) {
    gettimeofday(&end, 0);

    timersub(&end, &start, &diff);
    printf("MPI k-means: %ld.%06ld\n",
	   (long) diff.tv_sec, (long) diff.tv_usec);
  }

  MPI_Finalize();

}
