/**
 * kmeans_local.cc
 * Perform k-means on a test dataset on a single node.
 */

#ifdef __linux__
#define _BSD_SOURCE 500
#define _POSIX_C_SOURCE 2
#endif

#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>

#include "clamor/curl-handle.h"

#include "weld.h"

template <typename T>
struct weld_vector {
    T *data;
    int64_t length;
};

struct args {
  weld_vector<weld_vector<double>> data_vec;
  weld_vector<weld_vector<double>> means;
  int64_t k;
  int64_t iters;
};

template <typename T>
weld_vector<T> make_weld_vector(T *data, int64_t length) {
    struct weld_vector<T> vector;
    vector.data = data;
    vector.length = length;
    return vector;
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

double l2_norm(double* x, double* y, uint64_t d) {
  double result = 0.0;
  for ( uint64_t i = 0; i < d; i++ ){
    result += pow(x[i]-y[i], 2);
  }
  return sqrt(result);
}

uint64_t argmin(double* vec, uint64_t k) {
  uint64_t argmin = 0;
  
  for ( uint64_t i = 1; i < k; i++ ) {
    if ( vec[i] < vec[argmin] ) {
      argmin = i;
    }
  }

  return argmin;
}

// adds x+y and stores in x
void elemwise_add(double* x, double* y, uint64_t d) {
  for ( uint64_t i = 0; i < d; i++ ) {
    x[i] += y[i];
  }
}

/* ret should be a length-k array of length-d doubles */
void run_query(double** data, uint64_t nitems, uint64_t d, uint64_t k, uint64_t niters, double** ret) {
  // preprocess inputs
  for ( uint64_t i = 0; i < nitems; i++ ) {
    for ( uint64_t j = 0; j < d; j++ ) {
      data[i][j] *= 2;
      printf("%d, ", data[i][j]);
    }
    printf("%\n");
  }
  
  // deterministically set the initial means
  for ( uint64_t i = 0; i < k; i++ ) {
    for ( uint64_t j = 0; j < d; j++ ){
      ret[i][j] = data[i][j];
    }
  }

  // storage for intermediates -- these get allocated once, they can be reused
  double** norms = (double**)malloc(sizeof(double*)*nitems);
  for ( uint64_t j = 0; j < nitems; j++ ) {
    norms[j] = (double*)malloc(sizeof(double)*k);
  }

  uint64_t* counts = (uint64_t*)malloc(sizeof(uint64_t)*k);
  uint64_t* cluster_assignments = (uint64_t*)malloc(sizeof(uint64_t)*nitems);
  
  for ( uint64_t i = 0; i < niters; i++ ) {
    // compute norms to each mean for each point
    for ( uint64_t m = 0; m < k; m++ ) {
      for ( uint64_t j = 0; j < nitems; j++ ) {
	norms[j][m] = l2_norm(data[j], ret[m], d);
      }
    }

    // re-initialize
    for ( uint64_t j = 0; j < k; j++ ) {
      counts[j] = 0;
    }
    
    // find the index of min mean (cluster index) for each point
    for ( uint64_t j = 0; j < nitems; j++ ) {
      cluster_assignments[j] = argmin(norms[j], k);
      counts[cluster_assignments[j]]++;
    }

    // compute updated means based on new clusters
    for ( uint64_t j = 0; j < k; j++ ) {
      for ( uint64_t t = 0; t < d; t++ ) {
	ret[j][t] = 0;
      }
    }

    // compute sums
    for ( uint64_t j = 0; j < nitems; j++ ) {
      elemwise_add(ret[cluster_assignments[j]], data[j], d);
    }

    // divide by count to get mean
    for ( uint64_t j = 0; j < k; j++ ) {
      for ( uint64_t t = 0; t < d; t++ ) {
	ret[j][t] /= (double)(counts[j]);
      }
    }
  }
}

weld_vector<weld_vector<double>> run_query_weld(double** data, uint64_t nitems, uint64_t d, uint64_t k, uint64_t niters) {
    // Compile Weld module.
    weld_error_t e = weld_error_new();
    weld_conf_t conf = weld_conf_new();

    FILE *fptr = fopen("kmeans.weld", "r");
    fseek(fptr, 0, SEEK_END);
    int string_size = ftell(fptr);
    rewind(fptr);
    char *program = (char *) malloc(sizeof(char) * (string_size + 1));
    fread(program, sizeof(char), string_size, fptr);
    program[string_size] = '\0';

    struct timeval start, end, diff;
    gettimeofday(&start, 0);
    weld_module_t m = weld_module_compile(program, conf, e);
    weld_conf_free(conf);
    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("Weld compile time: %ld.%06ld\n",
            (long) diff.tv_sec, (long) diff.tv_usec);

    if (weld_error_code(e)) {
        const char *err = weld_error_message(e);
	printf("Weld failed with error: %s\n", err);
        exit(1);
    }

   gettimeofday(&start, 0);
   
   struct args args;
   args.data_vec.data = (weld_vector<double>*)malloc(sizeof(weld_vector<double>)*nitems);
   args.data_vec.length = nitems;
   for ( uint64_t i = 0; i < nitems; i++ ) {
     args.data_vec.data[i] = make_weld_vector(data[i], d);
   }

   double** means = generate_means(d, k);
   args.means.data = (weld_vector<double>*)malloc(sizeof(weld_vector<double>)*k);
   args.means.length = k;
   for ( uint64_t i = 0; i < k; i++ ) {
     args.means.data[i] = make_weld_vector(means[i], d);
   }
   
   args.k = k;
   args.iters = niters;
   
   weld_value_t weld_args = weld_value_new(&args);

   // Run the module and get the result.
   conf = weld_conf_new();
   weld_context_t context = weld_context_new(conf);
   weld_value_t result = weld_module_run(m, context, weld_args, e);

   if (weld_error_code(e)) {
     const char *err = weld_error_message(e);
     printf("Weld failed with error: %s\n", err);
     exit(1);
   }
   
   weld_vector<weld_vector<double>>* result_data = (weld_vector<weld_vector<double>>*) weld_value_data(result);
   weld_vector<weld_vector<double>> final_result = *result_data;

   // Free the values.
   weld_value_free(result);
   weld_value_free(weld_args);
   weld_conf_free(conf);
   
   weld_error_free(e);
   weld_module_free(m);
   gettimeofday(&end, 0);
   timersub(&end, &start, &diff);
   
   return final_result;
}

/** Generates input data.
 *
 * @param num_items the number of line items.
 * @param d the dimension of each point.
 * @param k the number of clusters.
 * @return the generated sequence of dummy inputs.
 */
double** generate_data(int num_items, int d, int k) {
  double** data = (double**)malloc(sizeof(double*)*num_items);
  for ( uint64_t i = 0; i < num_items; i++ ) {
    data[i] = (double*)malloc(sizeof(double)*d);
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

void free_generated_data(double** d, int k) {
  for ( uint32_t i = 0; i < k; i++ ){
    free(d[i]);
  }

  free(d);
}

int main(int argc, char **argv) {
    int num_items = 625000000;// (1E8 / sizeof(int));
    int k = 10;
    int d = 2;
    int num_iters = 10;

    int ch;
    while ((ch = getopt(argc, argv, "n:k:d:i:")) != -1) {
        switch (ch) {
	case 'n':
	  num_items = atoi(optarg);
	  break;
	case 'k':
	  k = atoi(optarg);
	  break;
	case 'd':
	  d = atoi(optarg);
	  break;
	case 'i':
	  num_iters = atoi(optarg);
	  break;
	case '?':
	default:
	  fprintf(stderr, "invalid options");
	  exit(1);
        }
    }

    // Check parameters.
    assert(num_items > 0);
    assert(k > 0);
    assert(d > 0);

    std::string url = "https://weld-dsm-east.s3.amazonaws.com/kmeans-small";
    CurlHandle handle;
    MemoryStruct chunk;
    handle.download_data(&chunk, -1, -1, url);
    
    double** data = (double**)chunk.memory;
    double** ret_c = (double**)malloc(sizeof(double*)*k);
    for ( uint64_t i = 0; i < k; i++ ) {
      ret_c[i] = (double*)malloc(sizeof(double)*d);
    }
    struct timeval start, end, diff;

    gettimeofday(&start, 0);
    run_query(data, num_items, d, k, num_iters, ret_c);
    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("Single-threaded C++: %ld.%06ld\n",
	   (long) diff.tv_sec, (long) diff.tv_usec);
    free_generated_data(data, k);

    for ( uint64_t i = 0; i < k; i++ ) {
      for ( uint64_t j = 0; j < d; j++ ) {
	printf("%f ", ret_c[i][j]);
      }
      printf("\n");
    }
    
    free(ret_c);

    weld_vector<weld_vector<double>> ret_weld = run_query_weld(data, num_items, d, k, num_iters);

    for ( uint64_t i = 0; i < k; i++ ) {
      for ( uint64_t j = 0; j < d; j++ ) {
	printf("%f ", ret_weld.data[i].data[j]);
      }
      printf("\n");
    }
    //printf("Difference: %f\n", result_weld - result_c);

    free(data);
    return 0;
}
