/* write k-means inputs to file as binary */

#include <fstream>
#include <random>
#include <iostream>
#include <string>

#include <unistd.h>

using namespace std;

int main( int argc, char** argv ) {

  default_random_engine generator;
  normal_distribution<double> normal(0.0, 1.0);
  uniform_real_distribution<double> unif(0.0, 100.0);

  string fname = ""; // file to write to

  uint32_t d = 2; // dimensions
  uint64_t c = 10; // num clusters
  uint64_t n = 62500000; // num data points
  double spread = 10.0;
  
  int ch;
  while ((ch = getopt(argc, argv, "d:n:c:s:f:")) != -1) {
    switch (ch) {
    case 'd':
      d = atoi(optarg);
      break;
    case 'n':
      n = atoi(optarg);
      break;
    case 'c':
      c = atoi(optarg);
      break;
    case 's':
      spread = atof(optarg);
      break;
    case 'f':
      fname = optarg;
      break;
    case '?':
    default:
      fprintf(stderr, "invalid options");
      exit(1);
    }
  }

  uniform_int_distribution<int> cent_idx(0, c-1);
  
  if ( fname.size() < 1 ) return 0;
  
  cout << "Writing " << n << " integers..." << endl;

  // generate centroids
  vector<vector<double>> centroids;
  for ( uint32_t i = 0; i < c; i++ ) {
    vector<double> cent;
    for ( uint32_t j = 0; j < d; j++ ) {
      cent.push_back(unif(generator));
    }
    centroids.push_back(cent);
  }

  double buffer[] = {1.0};
  FILE *fp = fopen(fname.c_str(), "w");

  for ( uint64_t i = 0; i < n; i++ ) {
    if ( i % 10000 == 0 ) printf("%lu\n", i);
    int next_idx = cent_idx(generator);
    for ( uint64_t j = 0; j < d; j++ ) {
      double next_val = centroids[next_idx][j] + (normal(generator) * spread);
      buffer[0] = next_val;
      fwrite(buffer, sizeof(double), 1, fp);
    }
  }

  fclose(fp);
  
  return 0;
}
