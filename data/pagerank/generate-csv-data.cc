/* write pagerank inputs to file as binary */

#include <fstream>
#include <random>
#include <iostream>
#include <string>
#include <vector>

#include <unistd.h>

using namespace std;

int main( int argc, char** argv ) {
  default_random_engine generator;
  uniform_real_distribution<double> unif(0.0, 1.0);

  string fname = ""; // file prefix to write to

  uint64_t n = 1000; // num nodes
  double p = 0.05; // connectedness
  uint64_t nfiles = 1; // num files to write to
  
  int ch;
  while ((ch = getopt(argc, argv, "p:n:k:f:")) != -1) {
    switch (ch) {
    case 'p':
      p = atof(optarg);
      break;
    case 'n':
      n = atoi(optarg);
      break;
    case 'k':
      nfiles = atoi(optarg);
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

  if ( fname.size() < 1 ) return 0;
  
  // generate outlinks by randomly sampling
  // (directed) edges with probability p
  vector<vector<double>> outlinks;
  for ( uint32_t i = 0; i < n; i++ ) {
    vector<double> links;
    for ( uint32_t j = 0; j < n; j++ ) {
      if (unif(generator) < p) {
        links.push_back(j);
      }
    }
    outlinks.push_back(links);
  }

  uint64_t nodes_per_file = n/nfiles;
  for ( uint64_t k = 0; k < nfiles; k++ ) {
    std::ofstream out(fname + "-" + std::to_string(k) + ".csv");

    for ( uint64_t i = k*nodes_per_file; i < (k+1)*nodes_per_file; i++ ) {
      for ( uint64_t j = 0; j < outlinks[i].size(); j++ ) {
        out << outlinks[i][j];
        if ( j < outlinks[i].size() - 1 ) {
          out << ", ";
        } else if ( i < outlinks.size() - 1 ) {
          out << "\n";
        }
      }
    }
    out.close();
  }
  
  return 0;
}
