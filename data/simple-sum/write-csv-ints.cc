/* small program to write n integers to file, as binary */

#include <iostream>
#include <fstream>
#include <string>

#include <unistd.h>

using namespace std;

int main( int argc, char** argv ) {
    // Number of ints to write
    // uint64_t n = (1E10 / sizeof(int));
  uint64_t n = 10000000000 / sizeof(int);
    // File to write to
    string fname = "";

    int ch;
    while ((ch = getopt(argc, argv, "f:n:")) != -1) {
      switch (ch) {
      case 'n':
        n = atoi(optarg);
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

    cout << "Writing " << n << " integers..." << endl;
    
    std::ofstream out(fname);
    for ( uint32_t i = 1; i < n+1; i++ ) {
      out << "1\n";
    }
    out << "1";
    out.close();
    
    return 0;
}
