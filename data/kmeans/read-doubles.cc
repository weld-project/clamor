/* small program to parse + read ints from file */

#include <iostream>
#include <fstream>

#include <unistd.h>

using namespace std;

int main( int argc, char** argv ) {
    // Number of ints to read
    uint32_t n = (1E8 / sizeof(int));
    // File to read from
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

    FILE *fp = fopen(fname.c_str(), "r");
    double* buffer = (double*) malloc (sizeof(double) * n);
    if ( buffer == NULL ) exit(1);

    size_t result = fread( buffer, sizeof(double), n, fp );
    cout << "Read " << result << " doubles..." << endl;

    for ( size_t i = 0; i < result; i++ ) {
      cout << buffer[i] << " ";
    }
    cout << endl;
    
    fclose(fp);
    free(buffer);
    
    return 0;
}
