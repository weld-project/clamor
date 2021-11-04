/* write inputs to file as binary */

#include <fstream>
#include <random>
#include <iostream>
#include <string>

#include <unistd.h>

using namespace std;

int main( int argc, char** argv ) {
  string out_fname = "";
  string in_fname = "";

  int ch;
  while ((ch = getopt(argc, argv, "o:f:")) != -1) {
    switch (ch) {
    case 'o':
      out_fname = optarg;
      break;
    case 'f':
      in_fname = optarg;
      break;
    case '?':
    default:
      fprintf(stderr, "invalid options");
      exit(1);
    }
  }

  std::ifstream in(in_fname);
  std::string next;
  getline(in, next);
  cout << next << endl;

  uint32_t buffer[] = {1};
  FILE *fp = fopen(out_fname.c_str(), "w");

  // write the strings
  

  // write the weld_vectors
  
  
  while ( getline(in, next) ) {
    uint32_t next_int = atoi(next.c_str());
    buffer[0] = next_int;
    fwrite(buffer, sizeof(uint32_t), 1, fp);
  }

  fclose(fp);
  in.close();
}
