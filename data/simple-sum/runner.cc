#include <unistd.h>

#include <string>
#include <vector>

using namespace std;

template <typename T>
struct weld_vector {
    T *data;
    int64_t length;
};

/** 
 * Run the submitted distributed program.
 * The input to the program will be specified by the S3 URL.
 */
int runprog(string s3_url, size_t data_size,
            vector<string> workers) {
  
}

/** 
 * Run the submitted distributed program on the given input.
 */
int runprog(vector<weld_vector> input, vector<string> workers) {
  
}

int main(int argc, char** argv) {
  string s3_url = "";
  size_t size = 0;

  int ch;
  while ((ch = getopt(argc, argv, "u:l:")) != -1) {
    switch (ch) {
    case 'u':
      s3_url = optarg;
      break;
    case 'l':
      size = atoi(optarg);
      break;
    case '?':
    default:
      fprintf(stderr, "invalid options");
      exit(1);
    }
  }

}