#include "curl-handle.h"

#include <string.h>

#include <iostream>
#include <string>

#include <curl/curl.h>

using namespace std;

size_t write_callback(char* contents, size_t size, size_t nmemb, void* userp) {
  size_t realsize = size * nmemb;
  struct MemoryStruct *mem = (struct MemoryStruct *)userp;
 
  mem->memory = (char*)realloc(mem->memory, mem->size + realsize + 1);

  if (mem->memory == NULL) {
    /* out of memory! */ 
    printf("not enough memory (realloc returned NULL)\n");
    return 0;
  }
 
  memcpy(&(mem->memory[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->memory[mem->size] = 0;

  return realsize;
}

/* Set range_start or range_end to -1 if unspecified */
void CurlHandle::download_data(MemoryStruct* chunk, int64_t range_start, int64_t range_end, string url) {
  chunk->memory = (char*)malloc(1);  /* will be grown as needed by the realloc above */ 
  chunk->size = 0;    /* no data at this point */ 

  CURLcode res;
  if (handle_) {
    cout << "Curling: " << url << endl;

    set_url(url);
    set_range(range_start, range_end);
    set_write_callback(write_callback, chunk);

    cout << "Making request..." << flush;
    make_request();
    cout << "done." << endl;
  }
}

double CurlHandle::get_filesize_only(string url) {
  CURLcode res;

  if (handle_) {
    set_url(url);

    curl_easy_setopt(handle_, CURLOPT_HEADER, 1); 
    curl_easy_setopt(handle_, CURLOPT_NOBODY, 1);
    
    /* Perform the request, res will get the return code */
    make_request();

    double cl;
    res = curl_easy_getinfo(handle_, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    return cl;
  }

  return 0;
}

void CurlHandle::set_url(string url) {
  curl_easy_setopt(handle_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(handle_, CURLOPT_FOLLOWLOCATION, 1L);
}

void CurlHandle::set_range(int64_t range_start, int64_t range_end) {
  cout << "Range start: " << range_start << " range end: " << range_end << endl;
  /* download bytes */
  char range[1000];
  if ( range_start >= 0 ) {
    if ( range_end >= 0 ) {
      snprintf(range, 1000, "%ld-%ld", range_start, range_end);
    } else {
      // start but no end
      snprintf(range, 1000, "%ld-", range_start);
    }
  } else {
    if ( range_end >= 0 ) {
      // end but no start
      snprintf(range, 1000, "-%ld", range_start);
    } else {
      // no range specified
      snprintf(range, 1000, "", range_start);
    }
  }

  curl_easy_setopt(handle_, CURLOPT_RANGE, range);
  cout << range << endl;
}

void CurlHandle::set_write_callback(size_t (*write_callback)(char*, size_t, size_t, void*), MemoryStruct* chunk) {
  /* send all data to this function  */ 
  curl_easy_setopt(handle_, CURLOPT_WRITEFUNCTION, write_callback);
  
  /* we pass our 'chunk' struct to the callback function */ 
  curl_easy_setopt(handle_, CURLOPT_WRITEDATA, (void *)chunk);
}

void CurlHandle::make_request() {
  CURLcode res = curl_easy_perform(handle_);
  
  /* Check for errors */ 
  if (res != CURLE_OK) {
    fprintf(stderr, "curl_easy_perform() failed: %s\n",
            curl_easy_strerror(res));
  }
}
