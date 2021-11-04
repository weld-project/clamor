#ifndef CURL_HANDLE_H
#define CURL_HANDLE_H

#include <stdint.h>

#include <curl/curl.h>

#include <string>

struct MemoryStruct {
  char* memory;
  size_t size;
};

/*
 * wrapper class for a CURL handle 
 * use one per thread 
 */
class CurlHandle {
 public:
  CurlHandle() {
      handle_ = curl_easy_init();
  }

  ~CurlHandle() {
      curl_easy_cleanup(handle_);
  }

  void download_data(MemoryStruct* chunk, int64_t range_start, int64_t range_end, std::string url);
  double get_filesize_only(std::string url);
  
  CURL* handle_;

 private:
  void set_url(std::string url);
  void set_range(int64_t range_start, int64_t range_end);
  void set_write_callback(size_t (*write_callback)(char*, size_t, size_t, void*), MemoryStruct* chunk);
  void make_request();
};

#endif
