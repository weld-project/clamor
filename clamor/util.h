#ifndef UTIL_H
#define UTIL_H

#include <sys/time.h>
#include <unistd.h>
#include "pthread.h"

#include "debug.h"
#include "lock.h"

#include <mutex>
#include <iostream>
#include <queue>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace Util {
  std::string read( std::string infile );
  std::vector< std::string > readlines( std::string infile );
  std::vector< std::string > readlines_strip_empty( std::string infile );
  std::vector< std::string > readlines_from_char( const char* input );
  std::vector< std::string > split( char* input, char* delimiter );
  std::vector< std::string > split( std::string input, char delimiter );
  std::string join( std::vector< std::string > vec, std::string delimiter );
  std::string join( std::pair< std::string, std::string > vec, std::string delimiter );

  /* 
   * make n shards with size an integer multiple of increment, with one possible exception
   * having a non-multiple remainder (by default, the last shard).
   * If there is only enough data for k < n shards, assign increment worth of data to
   * the first k workers.
   */
  std::vector<size_t> shard(size_t data_size, uint32_t nshards, size_t increment);
  std::vector<size_t> shard_units(size_t n_elements, uint32_t nshards);

  /* push_queue and pop_queue are not thread-safe and should be called with a lock_guard. */
  template<typename T>
  T pop_queue(std::queue<T> & q);

  template<typename T>
  void push_queue(T item, std::queue<T> & q);

  template<typename T>
  bool vec_contains(const T & element, std::vector<T> & vec);

  /* Throws an exception if element is not found 
     note: returns uint32_t, not uint64_t */
  template<typename T>
  uint32_t vec_idx(const T & element, std::vector<T>* vec);

  template<typename T>
  bool set_contains(const T & element, std::unordered_set<T> & set);

  template<typename T, typename U>
  bool map_contains(const T & element, std::unordered_map<T, U> & map);

  template<typename T, typename U>
  std::vector<T> sorted_keys(std::unordered_map<T, U> & map);
  
  uint64_t time_diff_ms(const struct timeval & start_ts, const struct timeval & end_ts) ;
}

#endif // UTIL_H
