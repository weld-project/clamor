#include "util.h"

#include <algorithm>

template<typename T>
T Util::pop_queue(std::queue<T> & q) {
  T ret;
  SPDLOG_DEBUG("Popping event");
  ret = std::move(q.front());

  q.pop();
  return ret;
}

template<typename T>
void Util::push_queue(T item, std::queue<T> & q) {
  SPDLOG_DEBUG("Pushing event");
  q.push(std::move(item));
}

template<typename T>
bool Util::vec_contains(const T & element, std::vector<T> & vec) {
  return (std::find(vec.begin(), vec.end(), element) != vec.end());
}

template<typename T>
uint32_t Util::vec_idx(const T & element, std::vector<T>* vec) {
  auto it = std::find(vec->begin(), vec->end(), element);
  if ( it != vec->end() ) {
    return (uint32_t)(it - vec->begin());
  } else {
    throw std::invalid_argument("Element not found");
  }
}

template<typename T>
bool Util::set_contains(const T & element, std::unordered_set<T> & set) {
  return (set.find(element) != set.end());
}

template<typename T, typename U>
bool Util::map_contains(const T & element, std::unordered_map<T, U> & map) {
  return (map.find(element) != map.end());
}

template<typename T, typename U>
std::vector<T> Util::sorted_keys(std::unordered_map<T, U> & map) {
  std::vector<T> keys;
  keys.reserve(map.size());
  for ( auto & x : map ) {
    keys.push_back(x.first);
  }

  sort(keys.begin(), keys.end());
  return keys;
}
