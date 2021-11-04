#ifndef NET_UTIL_H
#define NET_UTIL_H

#include <boost/functional/hash.hpp>

#include <stdint.h>

#include <string>

typedef struct Addr {
  std::string ip;
  uint32_t port;

  Addr() {
    ip = "";
    port = 0;
  }

  Addr(std::string s_ip, uint32_t s_port) {
    ip = s_ip;
    port = s_port;
  }
  
  std::string str() const {
    return ip + ":" + std::to_string(port);
  }

  std::string str_port() const {
    return std::to_string(port);
  }

  bool operator==(const Addr &other) {
    return ip == other.ip && port == other.port;
  }

} Addr;

inline Addr empty_addr() {
  Addr ret;
  ret.ip = "";
  ret.port = 0;
  return ret;
}

inline bool operator==(const Addr & lhs, const Addr & rhs) {
  return ((lhs.ip == rhs.ip) &&
	  (lhs.port == rhs.port));
}

inline bool operator!=(const Addr & lhs, const Addr & rhs) {
  return ((lhs.ip != rhs.ip) ||
	  (lhs.port != rhs.port));
}

template<>
struct std::hash<Addr> {
  std::size_t operator()(const Addr& a) const {
    size_t h = 0;
    boost::hash_combine(h, a.ip);
    boost::hash_combine(h, a.port);

    return h;
  }
};

#endif // NET_UTIL_H
