#ifndef EXCEPTIONS_H
#define EXCEPTIONS_H

/**
 * Clamor-specific exception classes, mainly for handling recoverable failures.
 */

#include <exception>
#include <iostream>

#include <cxxabi.h>

#include "net-util.h"

class page_error : public std::exception
{
 public:
 page_error(uint64_t pagenum, std::string error)
   : pagenum_(pagenum),
    error_(error)
    {}
  
  const char * what( void ) const noexcept {
    return error_.c_str();
  }
  
  uint64_t pagenum() {
    return pagenum_;
  }

 protected:
  std::string error_;
  uint64_t pagenum_;
};

class invalid_requester_error : public page_error
{
 public:
 invalid_requester_error(Addr requester_id, uint64_t pagenum)
   : page_error(pagenum, "Requester " + requester_id.str() +
                "not allowed to request page " + std::to_string(pagenum))
      {
      }
};

class cloud_map_error : public page_error
{
public:
  cloud_map_error(uint64_t pagenum)
    : page_error(pagenum, "S3 page mapped in invalid location: " + std::to_string(pagenum))
    {
    }
};

class fetch_fail_error : public page_error
{
public:
  fetch_fail_error(uint64_t pagenum)
    : page_error(pagenum, "Failed to fetch page " + std::to_string(pagenum))
    {
    }
};

class retry_error : public page_error
{
public:
  retry_error(uint64_t pagenum)
    : page_error(pagenum, "Page " + std::to_string(pagenum) + " unavailable, will resubmit task")
    {
    }
};

class abort_replica_signal : public std::exception
{
private:
  std::string error_;

public:
  abort_replica_signal()
    : error_("Replica no longer needed, aborting")
    {
    }
  
  const char * what( void ) const noexcept {
    return error_.c_str();
  }
};

class cache_error : public std::exception {
 public:
  cache_error(std::string error) : error_(std::move(error)) {}
  const char * what(void) const noexcept {
    return error_.c_str();
  }

 private:
  std::string error_;
};

class cache_insertion_error : public cache_error {
 public:
  cache_insertion_error() : cache_error("Invalid cache insertion.") {}
};

inline void print_exception( const std::exception & e, std::ostream & output = std::cerr ) {
    output << "Died on " << abi::__cxa_demangle( typeid( e ).name(), nullptr, nullptr, nullptr ) << ": " << e.what() << std::endl;
}

#endif // EXCEPTIONS_H
