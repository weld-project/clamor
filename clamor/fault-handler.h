#ifndef FAULT_HANDLER_H
#define FAULT_HANDLER_H

#include <signal.h>
#include <stdint.h>
#include <pthread.h>

#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <unordered_map>
#include <utility>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/sum.hpp>

#include <google/protobuf/repeated_field.h>

#include "curl-handle.h"
#include "dsm-client.h"
#include "worker-client.h"

#include "segfault.h"
#include "task.h"

extern "C" {
  #include "smalloc/mem.h"
}

extern char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

class DSMClient;

class FaultHandler {
 public:
  FaultHandler();
  void add_requester(DSMClient* requester);
  void set_is_driver(bool is_driver);
  void set_addr(Addr addr);
  
  static FaultHandler fh;
  static struct sigaction default_handler;

  static bool is_shared(uintptr_t addr);

  int read_to_addr(void* addr, int64_t start_byte, int64_t end_byte, std::string fname);
  int download_to_addr(void* addr, int64_t start_byte, int64_t end_byte, std::string url);
  void download_to_chunk(MemoryStruct* chunk, int64_t start_byte, int64_t end_byte, std::string url);

  bool read_handler(uintptr_t pgaddr);
  bool write_handler(uintptr_t pgaddr);

  bool lock_page(uintptr_t pgnum);
  bool unlock_page(uintptr_t pgnum);
  
  std::unordered_map<task_id, std::vector<uint64_t>> write_requests_;
  std::unordered_map<task_id, std::vector<uint64_t>> read_requests_;

  task_id current_task;

  boost::accumulators::accumulator_set<double,
    boost::accumulators::stats<boost::accumulators::tag::sum > > driver_request_time;
  boost::accumulators::accumulator_set<double,
    boost::accumulators::stats<boost::accumulators::tag::sum > > peer_request_time;

  std::pair<double, double> get_request_times();
  void reset_request_time();
  
  std::unordered_set<uint64_t> cached_pages_;
  pthread_mutex_t cache_m;

  void cache_insert(uint64_t pgnum);
  void cache_erase(uint64_t pgnum);

 private:
  DSMClient* requester_;
  CurlHandle handle_;
  bool is_driver_;

  Addr self_addr_;
  
  pthread_mutex_t pgmutex_[NUM_PAGES];
 
  mutable std::mutex worker_clients_mutex_;
  mutable std::shared_mutex cache_partition_mutex_;
  // Key is start page of the cache partition. We assume cache partitions are
  // disjoint, thus end page is not necessary.
  // TODO: We should use RCU for performance.
  std::unordered_map<int32_t, std::vector<std::pair<Addr, std::shared_ptr<WorkerClient>>>>
      cache_partition_to_server_;
  std::unordered_map<Addr, std::shared_ptr<WorkerClient>> worker_clients_;
  std::vector<Addr> client_addrs_;

  std::shared_ptr<WorkerClient> add_or_get_peer(const Addr & client_addr);
  
  void set_sigaction(sigact_callback func);
  void unblock();
  
  bool page_in_driver(uint64_t pgnum);
  bool cache_contains(uint64_t pgnum);

  bool read_helper(uint64_t pgnum, dsm::Page & retpage);
  bool write_helper(uint64_t pgnum, dsm::Page & retpage);
};

static void fault_handler(int sig, siginfo_t* info, void* ctx);

#endif
