#ifndef WORKER_POOL_H
#define WORKER_POOL_H

#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "net-util.h"
#include "task.h"

struct WorkerInfo {
  Addr name;
  struct timeval available_ts;
  bool idle = true;
  
  WorkerInfo(Addr name)
    : name(name)
  {
    mark_available();
  }

  void mark_running() {
    idle = false;
  }
  
  void mark_available() {
    gettimeofday(&available_ts, 0);
    idle = true;
  }
  
  // How long this worker has been idle.
  uint64_t idle_time_ms() {
    if ( !idle ) return 0;
    
    struct timeval cur_ts;
    gettimeofday(&cur_ts, 0);
    return Util::time_diff_ms(available_ts, cur_ts);
  }
};

/* Manages workers on a single host. */
class WorkerPool {
 public:
  WorkerPool(std::string ip);
  
  void register_worker(Addr name);
  void mark_available(Addr name);
  void mark_running(Addr name, task_id t_id);
  void mark_failed(Addr name);

  void mark_node_failed();

  uint64_t idle_time(Addr name);
  bool is_available(Addr name);
  std::optional<Addr> get_available_worker();
  bool assign_worker(Addr name); // Returns true if this worker was available and not already assigned
  std::optional<task_id> get_worker_task(Addr name);

  bool is_alive();
  
 private:
  std::recursive_mutex pool_m_;
  
  std::string ip_;
  
  std::unordered_set<Addr> available_workers_; // TODO: Ideally this would be a priority queue sorted by idle time.
  std::unordered_set<Addr> running_workers_;
  std::unordered_map<Addr, task_id> worker_tasks_;
  std::unordered_set<Addr> failed_workers_;
  std::unordered_map<Addr, WorkerInfo> worker_infos_;
  
  bool alive_ = true;
};

/* Manages hosts. */
class ClusterManager {
 public:
  ClusterManager(bool local) :
    local_(local),
    pools_(),
    failed_nodes_()
  {}

  void add_pool_for_node(std::string ip);
    
  /* Worker management */
  void register_worker(Addr name);
  void mark_available(Addr name);
  void mark_running(Addr name, task_id t_id);
  void mark_failed(Addr name);

  bool is_failed(Addr name);
  bool is_failed_addr(Addr name); // Mainly for heartbeat bookkeeping -- don't ping twice if failed.
  bool is_available(Addr name);
  
  bool assign_worker(Addr name);
  bool partition_completed(Addr name, uint64_t partition_id);

  /**
   * Find a worker for this task, with preference given to the worker
   * that was assigned this partition ID for the previous stage. 
   * If the worker is not available but still alive, returns <nullopt, true>
   * to indicate that the scheduler should try again later.
   * If looking for a worker on which to place a speculative task,
   * will try to return a worker on a host not currently running this task;
   * if no such worker is available will return nullopt. 
   */
  std::pair<std::optional<Addr>, bool> find_worker(Task & task, bool speculating);
  std::optional<task_id> get_worker_task(Addr name);

 private:
  std::recursive_mutex cluster_m_;

  bool local_;
  
  std::unordered_map<std::string, WorkerPool> pools_;
  std::unordered_set<std::string> failed_nodes_; // hostnames/IPs of failed nodes
  std::unordered_set<Addr> failed_addrs_; // hostnames/IPs of failed nodes

  std::unordered_map<int64_t, Addr> partition_to_worker_; // preferred locations
};

#endif // WORKER_POOL_H
