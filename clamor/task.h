#ifndef TASK_H
#define TASK_H

#include <exception>
#include <optional>
#include <string>
#include <vector>

#include <sys/time.h>

#include <boost/functional/hash.hpp>

#include "net-util.h"
#include "util.h"

typedef struct task_id {
  int64_t stage_id;
  int64_t partition_id;

  task_id() {
    stage_id = -1;
    partition_id = -1;
  }

  task_id( uint64_t s_id, uint64_t p_id ) {
    stage_id = s_id;
    partition_id = p_id;
  }

  const std::string str() const {
    return std::to_string(stage_id) + ":" + std::to_string(partition_id);
  }

  const static task_id UNASSIGNED_ID; // (-1, -1) before page has been written
} task_id;

template<>
struct std::hash<task_id> {
  std::size_t operator()(const task_id & p) const {
    size_t h = 0;
    boost::hash_combine(h, p.stage_id);
    boost::hash_combine(h, p.partition_id);

    return h;
  }
};

inline bool operator==(const task_id & lhs, const task_id & rhs) {
  return ((lhs.stage_id == rhs.stage_id) &&
	  (lhs.partition_id == rhs.partition_id));
}

inline bool operator!=(const task_id & lhs, const task_id & rhs) {
  return ((lhs.stage_id != rhs.stage_id) ||
	  (lhs.partition_id != rhs.partition_id));
}

// Used for task submission.
typedef struct task_info {
  task_id t_id;
  bool is_backup;   // Backup workers are launched when a page is missing.
  bool is_replica;  // Replicas are launched during task speculation.

  bool return_vectors; // Whether to return the full result.
  
  task_info()
  {};

  task_info(task_id t_id, bool is_backup, bool is_replica, bool return_vectors) :
    t_id(t_id),
    is_backup(is_backup),
    is_replica(is_replica),
    return_vectors(return_vectors)
  {}
} task_info;

template<>
struct std::hash<task_info> {
  std::size_t operator()(const task_info & p) const {
    size_t h = 0;
    boost::hash_combine(h, p.t_id.stage_id);
    boost::hash_combine(h, p.t_id.partition_id);
    boost::hash_combine(h, p.is_backup);
    boost::hash_combine(h, p.is_replica);

    return h;
  }
};

inline bool operator==(const task_info & lhs, const task_info & rhs) {
  return ((lhs.t_id == rhs.t_id) &&
	  (lhs.is_backup == rhs.is_backup) &&
	  (lhs.is_replica == rhs.is_replica));
}

typedef struct worker_info {
  Addr worker_ip;
  struct timeval start_time;
  struct timeval end_time;
  bool is_replica = false;
  bool is_backup = false;
  bool finished = false;
  bool failed = false;
} worker_info;

typedef struct Task {
  task_id id;
  void* data_addr;
  std::string code;
  
  /* Fields following are all written after task completes. */
  std::vector<uint64_t> pages_read;
  std::vector<uint64_t> pages_written;

  std::unordered_set<task_id> parent_partitions;

  bool complete = false;
  bool written_once = false;

  uint64_t result;

  /* { worker : start_time } */
  std::unordered_map<Addr, worker_info> workers;
  
  Task(int64_t _stage_id, int64_t _partition_id,
       void* _data_addr, std::string _code) :
    id(_stage_id, _partition_id),
    data_addr(_data_addr),
    code(_code)
  {}

  Task() {};

  void register_start(Addr worker, task_info t_info);

  /* Returns elapsed time in milliseconds and records current time as end time */
  uint64_t register_completion(Addr worker);

  void register_failure(Addr worker);

  bool was_complete(Addr worker);
  
  /* Returns elapsed time in milliseconds */
  uint64_t get_elapsed_time(const Addr worker, struct timeval & end_ts) const;

  // TODO maybe just have a counter
  uint64_t copies_running() {
    uint64_t ret = 0;
    for ( auto & wi : workers ) {
      if ( !wi.second.finished && !wi.second.failed ) ret++;
    }

    return ret;
  }

  std::vector<std::string> hosts_running_task() {
    std::vector<std::string> ret;
    for ( auto & wi : workers ) {
      if ( !wi.second.finished && !wi.second.failed ) ret.push_back(wi.first.ip);
    }
    return ret;
  }

  std::optional<worker_info> get_info(Addr worker) {
    try {
      return workers.at(worker);
    } catch ( std::exception & e ) {
      return std::nullopt;
    }
  }
} Task;

#endif // TASK_H
