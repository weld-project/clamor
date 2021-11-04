#ifndef STAGE_H
#define STAGE_H

/* Handles bookkeeping for task completion times and speculation. */

#include <sys/time.h>

#include <mutex>
#include <optional>
#include <vector>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/median.hpp>

#include "net-util.h"
#include "task.h"

#include "dsm.grpc.pb.h"

typedef enum StageStatus {
  PENDING,  // some partitions have not been submitted
  RUNNING,  // all partitions are either submitted or complete
  COMPLETE, // all partitions complete
} StageStatus;

struct Stage {
  Stage(int64_t stage_id, dsm::Result* result,
	std::condition_variable* caller_cv, std::mutex* caller_m,
	int64_t expected_results);

  int64_t stage_id;
  
  std::unordered_map<uint64_t, Task> tasks; /* Partitions in this stage. */
    
  boost::accumulators::accumulator_set<
    double, boost::accumulators::stats<
    boost::accumulators::tag::median>> completion_time_acc;
  uint64_t finished_tasks = 0;
  
  StageStatus status;
  
  uint64_t returned_results = 0;
  int64_t expected_results = -1;
  dsm::Result* result; /* this will be sent back to caller */

  /* used to notify caller */
  std::mutex* caller_m; 
  std::condition_variable* caller_cv;

  bool complete = false; // Set to true when all partitions are computed and present on an available node.

  void register_start(Addr worker, uint64_t partition_id, task_info t_info);
  void register_failure(Addr worker, uint64_t partition_id);
  void register_completion(Addr worker, uint64_t partition_id, struct timeval end_ts);

  std::optional<std::unordered_set<uint64_t>> get_tasks_for_worker(Addr worker);
  void clear_completed_tasks(Addr worker);
  
  /**
   * Check elapsed times of workers and return the IDs of tasks that are taking too long. 
   * Workers are compared against other workers running a partition in this stage (similar computation). 
   */
  std::vector<task_id> check_speculatable_tasks();

  void mark_missing(uint64_t partition_id);

  void write_results();

  std::optional<worker_info> get_info(Addr worker, uint64_t partition_id) {
    return tasks.at(partition_id).get_info(worker);
  }

  uint64_t present_partitions() {
    return present_partitions_.size();
  }

  std::unordered_set<uint64_t> missing_partitions_;

private:
  static constexpr double SPECULATION_QUANTILE = 0.75;
  static constexpr double SPECULATION_MULTIPLIER = 1.25;

  std::unordered_set<int64_t> present_partitions_;
  std::unordered_map<Addr, std::unordered_set<uint64_t>> completed_tasks_; // { worker: vec<partition ID> }
};

#endif // STAGE_H
