#include "stage.h"

#include "debug.h"
#include "task-manager.h"
#include "util-templates.cc"

using namespace std;
using namespace boost::accumulators;

Stage::Stage(int64_t stage_id, dsm::Result* result,
	     condition_variable* caller_cv, mutex* caller_m, int64_t expected_results) :
  stage_id(stage_id),
  result(result),
  caller_cv(caller_cv),
  caller_m(caller_m),
  expected_results(expected_results),
  present_partitions_(),
  missing_partitions_()
{
  //for ( uint64_t i = 0; i < TaskManager::NUM_PARTITIONS; i++ ) {
  for ( uint64_t i = 0; i < expected_results; i++ ) {
    missing_partitions_.insert(i);
  }
}

void Stage::register_start(Addr worker, uint64_t partition_id, task_info t_info) {
  tasks.at(partition_id).register_start(worker, t_info);
}

void Stage::register_completion(Addr worker, uint64_t partition_id,
				struct timeval end_ts) {
  uint64_t elapsed = tasks.at(partition_id).register_completion(worker);
  SPDLOG_DEBUG("Elapsed: {}", elapsed);
  completion_time_acc(elapsed);
  finished_tasks++;
    
  present_partitions_.insert(partition_id);
  missing_partitions_.erase(partition_id);
  completed_tasks_[worker].insert(partition_id);
}

optional<unordered_set<uint64_t>> Stage::get_tasks_for_worker(Addr worker) {
  try {
    return completed_tasks_.at(worker);
  } catch ( exception & e ) {
    return nullopt;
  }
}

// Do this when we have resubmitted all partitions for this worker so we don't submit them multiple times
void Stage::clear_completed_tasks(Addr worker) {
  completed_tasks_[worker].clear();
}

void Stage::register_failure(Addr worker, uint64_t partition_id) {
  tasks.at(partition_id).register_failure(worker);
  present_partitions_.erase(partition_id);
  missing_partitions_.insert(partition_id);
}

void Stage::mark_missing(uint64_t partition_id) {
  present_partitions_.erase(partition_id);
  missing_partitions_.insert(partition_id);
}

vector<task_id> Stage::check_speculatable_tasks() {
  vector<task_id> speculatable_tasks;

  // Don't speculate based on too little information
  // uint64_t min_finished_for_speculation = SPECULATION_QUANTILE * tasks.size();
  
  uint64_t min_finished_for_speculation = 10;
  SPDLOG_DEBUG("Finished tasks: {:d}", finished_tasks);
  if ( complete or (finished_tasks < min_finished_for_speculation) ) {
    SPDLOG_DEBUG("Returning...");
    return speculatable_tasks;
  }

  SPDLOG_DEBUG("Checking times");
  struct timeval cur_ts;
  SPDLOG_DEBUG("Constructed ts");
  gettimeofday(&cur_ts, 0);
  SPDLOG_DEBUG("Got time");
  double med_time = median(completion_time_acc);
  SPDLOG_DEBUG("Got med time");
  SPDLOG_DEBUG("Med time: {}", med_time);
  double threshold = SPECULATION_MULTIPLIER * med_time;
  SPDLOG_DEBUG("Threshold {}", threshold);

  for ( auto & id_task : tasks ) {
    Task & task = id_task.second;
    if ( not Util::set_contains(task.id.partition_id, present_partitions_) ) { // still running
      for ( auto & worker_time : task.workers ) {
	double tdiff = (double)(task.get_elapsed_time(worker_time.first, cur_ts));
	SPDLOG_DEBUG("Elapsed time for {:s}: {:d}", task.id.str(), tdiff);
	if ( tdiff > threshold ) {
     	  // worker has been running for too long, speculate!
	  speculatable_tasks.push_back(task.id);
	}
      }
    }
  }

  return speculatable_tasks;
}

void Stage::write_results() {
  lock_guard<mutex> lk(*caller_m);
  for ( uint64_t i = 0; i < tasks.size(); i++ ) {
    /* write all results */
    (result)->add_results_list(tasks[i].result);
  }
  
  SPDLOG_DEBUG("Marking stage {:d} as complete", stage_id);
  complete = true;
}
