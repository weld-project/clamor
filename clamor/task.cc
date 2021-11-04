#include "task.h"

#include <stdexcept>

#include "util-templates.cc"

using namespace std;

const task_id task_id::UNASSIGNED_ID(-1, -1);

/* TODO check if this worker was already registered */
void Task::register_start(Addr worker, task_info t_info) {
  struct timeval start_ts;
  gettimeofday(&start_ts, 0);
  
  workers[worker].start_time = move(start_ts);
  workers[worker].is_replica = t_info.is_replica;
  workers[worker].is_backup = t_info.is_backup;
}

uint64_t Task::register_completion(Addr worker) {
  struct timeval end_ts;
  gettimeofday(&end_ts, 0);

  workers.at(worker).end_time = move(end_ts);
  workers.at(worker).finished = true;

  return Util::time_diff_ms(workers.at(worker).start_time, workers.at(worker).end_time);
}

bool Task::was_complete(Addr worker) {
  return workers.at(worker).finished;
}

void Task::register_failure(Addr worker) {
  workers.at(worker).failed = true;
}

uint64_t Task::get_elapsed_time(const Addr worker, struct timeval & end_ts) const {
  return Util::time_diff_ms(workers.at(worker).start_time, end_ts);
}
