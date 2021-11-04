#include "worker-pool.h"

#include "util-templates.cc"

#include <utility>

using namespace std;

const uint64_t RESCHEDULE_DELAY_MS = 300; // If a worker is idle for more than 1s, let a non-local task be scheduled on it.

WorkerPool::WorkerPool(string ip) :
  ip_(ip),
  available_workers_(),
  running_workers_(),
  failed_workers_()
{
}

void WorkerPool::register_worker(Addr name) {
  lock_guard<recursive_mutex> lk(pool_m_);
  available_workers_.insert(name);
  worker_infos_.emplace(name, name);
}

optional<Addr> WorkerPool::get_available_worker() {
  lock_guard<recursive_mutex> lk(pool_m_);
  if ( available_workers_.size() > 0 ) {
    Addr next_worker = *(available_workers_.begin());
    return optional<Addr>{ next_worker };
  } else {
    return nullopt;
  }
}

bool WorkerPool::assign_worker(Addr name) {
  if ( Util::set_contains(name, available_workers_) ) {
    available_workers_.erase(name);
    return true;
  }

  return false;
}

optional<task_id> WorkerPool::get_worker_task(Addr name) {
  lock_guard<recursive_mutex> lk(pool_m_);
  try {
    SPDLOG_DEBUG("Retrieving task for name {:s}", name.str());
    SPDLOG_DEBUG("Task for {:s} is {:s}", name.str(), worker_tasks_.at(name).str());
    return optional<task_id>{ worker_tasks_.at(name) };
  } catch ( exception & e ) {
    return nullopt;
  }
}

void WorkerPool::mark_available(Addr name) {
  lock_guard<recursive_mutex> lk(pool_m_);
  SPDLOG_DEBUG("Worker {:s} available, erasing from running...", name.str());
  running_workers_.erase(name);
  available_workers_.insert(name);
  worker_infos_.at(name).mark_available();
}

void WorkerPool::mark_running(Addr name, task_id t_id) {
  lock_guard<recursive_mutex> lk(pool_m_);
  assert( not Util::set_contains(name, available_workers_) and
	  not Util::set_contains(name, running_workers_) ); // don't let the worker be assigned twice!
  SPDLOG_DEBUG("Inserting task {:s} for worker {:s}", t_id.str(), name.str());
  running_workers_.insert(name);
  worker_tasks_[name] = t_id;
  worker_infos_.at(name).mark_running();
}

uint64_t WorkerPool::idle_time(Addr name) {
  return worker_infos_.at(name).idle_time_ms();
}

void WorkerPool::mark_failed(Addr name) {
  lock_guard<recursive_mutex> lk(pool_m_);
  running_workers_.erase(name);
  failed_workers_.insert(name);
}

bool WorkerPool::is_available(Addr name) {
  lock_guard<recursive_mutex> lk(pool_m_);
  return alive_ && Util::set_contains(name, available_workers_);
}

bool WorkerPool::is_alive() {
  lock_guard<recursive_mutex> lk(pool_m_);
  return alive_;
}

void WorkerPool::mark_node_failed() {
  lock_guard<recursive_mutex> lk(pool_m_);
  alive_ = false;
}

void ClusterManager::add_pool_for_node(string ip) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  pools_.emplace(ip, ip); // note that emplace will construct a new WorkerPool in place
}

void ClusterManager::mark_running(Addr name, task_id t_id) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  pools_.at(name.ip).mark_running(name, t_id);
}

void ClusterManager::mark_available(Addr name) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  pools_.at(name.ip).mark_available(name);
}

void ClusterManager::mark_failed(Addr name) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  if ( local_ ) {
    failed_nodes_.insert(name.str());
  } else {
    // We assume the entire node is down if any one worker fails
    failed_nodes_.insert(name.ip);
    pools_.at(name.ip).mark_node_failed();
  }

  pools_.at(name.ip).mark_failed(name);
  failed_addrs_.insert(name);
}

void ClusterManager::register_worker(Addr name) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  try { 
    pools_.at(name.ip).register_worker(name);
  } catch ( exception & e ) {
    add_pool_for_node(name.ip);
    pools_.at(name.ip).register_worker(name);
  }
}

bool ClusterManager::is_failed(Addr name) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  if ( local_ ) {
    return Util::set_contains(name.str(), failed_nodes_);
  } else {
    return Util::set_contains(name.ip, failed_nodes_);
  } 
}

bool ClusterManager::is_failed_addr(Addr name) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  return Util::set_contains(name, failed_addrs_);
}

optional<task_id> ClusterManager::get_worker_task(Addr name) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  return pools_.at(name.ip).get_worker_task(name);
}

pair<optional<Addr>, bool> ClusterManager::find_worker(Task & task, bool speculating) {
  lock_guard<recursive_mutex> lk(cluster_m_);

  if ( Util::map_contains(task.id.partition_id, partition_to_worker_) and not speculating ) {
    // Try to assign to the last worker that was running this partition ID
    auto & worker = partition_to_worker_.at(task.id.partition_id);
    SPDLOG_DEBUG("Got current worker {:s} for task {:s}",  worker.str(), task.id.str());
    if ( not is_failed(worker) ) {
      if ( pools_.at(worker.ip).is_available(worker) ) {
	SPDLOG_DEBUG("Worker available, returning...");
	return make_pair(worker, false);
      } else {
	SPDLOG_DEBUG("Worker alive but not available, checking for non-local idle workers...");
	return make_pair(nullopt, true);
	
	for ( auto & pool : pools_ ) {
	  if (!pool.second.is_alive()) continue;
	  
	  optional<Addr> backup = pool.second.get_available_worker();
	  if ( backup != nullopt ) {
	    uint64_t idle_time = pools_.at(backup.value().ip).idle_time(backup.value());
	    SPDLOG_DEBUG("Idle time for {:s}: {:d} ms", backup.value().str(), idle_time);
	    if ( not is_failed(backup.value())
		 // Only schedule non-local if the worker has been sitting idle (starving).
		 and (idle_time > RESCHEDULE_DELAY_MS) )  {
	      SPDLOG_DEBUG("Rescheduling!");
	      return make_pair(backup, false);
	    }
	  }
	}
	return make_pair(nullopt, true);
      }
    }
  } else if ( speculating ) {
    // Assign to a worker on a host that's not already running this task
    auto hosts = task.hosts_running_task();
    for ( auto & pool : pools_ ) {
      if ( !local_ ) { // only useful for testing - if local, just assign to a new worker
	if ( Util::vec_contains(pool.first, hosts) ) continue;
      }	

      optional<Addr> worker = pool.second.get_available_worker();
      if ( worker != nullopt and not is_failed(worker.value()) ) return make_pair(worker, false);
    }
  }
  
  // No previous worker found for this partition (or forcing move), and not speculating
  SPDLOG_DEBUG("Assigning new worker...");
  for ( auto & pool : pools_ ) {
    if (!pool.second.is_alive()) continue;
    
    optional<Addr> worker = pool.second.get_available_worker();
    if ( worker != nullopt and not is_failed(worker.value()) ) return make_pair(worker, false);
  }
  
  return make_pair(nullopt, false); // no previous workers found, and none currently available
}

bool ClusterManager::assign_worker(Addr name) {
  lock_guard<recursive_mutex> lk(cluster_m_);
  return pools_.at(name.ip).assign_worker(name);
}

bool ClusterManager::partition_completed(Addr name, uint64_t partition_id) {
  partition_to_worker_[partition_id] = name;
}
