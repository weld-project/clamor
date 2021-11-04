#include "task-manager.h"

#include "worker-client.h"
#include "dsm.grpc.pb.h"

#include "weld-utils.h"

#include "lock.h"
#include "grpc-util.h"
#include "debug.h"
#include "page.h"
#include "page-utils.h"
#include "util-templates.cc"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <memory>
#include <utility>

#include <atomic>

using grpc::ClientAsyncResponseReader;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;

using namespace std;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace boost::accumulators;

atomic_int qc;
atomic_int driver_qc;

char ro_cache[WORKER_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

#ifdef clamor_local // allocate less memory for local testing
const uint64_t TaskManager::NUM_PARTITIONS = 4; // TODO
#else
const uint64_t TaskManager::NUM_PARTITIONS = 64; // TODO
#endif

constexpr auto EVENT_POLL_TIMEOUT = 10ms;
constexpr auto HEARTBEAT_PERIOD = 500ms;
constexpr auto SUBMIT_TIMEOUT = 10ms;

Stage & TaskManager::stage_id_to_stage(const uint64_t stage_id) {
  return stages_[stage_id];
}

Task & TaskManager::task_id_to_task(const task_id id) {
  return stages_[id.stage_id].tasks[id.partition_id];
}

Task & TaskManager::task_id_to_task(const uint64_t stage_id, const uint64_t partition_id) {
  return stages_[stage_id].tasks[partition_id];
}

TaskManager::TaskManager(Addr driver_addr, uint32_t weld_range_start,
			 vector<string> worker_ips, uint32_t nprocs,
			 uint64_t num_partitions,
			 bool local, bool speculate) :
  cache_servers_(),
  local_(local),
  speculate_(speculate),
  driver_addr_(driver_addr),
  heap_allocator_(num_partitions),
  event_queue_(),
  servers_(),
  servers_vec_(),
  pending_tasks_(),
  cluster_manager_(local),
  invalidator_(&servers_, &servers_vec_, &cluster_manager_, driver_addr),
  pagetable_(&cluster_manager_, &invalidator_, &servers_vec_, driver_addr),
  stages_(),
  waiting_tasks_(),
  running_tasks_(),
  failed_tasks_() {}

bool TaskManager::check_dependencies(Task & task) {
  bool runnable = true;

  for ( uint64_t p : task.pages_read ) {
    runnable &= pagetable_.page_present(p);
  }

  return runnable;
}

/* Main event loop. */
void TaskManager::RunManager() {
  while (true) {
    // TODO go back to condition variable?
    this_thread::sleep_for(chrono::milliseconds(EVENT_POLL_TIMEOUT));

    {
      scoped_lock<recursive_mutex> lk(event_m_);
      while ( event_queue_.size() > 0 ) {
	SPDLOG_DEBUG("Got event, processing...");
	
	// check event
	unique_ptr<TaskManager::Event> next;
	next = Util::pop_queue<unique_ptr<Event>>(event_queue_);
	
	// process event
	next->process(this);
	SPDLOG_DEBUG("Done processing.");
      }
    }

    //    SPDLOG_DEBUG("Acquiring task lock");
    // submit/resubmit tasks
    scoped_lock<recursive_mutex> tl(tasks_m_);
    //    SPDLOG_DEBUG("...acquired.");
    vector<task_id> to_submit;

    unordered_set<task_id> waiting_ids; // just needed for check in the next loop
    //    SPDLOG_DEBUG("Create waiting ids");
    for ( auto wi : waiting_tasks_ ) {
      waiting_ids.insert(wi.t_id);
    }

    //    SPDLOG_DEBUG("Checking failed tasks");
    for ( auto & fp : failed_tasks_ ) {
      // gather all missing parent partitions that need to be reconstructed
      unordered_set<task_id> missing_parents;
      Task & f_part = task_id_to_task(fp);
      SPDLOG_DEBUG("Got failed partition: {:d}, {:d}", fp.stage_id, fp.partition_id);
      
      for ( uint64_t p : f_part.pages_read ) {
        get_missing_parents(p, &missing_parents);
      }
      if ( (missing_parents.size() == 0) and (f_part.written_once) ) {
        SPDLOG_DEBUG("Partition {:d}:{:d} has no missing parents; resubmitting...", fp.stage_id, fp.partition_id);
        enqueue_task(task_info(fp, true, false, /*return_vectors=*/false));
      } else {
	SPDLOG_DEBUG("Partition {:d}:{:d} has {:d} missing parents, or not previously written", fp.stage_id, fp.partition_id, missing_parents.size());
	waiting_tasks_.insert(task_info(fp, true, false, /*return_vectors=*/false));
	waiting_ids.insert(fp);

	for ( auto & t_id : missing_parents ) {
	  // be sure to mark these as failed too so they get reconstructed...
	  if ( not Util::set_contains(t_id, waiting_ids) and
	       not Util::set_contains(t_id, running_tasks_) and
	       not Util::set_contains(t_id, failed_tasks_) ) {
	    to_submit.push_back(t_id);
	  }
	}
      }
    }

    //    SPDLOG_DEBUG("Done checking failed tasks.");
    failed_tasks_.clear();
    for ( auto & x : to_submit ) {
      SPDLOG_DEBUG("Adding parent {:d} {:d} to failed", x.stage_id, x.partition_id);
      //task_id_to_task(x).complete = false;
      failed_tasks_.insert(x);
      Stage & s = stage_id_to_stage(x.stage_id);
      s.mark_missing(x.partition_id); // make sure we register that this is missing
    }

    //    SPDLOG_DEBUG("Checking waiting tasks");
    vector<task_info> submit_success;
    //bool all_present = all_pages_present(); // This is an expensive check, so don't repeat it for every task
    
    for ( task_info wi : waiting_tasks_ ) {
      // can we run any of these now?
      Task & w_part = task_id_to_task(wi.t_id);
      bool runnable = true;
      if ( w_part.written_once ) {
	runnable = check_dependencies(w_part);
      } else {
	/* No pages recorded for this task yet. Only launch if all previously-written pages are present. */
	//runnable = all_present;
      }

      if ( runnable ) {
	enqueue_task(wi);
	SPDLOG_DEBUG("Enqueued task {:s}", wi.t_id.str());
	submit_success.push_back(wi);
      }
    }

    //    SPDLOG_DEBUG("Done checking waiting tasks.");

    for ( task_info & x : submit_success ) {
      SPDLOG_DEBUG("Erasing task {:s} from waiting", x.t_id.str());
      waiting_tasks_.erase(x);
    }

    //    SPDLOG_DEBUG("Done with run loop.");
  }
}

/* Periodic timed heartbeat messages to servers. */
void TaskManager::Heartbeat() {
  while (true) {
    this_thread::sleep_for(chrono::milliseconds(HEARTBEAT_PERIOD));

    {
      scoped_lock<mutex> lk(workers_m_);

      for ( auto & w_pair : servers_ ) {
        if ( cluster_manager_.is_failed_addr(w_pair.first) ) {
          continue;
        }
      
        Status status = w_pair.second.heartbeat();
	if ( !status.ok() ) {
	  auto retry_ms = HEARTBEAT_PERIOD;
	  for ( int i = 0; i < 4; i++ ) {
	    this_thread::sleep_for(chrono::milliseconds(retry_ms));
	    status = w_pair.second.heartbeat();
	    if ( status.ok() ) break;
	  }
	}
	
        if (!status.ok()) {
          // Assume server failed.
          SPDLOG_INFO("Worker {:s} returned status {:d}: {:s}",
                  w_pair.first.str(),
                  status.error_code(),
                  status.error_message());

          // Remove the server from cache server lists.
          disable_cache_server(w_pair.first);
	
          mutex m;
          condition_variable cv;
	
          unique_ptr<WorkerFailedEvent> e = make_unique<WorkerFailedEvent>(w_pair.first, true, false);
	
          {
            /* put new task event on queue */
            scoped_lock<recursive_mutex> event_lk(event_m_);
            Util::push_queue<unique_ptr<Event>>(move(e), event_queue_);
          }

          {
            scoped_lock<recursive_mutex> task_lk(tasks_m_);
            cluster_manager_.mark_failed(w_pair.first);
          }
	
          /* notify scheduler */
          event_wake_cv_.notify_all();
        }
      }
    }
  }
}

void TaskManager::submit_loop() {
  while (true) {
    this_thread::sleep_for(chrono::milliseconds(SUBMIT_TIMEOUT));

    {
      //SPDLOG_DEBUG("workers {:d} tasks {:d}", available_workers_.size(), pending_tasks_.size());
      scoped_lock<recursive_mutex> tl(tasks_m_);
      vector<task_info> to_erase;
      if ( pending_tasks_.size() > 0 ) {
	SPDLOG_DEBUG("Launching {:d} tasks...", pending_tasks_.size());
      }
      //bool all_present = all_pages_present(); // This is an expensive check, so don't repeat it for every task

      for ( auto & next_task_info : pending_tasks_ ) {
	Task & task = task_id_to_task(next_task_info.t_id);
	pair<optional<Addr>, bool> assignment = cluster_manager_.find_worker(task, false);
	if ( assignment.first == nullopt and assignment.second ) {
	  // Worker alive but not yet free, try again later
	  SPDLOG_DEBUG("Worker not free, will try again...");
	  continue;
	}

        auto next_worker = assignment.first;
	if ( next_worker != nullopt ) {
	  bool runnable = true;
	  if ( task.written_once ) {
	    runnable = check_dependencies(task);
	  } else {
	    /* No pages recorded for this task yet. Only launch if all previously-written pages are present. */
	    //runnable = all_present;
	  }
	  
	  if (!runnable) {
	    SPDLOG_DEBUG("Probably lost some partitions for task {:s}, waiting to launch...", task.id.str());
	    continue;
	  }
	
	  SPDLOG_DEBUG("Launching task {:s} on worker {:s}",
		       task.id.str(), next_worker.value().str());
	  bool assigned = cluster_manager_.assign_worker(next_worker.value());
	  assert( assigned ); // should never return false here
	  submit_task(next_task_info, next_worker.value());
	  to_erase.push_back(next_task_info);
	} else {
	  // no assignable workers left
	  break;
	}
      }

      if (to_erase.size() > 0) {
        SPDLOG_DEBUG("Erasing {:d} launched tasks...", to_erase.size());
        for ( auto & x : to_erase ) {
          SPDLOG_DEBUG("Erasing launched task {:s}", x.t_id.str());
          pending_tasks_.erase(x);
        }
        SPDLOG_DEBUG("done.");
      }
        
      // Speculate if needed
      if ( speculate_ ) {
	//scoped_lock<recursive_mutex> stage_lk(stage_m_);
	for ( Stage & s : stages_ ) {
	  auto tasks = s.check_speculatable_tasks();
	  for ( auto next_task_id : tasks ) {
	    Task & task = task_id_to_task(next_task_id);

	    if ( task.hosts_running_task().size() > 1 ) {
	      // Don't speculate too many times
	      continue;
	    }
	    
	    pair<optional<Addr>, bool> assignment = cluster_manager_.find_worker(task, true);
	    auto next_worker = assignment.first;

	    if ( next_worker != nullopt ) {
	      SPDLOG_DEBUG("Launching speculative task {:s} on worker {:s}",
			   task.id.str(), next_worker.value().str());
	      bool assigned = cluster_manager_.assign_worker(next_worker.value());
	      assert( assigned ); // should never return false here

	      //// TODO make sure to enqueue task as replica here
	      submit_task(task_info(next_task_id, false, true, /*return_vectors=*/false),
			  next_worker.value());
	    } else {
	      // no assignable workers left
	      break; 
	    }
	  }
	}
      }
    }
  }
}

void TaskManager::enqueue_task(task_info t_info) {
  {
    scoped_lock<recursive_mutex> tl(tasks_m_);
    SPDLOG_DEBUG("Enqueueing task {:s}", t_info.t_id.str());
    if ( not Util::set_contains(t_info.t_id, running_tasks_) ) { // Already launched
      pending_tasks_.insert(t_info);
    }
  }
}

bool TaskManager::submit_task(task_info t_info, Addr worker_id) {
  scoped_lock<recursive_mutex> tl(tasks_m_);

  auto & t_id = t_info.t_id;
  SPDLOG_DEBUG("Submitting partition {:d}:{:d}", t_id.stage_id, t_id.partition_id);

  Task & task = task_id_to_task(t_id);
  
  SPDLOG_DEBUG("Constructing task");
  
  dsm::WeldTask submit_task;
  
  submit_task.set_data((uintptr_t)(task.data_addr));
  submit_task.set_code(task.code);
  
  submit_task.set_stage_id(task.id.stage_id);
  submit_task.set_partition_id(task.id.partition_id);

  submit_task.set_return_vectors(t_info.return_vectors);
  
  Stage & s = stage_id_to_stage(t_id.stage_id);
  s.register_start(worker_id, t_id.partition_id, t_info);

  auto task_heap = heap_allocator_.task_page_region(task.id.partition_id, task.id.stage_id);
  auto start_addr = PGNUM_TO_PGADDR(PageUtils::pgidx_to_absolute_pgnum(task_heap.first));
  auto end_addr = PGNUM_TO_PGADDR(PageUtils::pgidx_to_absolute_pgnum(task_heap.second));

  SPDLOG_DEBUG("Heap: {:x} {:x}", start_addr, end_addr);

  submit_task.set_heap_start(start_addr);
  submit_task.set_heap_end(end_addr);
  
  {
    shared_lock cache_servers_lock(cache_servers_mutex_);
    SPDLOG_DEBUG("Cache server size: {:d}", cache_servers_.size());
    for (const auto &cache_server : cache_servers_) {
      *submit_task.add_cache_servers() = cache_server; 
    }
  }

  SPDLOG_DEBUG("Got task");

  grpc::ClientContext context;

  RPCInfo rpc_info;
  rpc_info.task_status = dsm::TaskStatus::RUNNING;
  rpc_info.tag = (void*)(task.id.stage_id);
  
  SPDLOG_DEBUG("Contacting worker {:s}", worker_id.str());
  unique_ptr<ClientAsyncResponseReader<dsm::Empty> > rpc
    ((servers_.at(worker_id).server_stub_)->PrepareAsyncRunProgram(&context, submit_task, &(cq_)));
  
  /* StartCall initiates the RPC call. Don't wait for the response. */
  rpc->StartCall();
  rpc->Finish(&rpc_info.reply, &rpc_info.ret_status, &rpc_info.tag);

  SPDLOG_DEBUG("RPC constructed");

  cluster_manager_.mark_running(worker_id, task.id);

  SPDLOG_DEBUG("inserted running worker");
  running_tasks_.insert(task.id);

  SPDLOG_DEBUG("updated partitions");

  return true;
}

// Let manager know that a Weld worker is up.
Status TaskManager::RegisterWorker(grpc::ServerContext* context,
				   const dsm::Client* client,
				   dsm::Empty* empty) {
  string ip = client->ip();
  string port = client->port();
  Addr name;
  name.ip = ip;
  name.port = stoi(port);

  SPDLOG_DEBUG("Registering worker: {:s}", name.str());

  {
    scoped_lock<mutex> w_lk(workers_m_);
    auto chan = servers_.find(name);
    if ( chan == servers_.end() ) {
      /* create reverse channel to worker cache server for cache invalidation */
      SPDLOG_DEBUG("Creating channel...");
      WorkerClient c_client(grpc::CreateChannel(name.str(),
						grpc::InsecureChannelCredentials()), name);
      servers_.emplace(name, move(c_client));
      servers_vec_.push_back(name);
    }

    if ( not client->is_driver() ) {
      cluster_manager_.register_worker(name);
      SPDLOG_DEBUG("New worker available.");
    }
  }

  return Status::OK;
}

Status TaskManager::MapS3(grpc::ServerContext* context,
			  const dsm::S3Data* request,
			  dsm::Empty* response) {
  uintptr_t start_addr = request->start(); /* Assuming correctly mapped within client heap */
  size_t data_size = request->size();
  size_t num_pages = data_size / PG_SIZE;
  SPDLOG_DEBUG("Data size {:d}, num pages {:d}, start addr {:x}", data_size, num_pages, start_addr);
  if ( data_size % PG_SIZE != 0 ) num_pages++; /* allocate an extra page for remainder */

  uintptr_t pgnum = PGADDR_TO_PGNUM(start_addr); 
  uintptr_t start_idx = PageUtils::absolute_pgnum_to_pgidx(pgnum);
  SPDLOG_DEBUG("Mapping address {:x}", start_addr);
  
  uintptr_t byte_offset = start_addr - PGNUM_TO_PGADDR(pgnum); /* is address in the middle of a page? */
  if ( byte_offset > 0 ) {
    SPDLOG_DEBUG("Requested S3 page not page-aligned: {:x}, {:x}", start_addr, PGNUM_TO_PGADDR(pgnum));
    return Status(StatusCode::INVALID_ARGUMENT, "S3 pages must be page-aligned");
  }

  Addr client_addr = GRPCUtil::extract_client_addr(context);

  uint64_t cur_byte = 0;
  for ( uintptr_t i = 0; i < num_pages; i++ ) {
    SPDLOG_DEBUG("get page relative in S3 for {:x}", i + start_idx);
    auto & page = pagetable_.get_page_relative(i+start_idx);
    SPDLOG_DEBUG("(done) get page relative for {:x}", i + start_idx);
    
    if ( page.perm() == dsm::Permission::S3 ) { // already mapped by someone else!
      // TODO revert all other pages
      SPDLOG_DEBUG("Could not map S3 page {:x}: requested by {:s}",
		   pgnum + i, client_addr.str());
      
      return Status::CANCELLED;
    }

    //page.lock();
    /* Driver shouldn't write anything here 
     * and shouldn't be holding page lock */
    if ( page.has_writer(client_addr) ) {
      WriteLock wlock(page.lock_m_);
      dsm::Page retpage;
      Status invalidate_success = invalidator_.
        invalidate(i + start_idx,
                   client_addr, retpage, true);
      assert ( invalidate_success.ok() ); // driver never goes down so we'll speculatively assume this doesn't fail
      page.remove_writer(Util::vec_idx(client_addr, &servers_vec_));
    }

    pagetable_.map_page_S3(i+start_idx, request->url(), cur_byte,
                           std::min(data_size-1, cur_byte + PG_SIZE), request->local());

    cur_byte += PG_SIZE;

    SPDLOG_DEBUG("{:d} {:d} {:d}", i+start_idx, page.byte_start(), page.byte_end());

    //page.unlock();
  }

  return Status::OK;
}

void TaskManager::construct_response(uint64_t pgidx, dsm::Page* response) {
  SPDLOG_DEBUG("get page relative for {:x}", pgidx);
  auto & page = pagetable_.get_page_relative(pgidx);
  SPDLOG_DEBUG("(done) get page relative for {:x}", pgidx);

  response->mutable_pagenum()->set_pagenum(PageUtils::pgidx_to_absolute_pgnum(pgidx));
  page.construct_response(response);
}

/* Called when a partition is completed. 
 * TODO: move to page.cc */
Status TaskManager::mark_read_only(uint64_t pgidx, Addr client_id,
				   int64_t stage_id, int64_t partition_id) {
  SPDLOG_DEBUG("get page relative for {:x}", pgidx);
  auto & page = pagetable_.get_page_relative(pgidx);
  SPDLOG_DEBUG("(done) get page relative for {:x}", pgidx);
  WriteLock wlock(page.lock_m_);
  //page.lock();
  
  Status status = Status::OK;

  task_id t_id(stage_id, partition_id);
  
  switch(page.perm()) {
  case dsm::Permission::S3:
    status = Status(StatusCode::PERMISSION_DENIED, "Tried to mark S3 as read only");
    break;
  case dsm::Permission::READ:
    // TODO this could end up happening if server writes a page on driver, then someone requests read,
    // then server tries to mark read-only
    status = Status(StatusCode::PERMISSION_DENIED, "Tried to mark read-locked page as read-only");
    break;
  case dsm::Permission::WRITE: {
    /* First check that this client is a valid writer for this page. */
    if ( not page.has_writer(client_id) ) {
      status = Status(StatusCode::PERMISSION_DENIED, "Client is not current writer");
      break;
    }

    if ( page.in_driver() ) {
      // We might have given out local write access to this page -- ignore it
      SPDLOG_DEBUG("Page {:x} in driver, not adding to read-only");
      status = Status::OK;
      break;
    }
    
    SPDLOG_DEBUG("Marking page {:x} as read-only, client {}", PageUtils::pgidx_to_absolute_pgnum(pgidx), client_id.str());
    
    /* We assume this request is only sent once the client is done writing. 
     * Also, only the "winner" is marked as the owner. Invalidate all others. */
    page.set_perm(dsm::Permission::READ_ONLY);
    if ( (page.id() != task_id::UNASSIGNED_ID) and (t_id != page.id()) ) {
      // can't assign to a new ID!
      status = Status(StatusCode::PERMISSION_DENIED, "Client tried to overwrite read-only page with new task");
      break;
    }

    page.set_id(stage_id, partition_id);
    page.set_written();

    /* Invalidate but don't actually retrieve the data yet. */
    dsm::Page retpage;
    status = page.invalidate_writers(retpage, client_id, /*clear_cache=*/false, /*send_data=*/false);

    page.add_reader(Util::vec_idx(client_id, &servers_vec_)); // this client should have the page read-protected at this point
    SPDLOG_DEBUG("Inserted client {:s} for page {:x}, readers size now {:d}",
		 client_id.str(), PageUtils::pgidx_to_absolute_pgnum(pgidx), page.num_readers());
    
    break;
  }
  case dsm::Permission::NONE:
    status = Status(StatusCode::PERMISSION_DENIED, "Tried to mark 'none' permission as read only");
    break;
  case dsm::Permission::READ_ONLY:
    {
      /* TODO: do nothing? */
      break;
    }
  }

  //page.unlock();
  return status;
}

// Call with a lock on the page
Status TaskManager::request_read(uint64_t pgidx, Addr client_id,
				 int64_t stage_id, int64_t partition_id,
				 dsm::Page & retpage) {
  SPDLOG_DEBUG("get page relative for {:x}", pgidx);
  auto & page = pagetable_.get_page_relative(pgidx);
  SPDLOG_DEBUG("(done) get page relative for {:x}", pgidx);
  page.update_cache_locs();
  task_id t_id(stage_id, partition_id);
  
  SPDLOG_DEBUG("Serving read request from {:s} for {:x}, idx {:x}, current perm {:d}",
	       client_id.str(), PageUtils::pgidx_to_absolute_pgnum(pgidx),
               PageUtils::pgidx_to_absolute_pgnum(page.pgidx()), page.perm());

  /********************* new code ************************/
  /*if (page.perm() == dsm::Permission::READ_ONLY &&
      client_id.ip == driver_addr_.ip && client_id.port == driver_addr_.port) {
    SPDLOG_DEBUG("Fetch readonly page index {:d} from driver", pgidx);
    void *addr = (char*)ro_cache + PG_SIZE * (pgidx - DRIVER_PAGES);
    char pagedata[PG_SIZE];
    memcpy(pagedata, addr, PG_SIZE);
    retpage.set_pagedata(pagedata, PG_SIZE);
    return Status::OK;
    }*/
  /********************* new code ************************/

  return page.request_read(client_id, t_id, retpage);
}

// Call with a lock on the page
Status TaskManager::request_write(uint64_t pgidx, Addr client_id,
				  int64_t stage_id, int64_t partition_id,
				  dsm::Page & retpage) {
  PTEntry & page = pagetable_.get_page_relative(pgidx);
  page.update_cache_locs();
  task_id t_id(stage_id, partition_id);

  SPDLOG_DEBUG("Serving write request from {:s} for {:x}, idx {:x}, current perm {:d}",
	       client_id.str(), PageUtils::pgidx_to_absolute_pgnum(pgidx),
               PageUtils::pgidx_to_absolute_pgnum(page.pgidx()), page.perm());

  return page.request_write(client_id, t_id, retpage);
}

/* Return all the pages written in the previous stage. */
Status TaskManager::GetResults(ServerContext* context,
			       const dsm::Empty* request,
			       dsm::ResultList* response) {
  SPDLOG_DEBUG("In getresults"); 
  Stage & last_stage = stages_.at(stages_.size() - 1);
  for ( auto & t_pair : last_stage.tasks ) {
    Task & t = t_pair.second;
    SPDLOG_DEBUG("Task id {:d}", t.id.partition_id);
    dsm::Pages* t_pages = response->add_pages();
    t_pages->set_stage_id(t.id.stage_id);
    t_pages->set_task_id(t.id.partition_id);
    std::sort(t.pages_written.begin(), t.pages_written.end());
    uint64_t start_addr = PageUtils::pgidx_to_addr
      (PageUtils::absolute_pgnum_to_pgidx(t.pages_written.at(0)));
    t_pages->set_start_addr(start_addr);
    SPDLOG_DEBUG("Start addr {:x}", start_addr);
    for ( uint64_t pgnum : t.pages_written ) {
      // get the pages.
      SPDLOG_DEBUG("Getting page for {:x}", pgnum);
      auto & pg = pagetable_.get_page_absolute(pgnum);
      dsm::Page* retpage = t_pages->add_pages();
      Status status = pg.request_read(driver_addr_, task_id::UNASSIGNED_ID, *retpage);
      if ( !status.ok() ) {
	SPDLOG_DEBUG("Failed to request read from manager: {:d}", status.error_code());
	return status;
      }

      pg.clear_pagedata();
    }
  }

  return Status::OK;
}

Status TaskManager::GetResultPages(ServerContext* context,
				   const dsm::ResultRequest* request,
				   dsm::Pages* response) {
  SPDLOG_DEBUG("In getresults"); 
  Stage & last_stage = stages_.at(stages_.size() - 1);
  auto id = request->task_id();
  Task & t = last_stage.tasks.at(id);
  SPDLOG_DEBUG("Task id {:d}", t.id.partition_id);
  response->set_stage_id(t.id.stage_id);
  response->set_task_id(t.id.partition_id);
  std::sort(t.pages_written.begin(), t.pages_written.end());
  uint64_t start_addr = PageUtils::pgidx_to_addr
    (PageUtils::absolute_pgnum_to_pgidx(t.pages_written.at(0)));
  response->set_start_addr(start_addr);
  SPDLOG_DEBUG("Start addr {:x}", start_addr);
  for ( uint64_t pgnum : t.pages_written ) {
    // get the pages.
    SPDLOG_DEBUG("Getting page for {:x}", pgnum);
    auto & pg = pagetable_.get_page_absolute(pgnum);
    dsm::Page* retpage = response->add_pages();
    Status status = pg.request_read(driver_addr_, task_id::UNASSIGNED_ID, *retpage);
    if ( !status.ok() ) {
      SPDLOG_DEBUG("Failed to request read from manager: {:d}", status.error_code());
      return status;
    }

    pg.cached_pagedata = "";
    pg.clear_pagedata();
    //pg.cached_pagedata = nullopt; /* clear cache so we don't keep this around! */
    SPDLOG_DEBUG("Freed page {:x}", pgnum);
  }

  return Status::OK;
}

Status TaskManager::GetPage(ServerContext* context,
			    const dsm::Page* request,
			    dsm::Page* response) {

  SPDLOG_DEBUG("In getpage {:x}", request->pagenum().pagenum());
  steady_clock::time_point begin = steady_clock::now();

  dsm::Permission requested_perm = request->perm();
  uint64_t pgnum = request->pagenum().pagenum();
  uint64_t pgidx = PageUtils::absolute_pgnum_to_pgidx(pgnum);
  assert ( pgidx < NUM_PAGES );

  //pagetable_.start_timer(pgidx);
  
  SPDLOG_DEBUG("Extracting client");
  Addr client_addr;
  try {
    client_addr = GRPCUtil::extract_client_addr(context);
  } catch (exception & e) {
    // could not find client
    return Status(StatusCode::UNAUTHENTICATED, "");
  }
  SPDLOG_DEBUG("Got client id: {:s}", client_addr.str());
  
  dsm::Page retpage;
  Status status;
  if ( requested_perm == dsm::Permission::READ ) {
    status = request_read(pgidx, client_addr,
			  request->stage_id(),
			  request->partition_id(),
			  retpage);
    num_reads(1);
  } else if ( requested_perm == dsm::Permission::WRITE ) {
    status = request_write(pgidx, client_addr,
			   request->stage_id(),
			   request->partition_id(),
			   retpage);
    num_writes(1);
  } else {
    // error
    SPDLOG_DEBUG("Invalid permission {:s}",
		 dsm::Permission_descriptor()->FindValueByNumber(requested_perm)->name());
    return Status(StatusCode::INVALID_ARGUMENT, "Invalid permission requested: " +
		  dsm::Permission_descriptor()->FindValueByNumber(requested_perm)->name());
  }
  SPDLOG_DEBUG("Completed request for {:x} for worker {:s}", pgnum, client_addr.str());

  if ( (retpage.pagedata().size() > 0) or retpage.ptop() ) { // not an s3 request
    SPDLOG_DEBUG("Result size: {:d}", retpage.pagedata().size());
    //num_page_downloads(1);
  }
  response->set_pagedata(retpage.pagedata());
  if ( retpage.ptop() ) {
    response->set_ptop(true);
    response->set_url(retpage.url());
    response->set_port(retpage.port());
  }
  construct_response(pgidx, response);

  steady_clock::time_point end = steady_clock::now();
  duration<double> secs = end - begin;
  invalidate_times(secs.count());

  //pagetable_.end_timer(pgidx);

  return status;
}

/* Schedule a task and block on successful completion. */
Status TaskManager::SubmitTask(ServerContext* Context,
			       const dsm::Task* task,
			       dsm::Result* result) {
  SPDLOG_DEBUG("Entering submit...");
  
  mutex m;
  condition_variable cv;

  unique_ptr<NewStageEvent> e = make_unique<NewStageEvent>(task, result, &cv, &m);

  dsm::PerfTimes times = task->times();
  SPDLOG_INFO("Driver reads between stages: {:d}, driver writes: {:d}, driver request: {:d}, peer request: {:d}, compute time: {:d}",
	      sum(num_reads), sum(num_writes), times.request_time_driver(), times.request_time_peer(), times.compute_time());
  
  num_reads = {};
  num_writes = {};

  {
    /* put new task event on queue */
    scoped_lock<recursive_mutex> event_lk(event_m_);
    Util::push_queue<unique_ptr<Event>>(move(e), event_queue_);
  }

  /* notify scheduler */
  event_wake_cv_.notify_all();

  steady_clock::time_point begin = steady_clock::now();

  /* wait on job success */
  unique_lock<mutex> lk(m);
  cv.wait( lk, [result, task] { return result->results_list().size() == task->num_partitions(); } ); 
  
  steady_clock::time_point end = steady_clock::now();
  duration<double> secs = end - begin;

  SPDLOG_INFO("Time spent in stage {:d}: {:f}", stages_.size() - 1, secs.count());

  SPDLOG_INFO("Results list: {:d}, num partitions: {:d}",
	       result->results_list().size(), task->num_partitions());

  SPDLOG_INFO("Time spent in result process for stage {:d}: {:f}",
	       stages_.size()-1, sum(result_process_times));
  SPDLOG_INFO("Time spent in invalidate for stage {:d}: {:f}",
	       stages_.size()-1, mean(invalidate_times));
  SPDLOG_INFO("Number of read requests for stage {:d}: {:d}",
	       stages_.size()-1, sum(num_reads));
  SPDLOG_INFO("Number of write requests for stage {:d}: {:d}",
	       stages_.size()-1, sum(num_writes));

  //pagetable_.print_page_times();

  SPDLOG_INFO("Average worker times for stage {:d}: compile {:f}, request (driver) {:f}, request (peer) {:f}, compute {:f}",
	       stages_.size()-1, mean(avg_compile), mean(avg_request_driver), mean(avg_request_peer), mean(avg_compute));

  result_process_times = {};
  invalidate_times = {};
  num_reads = {};
  num_writes = {};
  avg_compile = {};
  avg_request_driver = {};
  avg_request_peer = {};
  avg_compute = {};
  SPDLOG_DEBUG("Exiting submit...");
  
  /* distributed task done. control returns to Weld */
  return Status::OK;
}

/* Process the result of a task computation. */
Status TaskManager::Return(ServerContext* context,
			   const dsm::Response* response,
			   dsm::Empty* empty) {
  SPDLOG_DEBUG("Entering return...");
  Addr client_id = GRPCUtil::extract_client_addr(context);

  unique_ptr<ResultReturnEvent> e = make_unique<ResultReturnEvent>(response, client_id);

  {
    /* put new task event on queue */
    SPDLOG_DEBUG("Acquiring event lock...");
    scoped_lock<recursive_mutex> event_lk(event_m_);
    Util::push_queue<unique_ptr<Event>>(move(e), event_queue_);
  }

  /* notify scheduler */
  event_wake_cv_.notify_all();

  SPDLOG_DEBUG("Exiting return...");
  /* let worker know we're processing response */
  return Status::OK;
}

Status TaskManager::Shutdown(ServerContext* context,
			     const dsm::Empty* request,
			     dsm::Empty* response) {
  for ( auto & cache_client : servers_ ) {
    cache_client.second.shutdown();
  }

  grpc::ClientContext c_context;
  
  dsm::Empty c_request;
  dsm::Empty c_response;
  for ( auto & worker : servers_ ) {
    (worker.second).shutdown();
  }

  shutdown.set_value();
  return Status::OK;
}

/* Received new job: dispatch it to the worker stubs. */
void TaskManager::NewStageEvent::process(TaskManager* t) {
  SPDLOG_DEBUG("Entering job process");
 
  t->initialize_cache_servers();
 
  scoped_lock<recursive_mutex> tl(t->tasks_m_);

  //smalloc_new_stage = 1;
  
  Stage stage((t->stages_).size(), result, cv, m, task.num_partitions());
  t->stages_.push_back(stage);

  // this needs to happen asynchronously
  for ( uint64_t i = 0; i < task.num_partitions(); i++ ) {
    void* addr = (void*)(task.data_addrs(i));
    SPDLOG_DEBUG("Address: {:p}", addr);

    task_id tid;
    Task part(stage.stage_id, i, addr, task.code());
    SPDLOG_DEBUG("New partition: {:s}", part.id.str());
    t->stage_id_to_stage(stage.stage_id).tasks.emplace(i, part);
    
    /* Don't directly enqueue; instead add to waiting tasks so that we can 
     * check that possible dependencies are present first. */
    t->waiting_tasks_.insert(task_info(part.id, false, false, task.return_vectors()));
  }

  SPDLOG_DEBUG("Exiting job process");
}

void TaskManager::update_readers(Addr failed_worker_id) {
  scoped_lock<recursive_mutex> tl(tasks_m_);

  unordered_set<task_id> waiting_ids; // just needed for check in the next loop (TODO remove later)
  for ( auto wi : waiting_tasks_ ) {
    waiting_ids.insert(wi.t_id);
  }

  for ( auto & s : stages_ ) {
    for ( auto & p : s.tasks ) {
      for ( uint64_t pgnum : p.second.pages_written ) {
	// remove this worker
	auto & page = pagetable_.get_page_absolute(pgnum);
	WriteLock wlock(page.lock_m_);
	//page.lock();
	
	SPDLOG_DEBUG("Previous size {:d}",
		     page.num_readers());
	page.remove_reader(Util::vec_idx(failed_worker_id, &servers_vec_));
	SPDLOG_DEBUG("Erased worker {:s} from page {:x}, now size is {:d}",
		     failed_worker_id.str(), pgnum, page.num_readers());
	
	if ( page.num_readers() == 0 ) {
	  // this page is no longer cached anywhere!
	  // mark the relevant partition for resubmission
	  if ( not Util::set_contains(page.id(), waiting_ids)
	       and not Util::set_contains(page.id(), running_tasks_) ) {
	    SPDLOG_DEBUG("No more readers for page {:x}, marking task {:s} for resubmit",
			 pgnum, page.id().str());
	    SPDLOG_DEBUG(">>> Insert");
	    failed_tasks_.insert(page.id());
	    Stage & s = stage_id_to_stage(page.id().stage_id);
	    s.mark_missing(page.id().partition_id); // make sure we register that this is missing
	  }
	}

	//page.unlock();
      }
    }
  }
}

/* Heartbeat returned UNAVAILABLE, worker is down.
 * Mark all relevant, non-cached pages as lost. */
void TaskManager::WorkerFailedEvent::process(TaskManager* t) {
  /* Figure out which tasks were cached on this worker. 
   * TODO: maybe this will be slow; we go through all stages, partitions, and pages. */
  SPDLOG_INFO("Worker {:s} failed!", worker_id.str());

  // we can assume everyone on this node will fail, so just make sure
  // not to use it for future backup tasks
  SPDLOG_DEBUG("Update failed nodes");

  scoped_lock<recursive_mutex> tl(t->tasks_m_);
  
  SPDLOG_DEBUG("Update readers");
  if ( worker_died ) {
    t->update_readers(worker_id);
  }
  
  SPDLOG_DEBUG("Check running partition");
  // If the worker was in the middle of running something, resubmit that, even if not dead
  optional<task_id> opt_t_id = t->cluster_manager_.get_worker_task(worker_id);
  if ( opt_t_id != nullopt ) {
    task_id t_id = opt_t_id.value();
    SPDLOG_DEBUG("Inserting failed partition {:d}:{:d}", t_id.stage_id, t_id.partition_id);
    SPDLOG_DEBUG(">>> Insert");
    t->failed_tasks_.insert(t_id);
    t->running_tasks_.erase(t_id);
    
    Stage & stage = t->stage_id_to_stage(t_id.stage_id);
    stage.register_failure(worker_id, t_id.partition_id);
  }

  // If the worker had any completed tasks on it for a stage that was not yet complete, resubmit those too
  for ( auto & stage : t->stages_ ) {
    if ( !stage.complete ) {
      auto tasks = stage.get_tasks_for_worker(worker_id);
      if ( tasks != nullopt ) {
	for ( uint64_t task : tasks.value() ) {
	  task_id t_id(stage.stage_id, task);
	  t->failed_tasks_.insert(t_id);
	}
      }

      stage.clear_completed_tasks(worker_id);
    }
  }
  
  if ( worker_died ) {
    t->cluster_manager_.mark_failed(worker_id);
  } else {
    t->cluster_manager_.mark_available(worker_id);
  }
  
  SPDLOG_DEBUG("Finished updating failed worker {:s}.", worker_id.str());
}

void TaskManager::get_missing_parents(uint64_t absolute_pgnum,
				      unordered_set<task_id>* missing_parents) {
  SPDLOG_DEBUG("Got pagenum {:x}", absolute_pgnum);
 
  auto & page = pagetable_.get_page_absolute(absolute_pgnum);
  
  SPDLOG_DEBUG("Getting missing parents for page {:x} ({:s})",
               absolute_pgnum, page.id().str());

  if ( page.in_driver() ) {
    // base case: written on driver
    SPDLOG_DEBUG("On driver, returning...");
    return;
  }
  
  Task & p = task_id_to_task(page.id());
  SPDLOG_DEBUG("Got partition {:s}", page.id().str());
    
  for ( auto parent_idx : p.pages_read ) {
    // recurse
    SPDLOG_DEBUG("Checking parent page {:x}", parent_idx);
    if ( not pagetable_.page_present(parent_idx) ) {
      auto & ppage = pagetable_.get_page_absolute(parent_idx);
      missing_parents->insert(ppage.id());
    }
      
    SPDLOG_DEBUG("Recurse...");
    get_missing_parents(parent_idx, missing_parents);
  }

  SPDLOG_DEBUG("done missing parents.");
}

/* Program returned result: update local status.
 * These are processed one at a time, so we don't need to lock the partition even if there are
 * multiple workers running the partition. */
void TaskManager::ResultReturnEvent::process(TaskManager* t) {
  SPDLOG_DEBUG("Entering result process with worker {:s}", worker_id.str());

  steady_clock::time_point begin = steady_clock::now();
  
  scoped_lock<recursive_mutex> tl(t->tasks_m_);

  SPDLOG_INFO("Stage id {:d}:{:d}, client {:s}, times: {:f} {:f} {:f} {:f}, pages written {:d}, pages read {:d}",
	       response.stage_id(),
	      worker_id.str(),
	       response.partition_id(),
	       response.times().compile_time(),
	       response.times().request_time_driver(),
	       response.times().request_time_peer(),
	       response.times().compute_time(),
	       response.pages_written().size(),
	       response.pages_read().size());
  
  t->avg_compile(response.times().compile_time());
  t->avg_request_driver(response.times().request_time_driver());
  t->avg_request_peer(response.times().request_time_peer());
  t->avg_compute(response.times().compute_time());
  
  Task & p = t->task_id_to_task(response.stage_id(),
				response.partition_id());

  Stage & stage = t->stage_id_to_stage(response.stage_id());
  SPDLOG_DEBUG("Stages size {:d}, partitions size {:d}, stage id {:d}, partition id {:d}, expected {:d}:{:d}; present partitions {:d}",
	       t->stages_.size(), stage.tasks.size(),
	       p.id.stage_id, p.id.partition_id, response.stage_id(), response.partition_id(),
	       stage.present_partitions());

  if ( stage.missing_partitions_.size() < 10 ) {
    string missing = "";
    for ( auto & x : stage.missing_partitions_ ) {
      missing += to_string(x) + " ";
    }
    SPDLOG_DEBUG("Missing partitions for stage {:d}: {:s}", response.stage_id(), missing);
  }
  
  switch(response.status()) {
  case dsm::TaskStatus::SUCCESS: {
    Stage & stage = t->stage_id_to_stage(response.stage_id());

    stage.register_completion(worker_id, response.partition_id(), end_timestamp);
    
    //p.complete = true;
    t->cluster_manager_.partition_completed(worker_id, response.partition_id());
    
    /* Write page region and dependencies. This should only need to happen once,
     * as future executions should deterministically access the same pages. */
    const auto & pages_written = response.pages_written();
    if (!p.written_once) {
      /* Mark partition as succeeded and write the result. */
      p.result = response.res();
      SPDLOG_DEBUG("Got result {:x} for {:d}:{:d}", p.result, response.stage_id(), response.partition_id());

      p.pages_written.insert(p.pages_written.begin(), pages_written.begin(), pages_written.end());

      const auto & pages_read = response.pages_read();
      for ( auto & pg : pages_read ) {
	if ( Util::vec_contains(pg, p.pages_written) ) continue;
	p.pages_read.push_back(pg);
      }
      
      // record partition dependencies
      for ( uint64_t pgnum : pages_read ) {
	auto & page = t->pagetable_.get_page_absolute(pgnum);
	WriteLock wlock(page.lock_m_);
	//page.lock();
	p.parent_partitions.insert(page.id());
	//page.unlock();
      }
      
      sort(p.pages_written.begin(), p.pages_written.end());
      if ( p.pages_written.size() > 0 ) {
	if ( !p.written_once ) {
	  auto last_pg_idx = PageUtils::absolute_pgnum_to_pgidx(*(p.pages_written.end() - 1));
	  SPDLOG_DEBUG("Last page for {:d}:{:d}: {:d}", p.id.stage_id, p.id.partition_id,
		       last_pg_idx);
	  t->heap_allocator_.end_task(p.id.partition_id, p.id.stage_id, last_pg_idx);
	}
      }
    
      p.written_once = true;
    }

    /********************* new code ************************/
    SPDLOG_DEBUG("Copy ro pages to cache: {:d}", response.written_pages_size());
    for (auto & written_pg : response.written_pages()) {
      uint64_t written_pgnum = written_pg.pagenum().pagenum();
      auto & page = t->pagetable_.get_page_absolute(written_pgnum);
      //page.cached_pagedata = optional<string>{ string(written_pg.pagedata().begin(),
      //						      written_pg.pagedata().end()) };
      page.cached_pagedata = written_pg.pagedata();
      //page.cached_pagedata = ""; // don't copy
    }
    /********************* new code ************************/

    // Also make sure to mark this worker as a reader for all read-only pages.
    for ( auto & p : pages_written ) {
      uint64_t pgidx = PageUtils::absolute_pgnum_to_pgidx(p);
      Status status = t->mark_read_only(pgidx, worker_id, response.stage_id(), response.partition_id());
      if ( not status.ok() ) {
	// TODO: Hmm, what do we do here?
	SPDLOG_DEBUG("Could not mark page {:x} read only: {:d}: {:s}",
		     p, status.error_code(), status.error_message());
	assert(false);
      }
    }

    // TODO what about replicas that are still running?
    t->running_tasks_.erase(task_id(response.stage_id(), response.partition_id()));

    //SPDLOG_DEBUG("Running partitions: {:d}", stage.running_partitions);
    
    if ( (stage.present_partitions() == stage.expected_results) && !stage.complete ) {
      /* done with stage! we can send back result now */
      {
	SPDLOG_DEBUG("Writing results...");
	stage.write_results();
      }
      
      SPDLOG_DEBUG("Results list: {:d}",
		   (stage.result)->results_list().size());

      /* wake up listener */
      SPDLOG_DEBUG("Notifying.");
      (stage.caller_cv)->notify_all();
    }
    
    t->cluster_manager_.mark_available(worker_id);
    
    break;
  }

  case dsm::TaskStatus::FETCH_FAIL: {
    /* Failed to fetch page. Mark page as missing, check ancestors,
       and put corresponding task back on queue. */
    SPDLOG_DEBUG("Entering fetch fail...");
    uint64_t failed_page = response.failed_page();
    
    mutex m;
    condition_variable cv;

    unique_ptr<WorkerFailedEvent> e = make_unique<WorkerFailedEvent>(worker_id, false, false);
    
    {
      /* put new task event on queue */
      scoped_lock<recursive_mutex> event_lk(t->event_m_);
      Util::push_queue<unique_ptr<Event>>(move(e), t->event_queue_);
    }

    /* notify scheduler */
    t->event_wake_cv_.notify_all();
    SPDLOG_DEBUG("Exiting fetch fail...");
    break;
  }

  case dsm::TaskStatus::RETRY: {
    /* Server told worker that the page is still being written,
     * and to try fetching it again later. Put the task back on the queue with a delay. */
    SPDLOG_DEBUG("Entering retry...");
    uint64_t failed_page = response.failed_page();
    
    mutex m;
    condition_variable cv;

    unique_ptr<WorkerFailedEvent> e = make_unique<WorkerFailedEvent>(worker_id, false, true);
    
    {
      /* put new task event on queue */
      scoped_lock<recursive_mutex> event_lk(t->event_m_);
      Util::push_queue<unique_ptr<Event>>(move(e), t->event_queue_);
    }

    /* notify scheduler */
    t->event_wake_cv_.notify_all();
    SPDLOG_DEBUG("Exiting retry...");
    break;
  }

  case dsm::TaskStatus::ABORT_REPLICA: {
    /* This worker was a replica we didn't need that aborted, so we can just throw it out. */
    t->cluster_manager_.mark_available(worker_id);
    break;
  }
  };

  steady_clock::time_point end = steady_clock::now();
  duration<double> secs = end - begin;
  t->result_process_times(secs.count());
  SPDLOG_DEBUG("Exiting result process");
}

void TaskManager::initialize_cache_servers() { 
  scoped_lock<mutex> workers_lock(workers_m_);
  unique_lock cache_servers_lock(cache_servers_mutex_);

  if (!cache_servers_.empty()) {
    return;
  }

  SPDLOG_DEBUG("Initialize cache servers...");

  int32_t partition = 0;
  for (const auto &server : servers_) {
    if (server.first == driver_addr_) continue;
    if (cluster_manager_.is_failed_addr(server.first)) continue; 

    dsm::CacheServer cache_server;
    cache_server.set_partition(partition++);
    cache_server.set_disabled(false);
    cache_server.set_ip(server.first.ip);
    cache_server.set_port(server.first.str_port());
    cache_servers_.push_back(std::move(cache_server));
  } 
}

void TaskManager::disable_cache_server(const Addr &addr) {
  unique_lock cache_servers_lock(cache_servers_mutex_);
  for_each(cache_servers_.begin(), cache_servers_.end(),
          [&addr](dsm::CacheServer &server) {
	     if (!server.disabled() && server.ip() == addr.ip &&
		 server.port() == addr.str_port()) {
	       server.set_disabled(true);
	     }
	   });
}
