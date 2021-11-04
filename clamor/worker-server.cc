#include "worker-server.h"

#include <sys/time.h>

#include <chrono>
#include <cstdlib>
#include <thread>

#include "debug.h"
#include "exceptions.h"
#include "fault-handler.h"
#include "mprotect.h"
#include "page-utils.h"
#include "task.h"
#include "weld-utils.h"

#include "util-templates.cc"

using grpc::Status;
using grpc::StatusCode;
using grpc::ServerContext;
using dsm::InvalidateRequest;
using dsm::Page;
using dsm::PageNum;

using namespace std;
using namespace std::chrono_literals;

constexpr auto SPEC_SLOWDOWN = 45000ms;

bool Worker::return_vectors = false;
bool Worker::cache_none = false;

Worker::Worker(Addr worker_addr,
	       Addr manager_addr,
	       bool is_driver,
	       bool slow,
	       bool reverse_channel) : // Can set to false to test one direction
  worker_addr_(worker_addr),
  manager_addr_(manager_addr),
  stages_run_(),
  slow_(slow),
  cache_server_(!is_driver)
{
    /* set up reverse channel to task manager */
    SPDLOG_DEBUG("Manager: {:s}", manager_addr_.str());

    /* The reverse channel is only used when running tasks and returning results, so we can
     * optionally not initialize it if we only want to test page cache state */
    if ( reverse_channel ) {
      /*uint32_t manager_port = manager_addr_.port;
      uint32_t offset = worker_addr_.port - 50000;
      uint32_t new_port = manager_port + offset;
      manager_addr_.port = new_port + offset;
      */
      client_.AddChannel(grpc::CreateChannel(manager_addr.str(),
                                             grpc::InsecureChannelCredentials()),
                         worker_addr); /* provide cache addr for reverse channel */
      
      client_.RegisterWorker(worker_addr, is_driver);
    }
  
    if ( is_driver ) {
      initialize_driver_heap();
    }
}

void protect_accessed_pages() {
  /* No need to erase these pages from cache yet, but protect-none
   * so that we still get faults and record page accesses in future tasks. 
   * TODO: make sure we also do this cleanup if we catch an exception. */
  auto cur_task = FaultHandler::fh.current_task;
  for ( uint64_t pgnum : FaultHandler::fh.write_requests_[cur_task] ) {
      FaultHandler::fh.lock_page(pgnum);
      protect_none(pgnum);
      FaultHandler::fh.unlock_page(pgnum);
  }

  for ( uint64_t pgnum : FaultHandler::fh.read_requests_[cur_task] ) {
      FaultHandler::fh.lock_page(pgnum);
      protect_none(pgnum);
      if ( Worker::cache_none ) {
	FaultHandler::fh.cache_erase(pgnum);
      }
      FaultHandler::fh.unlock_page(pgnum);
  }

  SPDLOG_DEBUG("Done unprotecting.");
}

Status Worker::InvalidatePage(ServerContext* context,
			      const InvalidateRequest* request,
			      Page* response) {
  /* stop writing to page - read protect */
  uintptr_t pgnum = request->pagenum().pagenum();
  SPDLOG_DEBUG("got-request | {:x} {:s}", pgnum);
  
  FaultHandler::fh.lock_page(pgnum);
  protect_read(pgnum);
 
  /* send back page */
  if ( request->send_data() ) {
    response->mutable_pagenum()->set_pagenum(pgnum);
    void* pgaddr = (void*)PGNUM_TO_PGADDR(pgnum);
    response->set_pagedata(pgaddr, PG_SIZE);
  }

  if ( request->is_peer() ) { num_peer_served(1); }

  Status status;
  if (request->clear_cache()) {
    /* no more reading or writing */
    protect_none(pgnum);
    
    /* update local permissions */
    FaultHandler::fh.cache_erase(pgnum);
    // if (cache_server_ && request->invalidate_driver_cache()) {
    //     SPDLOG_DEBUG("INVALIDATE CACHE........");
    //     status = PageCache::get_page_cache(&client_, worker_addr_).
    //                 cache_invalidate(pgnum);
    // }
  } else {
    /* no more writing */
    protect_read(pgnum);
  }

  FaultHandler::fh.unlock_page(pgnum);
  SPDLOG_DEBUG("got-request-done | {:x}", pgnum);
  
  return status;
}

void Worker::initialize_client() {
  /* initialize fault handler */
  if (!client_initialized_) {
    SPDLOG_DEBUG("add client");
    FaultHandler::fh.add_requester(&client_);
    FaultHandler::fh.set_is_driver(false);
      
    client_initialized_ = true;
  }
}

void Worker::initialize_driver_heap() {
  SPDLOG_DEBUG("Initializing driver with {} pages", DRIVER_PAGES);
  init_heap((void*)(membuf), (void*)(membuf) + (DRIVER_PAGES*PG_SIZE));
  initialize_client();
  FaultHandler::fh.set_is_driver(true);
}

void Worker::initialize_heap(uint64_t heap_start, uint64_t heap_end) {
  /* set up heap space */
  if ( !client_initialized_ ) {
    init_heap((void*)(heap_start), (void*)(heap_end));
    initialize_client();
  } else {
    restart_heap((void*)(heap_start), (void*)(heap_end));
  }
}

grpc::Status Worker::RunProgram(grpc::ServerContext* context,
				const dsm::WeldTask* task,
				dsm::Empty* empty) {
  task_id cur_task = task_id(task->stage_id(), task->partition_id());
  
  SPDLOG_DEBUG("Running code: {}", task->code());
  SPDLOG_DEBUG("Using data pointer: {:x}", task->data());

  std::pair<void*, perf_times> result;
  dsm::Response ret;
  ret.set_stage_id(task->stage_id());
  ret.set_partition_id(task->partition_id());

  num_peer_served = {};
  FaultHandler::fh.set_addr(worker_addr_);
  FaultHandler::fh.current_task = cur_task;
  SPDLOG_DEBUG("Running stage id: {:d}", task->stage_id());
  
  struct timeval tstart, tend, tdiff;

  if ( slow_ ) {
    // artificially slow the task down a bit to test speculation
    this_thread::sleep_for(chrono::milliseconds(SPEC_SLOWDOWN));
  }


  try {
    // weld program goes here
    // data should be a pointer to the struct of args
    SPDLOG_DEBUG("Start task: {:d}, {:d}", task->stage_id(), task->partition_id());
    initialize_heap(task->heap_start(), task->heap_end());

    gettimeofday(&tstart, 0);
    result = WeldUtils::run_query_weld(task->code(), (void*)(task->data()), false, 1);
    SPDLOG_DEBUG("Finish task: {:d}, {:d}", task->stage_id(), task->partition_id());
  } catch ( retry_error & e ) {
    /* If server told us a dependency is still being written, try again later */
    SPDLOG_DEBUG("Retry error");
    atexit(protect_accessed_pages); // Run after everyone else is done
    //exit(1);
    ret.set_status(dsm::TaskStatus::RETRY);
    ret.set_failed_page(e.pagenum());
    client_.ReturnResponse(ret);
    return grpc::Status::CANCELLED;
  } catch ( fetch_fail_error & e ) {
    /* Other retrieval failures indicate that a worker died */
    SPDLOG_DEBUG("Fetch fail error");
    atexit(protect_accessed_pages); // Run after everyone else is done
    //exit(1);
    ret.set_status(dsm::TaskStatus::FETCH_FAIL);
    ret.set_failed_page(e.pagenum());
    client_.ReturnResponse(ret);
    return grpc::Status::CANCELLED;
  } catch ( abort_replica_signal & e ) {
    SPDLOG_DEBUG("Abort replica error");
    //atexit(protect_accessed_pages); // Run after everyone else is done
    //exit(1);
    protect_accessed_pages();
    ret.set_status(dsm::TaskStatus::ABORT_REPLICA);
    client_.ReturnResponse(ret);
    return grpc::Status::CANCELLED;
  } catch ( exception & e ) {
    SPDLOG_DEBUG("Unknown exception {:s}", e.what());
    throw;
  }
  
  //SPDLOG_DEBUG("Result address: {:p}, result: {:d}", result, *(uint64_t*)(result));
  
  gettimeofday(&tend, 0);
  timersub(&tend, &tstart, &tdiff);

  /* Called asynchronously. Send result information back via reverse RPC. */
  ret.set_status(dsm::TaskStatus::SUCCESS);

  ret.set_res((uint64_t)(result.first));

  ret.mutable_times()->set_compile_time(result.second.compile);
  ret.mutable_times()->set_request_time_driver(result.second.request_driver);
  ret.mutable_times()->set_request_time_peer(result.second.request_peer);
  ret.mutable_times()->set_compute_time(result.second.compute);
  uint64_t num_served = boost::accumulators::sum(num_peer_served);
  ret.mutable_times()->set_num_peer_served(num_served);

  SPDLOG_INFO("{:f} {:f} {:f} {:f} {:d}", result.second.compile, result.second.request_driver,
	      result.second.request_peer, result.second.compute, num_served);
  
  auto stage_id = task->stage_id();

  /********************* new code ************************/
  SPDLOG_DEBUG("COPY ro pages..... {:d}", task->return_vectors());
  for (uint64_t written_pgnum : FaultHandler::fh.write_requests_[cur_task]) {
    Page * written_pg = ret.add_written_pages();
    written_pg->mutable_pagenum()->set_pagenum(written_pgnum);
    void* pgaddr = (void*)PGNUM_TO_PGADDR(written_pgnum);
    written_pg->set_pagedata(pgaddr, PG_SIZE);

    // Only copy the first page for now
    if ( not task->return_vectors() ) {
      break;
    };
  }
  SPDLOG_DEBUG("COPY ro pages finished.....");
  /********************* new code ************************/

  auto pgs_written = ret.mutable_pages_written();
  *pgs_written = {FaultHandler::fh.write_requests_[cur_task].begin(),
		  FaultHandler::fh.write_requests_[cur_task].end()};
  auto pgs_read = ret.mutable_pages_read();
  *pgs_read = {FaultHandler::fh.read_requests_[cur_task].begin(),
	       FaultHandler::fh.read_requests_[cur_task].end()};

  SPDLOG_INFO("Pages read: {:d}, pages written: {:d}",
	      FaultHandler::fh.read_requests_[cur_task].size(),
	      FaultHandler::fh.write_requests_[cur_task].size());
  
  atexit(protect_accessed_pages); // Run after everyone else is done
  
  bool success = client_.ReturnResponse(ret);
  if (!success) {
    SPDLOG_DEBUG("Failed to send response!");
    return grpc::Status::CANCELLED;
  }
  
  SPDLOG_DEBUG("return result!!!!!"); 
  return grpc::Status::OK;
}

Status Worker::Heartbeat(ServerContext* contect,
			 const dsm::Empty* empty,
			 dsm::Alive* response) {
  response->set_alive(true);

  return grpc::Status::OK;
}


Status Worker::FetchPage(ServerContext* context, const dsm::Page* request,
			 Page* response) {
  uint64_t pgnum = request->pagenum().pagenum();
  SPDLOG_DEBUG("Fetch page {:d} from cache...", pgnum);

  if (!PageUtils::page_in_driver(pgnum)) {
    SPDLOG_DEBUG("Fetch non-driver page from cache");
    return Status(StatusCode::INVALID_ARGUMENT,
                  "Fetch non-driver pages from cache is not supported.");
  }

  return Status::OK;
  
  //return PageCache::get_page_cache(&client_, worker_addr_)
  //           .lookup_page(context, request, response);
}
