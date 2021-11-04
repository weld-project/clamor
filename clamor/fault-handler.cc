#include "fault-handler.h"

#include "dsm-client.h"
#include "dsm.grpc.pb.h"

#include "debug.h"
#include "exceptions.h"
#include "lock.h"
#include "mprotect.h"
#include "page-utils.h"
#include "segfault.h"
#include "worker-client.h"

#include "util-templates.cc"

#include <fcntl.h>
#include <signal.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <fstream>
#include <iostream>

using namespace boost::accumulators;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace dsm;
using namespace std;
using google::protobuf::RepeatedPtrField;
using grpc::Status;
using grpc::StatusCode;

constexpr int MAX_NUM_RETRY = 3;

// Wait cache servers for at most 10 seconds before determing it fails.
constexpr int TIMEOUT_MS = 10 * 1000;

// Cache space is divided into chunks where each chunk contains
// a number of consecutive pages. Cache lookups for pages within a
// chunk go to the same cache server.
constexpr uint64_t NUM_PAGES_PER_CACHE_CHUNK = 1;

FaultHandler FaultHandler::fh;
struct sigaction FaultHandler::default_handler;

/* don't set up the handler yet - wait to initialize heap */
FaultHandler::FaultHandler() :
  handle_()
{
  /* set up mutexes for each page */
  for ( uint32_t i = 0; i < NUM_PAGES; i++ ) {
    Lock::init_default(&pgmutex_[i]);
  }

  Lock::init_default(&cache_m);
}

void FaultHandler::cache_insert(uint64_t pgnum) {
  SPDLOG_DEBUG("Inserting page {:x} into cache", pgnum);
  Lock::lock(&cache_m);
  cached_pages_.insert(pgnum);
  Lock::unlock(&cache_m);
  SPDLOG_DEBUG("Inserted page {:x} into cache", pgnum);
}

void FaultHandler::cache_erase(uint64_t pgnum) {
  SPDLOG_DEBUG("Removing page {:x} from cache", pgnum);
  Lock::lock(&cache_m);
  cached_pages_.erase(pgnum);
  Lock::unlock(&cache_m);
  SPDLOG_DEBUG("Removed page {:x} from cache", pgnum);
}

bool FaultHandler::cache_contains(uint64_t pgnum) {
  Lock::lock(&cache_m);
  bool result = (cached_pages_.find(pgnum) != cached_pages_.end());
  Lock::unlock(&cache_m);
  return result;
}

pair<double, double> FaultHandler::get_request_times() {
  double d_request_time = sum(driver_request_time);
  double p_request_time = sum(peer_request_time);
  SPDLOG_DEBUG("Request time: driver {:d} peer {:d}", d_request_time, p_request_time);
  return make_pair(d_request_time, p_request_time);
}

void FaultHandler::reset_request_time() {
  driver_request_time = {};
  peer_request_time = {};
}

bool FaultHandler::lock_page(uintptr_t pgnum) {
  SPDLOG_DEBUG("Acquiring lock for {:x}", pgnum);
  bool success = Lock::lock(&pgmutex_[pgnum - PGADDR_TO_PGNUM((uintptr_t)(&membuf))]);
  SPDLOG_DEBUG("Lock {:x}: {:d}", pgnum, success);
  return success;
}

bool FaultHandler::unlock_page(uintptr_t pgnum) {
  SPDLOG_DEBUG("Unlocking {:x}", pgnum);
  bool success = Lock::unlock(&pgmutex_[pgnum - PGADDR_TO_PGNUM((uintptr_t)(&membuf))]);
  SPDLOG_DEBUG("Unlock {:x}: {:d}", pgnum, success);
  return success;
}

void FaultHandler::set_sigaction(sigact_callback func) { /* sigact_callback is defined in segfault.h */
  struct sigaction intercept;
  
  intercept.sa_sigaction = func;
  sigemptyset(&intercept.sa_mask);
  intercept.sa_flags = SA_SIGINFO;
  if (sigaction(SIGSEGV, &intercept, &(FaultHandler::default_handler)) != 0) {
    fprintf(stderr, "sigaction failed\n");
  }
}

/* Unblock the signal if we're throwing an exception */
void FaultHandler::unblock() {
  sigset_t x;
  sigemptyset (&x);
  sigaddset(&x, SIGSEGV);
  sigprocmask(SIG_UNBLOCK, &x, NULL);
}

/* add client and set up page fault handler */
void FaultHandler::add_requester(DSMClient* requester)
{
  requester_ = requester;

  /* register page fault handler */
  set_sigaction(fault_handler);

  /* mmap and mprotect the common memory buffer */
  uintptr_t addr = map_aligned(&membuf, NUM_PAGES * PG_SIZE);
  if ( addr == 0 ) {
    cout << "Could not mmap memory!" << endl;
    exit(1);
  }
}

void FaultHandler::set_is_driver(bool is_driver) {
  is_driver_ = is_driver;
}

void FaultHandler::set_addr(Addr addr) {
  self_addr_ = addr;
}

bool FaultHandler::is_shared(uintptr_t addr) {
  return ( (addr >= (uintptr_t)(&membuf)) &&
           (addr < (uintptr_t)(&membuf) + (NUM_PAGES*PG_SIZE)) );
}

void fault_handler(int sig, siginfo_t* info, void* v_ctx) {
  SPDLOG_DEBUG("Entering fault handler");
  if ( sig != SIGSEGV ) {
    /* Not a segfault -- fall back to default */
    (FaultHandler::default_handler.sa_handler)(sig);
  }

  if ( not FaultHandler::is_shared((uintptr_t)(info->si_addr)) ) {
    /* Segfault not in shared buffer -- fall back to default */
    SPDLOG_DEBUG("Address {:x} not in shared buffer", info->si_addr);
    (FaultHandler::default_handler.sa_handler)(sig);
  }

  // Dispatch fault to a read or write handler.
  uintptr_t pgaddr = (uintptr_t) info->si_addr;
  if ( is_write_fault(v_ctx) ) {
    bool success = FaultHandler::fh.write_handler(pgaddr);
    if ( not success ) {
      SPDLOG_DEBUG("Failed to get write permission");
      /* fall back to default */
      FaultHandler::default_handler.sa_handler(sig);
    }
  } else {
    bool success = FaultHandler::fh.read_handler(pgaddr);
    if ( not success ) {
      SPDLOG_DEBUG("Failed to get read permission");
      /* fall back to default */
      FaultHandler::default_handler.sa_handler(sig);
    }
  }

  SPDLOG_DEBUG("Exiting fault handler");
}

int FaultHandler::read_to_addr(void* addr, int64_t start_byte, int64_t end_byte, string fname) {
  ifstream is(fname, ios::binary);
  if (is) {
    is.seekg(start_byte);
    is.read((char*)addr, end_byte-start_byte);
    is.close();
  }
}

int FaultHandler::download_to_addr(void* addr, int64_t start_byte, int64_t end_byte, string url) {
  MemoryStruct chunk;
  SPDLOG_DEBUG("Downloading to address {:x}", addr);
  handle_.download_data(&chunk, start_byte, end_byte, url);
  int ret = memcpy(addr, chunk.memory, min(chunk.size, (size_t)PG_SIZE)) == NULL;
  free(chunk.memory);
  return ret;
}

void FaultHandler::download_to_chunk(MemoryStruct* chunk, int64_t start_byte, int64_t end_byte, string url) {
  SPDLOG_DEBUG("Downloading to allocated chunk");
  handle_.download_data(chunk, start_byte, end_byte, url);
}

/* Assumes the page is already locked */
bool FaultHandler::read_helper(uint64_t pgnum, dsm::Page & retpage) {
  bool success = true;

  if ( !protect_write(pgnum) ) {  // temporarily set to write
    SPDLOG_DEBUG("Couldn't protect write: {:x}", pgnum);
    return false;
  }

  if ( cache_contains(pgnum) or (is_driver_ and page_in_driver(pgnum)) ) {
    // don't bother with memcpy
    SPDLOG_DEBUG("Already cached page {:x}", pgnum);
    if ( !protect_read(pgnum) ) {
      SPDLOG_DEBUG("Couldn't protect read: {:x}", pgnum);
      return false;
    }
    
    return true;
  }
  
  uintptr_t addr = PGNUM_TO_PGADDR(pgnum);
  if ( retpage.url() != "" ) {
    /* download from cloud */
    int ret;
    if ( retpage.local() ) {
      ret = read_to_addr((void*)addr,
			 retpage.byte_start(),
			 retpage.byte_end(),
			 retpage.url());
      
    } else {
      ret = download_to_addr((void*)addr,
			     retpage.byte_start(),
			     retpage.byte_end(),
			     retpage.url());
    }
    
    if ( ret < 0 ) {
      return false;
    }
  } else {
    /* copy page data from response */
    SPDLOG_DEBUG("Copying page data: {:x}", pgnum);
    int ret = memcpy((void*)addr,
		     retpage.pagedata().data(),
		     retpage.pagedata().size()) == NULL;
    if ( ret < 0 ) {
      SPDLOG_DEBUG("Couldn't copy page data: {:x}", pgnum);
      return false;
    }
  }
  
  if ( !protect_read(pgnum) ) {
    SPDLOG_DEBUG("Couldn't protect read: {:x}", pgnum);
    return false;
  }

  return true;
}

bool FaultHandler::page_in_driver(uint64_t pgnum) {
  return PageUtils::page_in_driver(pgnum);
}

namespace {

void process_error_status(const Status &status, uint64_t pgnum, bool is_driver) {
  if (status.error_message() == "RETRY" && !is_driver) {
    throw retry_error(pgnum);
  } else if (status.error_message() == "PAGE_LOST" && !is_driver) {
    throw fetch_fail_error(pgnum);
  } else if (status.error_message() == "ABORT_REPLICA") {
    throw abort_replica_signal();
  }
}

}  // namespace

/* Send page request to DSM manager with "read" permission */
bool FaultHandler::read_handler(uintptr_t pgaddr) {
  uintptr_t pgnum = PGADDR_TO_PGNUM(pgaddr);

  SPDLOG_DEBUG("In read handler with page {:x}, address {:x}, task id {:s}", pgnum, pgaddr, current_task.str());
  read_requests_[current_task].push_back(pgnum);

  if ( cache_contains(pgnum) ) {
    // don't bother making request?
    SPDLOG_DEBUG("Already cached page {:x}", pgnum);
    if ( !protect_read(pgnum) ) {
      SPDLOG_DEBUG("Couldn't protect read: {:x}", pgnum);
      unlock_page(pgnum);
      return false;
    }

    unlock_page(pgnum);
    return true;
  }

  dsm::Page reqpage;
  reqpage.set_perm(dsm::Permission::READ);
  reqpage.set_partition_id(current_task.partition_id);
  reqpage.set_stage_id(current_task.stage_id);
  reqpage.mutable_pagenum()->set_pagenum(pgnum);

  dsm::Page retpage;
  lock_page(pgnum); 
  
  try {
    SPDLOG_DEBUG("driver-fetch");
    steady_clock::time_point begin = steady_clock::now();
    Status ret = requester_->GetPage(reqpage, &retpage);
    steady_clock::time_point end = steady_clock::now();
    duration<double> secs = end - begin;
    double fetch_time = secs.count();
    driver_request_time(fetch_time);

    if ( retpage.ptop() ) {
      SPDLOG_DEBUG("peer-assigned | {:s} {:d}", retpage.url(), retpage.ptop());
    } else {
      SPDLOG_DEBUG("driver-fetch-done");
    }
    
    if (!ret.ok()) {
      process_error_status(ret, pgnum, is_driver_);
      SPDLOG_DEBUG("Read handler failed: {:s}", ret.error_message());
      unlock_page(pgnum);
      return false; /* failed to get page */
    } else if ( retpage.url() != "" and retpage.ptop() ) {
      // redirect page request to peer
      SPDLOG_DEBUG("peer-fetch | {:x} {:s}:{:d}", pgnum, retpage.url(), retpage.port());
      shared_ptr<WorkerClient> client = add_or_get_peer(Addr(retpage.url(), retpage.port()));

      retpage.Clear();

      begin = steady_clock::now();
      ret = client->invalidate_page(pgnum, /*clear_cache=*/false, /*send_data=*/true, &retpage, false, /*is_peer=*/true);
      end = steady_clock::now();
      secs = end - begin;
      fetch_time = secs.count();
      peer_request_time(fetch_time);

      if ( !ret.ok() ) {
	// TODO: Error fetching from peer, request new peer from manager...
	process_error_status(ret, pgnum, is_driver_);
	SPDLOG_DEBUG("Read handler failed (peer fetch): {:s}", ret.error_message());
	unlock_page(pgnum);
	return false; /* failed to get page */
      }
      
      SPDLOG_DEBUG("peer-fetch-done | {:x} {:s}:{:d}", pgnum, retpage.url(), retpage.port());
    }
  } catch (...) {
    // Rethrow, but make sure to unlock first
    unlock_page(pgnum);
    unblock();
    throw;
  }
  

  bool success = read_helper(pgnum, retpage);
  SPDLOG_DEBUG("Read helper returned: {:d}", success);

  cache_insert(pgnum);

  unlock_page(pgnum);
  return success;
}

bool FaultHandler::write_helper(uint64_t pgnum, dsm::Page & retpage) {
  if ( !protect_write(pgnum) ) {
    return false;
  }

  if ( cache_contains(pgnum) or (is_driver_ and page_in_driver(pgnum)) ) {
    // don't bother with memcpy
    SPDLOG_DEBUG("Already cached page {:x}", pgnum);
    return true;
  }
  
  uintptr_t addr = PGNUM_TO_PGADDR(pgnum);
  int ret = memcpy((void*)addr, retpage.pagedata().data(), retpage.pagedata().size()) == NULL;
  if ( ret < 0 ) {
    SPDLOG_DEBUG("Couldn't copy page data: {:x}", pgnum);
    return false;
  }

  return true;
}

/* Send page request to DSM manager with "write" permission */
bool FaultHandler::write_handler(uintptr_t pgaddr) {
  uintptr_t pgnum = PGADDR_TO_PGNUM(pgaddr);
  SPDLOG_DEBUG("In write handler with page {:x}, address {:x}, task id {:s}", pgnum, pgaddr, current_task.str());
  uintptr_t addr = PGNUM_TO_PGADDR(pgnum);
  
  write_requests_[current_task].push_back(pgnum);

  dsm::Page reqpage;
  reqpage.set_perm(dsm::Permission::WRITE);
  reqpage.set_partition_id(current_task.partition_id);
  reqpage.set_stage_id(current_task.stage_id);
  reqpage.mutable_pagenum()->set_pagenum(pgnum);
  
  dsm::Page retpage;
  lock_page(pgnum);
  try {
    steady_clock::time_point begin = steady_clock::now();
    Status ret = requester_->GetPage(reqpage, &retpage);
    steady_clock::time_point end = steady_clock::now();
    duration<double> secs = end - begin;
    double fetch_time = secs.count();
    driver_request_time(fetch_time);

    if (!ret.ok()) {
      process_error_status(ret, pgnum, is_driver_);
      SPDLOG_DEBUG("Write handler failed: {:s}", ret.error_message());
      unlock_page(pgnum);
      return false; /* failed to get page */
    }
  } catch (...) {
    // Rethrow, but make sure to unlock first
    unlock_page(pgnum);
    unblock();
    throw;
  }

  bool success = write_helper(pgnum, retpage);
  SPDLOG_DEBUG("Write helper returned: {:d}", success);

  cache_insert(pgnum);
  
  unlock_page(pgnum);
  return success;
}

shared_ptr<WorkerClient> FaultHandler::add_or_get_peer(const Addr & client_addr) {
  unique_lock worker_clients_lock(worker_clients_mutex_);

  if ( Util::map_contains(client_addr, worker_clients_) ) {
    // we have a server connection already.
    return worker_clients_.at(client_addr);
  } else {
    // initialize server connection
    worker_clients_.emplace(client_addr,
			    make_shared<WorkerClient>
			    (grpc::CreateChannel(client_addr.str(),
						 grpc::InsecureChannelCredentials()), self_addr_));
  }

  client_addrs_.push_back(client_addr);
  return worker_clients_.at(client_addr);
}
