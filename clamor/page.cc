#include "page.h"

#include "exceptions.h"
#include "page-utils.h"
#include "worker-server.h"

#include "util-templates.cc"

#include <iostream>
#include <random>
#include <utility>

using namespace grpc;
using namespace std;

PTEntry::PTEntry(uint64_t pgidx, ClusterManager* cluster_manager, Invalidator* invalidator, vector<Addr>* servers_vec)
  : pgidx_(pgidx),
    cluster_manager_(cluster_manager),
    invalidator_(invalidator),
    servers_vec_(servers_vec)
{
  //Lock::init_recursive(&lock_);
}
/*
bool PTEntry::lock() {
  //pthread_mutex_lock(&lock_);
  return true;
}

bool PTEntry::unlock() {
  //pthread_mutex_unlock(&lock_);
  return true;
  }*/

void PTEntry::construct_response(dsm::Page* response) {
  ReadLock rlock(lock_m_);

  if ( perm_ == dsm::Permission::S3 ) {
    // send write permission to write page data, but don't modify local S3 permission
    response->set_perm(dsm::Permission::WRITE); 
    response->set_url(url_);
    response->set_local(local_);
  } else {
    response->set_perm(perm_);
  }
  response->set_byte_start(byte_start_);
  response->set_byte_end(byte_end_);
}

Addr DriverPTEntry::get_random_reader() {
  default_random_engine generator;
  uniform_int_distribution<int> distribution(0, readers_.size() - 1);

  // TODO inefficient
  //vector<uint32_t> readers_vec(readers_.begin(), readers_.end());
  //SPDLOG_DEBUG("readers size {:d}, readers vec size {:d}, page {:x}",
  //readers_.size(), readers_vec_.size(), pgnum());

  Addr ret_addr = driver_addr_;
  uint32_t ret_idx = 1000000;
  while ( (ret_addr == driver_addr_) or not (Util::set_contains(ret_idx, readers_)) ) {
    uint32_t idx = distribution(generator);
    uint64_t it_idx = 0;
    auto it = readers_.begin();
    while ( it_idx < idx ) {
      it++; // not as nice as vector indexing, but saves memory overhead
      it_idx++;
    }
    uint32_t r_idx = *it;
    //SPDLOG_DEBUG("page {:x} addr {:s} driver addr {:s} idx {:d} r_idx {:d} total servers {:d}", pgnum(), ret_addr.str(), driver_addr_.str(), idx, r_idx, invalidator_->num_servers());
    ret_addr = invalidator_->server_idx_to_server(r_idx);
    ret_idx = r_idx;
  }

  SPDLOG_DEBUG("page {:x} addr {:s} driver addr {:s}", pgnum(), ret_addr.str(), driver_addr_.str());
  return ret_addr;
}

Status PTEntry::retrieve_page(dsm::Page & retpage, Addr & requester_id) {
  return invalidator_->retrieve(requester_id, pgidx_,
                                readers_, retpage);
}

Status PTEntry::invalidate_readers(dsm::Page & retpage,
                                   Addr & requester_id) {
  /* First get the page... */
  SPDLOG_DEBUG("Getting page...");
  Status status = invalidator_->retrieve(requester_id, pgidx_,
					 readers_, retpage);

  /* ... then invalidate readers */
  dsm::Page invalidate_retpage;
  SPDLOG_DEBUG("Invalidating {:d} readers...", readers_.size());
  auto invalidated = invalidator_->invalidate(requester_id, pgidx_,
					      readers_, invalidate_retpage,
					      /* clear_cache=*/true);

  for ( auto & x : invalidated ) {
    if ( x.second.ok() ) {
      SPDLOG_DEBUG("Reader {:s} invalidated, erasing...", servers_vec_->at(x.first).str());
      remove_reader(x.first);
    } else {
      status = x.second;
    }
  }
  
  return status;
}

Status PTEntry::invalidate_writers(dsm::Page & retpage,
                                   Addr & requester_id, bool clear_cache, bool send_data) {
  /* First stop writing... */
  dsm::Page invalidate_retpage;
  SPDLOG_DEBUG("Invalidating {:d} writers...", writers_.size());
  auto invalidated = invalidator_->invalidate(requester_id, pgidx_,
					      writers_, invalidate_retpage,
					      /*clear_cache=*/clear_cache);

  Status status = Status::OK;
  for ( auto & x : invalidated ) {
    if ( x.second.ok() ) {
      remove_writer(x.first);
    } else {
      status = x.second;
    }
  }

  if ( !clear_cache ) {
    for ( auto & x : invalidated ) {
      if ( x.second.ok() ) {
	SPDLOG_DEBUG("Writer {:s} to reader, erasing...", servers_vec_->at(x.first).str());
	add_reader(x.first);
      }
    }
  }

  /* ... then get the page if needed */
  if ( send_data ) {
    SPDLOG_DEBUG("Getting page...");
    Status ret_status = invalidator_->retrieve(requester_id, pgidx_,
					       readers_, retpage);
    if ( !ret_status.ok() ) status = ret_status;
  }

  SPDLOG_DEBUG("Invalidate: {:d}", status.error_code());
  return status;
}

Status PTEntry::request_read_from_s3(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  /* Remote pages should only be mapped on driver. */
  throw cloud_map_error(pgidx_);
}

Status PTEntry::request_read_from_read(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  /* Current read, requested read. */
  Status status;
  SPDLOG_DEBUG("Entering worker read request");
  if ( num_readers() == 0 ) { // not cached anywhere
    SPDLOG_DEBUG("No readers for page");
    if ( id() == t_id ) { // reconstructing
      return Status::OK; // ok to give permission
    }
  } 
      
  status = retrieve_page(retpage, client_id);
  return status;
}

Status PTEntry::request_read_from_write(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  SPDLOG_DEBUG("Page id {:s}, requested id {:s}",
               id().str(),
               t_id.str());
  if ( id() == t_id ) { // reconstructing or replica
    return Status::OK;
  }
	
  /* Invalid: can only request read once writer unlocks the page
   * (i.e. on READ_ONLY). Tell the client to abort and retry later. */
  SPDLOG_DEBUG("Page {:x} is currently being written, not in driver: retry", PageUtils::pgidx_to_absolute_pgnum(pgidx_));
  return Status(StatusCode::PERMISSION_DENIED, "RETRY");
}

Status PTEntry::request_read_from_ro(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  /* Current read-only, requested read. */
  Status status;
  if ( num_readers() == 0 ) { // This page needs to be cached somewhere for us to read it, since it was previously written
    SPDLOG_DEBUG("Page id {:s}, requested id {:s}",
                 id().str(),
                 t_id.str());
    if ( id() == t_id ) { // reconstructing
      SPDLOG_DEBUG("No readers and reconstructing, assigning permission...");
      status = Status::OK;
    } else {
      SPDLOG_DEBUG("Not reconstructing, returning fetch fail");
      status = Status(StatusCode::PERMISSION_DENIED, "PAGE_LOST");
    }
    //  } else if ( cached_pagedata != nullopt ) {
  } else if ( cached_pagedata.size() > 0 ) {
    // worker pushed data for this page
    status = Status::OK;
    SPDLOG_DEBUG("Returning cached data");
    //retpage.set_pagedata(cached_pagedata.value());
    retpage.set_pagedata(cached_pagedata);
  } else {
    status = retrieve_page(retpage, client_id);
  }

  return status;
}

Status PTEntry::request_read_from_none(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  return Status::OK;
}

Status PTEntry::request_read(Addr & client_id, task_id t_id,
                             dsm::Page & retpage) {
  grpc::Status status;

  SPDLOG_DEBUG("Serving read request for {:x}, current perm {:d}",
	       PageUtils::pgidx_to_absolute_pgnum(pgidx_), perm());

  ReadLock rlock(lock_m_, std::defer_lock);
  WriteLock wlock(lock_m_, std::defer_lock);

  rlock.lock();
  
  dsm::Permission old_perm = perm_;
  
  if ( old_perm != dsm::Permission::READ ) {
    SPDLOG_DEBUG("Unlocking read {:s}", client_id.str());
    rlock.unlock();
    SPDLOG_DEBUG("Locking write {:s}", client_id.str());
    wlock.lock();
    if ( perm_ == dsm::Permission::READ ) {
      // permission changed
      SPDLOG_DEBUG("Updating perm and taking new locks {:s}", client_id.str());
      old_perm = perm_;
      wlock.unlock();
      rlock.lock();
      SPDLOG_DEBUG("(Done) Updating perm and taking new locks {:s}", client_id.str());
    }
  } 

  SPDLOG_DEBUG("Got locks, read, client {:s}", client_id.str());
  //lock();
  
  // Switch on current state
  switch(perm_) {
  case dsm::Permission::S3:
    status = request_read_from_s3(client_id, t_id, retpage);
    break;
  case dsm::Permission::READ:
    status = request_read_from_read(client_id, t_id, retpage);
    break;
  case dsm::Permission::WRITE:
    status = request_read_from_write(client_id, t_id, retpage);
    break;
  case dsm::Permission::READ_ONLY:
    status = request_read_from_ro(client_id, t_id, retpage);
    break;
  case dsm::Permission::NONE:
    status = request_read_from_none(client_id, t_id, retpage);
    break;
  }

  SPDLOG_DEBUG("Read returned status: {:s}", status.error_message());
  
  if ( status.ok() ) {
    if ( old_perm == dsm::Permission::READ ) {
      SPDLOG_DEBUG("Locking write in status update {:s}", client_id.str());
      rlock.unlock();
      wlock.lock();
    }
    
    if ( perm_ != dsm::Permission::S3 && perm_ != dsm::Permission::READ_ONLY ) {
      set_perm(dsm::Permission::READ); // else, keep original permission
    }
    add_reader(Util::vec_idx(client_id, servers_vec_));

    if ( old_perm == dsm::Permission::READ ) {
      SPDLOG_DEBUG("Unlocking write in status update {:s}", client_id.str());
      wlock.unlock();
      rlock.lock();
    }
  }
  
  if ( old_perm != dsm::Permission::READ ) {
    SPDLOG_DEBUG("Unlocking write {:s}", client_id.str());
    wlock.unlock();
  } else {
    rlock.unlock();
  }
  //unlock();
  
  return status;
}

Status PTEntry::request_write_from_s3(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  return Status(StatusCode::PERMISSION_DENIED, "Tried to write-lock S3 page");
}

Status PTEntry::request_write_from_read(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  /* Current read, requested write. */
  SPDLOG_DEBUG("Stage id: {}", id().stage_id);
  if ( id().stage_id > 0 and id() != t_id ) {
    // TODO This shouldn't be an issue.
    SPDLOG_DEBUG("Tried to write page {:x} marked as read by another task", PageUtils::pgidx_to_absolute_pgnum(pgidx_));
    return Status(StatusCode::PERMISSION_DENIED, "Tried to write page already written by another task");
  }

  Status status = invalidate_readers(retpage, client_id);
  if ( status.ok() ||
       ((status.error_code() == StatusCode::UNAVAILABLE) && (readers_.size() == 0)) ) {
    set_id(t_id);
    return Status::OK;
  }
  
  return status;
}

Status PTEntry::request_write_from_write(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  /* Current write, requested write. Allowed; writes will only be visible if this task returns. */
  if ( id() != task_id::UNASSIGNED_ID and t_id != id() ) {
    return Status(StatusCode::PERMISSION_DENIED, "Tried to write page that was written by another task");
  }

  return Status::OK;
}

Status PTEntry::request_write_from_none(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  set_id(t_id);
  return Status::OK;
}

optional<uint32_t> PTEntry::cached_at() {
  for ( auto & r : readers_ ) { 
    // if ( r == client_id ) continue;
    if ( not cluster_manager_->is_failed(servers_vec_->at(r)) ) {
      return optional<uint32_t> { r };
    }    
  }

  return nullopt;
}

bool PTEntry::is_present() {
  return cached_at() != nullopt;
}

void PTEntry::update_cache_locs() {
  return;
  SPDLOG_DEBUG("Update cache locs: {:x}", pgnum());
  WriteLock wlock(lock_m_);
  SPDLOG_DEBUG("(Locked) Update cache locs: {:x}", pgnum());
  vector<uint32_t> r_remove;
  for ( auto & idx : readers_ ) {
    if ( cluster_manager_->is_failed(servers_vec_->at(idx)) ) {
      r_remove.push_back(idx);
    }
  }

  for ( auto & idx : r_remove ) {
    SPDLOG_DEBUG("Reader {:s} died, erasing...", servers_vec_->at(idx).str());
    remove_reader(idx);
  }

  vector<uint32_t> w_remove;
  for ( auto & idx : writers_ ) {
    if ( cluster_manager_->is_failed(servers_vec_->at(idx)) ) {
      w_remove.push_back(idx);
    }
  }

  for ( auto & idx : w_remove ) {
    remove_writer(idx);
  }
  SPDLOG_DEBUG("(Unlocking) Update cache locs: {:x}", pgnum());
}

Status PTEntry::request_write_from_ro(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  optional<uint32_t> caching_worker = cached_at();
  if ( caching_worker != nullopt ) {
    /* Some reader still has this page cached. Don't try to overwrite it. */
    SPDLOG_DEBUG("Tried to write page but worker {:s} still has it cached, assume straggler. Aborting...",
                 servers_vec_->at(caching_worker.value()).str());

    /* Aborting worker that wanted to write, no need to invalidate other readers. */
    return Status(StatusCode::PERMISSION_DENIED, "ABORT_REPLICA");
  } else {
    /* No readers left alive, so we assume this is being reconstructed */
    SPDLOG_DEBUG("Page id {:s}, requested id {:s}",
                 id().str(), t_id.str());
    if ( id() != t_id ) {
      return Status(StatusCode::PERMISSION_DENIED, "Tried to write read-only page with incompatble task id");
    }

    return Status::OK;
  }
}

Status PTEntry::request_write(Addr & client_id, task_id t_id,
                              dsm::Page & retpage) {
  grpc::Status status;

  SPDLOG_DEBUG("Serving write request for {:x}, current perm {:d}",
	       PageUtils::pgidx_to_absolute_pgnum(pgidx_), perm());

  ReadLock rlock(lock_m_, std::defer_lock);
  WriteLock wlock(lock_m_, std::defer_lock);

  rlock.lock();
  
  dsm::Permission old_perm = perm_;

  if ( old_perm != dsm::Permission::WRITE ) {
    rlock.unlock();
    wlock.lock();
  } 
  //lock();
  
  // Switch on current state
  switch(perm_) {
  case dsm::Permission::S3:
    status = request_write_from_s3(client_id, t_id, retpage);
    break;
  case dsm::Permission::READ:
    status = request_write_from_read(client_id, t_id, retpage);
    break;
  case dsm::Permission::WRITE:
    status = request_write_from_write(client_id, t_id, retpage);
    break;
  case dsm::Permission::READ_ONLY:
    status = request_write_from_ro(client_id, t_id, retpage);
    break;
  case dsm::Permission::NONE:
    status = request_write_from_none(client_id, t_id, retpage);
    break;
  }

  SPDLOG_DEBUG("Write returned status: {:s}", status.error_message());
  if ( status.ok() ) {
    //WriteLock(lock_m_);
    
    if ( old_perm == dsm::Permission::WRITE ) {
      rlock.unlock();
      wlock.lock();
    }

    set_perm(dsm::Permission::WRITE);
    add_writer(Util::vec_idx(client_id, servers_vec_));

    if ( old_perm == dsm::Permission::WRITE ) {
      wlock.unlock();
      rlock.lock();
    }
  }

  if ( old_perm != dsm::Permission::WRITE ) {
    wlock.unlock();
  } else {
    rlock.unlock();
  }
  //unlock();
  
  return status;
}

DriverPTEntry::DriverPTEntry(uint64_t pgidx, ClusterManager* cluster_manager, Invalidator* invalidator,
			     vector<Addr>* servers_vec,
                             Addr driver_addr)
  : PTEntry(pgidx, cluster_manager, invalidator, servers_vec),
    driver_addr_(driver_addr)
{
}

Status DriverPTEntry::request_read_from_s3(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  SPDLOG_DEBUG("Assigning S3 permission");
  return Status::OK; // The required write permission will be set in construct_response
}

// TODO version here?
Status DriverPTEntry::request_read_from_write(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  return invalidate_writers(retpage, client_id, false); // driver should be the only writer
}

Status DriverPTEntry::request_write_from_read(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  SPDLOG_DEBUG("Requesting driver write, invalidating {:d} readers...", readers_.size());
  //  cached_pagedata = nullopt; // clear cached data
  cached_pagedata = "";
  
  if (client_id != driver_addr_) {
    throw invalid_requester_error(client_id, pgidx_);
  }
  
  // Copy on write: version the page
  if ( is_cow_ ) {
    SPDLOG_DEBUG("Inserting version... {:s}", t_id.str());
    assert( id_ != task_id::UNASSIGNED_ID );
    char* data_ptr = (char*)(PageUtils::pgidx_to_addr(pgidx()));
    vector<char> data(data_ptr, data_ptr + PG_SIZE);
    versions_.insert(make_pair(id_.stage_id, data));
    is_cow_ = false; // end version
  }
  
  if ( Worker::cache_none ) {
    // no need to invalidate
    readers_.clear();
    return Status::OK;
  } else {
    Status status = invalidate_readers(retpage, client_id);
    if ( (status.error_code() == StatusCode::UNAVAILABLE) &&
	 (readers_.size() == 0) ) {
      return Status::OK; // no other readers => ok to write
    } else {
      return status;
    }
  }

  // TODO: if client_id != driver_addr_, allow local write but do not ever make it globally visible...
  // return retrieve_page(retpage, client_id);
}

Status DriverPTEntry::request_write_from_write(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  if (client_id != driver_addr_) {
    throw invalid_requester_error(client_id, pgidx_);
  }

  return retrieve_page(retpage, client_id);
}

Status DriverPTEntry::request_write_from_ro(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  if (client_id != driver_addr_) {
    throw invalid_requester_error(client_id, pgidx_);
  }

  // TODO: should we ever return this status? When is driver page marked read-only?
  SPDLOG_DEBUG("Attempt to write read-only page {:x} in driver", PageUtils::pgidx_to_absolute_pgnum(pgidx_));
  return Status(StatusCode::PERMISSION_DENIED, "Attempted to write read-only page in driver");
}

Status DriverPTEntry::request_write_from_none(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  if (client_id != driver_addr_) {
    throw invalid_requester_error(client_id, pgidx_);
  }

  return Status::OK;
}

Status DriverPTEntry::retrieve_page(dsm::Page & retpage, Addr & requester_id) {
  // Use this as the canonical version of the page
  // TODO may deadlock if requester is driver
  return invalidator_->invalidate_driver(pgidx_, retpage);
}

Status DriverPTEntry::request_read_from_read(Addr & client_id, task_id t_id, dsm::Page & retpage) {
  /* Current read, requested read. */
  Status status;

  SPDLOG_DEBUG("Entering driver read request");

  /* Check version if reconstructing */
  vector<uint64_t> versioned_stages = Util::sorted_keys(versions_);
  int num_versions = versioned_stages.size();

  SPDLOG_DEBUG("Stage id {:d}, versions {:d}", t_id.stage_id, versioned_stages.size());
  
  if ( (versioned_stages.size() == 0) or (t_id.stage_id > versioned_stages.at(num_versions - 1)) ) {
    // no applicable versions stored, get latest

    /* Retrieve latest page */
    //if ( true ) { 
    if ( num_readers() == 1 ) {
      //      if ( cached_pagedata != nullopt ) {
      if ( cached_pagedata.size() > 0 ) {
	SPDLOG_DEBUG("Returning cached data for {:x}", pgnum());
	status = Status::OK;
	//	retpage.set_pagedata(cached_pagedata.value());
	retpage.set_pagedata(cached_pagedata);
      } else {
	SPDLOG_DEBUG("Getting page {:x} from worker", pgnum());
	status = retrieve_page(retpage, client_id);
	//cached_pagedata = optional<string>{ retpage.pagedata() }; // cache data for future requests. TODO version
	cached_pagedata = retpage.pagedata();
      }
    } else {
      // pick random non-driver reader to redirect request to
      SPDLOG_DEBUG("Picking random reader for {:x}", pgnum());
      Addr reader = get_random_reader();
      SPDLOG_DEBUG("Got reader {:s} for {:x}", reader.str(), pgnum());
      retpage.set_url(reader.ip);
      retpage.set_port(reader.port);
      retpage.set_ptop(true);
      SPDLOG_DEBUG("Returning reader {:s} for {:x} client {:s}", reader.str(), pgnum(), client_id.str());
      status = Status::OK;
    }
  } else {
    // reconstructing, get previous version
    SPDLOG_DEBUG("Reconstructing {:x}, getting version...", pgnum());
    bool got_page = false;
    for ( size_t i = versioned_stages.size() - 1; i >= 0; i-- ) {
      if ( versioned_stages.at(i) <= t_id.stage_id ) {
        vector<char> & v = versions_.at(i);
        retpage.set_pagedata(string(v.begin(), v.end()));
        got_page = true;
      }
    }

    // sanity check
    assert( got_page );
  }
  
  return status;
}
