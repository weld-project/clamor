#include "invalidator.h"

#include "page-utils.h"

#include <future>

using grpc::Status;
using grpc::StatusCode;

using namespace std;

Invalidator::Invalidator(unordered_map<Addr, WorkerClient>* servers,
			 vector<Addr>* servers_vec_,
                         ClusterManager* cluster_manager,
                         Addr driver_addr) :
  servers_(servers),
  servers_vec_(servers_vec_),
  cluster_manager_(cluster_manager),
  driver_addr_(driver_addr)
{
}

Status Invalidator::invalidate(uint64_t pgidx, Addr worker_addr,
                               dsm::Page & retpage, bool clear_cache, bool send_data) {
  auto & client = servers_->at(worker_addr);
  uint64_t absolute_pgnum = PageUtils::pgidx_to_absolute_pgnum(pgidx);
  return client.invalidate_page(absolute_pgnum, clear_cache, send_data, &retpage);
}

Status Invalidator::invalidate_driver(uint64_t pgidx, 
                                      dsm::Page & retpage) {
  return invalidate(pgidx, driver_addr_, retpage, false);
}

unordered_map<uint32_t, Status> Invalidator::invalidate(Addr & requester_id,
						    uint64_t pgidx,
						    unordered_set<uint32_t> & workers,
						    dsm::Page & retpage,
						    bool clear_cache) {
  Status invalidate_success;
  unordered_map<uint32_t, Status> invalidated;
  for ( auto & x : workers ) invalidated[x] = Status::OK;
  
  vector<future<bool>> futures;
  for ( auto & idx : workers ) {
    Addr & x = servers_vec_->at(idx);
    futures.push_back( async( launch::async,
			      [this, &requester_id, &invalidated,
			       &pgidx, &retpage, &clear_cache] ( auto && name, auto && idx ) {
				if ( cluster_manager_->is_failed(name) ) {
				  return true;
				}
				
				if ( name == requester_id ) {
				  return true;
				}
				
				Status invalidate_success = invalidate(pgidx, name, retpage,
								       clear_cache, /*send_data=*/false);
	     
				invalidated[idx] = invalidate_success;
			      },
			      x, idx)
		       );
  }

  for ( auto & x : futures ) {
    x.get();
  }
  
  return invalidated;
}

Status Invalidator::retrieve(Addr & requester_id,
                             uint64_t pgidx,
                             unordered_set<uint32_t> & workers,
                             dsm::Page & retpage) {
  Status invalidate_success;

  for ( auto & idx : workers ) {
    Addr & name = servers_vec_->at(idx);
    if ( cluster_manager_->is_failed(name) ) {
      continue;
    }

    if ( name == requester_id ) {
      continue;
    }

    Status invalidate_success = invalidate(pgidx, name, retpage,
					   /*clear_cache=*/false, /*send_data=*/true);
    if ( invalidate_success.ok() ) {
      return Status::OK;
    } 
  }

  SPDLOG_DEBUG("Page {:x} has no available readers", PageUtils::pgidx_to_absolute_pgnum(pgidx));
  return Status(StatusCode::UNAVAILABLE, ""); // fetch fail
}
