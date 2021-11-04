#ifndef INVALIDATOR_H
#define INVALIDATOR_H

#include <grpc/grpc.h>

#include <unordered_map>
#include <utility>
#include <vector>

#include "net-util.h"
#include "worker-client.h"
#include "worker-pool.h"

/**
 * Handles cache invalidation requests for pages.
 * The scheduler holds a single instance of this invalidator,
 * which is passed to the page table so that pages can make
 * invalidate requests on an incoming read/write request.
 */

#include "worker-client.h"

class Invalidator {
 public:
  Invalidator(std::unordered_map<Addr, WorkerClient>* servers,
	      std::vector<Addr>* servers_vec,
              ClusterManager* cluster_manager,
              Addr driver_addr);
  
  grpc::Status invalidate(uint64_t pgidx, Addr worker_addr,
                          dsm::Page & retpage, bool clear_cache, bool send_data = true);

  /**
   * Driver should never have to clear its local cache, as it always has write access
   * to its own pages. However, if a worker has cached a page on the driver and the
   * driver wants to take back write access, we must increment the page version.
   */
  grpc::Status invalidate_driver(uint64_t pgidx, dsm::Page & retpage);

  /**
   * Ask every worker in workers to stop writing to the page
   * and also clear the local cached version if clear_cache is true.
   */
  std::unordered_map<uint32_t, grpc::Status> invalidate(Addr & requester_id,
						    uint64_t pgidx,
						    std::unordered_set<uint32_t> & workers,
						    dsm::Page & retpage,
						    bool clear_cache);

  /**
   * Retrieve the page from any one reader without clearing anyone's cache.
   * Should only be called on pages that are not being written.
   */
  grpc::Status retrieve(Addr & requester_id,
                        uint64_t pgidx,
                        std::unordered_set<uint32_t> & workers,
                        dsm::Page & retpage);

  Addr server_idx_to_server(uint32_t server_idx) { return servers_vec_->at(server_idx); }

  size_t num_servers() {
    return servers_vec_->size();
  }
  
 private:
  std::unordered_map<Addr, WorkerClient>* servers_;
  std::vector<Addr>* servers_vec_;
  ClusterManager* cluster_manager_;
  Addr driver_addr_;
};

#endif // INVALIDATOR_H
