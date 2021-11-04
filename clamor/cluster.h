/* Cluster management functions */

#ifndef CLUSTER_H
#define CLUSTER_H

#include <stdint.h>
#include <sys/time.h>

#include <vector>
#include <string>
#include <thread>
#include <utility>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "task-manager.h"
#include "dsm-client.h"
#include "worker-server.h"
#include "weld-utils.h"

namespace Cluster {
  typedef enum Role {
    MANAGER,
    DRIVER,
    WORKER,
  } Role;

  class Driver {
  public:
    Driver(std::string driver_ip, uint32_t driver_port,
           std::string manager_ip, uint32_t manager_port,
           std::vector<std::string> worker_ips, uint32_t nprocs, uint64_t npartitions,
	   bool reverse_channel = true);
    
    void* run_query(std::string query, void* args, uint64_t nworkers,
		    bool spec_return = false, bool cache_none = false);

    dsm::ResultList get_results();
    dsm::Pages get_pages(uint64_t idx);
    
    ~Driver() { /* shut down all servers */
      client_.Shutdown();

      worker_server_.shutdown_server();
    }
    
    /* Returns data pointer if successful, else null pointer. */
    std::pair<char*, size_t> map_url(std::string url, bool local = false);

  private:
    DSMClient client_;
    WorkerRunner worker_server_; // Only used here for cache invalidations
    uint64_t npartitions_; // should be same as manager
  };
  
  void start_task_manager(std::string manager_ip, uint32_t manager_port,
			  std::string driver_ip, uint32_t driver_port,
			  uint32_t weld_range_start,
			  std::vector<std::string> worker_ips,
			  uint32_t nprocs, uint64_t npartitions,
			  bool local, bool speculate=false); // speculation off by default

  void start_worker(std::string worker_ip, uint32_t worker_port,
                    std::string manager_ip, uint32_t manager_port,
                    std::string driver_ip, uint32_t driver_port,
		    bool slow = false, bool reverse_channel = true);
};

#endif
