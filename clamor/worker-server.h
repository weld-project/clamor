#ifndef WORKER_SERVER_H
#define WORKER_SERVER_H

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include "dsm.grpc.pb.h"

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/sum.hpp>

#include <iostream>

#include <future>
#include <vector>
#include <string>
#include <unordered_map>

#include "pthread.h"

#include "debug.h"
#include "fault-handler.h"
#include "net-util.h"

extern "C" {
  #include "smalloc/mem.h"
  #include "smalloc/smalloc.h"
}


/* Implement RPC methods for Worker service.
 * A Worker service runs on each worker machine and serves
 * requests to run tasks and invalidate pages. */
class Worker final : public dsm::Worker::Service {
 public:
  explicit Worker(Addr worker_addr,
		  Addr manager_addr,
		  bool is_driver,
		  bool slow,
                  bool reverse_channel = true);
  
  static bool return_vectors;
  static bool cache_none;

  grpc::Status InvalidatePage(grpc::ServerContext* context,
                              const dsm::InvalidateRequest* request,
                              dsm::Page* response) override;

  grpc::Status RunProgram(grpc::ServerContext* context,
			  const dsm::WeldTask* task,
			  dsm::Empty* empty) override;
  
  grpc::Status Heartbeat(grpc::ServerContext* context,
			 const dsm::Empty* empty,
			 dsm::Alive* response) override;
  
  grpc::Status Shutdown(grpc::ServerContext* context,
                        const dsm::Empty* request,
                        dsm::Empty* response) override {
    shutdown.set_value();
    return grpc::Status::OK;
  }

  grpc::Status FetchPage(grpc::ServerContext *context,
                         const dsm::Page *request,
                         dsm::Page *response) override;

  std::promise<void> shutdown;

  boost::accumulators::accumulator_set<int,
    boost::accumulators::stats<boost::accumulators::tag::sum > > num_peer_served = {};

 private:
  void initialize_client();
  void initialize_driver_heap();
  void initialize_heap(uint64_t heap_start, uint64_t heap_end);

  DSMClient client_;
  bool client_initialized_ = false;
  bool slow_; // For testing speculation. Artificially slow down this worker.

  Addr manager_addr_;
  Addr worker_addr_;

  std::unordered_set<uint64_t> stages_run_;
  bool cache_server_;
};

class WorkerRunner {
 public:
  // serve
 WorkerRunner(Addr worker_addr, Addr manager_addr, bool is_driver, bool slow, bool reverse_channel) :
  addr_(worker_addr),
     service_(worker_addr, manager_addr, is_driver, slow, reverse_channel)
  {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(worker_addr.str(), grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);

    server_ = builder.BuildAndStart();
    serving_thread_ = std::thread(&WorkerRunner::start_server, this);
  }

  ~WorkerRunner() {
    shutdown_future_ = service_.shutdown.get_future();
    shutdown_future_.wait();

    shutdown_server();
    serving_thread_.join();
  }

  // shutdown
  void shutdown_server() {
    server_->Shutdown();
  }

  void start_server() {
    SPDLOG_DEBUG("Cache server listening on {}", addr_.str());
    server_->Wait();
  }

 private:
  Worker service_;
  std::thread serving_thread_;
  Addr addr_;
  std::unique_ptr<grpc::Server> server_;
  std::future<void> shutdown_future_;
};
  
#endif // WORKER_SERVER_H
