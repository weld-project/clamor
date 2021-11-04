#ifndef WORKER_CLIENT_H
#define WORKER_CLIENT_H

#include <memory>
#include <string>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "dsm.grpc.pb.h"
#include "net-util.h"

extern "C" {
  #include "smalloc/mem.h"
}

class WorkerClient {
 public:
 WorkerClient(std::shared_ptr<grpc::Channel> channel, const Addr &id)
   : server_stub_(dsm::Worker::NewStub(channel)), id_(id)
    {}

  void run_program(std::string code, uintptr_t args);

  grpc::Status heartbeat();
  grpc::Status invalidate_page(uint64_t pagenum,
                               bool clear_cache,
			       bool send_data,
                               dsm::Page* retpage,
			       bool invalidate_driver_cache=true,
			       bool is_peer=false);
  
  grpc::Status shutdown(); /* Called by driver to tear down after query completes */

  grpc::Status FetchPage(dsm::Page reqpage, dsm::Page *retpage);

  std::unique_ptr<dsm::Worker::Stub> server_stub_;
  Addr id_;
};

#endif // WORKER_CLIENT_H
