#include "worker-client.h"

#include "debug.h"

#include <iostream>

#include "grpc-util.h" 

using grpc::ClientContext;
using grpc::Status;
using dsm::Page;
using dsm::PageNum;

using namespace dsm;
using namespace std;

/* send invalidate request */
Status WorkerClient::invalidate_page(uintptr_t pagenum, bool clear_cache,
				     bool send_data, Page* retpage,
				     bool invalidate_driver_cache,
				     bool is_peer) {
  InvalidateRequest req;
  req.mutable_pagenum()->set_pagenum(pagenum);
  req.set_clear_cache(clear_cache);
  req.set_send_data(send_data);
  req.set_invalidate_driver_cache(invalidate_driver_cache);
  req.set_is_peer(is_peer);
  SPDLOG_DEBUG("Requesting invalidate for {:x}, clear cache {:?}", pagenum, clear_cache);
  
  ClientContext context;
  Status status;

  status = server_stub_->InvalidatePage(&context, req, retpage);

  SPDLOG_DEBUG("Completed invalidate for {:x}", pagenum);
  return status;
}

Status WorkerClient::heartbeat() {
  Empty request;
  Alive result;
  
  ClientContext context;
  
  Status status = server_stub_->Heartbeat(&context, request, &result);
  return status;
}

Status WorkerClient::shutdown() {
  ClientContext context;
  
  Empty request;
  Empty response;
  Status status = server_stub_->Shutdown(&context, request, &response);
  return status;
}

void WorkerClient::run_program(std::string code, uintptr_t args) {
  SPDLOG_DEBUG("Running code: {:s}", code);
  
  WeldTask task;
  Empty ret; /* we'll entrust the server with sending result back to task manager */
  
  task.set_data(args);
  task.set_code(code);
  SPDLOG_DEBUG("Args addr: {:p}", (void*)args);
  
  // send task over the channel
  ClientContext context;
  Status status = server_stub_->RunProgram(&context, task, &ret);
  if (!status.ok()) {
    // This is fine, just log the error
    SPDLOG_DEBUG("RunProgram failed! {:d} {:s}", status.error_code(), status.error_message());
  }
}

Status WorkerClient::FetchPage(Page reqpage, Page *retpage) {
  ClientContext context;
  context.AddMetadata(GRPCUtil::CLIENT_IP_FIELD, id_.ip);
  context.AddMetadata(GRPCUtil::CLIENT_PORT_FIELD, id_.str_port());
  return server_stub_->FetchPage(&context, reqpage, retpage);
}
