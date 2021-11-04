#include "dsm-client.h"

#include <iostream>

#include "debug.h"
#include "exceptions.h"
#include "grpc-util.h"

using dsm::Client;
using dsm::MemInfo;
using dsm::Page;
using dsm::Pages;
using dsm::S3Data;
using dsm::Empty;
using grpc::ClientContext;
using grpc::Status;
using grpc::StatusCode;

using namespace std;
using namespace std::chrono_literals;

constexpr auto RETRY_DELAY = 1s;
const uint64_t RETRY_NUM_TRIES = 3;

int DSMClient::MapS3(uintptr_t start, size_t size, string url, bool local) {
  ClientContext context;
  BuildClientContext(&context);

  S3Data request;
  request.set_start(start);
  request.set_size(size);
  request.set_url(url);
  request.set_local(local);

  Empty response;

  cout << "mapping... " << endl;
  Status status = stub_->MapS3(&context, request, &response);

  if (status.ok()) {
    /* mapped page */
    cout << "Successfully mapped pages" << endl;
    return 0;
  } else {
    /* couldn't map page */
    cout << "Failed to map pages!" << endl;
    exit(1);
  }
}

/* send page request and wait for response
 * called on page fault or cache miss.*/
Status DSMClient::GetPage(Page reqpage, Page* retpage) {
  Status status;
  for ( int i = 0; (i < RETRY_NUM_TRIES) or (is_driver_); i++ ) {
    ClientContext context;
    BuildClientContext(&context);
 
    Status status = stub_->GetPage(&context, reqpage, retpage);
    if (status.ok()) return status;

    uint64_t pgnum = reqpage.pagenum().pagenum();
    /* couldn't get page */
    SPDLOG_DEBUG("Couldn't get page {:x}: {:d}: {:s}", pgnum,
                 status.error_code(), status.error_message());
    this_thread::sleep_for(chrono::milliseconds(RETRY_DELAY));
  }
  return status;
}

dsm::ResultList DSMClient::get_results() {
  Status status;
  dsm::ResultList results;
  dsm::Empty empty;
  ClientContext context;
  BuildClientContext(&context);
  status = stub_->GetResults(&context, empty, &results);
  if ( !status.ok() ) {
    SPDLOG_DEBUG("Couldn't get results: {:d}", status.error_code());
  }

  return results;
}

dsm::Pages DSMClient::get_pages(uint64_t idx) {
  Status status;
  dsm::Pages results;
  dsm::ResultRequest request;
  request.set_task_id(idx);
  ClientContext context;
  BuildClientContext(&context);
  status = stub_->GetResultPages(&context, request, &results);
  if ( !status.ok() ) {
    SPDLOG_DEBUG("Couldn't get results: {:d}", status.error_code());
  }

  return results;
}

bool DSMClient::RegisterWorker(Addr addr, bool is_driver) {
  ClientContext context;
  BuildClientContext(&context);

  Client client;
  client.set_ip(addr.ip);
  client.set_port(addr.str_port());
  client.set_is_driver(is_driver);
  
  Empty empty;
  
  Status status = stub_->RegisterWorker(&context, client, &empty);
  if ( not status.ok() ) {
    SPDLOG_DEBUG("Failed to register worker {}: error code {:d}: {}",
		 id_.str(), status.error_code(), status.error_message());
  } 

  return status.ok();
}

bool DSMClient::ReturnResponse(dsm::Response response) {
  ClientContext context;
  BuildClientContext(&context);

  Empty empty;

  Status status = stub_->Return(&context, response, &empty);

  return status.ok();
}

bool DSMClient::Shutdown() {
  ClientContext context;
  BuildClientContext(&context);
  
  Empty request;
  Empty response;
  Status status = stub_->Shutdown(&context, request, &response);

  return status.ok();
}

void DSMClient::BuildClientContext(ClientContext *context) const {
  context->AddMetadata(GRPCUtil::CLIENT_IP_FIELD, id_.ip);
  context->AddMetadata(GRPCUtil::CLIENT_PORT_FIELD, id_.str_port());
}
