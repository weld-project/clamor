#ifndef DSM_CLIENT_H
#define DSM_CLIENT_H

#include <memory>
#include <string>
#include <utility>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "dsm.grpc.pb.h"

#include "fault-handler.h"
#include "net-util.h"

extern "C" {
  #include "smalloc/smalloc.h"
}

class DSMClient {
 public:
 DSMClient()
   {}
 
 DSMClient(std::shared_ptr<grpc::Channel> channel, Addr id, bool is_driver=false)
   : stub_(dsm::DSM::NewStub(channel)), id_(id), is_driver_(is_driver)
 {}
 
 void AddChannel(std::shared_ptr<grpc::Channel> channel, Addr id) {
   stub_ = dsm::DSM::NewStub(channel);
   id_ = id;
 }
   
  int MapS3(uintptr_t start, size_t size, std::string url, bool local=false);
  grpc::Status GetPage(dsm::Page reqpage, dsm::Page* retpage);

  bool RegisterWorker(Addr addr, bool is_driver);
  //  meminfo RegisterClient(Addr addr, bool is_driver);
  bool ReturnResponse(dsm::Response result);
  bool Shutdown(); /* Called by driver to tear down after query completes */

  dsm::ResultList get_results();
  dsm::Pages get_pages(uint64_t idx);
  
  void ProcessGetPageStatus(grpc::Status status, uint64_t pagenum, bool should_retry);
  
 private:
  void BuildClientContext(grpc::ClientContext *context) const;

  std::unique_ptr<dsm::DSM::Stub> stub_;
  Addr id_;
  bool is_driver_;
};

#endif
