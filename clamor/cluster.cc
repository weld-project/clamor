#include "cluster.h"

#include "debug.h"
#include "task-manager.h"

#include "worker-server.h"

#include <experimental/filesystem>
#include <future>
#include <thread>

extern "C" {
  #include "smalloc/smalloc.h"
}

using namespace std;

/* Shared address buffer */
char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

/* npartitions should be the same as manager partitions */
void* Cluster::Driver::run_query(string query, void* args, uint64_t nworkers, bool spec_return,
				 bool cache_none) {
  /* dispatch computation */
  Worker::return_vectors = spec_return;
  SPDLOG_DEBUG("Set worker return to {:d}, spec {:d}", Worker::return_vectors, spec_return);
  SPDLOG_DEBUG("Set cache none to {:d}, spec {:d}", Worker::cache_none, cache_none);
  auto res = WeldUtils::run_query_weld(query, (void*)(args), true, npartitions_);
  return res.first;
}

pair<char*, size_t> Cluster::Driver::map_url(std::string url, bool local) {
  double size;
  
  SPDLOG_DEBUG("in map");
  if ( local ) {
    size = (double)(experimental::filesystem::file_size(url));
  } else {
    /* shadow-map S3 data */
    CurlHandle curl;
    size = curl.get_filesize_only(url);
    //size = 1000000000;//size = 10000;
  }

  SPDLOG_DEBUG("Filesize: {:f}", size);
  
  char* data = (char*)smalloc_aligned(size);
  SPDLOG_DEBUG("Data pointer {:x}", (uintptr_t)(data));
  client_.MapS3((uintptr_t)data, size, url, local);
  SPDLOG_DEBUG("exit map");

  return make_pair(data, size);
}

dsm::ResultList Cluster::Driver::get_results() {
  return client_.get_results();
}

dsm::Pages Cluster::Driver::get_pages(uint64_t idx) {
  return client_.get_pages(idx);
}

/* Note: nprocs is the number of *primary* workers, and there will be one backup
 * worker process for each primary. */
Cluster::Driver::Driver(string driver_ip, uint32_t driver_port,
                        string manager_ip, uint32_t manager_port,
                        vector<string> worker_ips, uint32_t nprocs, uint64_t npartitions,
			bool reverse_channel)
  : worker_server_(Addr(driver_ip, driver_port), Addr(manager_ip, manager_port), true, false, reverse_channel), // Driver heap initialized here
    npartitions_(npartitions)
{
  grpc::ChannelArguments ch_args;
  ch_args.SetMaxReceiveMessageSize(-1);
  client_ = DSMClient(grpc::CreateCustomChannel(manager_ip + ":" + std::to_string(manager_port),
						grpc::InsecureChannelCredentials(), ch_args),
		      Addr(driver_ip, driver_port),
		      true);
  /* connect driver program to manager */
  WeldUtils::manager_stub = dsm::DSM::NewStub(grpc::CreateChannel(manager_ip + ":" + to_string(manager_port),
								  grpc::InsecureChannelCredentials()));
  
  /* initialize fault handler */
  FaultHandler::fh.add_requester(&client_);
}

/* Starts blocking server. */
void Cluster::start_task_manager(string manager_ip, uint32_t manager_port,
				 string driver_ip,  uint32_t driver_port,
				 uint32_t weld_range_start,
				 vector<string> worker_ips, uint32_t nprocs,
				 uint64_t npartitions,
				 bool local, bool speculate) {
  /* start DSM server (to respond to page requests) */
  SPDLOG_DEBUG("Buffer address: {:p}", (void*)(&membuf));

  /* start in new thread */
  TaskManagerRunner server(Addr(manager_ip, manager_port),
			   Addr(driver_ip, driver_port),
			   weld_range_start, 
			   worker_ips, nprocs, npartitions,
			   local, speculate);
}

/* Starts server thread and blocks. */
void Cluster::start_worker(string worker_ip,   uint32_t worker_port, 
                           string manager_ip,  uint32_t manager_port,
                           string driver_ip,   uint32_t driver_port, bool slow, bool reverse_channel) {
  SPDLOG_DEBUG("Starting worker server...");

  WorkerRunner worker_server(Addr(worker_ip, worker_port),
			     Addr(manager_ip, manager_port), false, slow, reverse_channel);

  SPDLOG_DEBUG("...done.");
}
