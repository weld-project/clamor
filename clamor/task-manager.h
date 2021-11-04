#ifndef DSM_SERVER_H
#define DSM_SERVER_H

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include "dsm.grpc.pb.h"

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/sum.hpp>

#include "worker-client.h"

#include "debug.h"
#include "heap-allocator.h"
#include "invalidator.h"
#include "net-util.h"
#include "pagetable.h"
#include "stage.h"
#include "task.h"
#include "util.h"
#include "worker-pool.h"

#include <future>
#include <mutex>
#include <optional>
#include <queue>
#include <vector>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "pthread.h"

/**
 * This service handles task scheduling as well as page permissions.
 * In particular, it is responsible for handling page read/write requests,
 * tracking worker liveness, and handling task submission/dispatch.
 * These responsiblities are coupled because page read/write requests
 * require making cache invalidation RPCs to workers,
 * and we would prefer not to have a separate cache invalidation service to handle these.
 * The scheduler must also be able to track page permissions in order to determine whether a
 * page needs to be reconstructed when a worker fails, or is cached at another reader.
 */

extern "C" {
  #include "smalloc/mem.h"
  #include "smalloc/smalloc.h"
}

typedef struct RPCInfo {
  grpc::Status ret_status;
  dsm::Empty reply;
  dsm::TaskStatus task_status;
  void* tag;
} RPCInfo;

/* Implement RPC methods for DSM protobuf. */
class TaskManager final : public dsm::DSM::Service {
public:
  // Partitions for each task. In the future, could be computed dynamically
  static const uint64_t NUM_PARTITIONS;

  class Event {
  public:
    virtual void process(TaskManager* t) = 0; /* allow event to modify scheduler state */
  };
  
  // on entire stage submitted
  class NewStageEvent : public Event {
  public:
    NewStageEvent(const dsm::Task* task, dsm::Result* result,
		  std::condition_variable* cv, std::mutex* m) :
      task(*task), // invoke copy constructor
      result(result),
      cv(cv),
      m(m)
    {};

    void process(TaskManager* t);
    
    dsm::Task task;
    dsm::Result* result; // we block on result return, so this pointer stays valid
    std::condition_variable* cv;
    std::mutex* m;
  };
  
  // on task returned
  class ResultReturnEvent : public Event {
  public:
    ResultReturnEvent(const dsm::Response* response, Addr worker_id) :
      response(*response), // invoke copy constructor
      worker_id(worker_id),
      end_timestamp()
    {
      gettimeofday(&end_timestamp, 0);
    };
    
    void process(TaskManager* t);

    dsm::Response response;
    Addr worker_id;
    struct timeval end_timestamp;
  };

  // on heartbeat returns unavailable
  class WorkerFailedEvent : public Event {
  public:
    WorkerFailedEvent(Addr worker_id, bool worker_died, bool retry) :
      worker_id(worker_id),
      worker_died(worker_died),
      retry(retry)
    {};
    
    void process(TaskManager* t);
    
    Addr worker_id;

    // If the failure was due to a fetch failure, don't mark the worker as dead,
    // but still resubmit the task it was running.
    bool worker_died;

    // If the cause of failure was a forced abort, add a delay so that previous worker
    // has time to finish writing the dependency before retrying.
    bool retry;
  };

  explicit TaskManager(Addr driver_addr, uint32_t weld_range_start,
		       std::vector<std::string> worker_ips, uint32_t nprocs,
		       uint64_t num_partitions,
		       bool local, bool speculate);
  
  void RunManager();
  void RunTaskScheduler();
  void Heartbeat();
  void submit_loop();
  
  void enqueue_task(task_info t_info);
  bool submit_task(task_info t_info, Addr worker_id);

  void push_queue(std::unique_ptr<Event> e);
  std::unique_ptr<Event> pop_queue();

  grpc::Status MapS3(grpc::ServerContext* context,
		     const dsm::S3Data* request,
		     dsm::Empty* response) override;

  grpc::Status GetPage(grpc::ServerContext* context,
                       const dsm::Page* request,
                       dsm::Page* response) override;

  grpc::Status GetResults(grpc::ServerContext* context,
			  const dsm::Empty* request,
			  dsm::ResultList* response) override;

  grpc::Status GetResultPages(grpc::ServerContext* context,
			      const dsm::ResultRequest* request,
			      dsm::Pages* response) override;

  grpc::Status RegisterWorker(grpc::ServerContext* context,
			      const dsm::Client* client,
			      dsm::Empty* info) override;
  
  /* Schedule a task and block on successful completion. */
  grpc::Status SubmitTask(grpc::ServerContext* Context,
			  const dsm::Task* task,
			  dsm::Result* result) override;
    
  /* Called by the worker to return the result of a task. */
  grpc::Status Return(grpc::ServerContext* context,
		      const dsm::Response* result,
		      dsm::Empty* empty) override;
  
  grpc::Status Shutdown(grpc::ServerContext* context,
                        const dsm::Empty* request,
                        dsm::Empty* response) override;

  /* Allocate space for input buffer and return address */
  uintptr_t map_input_data(char* data, size_t size);
  
  std::promise<void> shutdown;

 private:
  friend class NewStageEvent;

  // Workers that serve as a cache for driver addr space.
  // For now, size of `cache_servers_` is fixed and determined when receiving
  // the first stage. If a worker serving a cache partition fails, that
  // partition will be unavailable until a backup worker takes it over. We don't
  // dynamically partition cache so far since we don't have a good understanding
  // of how moving partitions around can affect performance.
  std::vector<dsm::CacheServer> cache_servers_;
  mutable std::shared_mutex cache_servers_mutex_;

  bool local_;
  bool speculate_;
  Addr driver_addr_;
  
  PageTable pagetable_;
  ClusterManager cluster_manager_;
  Invalidator invalidator_;
  
  HeapAllocator heap_allocator_;
  std::recursive_mutex heap_m_;
  
  std::unordered_map<Addr, WorkerClient> servers_;
  std::vector<Addr> servers_vec_; // for compressing addresses in page metadata

  /* Queues */
  
  // Event manager
  std::queue<std::unique_ptr<Event>> event_queue_;
  std::recursive_mutex event_m_;
  //std::mutex event_wake_m_;
  std::condition_variable event_wake_cv_;

  // Outstanding task set
  std::unordered_set<task_info> pending_tasks_;
  
  // For task RPCs  
  grpc::CompletionQueue cq_;
  
  std::mutex workers_m_;
  
  std::vector<Stage> stages_;

  std::recursive_mutex tasks_m_; // we use this as a global lock for all task-related data structures
  std::unordered_set<task_info> waiting_tasks_;
  std::unordered_set<task_id> running_tasks_;
  std::unordered_set<task_id> failed_tasks_;

  /******** Scheduler statistics *********/
  boost::accumulators::accumulator_set<double,
    boost::accumulators::stats<boost::accumulators::tag::sum > > result_process_times;
  boost::accumulators::accumulator_set<double,
    boost::accumulators::stats<boost::accumulators::tag::mean > > invalidate_times;
  boost::accumulators::accumulator_set<int,
    boost::accumulators::stats<boost::accumulators::tag::sum > > num_reads;
  boost::accumulators::accumulator_set<int,
    boost::accumulators::stats<boost::accumulators::tag::sum > > num_writes;
  boost::accumulators::accumulator_set<double,
    boost::accumulators::stats<boost::accumulators::tag::mean > > avg_compile;
  boost::accumulators::accumulator_set<double,
    boost::accumulators::stats<boost::accumulators::tag::mean > > avg_request_driver;
  boost::accumulators::accumulator_set<double,
    boost::accumulators::stats<boost::accumulators::tag::mean > > avg_request_peer;
  boost::accumulators::accumulator_set<double,
    boost::accumulators::stats<boost::accumulators::tag::mean > > avg_compute;

  /****** Private methods *******/
  bool check_dependencies(Task & task);

  void get_missing_parents(uint64_t pagenum,
			   std::unordered_set<task_id>* missing_parents);

  Stage & stage_id_to_stage(const uint64_t stage_id);

  Task & task_id_to_task(const task_id id);
  Task & task_id_to_task(const uint64_t stage_id, const uint64_t partition_id);
  
  void update_readers(Addr failed_worker_id);
  
  grpc::Status mark_read_only(uint64_t page_offset, Addr client_id,
			      int64_t stage_id, int64_t partition_id);
  grpc::Status retrieve_page(uint64_t page_offset, dsm::Page & retpage);
  
  grpc::Status request_read  (uint64_t page_offset, Addr client_id,
			      int64_t stage_id, int64_t partition_id,
			      dsm::Page & retpage);
  grpc::Status request_write (uint64_t page_offset, Addr client_id,
			      int64_t stage_id, int64_t partition_id,
			      dsm::Page & retpage);
  void construct_response(uint64_t page_offset, dsm::Page* response);

  void initialize_cache_servers();
  void disable_cache_server(const Addr &addr);
};

class TaskManagerRunner {
 public:
  // serve
 TaskManagerRunner(Addr manager_server, Addr driver_addr,
                   uint32_t weld_range_start,
                   std::vector<std::string> worker_ips, uint32_t nprocs,
		   uint64_t npartitions,
                   bool local, bool speculate) :
    address_(manager_server),
    service_(driver_addr, weld_range_start,
	     worker_ips, nprocs, npartitions, local, speculate)
  {
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(INT_MAX);
    
    builder.AddListeningPort(manager_server.str(), grpc::InsecureServerCredentials());
    auto s = manager_server;
    for ( int i = 0; i < 8; i++ ) { // Listen on many ports.
      SPDLOG_DEBUG("Adding address {:s}", s.str());
      builder.AddListeningPort(s.str(), grpc::InsecureServerCredentials());
      break;
      s.port++;
    }
    builder.RegisterService(&service_);

    server_ = builder.BuildAndStart();
    auto serving_thread = std::thread(&TaskManagerRunner::start_server, this);
    auto heartbeat_thread = std::thread(&TaskManager::Heartbeat, &service_);
    auto scheduler_thread = std::thread(&TaskManager::RunManager, &service_);
    auto submit_thread = std::thread(&TaskManager::submit_loop, &service_);
    shutdown_future_ = service_.shutdown.get_future();
    shutdown_future_.wait();

    shutdown_server();
    serving_thread.join();
    // scheduler_thread.join();
  }
  
  // shutdown
  void shutdown_server() {
    server_->Shutdown();
  }

  void start_server() {
    SPDLOG_DEBUG("DSM server listening on {}", address_.str());
    server_->Wait();
  }

 private:
  TaskManager service_;
  Addr address_;
  std::unique_ptr<grpc::Server> server_;
  std::future<void> shutdown_future_;
};

#endif // DSM_SERVER_H
