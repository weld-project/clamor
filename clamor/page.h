#ifndef PAGE_H
#define PAGE_H

#include "dsm.grpc.pb.h"

#include "page-utils.h"
#include "invalidator.h"
#include "net-util.h"
#include "task.h"
#include "util.h"
#include "worker-pool.h"

#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <unordered_set>

#include <shared_mutex>
  
typedef std::shared_mutex lock_t;
typedef std::unique_lock< lock_t >  WriteLock;
typedef std::shared_lock< lock_t >  ReadLock;

extern char membuf[NUM_PAGES * PG_SIZE] __attribute__((aligned(PG_SIZE)));

class PTEntry {
public:
  PTEntry(uint64_t pgidx, ClusterManager* cluster_manager, Invalidator* invalidator, std::vector<Addr>* servers_vec);

  grpc::Status request_read(Addr & client_id, task_id t_id, dsm::Page & retpage);
  grpc::Status request_write(Addr & client_id, task_id t_id, dsm::Page & retpage);

  //bool lock();
  //bool unlock();

  /* Return the address of a worker that has this page cached, if any. */
  std::optional<uint32_t> cached_at();
  bool is_present();
  
  void set_url(std::string url) { url_ = url; }
  void set_byte_start(int64_t byte_start) { byte_start_ = byte_start; }
  void set_byte_end(int64_t byte_end) { byte_end_ = byte_end; }
  void set_local(bool local) { local_ = local; }
  
  /* cannot undo once set */
  void set_id(task_id id) {
    assert( id_ == task_id::UNASSIGNED_ID );
    id_ = id;
  }

  void set_id(int64_t stage_id, int64_t partition_id) {
    assert( id_ == task_id::UNASSIGNED_ID );
    id_.stage_id = stage_id;
    id_.partition_id = partition_id;
  }
  
  void set_perm(dsm::Permission perm) { perm_ = perm; }

  uint64_t num_readers() const { return readers_.size(); }
  uint64_t num_writers() const { return writers_.size(); }

  void add_writer(uint32_t writer) { writers_.insert(writer); }
  void remove_writer(uint32_t writer) { writers_.erase(writer); }
  void add_reader(uint32_t reader) {
    readers_.insert(reader);
    //  readers_vec_.push_back(reader);
  }

  void remove_reader(uint32_t reader) {
    readers_.erase(reader);
    /*while ( true ) {
      try {
	uint32_t reader_idx = Util::vec_idx(reader, &readers_vec_);
	readers_vec_.erase(readers_vec_.begin() + reader_idx);
      } catch ( const std::invalid_argument & e ) {
	SPDLOG_DEBUG("Erased all instances of idx {:d}", reader);
	break;
      }
      }*/
  }

  bool has_writer(Addr writer) { return Util::set_contains(Util::vec_idx(writer, servers_vec_), writers_); }
  bool has_reader(Addr reader) { return Util::set_contains(Util::vec_idx(reader, servers_vec_), readers_); } 
  
  void set_written() { written_once_ = true; } // cannot undo!

  uint64_t pgidx() const { return pgidx_; }
  const std::string url() const { return url_; }
  int64_t byte_start() const { return byte_start_; }
  int64_t byte_end() const { return byte_end_; }
  bool local() const { return local_; }
  const task_id id() const { return id_; }
  const dsm::Permission perm() const { return perm_; }

  bool in_driver() const { return in_driver_; } 
  bool written_once() const { return written_once_; }
  
  void construct_response(dsm::Page* response);

  /* Retrieve page contents from some one cached location without invalidating the local cache. */
  grpc::Status retrieve_page(dsm::Page & retpage, Addr & requester_id);

  /* Request a cache invalidation from all readers in order to give permission to a writer. */
  grpc::Status invalidate_readers(dsm::Page & retpage, Addr & requester_id);

  /* Request a cache invalidation from all writers in order to give permission to a 
   * reader or writer. If read = true, then the requester is a reader and the writer can keep the page
   * cached as read-only locally. If read = false, then the requester is a writer and writers should
   * invalidate their local copy.  */
  grpc::Status invalidate_writers(dsm::Page & retpage, Addr & requester_id,
				  bool clear_cache, bool send_data=true);

  grpc::Status mark_read_only(Addr & requester_id, task_id t_id);
  
  bool has_id(task_id tid) const { return id_ == tid; }

  uint64_t pgnum() { return PageUtils::pgidx_to_absolute_pgnum(pgidx_); }
  
  void update_cache_locs();
  
  //std::optional<std::string> cached_pagedata;
  std::string cached_pagedata = "";

  void clear_pagedata() {
    //cached_pagedata.value().clear();
    //cached_pagedata.value().shrink_to_fit();
    //cached_pagedata = std::nullopt;
    cached_pagedata = "";
  }
  
  lock_t lock_m_;

 protected:
  uint64_t pgidx_;
  std::string url_ = ""; /* source URL if downloading from S3 */

  /* position in the data */
  int64_t byte_start_ = 0;
  int64_t byte_end_ = 0;

  task_id id_ = task_id::UNASSIGNED_ID;
  
  dsm::Permission perm_ = dsm::Permission::NONE;
  //pthread_mutex_t lock_;
  std::unordered_set<uint32_t> writers_;
  std::unordered_set<uint32_t> readers_; /* unique identifier is the public hostname */

  bool in_driver_ = false;
  bool written_once_ = false;

  bool local_ = false;

  Invalidator* invalidator_;
  ClusterManager* cluster_manager_;
  std::vector<Addr>* servers_vec_;

  /**
   * Functions to process page lock changes.
   */
  virtual grpc::Status request_read_from_s3    (Addr & client_id, task_id t_id, dsm::Page & retpage);
  virtual grpc::Status request_read_from_read  (Addr & client_id, task_id t_id, dsm::Page & retpage);
  virtual grpc::Status request_read_from_write (Addr & client_id, task_id t_id, dsm::Page & retpage);
  virtual grpc::Status request_read_from_none  (Addr & client_id, task_id t_id, dsm::Page & retpage);
  virtual grpc::Status request_read_from_ro    (Addr & client_id, task_id t_id, dsm::Page & retpage);

  virtual grpc::Status request_write_from_s3    (Addr & client_id, task_id t_id, dsm::Page & retpage);
  virtual grpc::Status request_write_from_read  (Addr & client_id, task_id t_id, dsm::Page & retpage);
  virtual grpc::Status request_write_from_write (Addr & client_id, task_id t_id, dsm::Page & retpage);
  virtual grpc::Status request_write_from_none  (Addr & client_id, task_id t_id, dsm::Page & retpage);
  
  /* Requesting write access from read-only is valid if the page was lost and is being reconstructed.
   * This type of request can also happen if a straggler tries to write the page after the page
   * has already been written by a faster replica, in which case the straggler should be aborted. */
  virtual grpc::Status request_write_from_ro    (Addr & client_id, task_id t_id, dsm::Page & retpage);
};

class DriverPTEntry : public PTEntry {
 public:
  DriverPTEntry(uint64_t pgnum, ClusterManager* cluster_manager, Invalidator* invalidator, std::vector<Addr>* servers_vec,
                Addr driver_addr);

  grpc::Status retrieve_page(dsm::Page & retpage, Addr & requester_id);

  void persist_to_disk(uint64_t stage_id, std::vector<char>);
  void read_from_disk(uint64_t stage_id);

  void mark_cow() {
    is_cow_ = true;
  }
  
 private:
  bool in_driver = true;
  Addr driver_addr_;

  bool is_cow_ = false;

  std::unordered_map<uint64_t, std::vector<char>> versions_;

  /* Return random reader that is not driver itself */
  Addr get_random_reader();

  /**
   * Functions to process page lock changes.
   */
  virtual grpc::Status request_read_from_s3    (Addr & client_id, task_id t_id, dsm::Page & retpage) override;
  virtual grpc::Status request_read_from_read  (Addr & client_id, task_id t_id, dsm::Page & retpage) override;
  virtual grpc::Status request_read_from_write (Addr & client_id, task_id t_id, dsm::Page & retpage) override;

  virtual grpc::Status request_write_from_read  (Addr & client_id, task_id t_id, dsm::Page & retpage) override;
  virtual grpc::Status request_write_from_write (Addr & client_id, task_id t_id, dsm::Page & retpage) override;
  virtual grpc::Status request_write_from_none  (Addr & client_id, task_id t_id, dsm::Page & retpage) override;
  
  /* Requesting write access from read-only is valid if the page was lost and is being reconstructed.
   * This type of request can also happen if a straggler tries to write the page after the page
   * has already been written by a faster replica, in which case the straggler should be aborted. */
  virtual grpc::Status request_write_from_ro    (Addr & client_id, task_id t_id, dsm::Page & retpage) override;
};

#endif // PAGE_H
