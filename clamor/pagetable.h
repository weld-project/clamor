#ifndef PAGETABLE_H
#define PAGETABLE_H

#include <memory>
#include <vector>

#include "invalidator.h"
#include "page.h"
#include "worker-pool.h"

class PageTable {
 public:
  PageTable(ClusterManager* cluster_manager, Invalidator* invalidator, std::vector<Addr>* servers_vec, Addr driver_addr);

  bool page_present(uint64_t pgidx);
  bool all_pages_present();

  PTEntry & get_page_absolute(uint64_t absolute_pgnum);
  PTEntry & get_page_relative(uint64_t pgidx);
  PTEntry & get_page_from_addr(uintptr_t addr);

  //bool lock_page(uint64_t pgidx);
  //bool unlock_page(uint64_t pgidx);

  bool map_page_S3(uint64_t pgidx, std::string url, uint64_t byte_start, uint64_t byte_end, bool local=false);

  void update_cache_locs(uint64_t pgidx);

  /* mark driver pages as copy on write */
  void mark_cow();
  
 private:
  std::vector<std::unique_ptr<PTEntry>> pagetable_;

  ClusterManager* cluster_manager_;
};

#endif //PAGETABLE_H

