#ifndef HEAP_ALLOCATOR_H
#define HEAP_ALLOCATOR_H

/* Bookkeeping for heap regions. */

#include <unordered_map>
#include <utility>
#include <vector>

extern "C" {
  #include "smalloc/mem.h"
}

class HeapAllocator {
public:
  /* We assume tasks are always split into the same number of data partitions,
   * so heap regions are allocated by partition ID.
   * Each partition ID starts with the same amount of potential heap space allocated. */
  HeapAllocator(uint64_t npartitions);

  // register the end of a task so we can move region pointer forward
  void end_task(uint64_t partition_id, uint64_t stage_id, uint64_t n_written_pages);

  uint64_t task_start_page(uint64_t partition_id, uint64_t stage_id);
  uint64_t task_end_page(uint64_t partition_id, uint64_t stage_id);
  uint64_t partition_end_page(uint64_t partition_id);
  std::pair<uint64_t, uint64_t> task_page_region(uint64_t partition_id, uint64_t stage_id);
  
private:
  uint64_t npartitions_;

  // {partition id, vector<stage start page>}
  std::unordered_map<uint64_t, std::vector<uint64_t>> task_regions;

  // {partition id, {partition start page, partition end page}}
  // this will not change once initialized
  std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> partition_regions;
};

#endif // HEAP_ALLOCATOR_H
