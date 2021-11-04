#include "heap-allocator.h"

#include "debug.h"

#include <stdexcept>

using namespace std;

HeapAllocator::HeapAllocator(uint64_t npartitions)
  : npartitions_(npartitions)
{
  uint64_t pages_per_partition = WORKER_PAGES / npartitions;
  SPDLOG_DEBUG("Pages per partition: {:d} (num pages: {:d}, partitions: {:d})", pages_per_partition, WORKER_PAGES, npartitions);
  
  uint64_t cur = DRIVER_PAGES;
  for ( uint64_t i = 0; i < npartitions; i++ ) {
    vector<uint64_t> pages;
    pages.push_back(cur);
    task_regions.emplace(i, move(pages));

    pair<uint64_t, uint64_t> region(cur, cur + pages_per_partition);
    partition_regions.emplace(i, move(region));
    cur += pages_per_partition;
  }
}

void HeapAllocator::end_task(uint64_t partition_id, uint64_t stage_id,
			     uint64_t last_page_written) {
  auto & pages = task_regions.at(partition_id);
  if ( stage_id < pages.size() - 1 ) {
    // already wrote this task, don't update region
    return;
  }

  //uint64_t task_start_page = pages.at(pages.size() - 1);
  pages.push_back(last_page_written + 1); // next start addr
}

uint64_t HeapAllocator::task_start_page(uint64_t partition_id,
					uint64_t stage_id) {
  SPDLOG_DEBUG("entering start page");
  auto & pages = task_regions.at(partition_id); // error if no region allocated for this partition ID
  SPDLOG_DEBUG("got region partition, returning");
  return pages.at(stage_id); // error if invalid task ID
}

uint64_t HeapAllocator::partition_end_page(uint64_t partition_id) {
  return partition_regions.at(partition_id).second;
}

uint64_t HeapAllocator::task_end_page(uint64_t partition_id,
				      uint64_t stage_id) {
  SPDLOG_DEBUG("entering task end page");
  auto & pages = task_regions.at(partition_id); // error if no region allocated for this partition ID
  SPDLOG_DEBUG("got region partition id");
  if ( stage_id == pages.size() - 1) { // end of this task has not been previously registered
    SPDLOG_DEBUG("returning end of partition");
    return partition_end_page(partition_id);
  } else {
    SPDLOG_DEBUG("returning end of existing task");
    return pages.at(stage_id + 1); // error if invalid task ID
  }
}

pair<uint64_t, uint64_t> HeapAllocator::task_page_region(uint64_t partition_id, uint64_t stage_id) {
  return make_pair(task_start_page(partition_id, stage_id),
		   task_end_page(partition_id, stage_id));
} 
