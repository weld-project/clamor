#ifndef PAGE_UTILS_H
#define PAGE_UTILS_H

#include "fault-handler.h"

#include <vector>

namespace PageUtils {
  static const uintptr_t BUF_START = (uintptr_t)(&membuf);
  static const uint64_t START_PG = (uint64_t)(PGADDR_TO_PGNUM(BUF_START));

  uint64_t absolute_pgnum_to_pgidx(uint64_t absolute_pgnum);
  uint64_t pgidx_to_absolute_pgnum(uint64_t pgidx);
  uint64_t addr_to_pgidx(uintptr_t addr);
  uint64_t pgidx_to_addr(uint64_t pgidx);
  bool page_in_driver(uint64_t pgnum);
}

#endif // PAGE_UTILS_H
