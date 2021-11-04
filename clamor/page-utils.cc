#include "page-utils.h"

extern "C" {
  #include "smalloc/mem.h"
}

#include <utility>

using namespace std;

uint64_t PageUtils::absolute_pgnum_to_pgidx(uint64_t absolute_pgnum) {
  return absolute_pgnum - START_PG;
}

uint64_t PageUtils::addr_to_pgidx(uintptr_t addr) {
  uintptr_t pgnum = PGADDR_TO_PGNUM(addr); 
  uintptr_t pgidx = absolute_pgnum_to_pgidx(pgnum);
  return pgidx;
}

uint64_t PageUtils::pgidx_to_absolute_pgnum(uint64_t pgidx) {
  return pgidx + START_PG;
}

uint64_t PageUtils::pgidx_to_addr(uint64_t pgidx) {
  uintptr_t pgnum = pgidx_to_absolute_pgnum(pgidx);
  return PGNUM_TO_PGADDR(pgnum);
}

bool PageUtils::page_in_driver(uint64_t pgnum) {
  return absolute_pgnum_to_pgidx(pgnum) < DRIVER_PAGES;
}
  
