#include "pagetable.h"

#include "page-utils.h"

#include <iostream>

using namespace std;

extern "C" {
  #include "smalloc/mem.h"
}


PageTable::PageTable(ClusterManager* cluster_manager,
                     Invalidator* invalidator,
		     vector<Addr>* servers_vec,
                     Addr driver_addr) :
  pagetable_(),
  cluster_manager_(cluster_manager)
{
  for (uint64_t i = 0; i < DRIVER_PAGES; i++) {
    pagetable_.emplace_back(new DriverPTEntry(i, cluster_manager, invalidator, servers_vec, driver_addr));
  }

  for (uint64_t i = DRIVER_PAGES; i < NUM_PAGES; i++) {
    pagetable_.emplace_back(new PTEntry(i, cluster_manager, invalidator, servers_vec));
  }
}

/*bool PageTable::lock_page(uint64_t pgidx) {
  bool locked = pagetable_.at(pgidx)->lock();
  return locked;
}

bool PageTable::unlock_page(uint64_t pgidx) {
  return pagetable_.at(pgidx)->unlock();
  }*/

/*void PageTable::start_timer(uint64_t pgidx) {
  pagetable_.at(pgidx)->start_timer();
}

void PageTable::end_timer(uint64_t pgidx) {
  pagetable_.at(pgidx)->end_timer();
  }*/

PTEntry & PageTable::get_page_absolute(uint64_t absolute_pgnum) {
  return *(pagetable_.at(PageUtils::absolute_pgnum_to_pgidx(absolute_pgnum)));
}

PTEntry & PageTable::get_page_relative(uint64_t pgidx) {
  SPDLOG_DEBUG("Getting idx {:x} from pagetable size {:x}", pgidx, pagetable_.size());
  return *(pagetable_.at(pgidx));
}

PTEntry & PageTable::get_page_from_addr(uintptr_t addr) {
  return get_page_relative(PageUtils::addr_to_pgidx(addr));
}

bool PageTable::all_pages_present() {
  bool all_present = true;
  for ( uint64_t i = DRIVER_PAGES; i < NUM_PAGES; i++ ) {
    auto & page = get_page_relative(i);
    if ( page.written_once() ) { // this page might be a dependency as it was written by an earlier stage
      bool present = page.is_present();
      if ( not present ) {
        SPDLOG_DEBUG("Page {:x} with id {:s} not present", i, page.id().str(), present);
      }
      all_present &= present;
    }
  }

  return all_present;
}

bool PageTable::map_page_S3(uint64_t pgidx, string url, uint64_t byte_start,
			    uint64_t byte_end, bool local) {
  auto & page = get_page_relative(pgidx);

  WriteLock(page.lock_m_);
  
  //page.lock();

  page.set_perm(dsm::Permission::S3);
  page.set_url(url);
  page.set_byte_start(byte_start);
  page.set_byte_end(byte_end);
  page.set_local(local);

  //page.unlock();
}

bool PageTable::page_present(uint64_t pgidx) {
  auto & page = get_page_relative(pgidx);
  return page.is_present();
}

void PageTable::mark_cow() {
  for (uint64_t i = 0; i < DRIVER_PAGES; i++) {
    static_cast<DriverPTEntry*>(pagetable_.at(i).get())->mark_cow();
  }
}
