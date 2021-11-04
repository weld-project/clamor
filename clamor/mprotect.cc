#include "mprotect.h"

#include <errno.h>

#include "debug.h"

extern "C" {
  #include "smalloc/mem.h"
}

uintptr_t map_aligned(void* start_addr, size_t bufsize) {
  int zero_fd = open("/dev/zero", O_RDONLY, 0644);
  void* buf_addr = start_addr;
  
  SPDLOG_DEBUG("{:x} {:d}\n", buf_addr, bufsize);

  void* mapped_addr = mmap(buf_addr,
			   bufsize,
			   PROT_NONE,
			   MAP_PRIVATE | MAP_FIXED,
			   zero_fd,
			   0);

  if ((mapped_addr < 0) || (mapped_addr == NULL)) {
    SPDLOG_DEBUG("mmap failed.\n");
    return 0;
  }
  
  assert (mapped_addr == buf_addr);
  return (uintptr_t)buf_addr;
}

bool protect_read(uintptr_t pagenum) {
  SPDLOG_DEBUG("Protecting {:x} as read", pagenum);
  void* addr = (void*)PGNUM_TO_PGADDR(pagenum);
  int ret = mprotect(addr, PG_SIZE, PROT_READ);
  if ( ret < 0 ) {
    int err = errno;
    SPDLOG_DEBUG("Failed to protect {:x} as read: {:d}: {:s}", pagenum, err, strerror(err));
  }
  return (ret == 0);
}

bool protect_write(uintptr_t pagenum) {
  SPDLOG_DEBUG("Protecting {:x} as write", pagenum);
  void* addr = (void*)PGNUM_TO_PGADDR(pagenum);
  int ret = mprotect(addr, PG_SIZE, PROT_READ | PROT_WRITE);
  if ( ret < 0 ) {
    int err = errno;
    SPDLOG_DEBUG("Failed to protect {:x} as write: {:d}: {:s}", pagenum, err, strerror(err));
  }
  return (ret == 0);
}

bool protect_none(uintptr_t pagenum) {
  SPDLOG_DEBUG("Protecting {:x} as none", pagenum);
  void* addr = (void*)PGNUM_TO_PGADDR(pagenum);
  int ret = mprotect(addr, PG_SIZE, PROT_NONE);
  if ( ret < 0 ) {
    int err = errno;
    SPDLOG_DEBUG("Failed to protect {:x} as none: {:d}: {:s}", pagenum, err, strerror(err));
  }
  return (ret == 0);
}

