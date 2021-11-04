#ifndef MEM_H
#define MEM_H

#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#define PG_BYTES 20
#define PG_SIZE (1 << (PG_BYTES)) /* 32 normal 4096-byte pages */

#define GB_TO_BYTES (1 << 30)

#ifdef clamor_local // allocate less memory for local testing
#define DRIVER_BYTES (15L * GB_TO_BYTES)
#define WORKER_BYTES (40L * GB_TO_BYTES)
#else
#define DRIVER_BYTES (150L * GB_TO_BYTES)
#define WORKER_BYTES (6000L * GB_TO_BYTES)
#endif

#define DRIVER_PAGES (DRIVER_BYTES / PG_SIZE)
#define WORKER_PAGES (WORKER_BYTES / PG_SIZE)

#define NUM_PAGES (DRIVER_PAGES + WORKER_PAGES)

// Convert address to the start of the page.
static inline uintptr_t PGADDR(uintptr_t addr) {
  return addr & ~(PG_SIZE - 1);
}

// Convert page number to address of start of page.
static inline uint64_t PGNUM_TO_PGADDR(uintptr_t pgnum) {
  return pgnum << PG_BYTES;
}

// Convert page address to the page number.
static inline uint64_t PGADDR_TO_PGNUM(uintptr_t addr) {
  return addr >> PG_BYTES;
}
 
#endif // MEM_H
