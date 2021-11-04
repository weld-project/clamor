#ifndef MPROTECT_H
#define MPROTECT_H

#include <stdint.h>
#include <sys/mman.h>

/* Utilities for memory mapping */

/* Assumes an aligned start_addr. */
uintptr_t map_aligned(void* start_addr, size_t bufsize);

bool protect_read(uintptr_t pagenum);
bool protect_write(uintptr_t pagenum);
bool protect_none(uintptr_t pagenum);

#endif // MPROTECT_H
