#ifndef SMALLOC_H
#define SMALLOC_H

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <dlfcn.h>

struct memblock;

typedef struct meminfo {
  uintptr_t heap_start;
  uintptr_t heap_end;
} meminfo;

typedef struct memhdr {
  struct memblock* prev;
  struct memblock* next;
  size_t data_size;
  size_t alignment = 1;
  bool aligned; /* for realloc */
} memhdr;

/* Heap blocks. */
typedef struct memblock {
  memhdr header;
  void*  data;
} memblock;

memblock* data_to_memblock(void* ptr);

typedef struct free_list {
  memblock* free_list_head = NULL;
  memblock* free_list_tail = NULL;
} free_list;

static void* heap;
static size_t max_heap_size;
static void* heap_end;

void cleanup_heap_locks();

/* We link Weld with these functions. */
extern "C" void* smalloc(size_t nbytes);
extern "C" void* srealloc(void* ptr, size_t nbytes);
extern "C" void* smalloc_aligned(size_t nbytes);

/* Allocate the requested number of bytes, aligned to specified number of bytes.
 * Used by Rust libraries to allocate according to layout. 
 * TODO: implement srealloc_byte_aligned. */
extern "C" void* smalloc_byte_aligned(size_t nbytes, size_t alignment);

extern "C" memblock* get_free_block(size_t nbytes, size_t aligment);

extern "C" int   sfree(void* ptr);
extern "C" int   sfree_reuse(void* ptr);

/* Python has slightly different malloc signatures and semantics. */
void* py_smalloc(void* ctx, size_t nbytes);
void* py_srealloc(void* ctx, void* ptr, size_t nbytes);
void* py_scalloc(void* ctx, size_t nelements, size_t elementsize);

void  py_sfree(void* ctx, void* ptr);

// On stage boundaries, set to true; smalloc_aligned and set to false.
//extern int smalloc_new_stage;

//int   init_heap(void* start, size_t max_size);
extern "C" int   init_heap(void* start, void* end);
extern "C" int   restart_heap(void* start, void* end);
extern "C" void  free_heap();

#endif // SMALLOC_H
