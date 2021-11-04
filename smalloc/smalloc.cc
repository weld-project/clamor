/* malloc + free for shared memory */

#include <assert.h>
#include <error.h>
#include <pthread.h>
#include <signal.h>
#include <stdalign.h>
#include <stdbool.h>
#include <string.h>

#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "smalloc.h"
#include "mem.h"

using namespace std;

pthread_mutex_t heapl; /* just lock the entire heap for now */

// we are writing C++. no linked lists here
unordered_map<uint64_t, vector<memblock*>> free_blocks; 

int freelist_start = 3;
int freelist_end = 20;

void init_free_lists() {
  // reinitialize the free lists
  for ( uint64_t i = freelist_start; i < freelist_end; i++ ) {
    vector<memblock*> blocks;
    free_blocks.emplace((uint64_t)(pow(2, i)), blocks);
  }
}

memblock* data_to_memblock(void* ptr) {
  return (memblock*)((char*)ptr - sizeof(memhdr)); /* TODO: not sure this subtraction works. */
}

void cleanup_heap_locks() {
  printf("Exec atexit cleanup hook\n");
  // pthread_mutex_unlock(&heapl);
}

/* Allocate the requested number of bytes.
 */
void* smalloc(size_t nbytes) {
  //printf("smalloc request %ld bytes\n", nbytes);
  memblock* cur_block = (memblock*)(heap);
  size_t tail_data_size = (cur_block->header).data_size;
  size_t tail_block_size = sizeof(memhdr) + tail_data_size;

  memblock* next_block = (memblock*)((char*)heap + tail_block_size);
  
  (next_block->header).prev = cur_block;
  (next_block->header).next = NULL;
  (next_block->header).data_size = nbytes;
  (next_block->header).aligned = false;
  next_block->data = NULL;

  (cur_block->header).next = next_block;

  //printf("Data address: %x, num bytes %ld\n", &(next_block->data), nbytes);
  //printf("Memblock data address: %x, heap %x\n", (void*)(next_block), (void*)heap);
  
  heap = next_block;
  if (heap > heap_end) {
    printf("smalloc failed: not enough space in local heap!\n");
    exit(1);
  }

  return &(next_block->data);
}

/* data should be a pointer to the start of a data region in an allocated block. */
void* srealloc(void* ptr, size_t nbytes) {
  memblock* old_block = data_to_memblock(ptr); 
  memblock* prev_block = (old_block->header).prev;
  memblock* next_block = (old_block->header).next;

  size_t old_size = (old_block->header).data_size;
  
  void* new_data = NULL;
  
  if ( next_block == NULL ) { 
    /* this is the last block in the list so we can just change the size */
    (old_block->header).data_size = nbytes;
    new_data = ptr;
  } else if ( old_size >= nbytes ) { 
    /* contracting the block */
    (old_block->header).data_size = nbytes;
    new_data = ptr;
  } else {
    /* not the last block; just allocate a new block */
    bool was_aligned = (old_block->header).aligned;
    if ( was_aligned ) {
      new_data = smalloc_aligned(nbytes);
    } else {
      new_data = smalloc(nbytes);
    }
    fprintf(stdout, "**** Allocated new block at %p ****\n", new_data);
    
    if ( old_size < nbytes ) { /* contracted the block */
      memcpy(new_data, ptr, old_size);
    } else {
      memcpy(new_data, ptr, nbytes);
    }

    fprintf(stdout, "**** Completed memcpy ****\n");

    /* We could free the old block here since we allocated a new block, but right now we don't. */
    (prev_block->header).next = data_to_memblock(new_data);
    if ( next_block != NULL ) {
      (next_block->header).prev = data_to_memblock(new_data);
    }
  }

  assert( new_data != NULL );
  
  return new_data;
}

bool is_aligned(uintptr_t addr) {
  return (PGADDR(addr) == addr);
}

memblock* get_free_block(size_t nbytes, size_t alignment) {
  //printf("nbytes %ld, idx %d, size %ld align %ld\n", nbytes, idx, free_blocks.size(), alignment);
  auto idx = free_blocks.find(nbytes);
  if ( idx != free_blocks.end() ) {
    auto & blocks = free_blocks.at(nbytes);
    if ( blocks.size() > 0 ) {
      auto it = blocks.begin();
      memblock* ret = *it;
      // find block with correct alignment
      while ( it != blocks.end() ) {
	ret = *it;
	if ( ret->header.alignment == alignment ) {
	  break;
	}
	it++;
      }
      if ( it != blocks.end() ) {
	blocks.erase(it);
	return ret;
      } else {
	return NULL;
      }
    }
  } else {
    return NULL;
  }
}

void* smalloc_byte_aligned(size_t nbytes, size_t alignment) {
  //printf("Requested bytes %ld, alignment %ld\n", nbytes, alignment);

  memblock* free_block = get_free_block(nbytes, alignment);
  if ( free_block != NULL ) {
    return &(free_block->data);
  }
  
  memblock* cur_block = (memblock*)(heap);
  size_t tail_data_size = (cur_block->header).data_size;
  size_t tail_block_size = sizeof(memhdr) + tail_data_size;

  // get projected location for next data allocation
  uintptr_t next_block_addr = (uintptr_t)((char*)(cur_block) +
					  sizeof(memhdr) +
					  (cur_block->header).data_size);
  uintptr_t next_data_addr = next_block_addr + sizeof(memhdr);
  uintptr_t aligned_addr = (next_data_addr + alignment-1) & ~(alignment-1);

  memblock* next_block = (memblock*)(aligned_addr - sizeof(memhdr)); // page aligned block
  
  (next_block->header).prev = cur_block;
  (next_block->header).next = NULL;
  (next_block->header).data_size = nbytes;
  (next_block->header).aligned = false;
  (next_block->header).alignment = alignment;
  next_block->data = NULL;

  (cur_block->header).next = next_block;
  
  heap = next_block;
  if (heap > heap_end) {
    printf("smalloc failed: not enough space in local heap!\n");
    exit(1);
  }

  return &(next_block->data);
}

/* Allocate the requested number of bytes, aligned to page boundary. */
void* smalloc_aligned(size_t nbytes) {
  memblock* cur_block = (memblock*)(heap);

  // get projected location for next data allocation
  uintptr_t next_block_addr = (uintptr_t)((char*)(cur_block) +
					  sizeof(memhdr) +
					  (cur_block->header).data_size);
  uintptr_t next_data_addr = next_block_addr + sizeof(memhdr);

  uintptr_t pgnum = (uintptr_t)PGADDR_TO_PGNUM(next_data_addr);
  uintptr_t aligned_addr = PGNUM_TO_PGADDR(pgnum);

  if ( aligned_addr < next_data_addr ) pgnum++; // align data to next page boundary
  aligned_addr = PGNUM_TO_PGADDR(pgnum);

  if ( (aligned_addr - next_block_addr) < sizeof(memhdr) ) { // can't fit header in remaining space
    pgnum++;
    aligned_addr = PGNUM_TO_PGADDR(pgnum);
  }

  memblock* next_block = (memblock*)(aligned_addr - sizeof(memhdr)); // page aligned block
  //fprintf(stderr, "got blocks\n");
  (cur_block->header).next = next_block;
  (next_block->header).prev = cur_block;
  (next_block->header).next = NULL;
  (next_block->header).data_size = nbytes;
  (next_block->header).aligned = true;
  
  size_t rem = nbytes % PG_SIZE;
  if ( rem != 0 ) {
    (next_block->header).data_size += (PG_SIZE - rem);
  }
  
  next_block->data = NULL;
  
  heap = next_block;
  if (heap > heap_end) {
    printf("smalloc_aligned failed: not enough space in local heap!\n");
    //    pthread_mutex_unlock(&heapl);
    exit(1);
  }
    
  return &(next_block->data);
}

/* Free smalloc'd data.
 * Returns 0 on success, -1 if the requested pointer is not a valid pointer to free (?).
 * TODO: make free blocks available for reuse.
 */
int sfree(void* ptr) {
  //return 0; // Do nothing.
  return sfree_reuse(ptr);
}

int sfree_reuse(void* ptr) {
  //printf("In sfree reuse\n");
  memblock* req_block = data_to_memblock(ptr); /* TODO: not sure this subtraction works. */
  //printf("Freeing bytes %ld, alignment %ld\n", (req_block->header).data_size, (req_block->header).alignment);
  
  memblock* prev_block = (req_block->header).prev;
  memblock* next_block = (req_block->header).next;

  auto nbytes = (req_block->header).data_size;
  auto idx = free_blocks.find(nbytes);
  if ( idx == free_blocks.end() ) {
    vector<memblock*> blocks;
    free_blocks.emplace(nbytes, blocks);
  }
  auto & freelist = free_blocks.at(nbytes);
  freelist.push_back(req_block);
  
  /*if ( prev_block != NULL ) {
    (prev_block->header).next = next_block;
  }
  if ( next_block != NULL ) {
    (next_block->header).prev = prev_block;
    }*/

  return 0;
}

void* py_smalloc(void* ctx, size_t nbytes) {
  return smalloc(nbytes);
}

void* py_srealloc(void* ctx, void* ptr, size_t nbytes) {
  if ( ptr == NULL ) {
    // According to the spec for PyMem_Realloc, we expect a call to realloc on a null pointer
    // to be equivalent to a call to malloc.
    return py_smalloc(ctx, nbytes);
  }

  return srealloc(ptr, nbytes);
}

void* py_scalloc(void* ctx, size_t nelements, size_t elementsize) {
  size_t nbytes = nelements * elementsize; // TODO safe multiply here to prevent overflow
  void* ret_data = py_smalloc(ctx, nbytes);
  return memset(ret_data, 0, nbytes);
}

void py_sfree(void* ctx, void* ptr) {
  if ( ptr == NULL ) {
    // According to spec for PyMem_Free, do nothing.
    //    pthread_mutex_unlock(&heapl);
    return;
  }

  sfree(ptr);
}

/* store location of heap, to use in calls to malloc */
int init_heap(void* start, void* end) {
  printf("entering init\n");
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE); // Primarily for realloc

  pthread_mutex_init(&heapl, &attr);
  //  pthread_mutex_lock(&heapl);

  memblock* m = (memblock*)(start);
  (m->header).prev = NULL;
  (m->header).next = NULL;
  m->data = NULL;
  
  heap = start;
  //max_heap_size = max_size;
  heap_end = end;
  
  init_free_lists();

  //  pthread_mutex_unlock(&heapl);
  return 0;
}

int restart_heap(void* start, void* end) {
  printf("entering restart\n");
  //pthread_mutex_lock(&heapl);
  printf("set heap\n");
  memblock* m = (memblock*)(start);
  (m->header).prev = NULL;
  (m->header).next = NULL;
  m->data = NULL;
  
  heap = start;
  heap_end = end;
  
  init_free_lists();

  printf("unlock heap\n");
  //pthread_mutex_unlock(&heapl);
  printf("return\n");
  return 0;
}

void free_heap() {
  pthread_mutex_destroy(&heapl);
}
