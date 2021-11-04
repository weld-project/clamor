#ifndef SEGFAULT_H
#define SEGFAULT_H

/* Some utilities for handling page faults */

#include <signal.h>

const int PG_PRESENT = 0x1;
const int PG_WRITE = 0x2;

typedef void (*sigact_callback)(int, siginfo_t*, void*);

bool is_write_fault(void* v_ctx);

#endif
