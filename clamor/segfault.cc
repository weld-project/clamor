#include "segfault.h"

bool is_write_fault(void* v_ctx) {
  ucontext_t* ctx = (ucontext_t*)v_ctx; // cast back to ucontext
  return ctx->uc_mcontext.gregs[REG_ERR] & PG_WRITE;
}
