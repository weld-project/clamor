#ifndef LOCK_H
#define LOCK_H

#include "pthread.h"

/* Wrappers for pthread_mutex_t locks. */

namespace Lock {
  bool init_default(pthread_mutex_t* lock);
  bool init_recursive(pthread_mutex_t* lock);
  bool lock(pthread_mutex_t* lock);
  bool unlock(pthread_mutex_t* lock);
}

#endif // LOCK_H 
