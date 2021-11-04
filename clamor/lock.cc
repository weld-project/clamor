#include "lock.h"

#include "debug.h"

bool Lock::init_default(pthread_mutex_t* lock) {
  int err = pthread_mutex_init(lock, NULL);
  if ( err < 0 ) SPDLOG_DEBUG("Lock initialization failed with {:d}", err);
  return ( err == 0 );
}

bool Lock::init_recursive(pthread_mutex_t* lock) {
  pthread_mutexattr_t attr;

  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(lock, &attr);

  int err = pthread_mutex_init(lock, &attr);
  if ( err < 0 ) SPDLOG_DEBUG("Lock initialization failed with {:d}", err);
  return ( err == 0 );
}

bool Lock::lock(pthread_mutex_t* lock) {
  int err = pthread_mutex_lock(lock);
  if ( err < 0 ) SPDLOG_DEBUG("Lock failed with {:d}", err);
  return ( err == 0 );
}

bool Lock::unlock(pthread_mutex_t* lock) {
  int err = pthread_mutex_unlock(lock);
  if ( err < 0 ) SPDLOG_DEBUG("Unlock failed with {:d}", err);
  return ( err == 0 );
}
