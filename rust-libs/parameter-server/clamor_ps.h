#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <new>

template<typename K, typename V, typename S>
struct HashMap;

///! A prototype parameter server, implemented as a simple hash map.
struct KeyHasher;

extern "C" {

HashMap<uint64_t, double, KeyHasher> *ps_create(uint64_t size);

void ps_insert(HashMap<uint64_t, double, KeyHasher> *hm, uint64_t k, double v);

double ps_lookup(HashMap<uint64_t, double, KeyHasher> *hm, uint64_t idx);

///! Regularize the current weights in-place before adding gradient updates
void ps_regularize(HashMap<uint64_t, double, KeyHasher> *hm);

///! Updates weights in-place using aggregated gradients
void ps_update(HashMap<uint64_t, double, KeyHasher> *hm, uint64_t update_idx, double grad);

} // extern "C"
