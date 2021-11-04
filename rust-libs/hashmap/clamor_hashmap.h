#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <new>

template<typename K, typename V, typename S>
struct HashMap;

struct KeyHasher;

extern "C" {

HashMap<uint64_t, uint64_t, KeyHasher> *hm_create(uint64_t size);

uint64_t hm_lookup(HashMap<uint64_t, uint64_t, KeyHasher> *hm, uint64_t idx);

void hm_update(HashMap<uint64_t, uint64_t, KeyHasher> *hm, uint64_t idx);

} // extern "C"
