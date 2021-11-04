#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <new>

template<typename A, typename T, typename U>
struct KdTree;

struct Payload {
  uint8_t arr[2048];
};

struct ResTuple {
  uintptr_t point_idx;
  double dist;
};

extern "C" {

KdTree<double, uintptr_t, double[3]> *kdtree_create(uint64_t size);

KdTree<double, Payload, double[2]> *kdtree_from_csv();

ResTuple kdtree_lookup(KdTree<double, Payload, double[2]> *kdtree, const double (*idx)[2]);

} // extern "C"
