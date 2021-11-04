#ifndef S3UTILS_H
#define S3UTILS_H

#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

#define NATIONS 25
#define REGIONS 5
#define SUPPS_PER_SF 10000
#define PARTS_PER_SF 200000
#define CUSTOMERS_PER_SF 150000
#define ORDERS_PER_SF 1500000
#define LINE_ITEM_PER_SF 6000000

// macros for MPI, via https://stackoverflow.com/a/40808411
#include <stdint.h>
#include <limits.h>

#if SIZE_MAX == UCHAR_MAX
   #define my_MPI_SIZE_T MPI_UNSIGNED_CHAR
#elif SIZE_MAX == USHRT_MAX
   #define my_MPI_SIZE_T MPI_UNSIGNED_SHORT
#elif SIZE_MAX == UINT_MAX
   #define my_MPI_SIZE_T MPI_UNSIGNED
#elif SIZE_MAX == ULONG_MAX
   #define my_MPI_SIZE_T MPI_UNSIGNED_LONG
#elif SIZE_MAX == ULLONG_MAX
   #define my_MPI_SIZE_T MPI_UNSIGNED_LONG_LONG
#else
   #error "what is happening here?"
#endif

typedef enum {
    L_ORDERKEY = 0,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT,
} LineitemKeyNames;

typedef enum {
  O_ORDERKEY = 0,
  O_CUSTKEY,
  O_ORDERSTATUS,
  O_TOTALPRICE,
  O_ORDERDATE,
  O_ORDERPRIORITY,
  O_CLERK,
  O_SHIPPRIORITY,
  O_COMMENT,
} OrderKeyItens;

// Sorted by orderkey.
struct Lineitem {
  int orderkey;
  int suppkey;
  int quantity;
  int partkey;
  double extendedprice;
  double discount;
  double tax;
  int commitdate;
  int shipdate;
  int receiptdate;
  int shipinstruct;
  int shipmode;
  int returnflag;
  int linestatus;
};

struct Supplier {
  int suppkey;
};

// Sorted by orderkey.
struct Order {
  int orderkey;
  int custkey;
  int orderdate;
  int orderpriority;
  int shippriority;
};

struct Part {
  int partkey;
  int brand;
  int size;
  int container;
  int promo_str;
};

// Here index + 1 is the order key.
struct Customer {
  int mktsegment;
};

/** Sets SF from the command line parameters.
 *
 * @param argc num of arguments
 * @param argv command line arguments
 * @param SF scale factor
 *
 * @return true if successful else false
 */
bool load_sf(int argc, char** argv, int& SF);

/** Uses binary search to return index of element in array.
 *
 * @param arr array
 * @param len length of array
 * @param e element
 *
 * @return integer representing index of element in array. returns
 * -1 if element not found.
 */
int binary_search(int* arr, int len, int e);

/** Parse a date formated as YYYY-MM-DD into an integer, maintaining
 * sort order. Exits on failure.
 *
 * @param date a properly formatted date string.
 *
 * @return an integer representing the date. The integer maintains
 * ordering (i.e. earlier dates have smaller values).
 *
 */
int parse_date(const char* d);

/** Loads orders file.
 * If orders file is partitioned into many parts, each part can be loaded
 * in parallel.
 *
 * @param orders array of uninitialized orders
 * @param tbl file pointer to table
 * @param partition file partition being loaded, used to
 * offset into arders array
 * @param num_parts total number of partitions
 * @param sf scale factor
 *
 */
void load_orders(Order* orders, FILE* tbl, int partition, int num_parts, int sf);

/** Loads customers file.
 * If customers file is partitioned into many parts, each part can be loaded
 * in parallel.
 *
 * @param customers array of uninitialized customers
 * @param tbl file pointer to table
 * @param partition file partition being loaded, used to
 * offset into arders array
 * @param num_parts total number of partitions
 * @param sf scale factor
 *
 */
void load_customers(Customer* customers, FILE* tbl, int partition, int num_parts, int sf);

/** Loads lineitems file.
 * If lineitems file is partitioned into many parts, each part can be loaded
 * in parallel.
 *
 * @param lineitems array of uninitialized customers
 * @param tbl file pointer to table
 * @param offset offset into the lineitems array
 *
 */
int load_lineitems(Lineitem* lineitems, char* tbl, int offset);

/** Loads the parts file
 *  If parts file is partitioned into many parts, each part can be loaded
 *  in parallel.
 * @param parts array of unintialized parts
 * @param tbl file pointer to table
 * @param offset offset into the parts array
 */
void load_parts(Part* parts, FILE* tbl, int offset);

template<typename T>
void load_from_file(size_t start_idx, T* buf, size_t nitems, std::string fname);

#endif // S3UTILS_H
