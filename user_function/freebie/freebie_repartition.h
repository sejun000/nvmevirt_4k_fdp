#ifndef FREEBIE_REPARTITION_H
#define FREEBIE_REPARTITION_H

#include "gerror.h"
#include "gtypes.h"
#include "freebie_delta_mgr.h"

#define ENABLE_HELPER_TASK

#define DELETE_RELATION_ID (16662)

#define MAX_PARALLEL_REPARTITION (8)

#ifdef ENABLE_HELPER_TASK
#define NR_HELPER_TASK 1
#else
#define NR_HELPER_TASK 0
#endif

/*
 * Reverse order (Normal column):
 *
 *  <- data_bytes -> <-- phdr_bytes ---> <-- def_bytes -->
 * +----------------+-------------------+-----------------+
 * |      Data      |    Page header    |    Def level    |
 * +----------------+-------------------+-----------------+
 * ↑
 * offset
 *
 * Reverse order (Delta column):
 *
 *  <- data_bytes -> <-- phdr_bytes ---> <-------------- def_bytes ------------->
 * +----------------+-------------------+------------------+---------------------+
 * |   Delta data   | Delta page header | Null page header | Null page def level |
 * +----------------+-------------------+------------------+---------------------+
 * ↑
 * offset
 */
typedef struct {
  guint offset;
  guint data_bytes;
  guint phdr_bytes;
  guint def_bytes;
} ReverseOrderInfo;

typedef struct __FreeBIESGPair {
  guint size;
  guint generation;
} FreeBIESGPair;

typedef struct __FreeBIERepartitionConfig FreeBIERepartitionConfig;
struct __FreeBIERepartitionConfig
{
  /* Configuration */
  guint spread_factor;
  guint last_level;

  /* Target partition info. */
  guint current_level;
  guint partition_no;
  guint generation_no;

  /*
   * Source/Destination partition file names
   * TODO: replace file names to extents
   */
  guint input_file_cnt;
  gchar **input_file_names;
  gchar **output_file_names;
};

typedef struct __FreeBIEShared FreeBIEShared;
struct __FreeBIEShared
{
  guint8 *child_idxs;
  DeltaHT *delta_ht;
  DeltaList *delta_list;
  FreeBIEDeltaMgr *delta_mgr;
  ReverseOrderInfo **reverse_order_info;
  GPtrArray *helper_readers;
  GPtrArray *helper_writers;
};

gboolean freebie_repartition (void *task);
gboolean freebie_repartition_with_delete (void *task);
gboolean freebie_helper_repartition (void *_task);

#endif /* FREEBIE_REPARTITION_H */
