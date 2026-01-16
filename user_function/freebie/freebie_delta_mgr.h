/* ----------------------------------------------------------------------------
 * FreeBIE Delta manager
 * -------------------------------------------------------------------------- */

#ifndef FREEBIE_DELTA_MGR_H
#define FREEBIE_DELTA_MGR_H

#include "parquet.h"


// This data structure is created per ETL process
typedef struct _FreeBIEDeltaMgr FreeBIEDeltaMgr;
struct _FreeBIEDeltaMgr
{
  GPtrArray *schema;
  guint rid_map;
  guint8 *base_ptr;
  int delta_list_entry_pool_idx;
};

#define DELTA_LIST_ENTRY_POOL_SIZE    (400000)
#define DELTA_LIST_ENTRY_INVALID_IDX  (0) // 1-base

typedef struct _DeltaList DeltaList;
struct _DeltaList
{
  guint head_idx;
  guint tail_idx;
  int delta_list_entry_pool_idx;
};

#define DELTA_HT_BUCKET_MASK (0x3FF)
#define DELTA_HT_BUCKET_SIZE (DELTA_HT_BUCKET_MASK + 1) // 1024

typedef struct _DeltaHT DeltaHT;
struct _DeltaHT
{
  DeltaList buckets[DELTA_HT_BUCKET_SIZE];
  guint iter_bucket_idx;
};

typedef struct _FreeBIEDeltaEntry FreeBIEDeltaEntry;
struct _FreeBIEDeltaEntry
{
  guint32 len;
  guint32 value_map;
  guint32 part_key;
  /* -- header -- */

  guint8 data[];
};

typedef struct _FreeBIEDeltaListEntry FreeBIEDeltaListEntry;
struct _FreeBIEDeltaListEntry
{
  guint next_idx;

  guint row_idx;
  gint delta_entry_offset; // Can be negative
};

#define freebie_delta_mgr_get_entry(mgr, arg) \
    ((FreeBIEDeltaEntry *)(((mgr)->base_ptr) + (arg)->delta_entry_offset))


/* Delta list entry pool ---------------------------------------------------------- */
void check_delta_list_entry_pool (void);
void delta_list_entry_pool_init (void);

/* Delta Mgr ---------------------------------------------------------- */

FreeBIEDeltaMgr *freebie_delta_mgr_init (GPtrArray *_schema, guint _rid_map);

void freebie_delta_mgr_finish(FreeBIEDeltaMgr *mgr);

gboolean freebie_delta_mgr_apply_entry (FreeBIEDeltaMgr *mgr, Value *dst,
                                        FreeBIEDeltaListEntry *src,
                                        guint col_idx);

gboolean freebie_delta_mgr_is_tombstone (FreeBIEDeltaMgr *mgr, FreeBIEDeltaListEntry *src);

/* Delta hashtable ---------------------------------------------------------- */

void freebie_delta_mgr_ht_init (DeltaHT **delta_ht, int pool_idx);

void freebie_delta_mgr_ht_close (DeltaHT **delta_ht);

void freebie_delta_mgr_ht_put (FreeBIEDeltaMgr *mgr, DeltaHT *delta_ht, Value *delta_entry);

FreeBIEDeltaListEntry *freebie_delta_mgr_ht_find_and_extract (FreeBIEDeltaMgr *mgr,
                                                              DeltaHT *delta_ht, Value *key);

FreeBIEDeltaListEntry *freebie_delta_mgr_ht_get_next (DeltaHT *delta_ht,
                                                      FreeBIEDeltaListEntry *iter);

/* Delta list --------------------------------------------------------------- */

void freebie_delta_mgr_list_init (DeltaList **delta_list, int pool_idx);
void freebie_delta_mgr_list_close (DeltaList **delta_list);
void freebie_delta_mgr_list_put (DeltaList *delta_list,
                                  FreeBIEDeltaListEntry *delta_list_entry,
                                  guint row_idx);
FreeBIEDeltaListEntry *
freebie_delta_mgr_list_get_next (DeltaList *delta_list, FreeBIEDeltaListEntry *iter);
#endif /* FREEBIE_DELTA_MGR_H */
