#include "freebie_delta_mgr.h"
#include "freebie_repartition.h"
#include "garray.h"
#include "gtypes.h"
#include "parquet_types.h"
#include "utils.h"

/* Delta manager ------------------------------------------------------------ */
static int __get_new_pool_idx(void);
static void __release_pool_idx (int pool_idx);

FreeBIEDeltaMgr *freebie_delta_mgr_init (GPtrArray *_schema, guint _rid_map)
{
  FreeBIEDeltaMgr *mgr = kmalloc_node(sizeof(FreeBIEDeltaMgr), GFP_KERNEL, 1);
  mgr->schema = _schema;
  mgr->rid_map = _rid_map;
  mgr->base_ptr = NULL;
  mgr->delta_list_entry_pool_idx = __get_new_pool_idx();

  return mgr;
}

void freebie_delta_mgr_finish (FreeBIEDeltaMgr *mgr)
{
  __release_pool_idx(mgr->delta_list_entry_pool_idx);
  kfree(mgr);
}

/* Delta entry -------------------------------------------------------------- */

static inline void delta_entry_get_rid (FreeBIEDeltaMgr *mgr, Value *dst, FreeBIEDeltaEntry *src)
{
  GPtrArray *schema = mgr->schema;

  for (guint i = 0; i < schema->len - 1; i++) {
    if ((mgr->rid_map >> i) & 0x1) {
      SchemaElement *schema_elem;

      schema_elem = g_ptr_array_index (schema, i + 1);

      switch (schema_elem->type) {
      case TYPE_INT32: value_set_i32 (dst, src->data); break;
      case TYPE_INT64: value_set_i64 (dst, src->data); break;
      default:
        g_assert (FALSE, "Unimplemented row id type");
      }

      break;
    }
  }
}

static inline void delta_entry_get_data (FreeBIEDeltaMgr *mgr, Value *dst, FreeBIEDeltaEntry *src,
                                        guint col_idx)
{
  guint value_map = mgr->rid_map | src->value_map;
  guint offset = 0;
  SchemaElement *schema_elem;
  GPtrArray *schema = mgr->schema;

  g_assert (col_idx < schema->len - 1, "Invalid column index");

  for (guint i = 0; i < col_idx; i++) {
    if ((value_map >> i) & 0x1) {
      schema_elem = g_ptr_array_index (schema, i + 1);
      if (schema_elem == NULL) {
        printk("Schema element is NULL\n");
        printk("i: %u\n", i);
        printk("schema->len: %u\n", schema->len);
        BUG();
      }

      switch (schema_elem->type) {
      case TYPE_BOOLEAN: offset++; break;
      case TYPE_INT32:
      case TYPE_FLOAT: offset += 4; break;
      case TYPE_INT64:
      case TYPE_DOUBLE: offset += 8; break;
      case TYPE_BYTE_ARRAY:
      {
        guint len = le_to_i32 (src->data + offset);
        offset += 4 + len;
        break;
      }
      default:
        g_assert (FALSE, "Unimplemented column type");
      }
    }
  }

  g_assert (offset + sizeof(src->value_map) < src->len, "Invalid offset");

  schema_elem = g_ptr_array_index (schema, col_idx + 1);
  if (schema_elem == NULL) {
    printk("Schema element is NULL\n");
    printk("col_idx: %u\n", col_idx);
    printk("schema->len: %u\n", schema->len);
    BUG();
  }

  switch (schema_elem->type)
  {
    case TYPE_BOOLEAN: value_set_bool (dst, src->data[offset]); break;
    case TYPE_INT32: value_set_i32 (dst, src->data + offset); break;
    case TYPE_INT64: value_set_i64 (dst, src->data + offset); break;
    case TYPE_FLOAT: value_set_float (dst, src->data + offset); break;
    case TYPE_DOUBLE: value_set_double (dst, src->data + offset); break;
    case TYPE_BYTE_ARRAY:
    {
      guint len;

      len = le_to_i32 (src->data + offset);
      value_set_byte_array (dst, src->data + offset, 4 + len);
      break;
    }
    default:
      g_assert (FALSE, "Unimplemented column type");
  }

}

gboolean freebie_delta_mgr_apply_entry (FreeBIEDeltaMgr *mgr, Value *dst,
                                        FreeBIEDeltaListEntry *src, guint col_idx)
{
  FreeBIEDeltaEntry *delta;

  /* Invalid entry */
  if (!src)
    return FALSE;

  g_assert (mgr->base_ptr, "Base pointer not initiated");
  delta = freebie_delta_mgr_get_entry (mgr, src);

  /* Not my column */
  if (!((delta->value_map >> col_idx) & 0x1))
    return FALSE;

  delta_entry_get_data (mgr, dst, delta, col_idx);

  return TRUE;
}

gboolean
freebie_delta_mgr_is_tombstone (FreeBIEDeltaMgr *mgr, FreeBIEDeltaListEntry *src)
{
  FreeBIEDeltaEntry *delta;

  /* Invalid entry */
  if (!src)
    return FALSE;

  g_assert (mgr->base_ptr, "Base pointer not initiated");
  delta = freebie_delta_mgr_get_entry (mgr, src);

  return delta->value_map == 0;
}

typedef gboolean (*delta_entry_compare_func_t) (FreeBIEDeltaMgr *mgr, FreeBIEDeltaListEntry *, Value *);

static inline gboolean delta_entry_compare_rid (FreeBIEDeltaMgr *mgr, FreeBIEDeltaListEntry *x, Value *y)
{
  FreeBIEDeltaEntry *delta;
  guint8 *delta_rid_ptr;
  guint8 *value_ptr;
  GPtrArray *schema = mgr->schema;

  g_assert (mgr->base_ptr, "Base pointer not initiated");
  delta = freebie_delta_mgr_get_entry (mgr, x);
  value_ptr = y->data.ptr;

  /* XXX: Assuming the row id data comes first */
  delta_rid_ptr = delta->data;

  for (guint i = 0; i < schema->len - 1; i++) {
    if ((mgr->rid_map >> i) & 0x1) {
      SchemaElement *schema_elem;

      schema_elem = g_ptr_array_index (schema, i + 1);

      /* XXX: Assuming we have only one row id */
      switch (schema_elem->type) {
      case TYPE_INT32:
          return le_to_i32 (delta_rid_ptr) == le_to_i32 (value_ptr);
      case TYPE_INT64:
          return le_to_i64 (delta_rid_ptr) == le_to_i32 (value_ptr);
      default:
        g_assert(FALSE, "Unimplemented row id type");
      }
    }
  }

  g_assert (FALSE, "Row id not exists");
  return FALSE;
}

/* Delta list entry pool ---------------------------------------------------- */
static FreeBIEDeltaListEntry delta_list_entry_pool[MAX_PARALLEL_REPARTITION][DELTA_LIST_ENTRY_POOL_SIZE];
static guint delta_list_entry_pool_free_head[MAX_PARALLEL_REPARTITION];
static guint delta_list_entry_pool_free_cnt[MAX_PARALLEL_REPARTITION];
spinlock_t delta_list_entry_pool_lock;
uint16_t delta_list_entry_pool_free_bitmap;

#define delta_list_entry_pool_get(pool_idx, idx) \
  (idx < 1 ? NULL \
   : idx > DELTA_LIST_ENTRY_POOL_SIZE ? NULL \
   : ((FreeBIEDeltaListEntry *)(&delta_list_entry_pool[pool_idx][idx - 1])))

#define delta_list_entry_get_pool_idx(pool_idx, entry) \
    ((entry) ? (guint)((entry) - &delta_list_entry_pool[pool_idx][0]) + 1 : 0)

static void __release_pool_idx(int pool_idx)
{
    if (pool_idx < 0 || pool_idx >= MAX_PARALLEL_REPARTITION)
        return;
    BUG_ON(delta_list_entry_pool_free_cnt[pool_idx] != DELTA_LIST_ENTRY_POOL_SIZE);

    spin_lock(&delta_list_entry_pool_lock);
    BUG_ON(!((delta_list_entry_pool_free_bitmap >> pool_idx) & 1));
    delta_list_entry_pool_free_bitmap &= ~((uint64_t)1 << pool_idx);
    spin_unlock(&delta_list_entry_pool_lock);
}

static int __get_new_pool_idx(void)
{
    int ret = -1;
    spin_lock(&delta_list_entry_pool_lock);

    for (int i = 0; i < MAX_PARALLEL_REPARTITION; i++) {
        if (((uint64_t)1 << i) & delta_list_entry_pool_free_bitmap)
            continue; // bit in use
        delta_list_entry_pool_free_bitmap |= ((uint64_t)1 << i);
        ret = i;
        break;
    }

    spin_unlock(&delta_list_entry_pool_lock);
    return ret; // -1 means no available index
}

void check_delta_list_entry_pool (void)
{
  for (int i = 0; i < MAX_PARALLEL_REPARTITION; i++) {
    if (delta_list_entry_pool_free_cnt[i] == DELTA_LIST_ENTRY_POOL_SIZE) {
      printk("DELTA_LIST_ENTRY_POOL[%d] is all freed\n", i);
    } else {
      printk("There is a leak in DELTA_LIST_ENTRY_POOL\n");
    }
  }
}

void delta_list_entry_pool_init (void)
{
  printk("Size of delta list entry pool: %lu\n", sizeof(delta_list_entry_pool));
  FreeBIEDeltaListEntry *delta;
  spin_lock_init(&delta_list_entry_pool_lock);

  for (int i = 0; i < MAX_PARALLEL_REPARTITION; i++) {
    for (gint idx = 1; idx < DELTA_LIST_ENTRY_POOL_SIZE; idx++) {
      delta = delta_list_entry_pool_get (i, idx);
      delta->next_idx = idx + 1;
      delta->delta_entry_offset = 0;
      delta->row_idx = 0;
    }

    delta = delta_list_entry_pool_get (i, DELTA_LIST_ENTRY_POOL_SIZE);
    delta->next_idx = DELTA_LIST_ENTRY_INVALID_IDX;
    delta->delta_entry_offset = 0;
    delta->row_idx = 0;

    delta_list_entry_pool_free_head[i] = 1;
    delta_list_entry_pool_free_cnt[i] = DELTA_LIST_ENTRY_POOL_SIZE;
  }
}

static inline FreeBIEDeltaListEntry *delta_list_entry_pool_pop (int pool_idx)
{
  FreeBIEDeltaListEntry *delta;
  gint32 idx;

  g_assert (delta_list_entry_pool_free_cnt[pool_idx] > 0, "delta list entry pool limit exceeded");

  idx = delta_list_entry_pool_free_head[pool_idx];
  delta = delta_list_entry_pool_get (pool_idx, idx);
  delta_list_entry_pool_free_head[pool_idx] = delta->next_idx;
  delta->next_idx = DELTA_LIST_ENTRY_INVALID_IDX;
  delta_list_entry_pool_free_cnt[pool_idx]--;

  return delta;
}

static void delta_list_entry_pool_push (FreeBIEDeltaListEntry *delta, int pool_idx)
{
  delta->next_idx = delta_list_entry_pool_free_head[pool_idx];
  delta_list_entry_pool_free_head[pool_idx] = delta_list_entry_get_pool_idx (pool_idx, delta);
  delta_list_entry_pool_free_cnt[pool_idx]++;
}

/* Delta list data structure mgmt ------------------------------------------- */

#define delta_list_get_next(pool_idx, list, iter) \
    (delta_list_entry_get_pool_idx(pool_idx, iter) != (list)->tail_idx \
     ? delta_list_entry_pool_get(pool_idx, (iter)->next_idx) \
     : NULL)

static inline FreeBIEDeltaListEntry *
delta_list_entry_alloc (FreeBIEDeltaMgr *mgr, Value *delta_entry, guint row_idx)
{
  FreeBIEDeltaListEntry *new;
  guint8 *ptr;

  new = delta_list_entry_pool_pop (mgr->delta_list_entry_pool_idx);
  ptr = delta_entry->data.ptr;

  if (!mgr->base_ptr)
    mgr->base_ptr = ptr;

  new->delta_entry_offset = ptr - mgr->base_ptr;
  new->row_idx = row_idx;

 return new;
}

static void
delta_list_init (DeltaList *list, int pool_idx)
{
  list->head_idx = DELTA_LIST_ENTRY_INVALID_IDX;
  list->tail_idx = DELTA_LIST_ENTRY_INVALID_IDX;
  list->delta_list_entry_pool_idx = pool_idx;
}

static inline void
delta_list_put (DeltaList *list, FreeBIEDeltaListEntry *new)
{
  if (list->head_idx == DELTA_LIST_ENTRY_INVALID_IDX &&
      list->tail_idx == DELTA_LIST_ENTRY_INVALID_IDX) {
    list->head_idx = delta_list_entry_get_pool_idx (list->delta_list_entry_pool_idx, new);
    list->tail_idx = delta_list_entry_get_pool_idx (list->delta_list_entry_pool_idx, new);
    new->next_idx = DELTA_LIST_ENTRY_INVALID_IDX;
  } else {
    FreeBIEDeltaListEntry *tail = delta_list_entry_pool_get (list->delta_list_entry_pool_idx, list->tail_idx);

    tail->next_idx = delta_list_entry_get_pool_idx (list->delta_list_entry_pool_idx, new);
    new->next_idx = DELTA_LIST_ENTRY_INVALID_IDX;
    list->tail_idx = delta_list_entry_get_pool_idx (list->delta_list_entry_pool_idx, new);
  }
}

static inline FreeBIEDeltaListEntry *
delta_list_extract_next (DeltaList *list, FreeBIEDeltaListEntry *iter,
                         FreeBIEDeltaListEntry *next)
{
  g_assert (list->head_idx != list->tail_idx, "Invalid list state");
  g_assert (iter->next_idx == delta_list_entry_get_pool_idx(list->delta_list_entry_pool_idx, next),
            "Invalid list state");

  iter->next_idx = next->next_idx;

  /* Next is tail */
  if (list->tail_idx == delta_list_entry_get_pool_idx (list->delta_list_entry_pool_idx, next))
    list->tail_idx = delta_list_entry_get_pool_idx (list->delta_list_entry_pool_idx, iter);

  next->next_idx = DELTA_LIST_ENTRY_INVALID_IDX;

  return next;
}

static inline FreeBIEDeltaListEntry *
delta_list_find_and_extract (FreeBIEDeltaMgr *mgr, DeltaList *list, Value *key,
                             delta_entry_compare_func_t compare_func)
{
  FreeBIEDeltaListEntry *iter = delta_list_entry_pool_get (list->delta_list_entry_pool_idx, list->head_idx);

  if (iter && compare_func (mgr, iter, key)) {
    list->head_idx = iter->next_idx;

    if (list->tail_idx == delta_list_entry_get_pool_idx (list->delta_list_entry_pool_idx, iter))
      list->tail_idx = DELTA_LIST_ENTRY_INVALID_IDX;

    iter->next_idx = DELTA_LIST_ENTRY_INVALID_IDX;

    return iter;
  }

  while (iter) {
    FreeBIEDeltaListEntry *next = delta_list_get_next (list->delta_list_entry_pool_idx, list, iter);

    if (next && compare_func (mgr, next, key))
      return delta_list_extract_next (list, iter, next);

    iter = next;
  }

  return NULL;
}

static void
delta_list_close (DeltaList *list)
{
  FreeBIEDeltaListEntry *iter = delta_list_entry_pool_get (list->delta_list_entry_pool_idx, list->head_idx);

  while (iter) {
    FreeBIEDeltaListEntry *next = delta_list_get_next (list->delta_list_entry_pool_idx, list, iter);
    delta_list_entry_pool_push (iter, list->delta_list_entry_pool_idx);
    iter = next;
  }
}

/* Delta hashtable data structure mgmt -------------------------------------- */

static inline guint
delta_ht_get_bucket_idx (Value *rid)
{
  guint hash = 0;
  guint8 *data = rid->data.ptr;

  for (guint i = 0; i < rid->len; i++)
    hash = hash * 31 + data[i];

  return hash & DELTA_HT_BUCKET_MASK;
}

static void delta_ht_init (DeltaHT *ht, int pool_idx)
{
  for (gint i = 0; i < DELTA_HT_BUCKET_SIZE; i++)
    delta_list_init (ht->buckets + i, pool_idx);
  ht->iter_bucket_idx = 0;
}

static inline FreeBIEDeltaListEntry * delta_ht_find_and_extract (FreeBIEDeltaMgr *mgr, DeltaHT *ht,
                                                        Value *key,
                                                        delta_entry_compare_func_t compare_func)
{
  guint bucket_idx;
  DeltaList *bucket;

  bucket_idx = delta_ht_get_bucket_idx (key);
  bucket = ht->buckets + bucket_idx;

  return delta_list_find_and_extract (mgr, bucket, key, compare_func);
}

static inline void delta_ht_put (FreeBIEDeltaMgr *mgr, DeltaHT *ht, FreeBIEDeltaListEntry *delta)
{
  FreeBIEDeltaEntry *delta_entry;
  Value rid = {0};
  guint bucket_idx;
  DeltaList *bucket;
  FreeBIEDeltaListEntry *old;

  g_assert (mgr->base_ptr, "Base pointer not initiated");
  delta_entry = freebie_delta_mgr_get_entry (mgr, delta);
  delta_entry_get_rid (mgr, &rid, delta_entry);
  bucket_idx = delta_ht_get_bucket_idx (&rid);
  bucket = ht->buckets + bucket_idx;

  /* Impeach the old */
  if ((old = delta_list_find_and_extract (mgr, bucket, &rid, delta_entry_compare_rid))) {
    delta_list_entry_pool_push (old, mgr->delta_list_entry_pool_idx);
  }

  delta_list_put (bucket, delta);
}

static inline FreeBIEDeltaListEntry *delta_ht_iter_get_next (DeltaHT *ht,
                                                            FreeBIEDeltaListEntry *iter)
{
  DeltaList *bucket;

  /* Start iteration */
  if (!iter) {
    ht->iter_bucket_idx = 0;
    return delta_list_entry_pool_get (ht->buckets[0].delta_list_entry_pool_idx, ht->buckets[0].head_idx);
  }

  bucket = ht->buckets + ht->iter_bucket_idx;
  iter = delta_list_get_next (bucket->delta_list_entry_pool_idx, bucket, iter);

  /* Next bucket */
  if (!iter) {
    ht->iter_bucket_idx++;
    if (ht->iter_bucket_idx >= DELTA_HT_BUCKET_SIZE) {
      return NULL;
    }
    bucket = ht->buckets + ht->iter_bucket_idx;
    iter = delta_list_entry_pool_get (bucket->delta_list_entry_pool_idx, bucket->head_idx);
  }

  return iter;
}

static void delta_ht_close (DeltaHT *ht)
{
  for (gint i = 0; i < DELTA_HT_BUCKET_SIZE; i++)
    delta_list_close (ht->buckets + i);
}

/* Delta hashtable ---------------------------------------------------------- */

void
freebie_delta_mgr_ht_put (FreeBIEDeltaMgr *mgr,  DeltaHT *delta_ht, Value *delta_entry)
{
  delta_ht_put (mgr, delta_ht, delta_list_entry_alloc (mgr, delta_entry, 0));
}

FreeBIEDeltaListEntry *
freebie_delta_mgr_ht_find_and_extract (FreeBIEDeltaMgr *mgr, DeltaHT *delta_ht, Value *key)
{
  return delta_ht_find_and_extract (mgr, delta_ht, key, delta_entry_compare_rid);
}

FreeBIEDeltaListEntry *
freebie_delta_mgr_ht_get_next (DeltaHT *delta_ht, FreeBIEDeltaListEntry *iter)
{
  return delta_ht_iter_get_next (delta_ht, iter);
}

void
freebie_delta_mgr_ht_init (DeltaHT **delta_ht, int pool_idx)
{
  BUG_ON(*delta_ht != NULL);
  *delta_ht = (DeltaHT *)kmalloc_node(sizeof(DeltaHT), GFP_KERNEL, 1);
  delta_ht_init (*delta_ht, pool_idx);
}

void
freebie_delta_mgr_ht_close (DeltaHT **delta_ht)
{
  BUG_ON(*delta_ht == NULL);
  delta_ht_close (*delta_ht);
  kfree(*delta_ht);
}

/* Delta list --------------------------------------------------------------- */

void freebie_delta_mgr_list_put (DeltaList *delta_list,
                            FreeBIEDeltaListEntry *delta_list_entry,
                            guint row_idx)
{
  delta_list_entry->row_idx = row_idx;
  delta_list_put (delta_list, delta_list_entry);
}

FreeBIEDeltaListEntry * freebie_delta_mgr_list_get_next (DeltaList *delta_list,
                                                        FreeBIEDeltaListEntry *iter)
{
  /* Begin */
  if (!iter)
    return delta_list_entry_pool_get (delta_list->delta_list_entry_pool_idx, delta_list->head_idx);

  return delta_list_get_next (delta_list->delta_list_entry_pool_idx, delta_list, iter);
}

void freebie_delta_mgr_list_init (DeltaList **delta_list, int pool_idx)
{
  BUG_ON(*delta_list != NULL);
  *delta_list = (DeltaList *)kmalloc_node(sizeof(DeltaList), GFP_KERNEL, 1);
  delta_list_init (*delta_list, pool_idx);
}

void freebie_delta_mgr_list_close (DeltaList **delta_list)
{
  BUG_ON(*delta_list == NULL);
  delta_list_close (*delta_list);
  kfree(*delta_list);
}
