#ifndef G_ARRAY_H
#define G_ARRAY_H

#include "gtypes.h"

// GArray --------------------------------------------------------------------
typedef struct _GArray GArray;
struct _GArray
{
  gchar *data;
  guint len;
};

#define g_array_index(a,t,i) (((t*) (void *) (a)->data) [(i)])

void g_array_maybe_expand (GArray *farray, guint len);

GArray* g_array_sized_new (gboolean zero_terminated, gboolean clear_,
                           guint element_size, guint reserved_size);

GArray* g_array_new (gboolean zero_terminated, gboolean clear_,
                     guint element_size);

GArray* g_array_new_take (gpointer data, gsize len, gboolean clear,
                          gsize element_size);

GArray* g_array_append_vals (GArray *farray, gconstpointer data, guint len);

GArray* g_array_append_without_expand (GArray *farray, gconstpointer data,
                                       guint len);

void g_array_unref (GArray *array);

GArray *g_array_ref (GArray *array);

void g_array_init (void);
void check_g_array (void);

// GByteArray ----------------------------------------------------------------
typedef struct _GByteArray GByteArray;
struct _GByteArray
{
  guint8 *data;
  guint	len;
};

#define g_byte_array_maybe_expand(b_arr, len) \
  g_array_maybe_expand ((GArray *) b_arr, len)

GByteArray* g_byte_array_new (void);

GByteArray* g_byte_array_sized_new (guint reserved_size);

#define g_byte_array_append(b_arr, dat, len) \
  g_array_append_vals ((GArray *) b_arr, (guint8 *) dat, len)

#define g_byte_array_append_without_expand(b_arr, dat, len) \
  g_array_append_without_expand ((GArray *) b_arr, (guint8 *) dat, len)

void g_byte_array_unref (GByteArray *array);
void g_byte_array_unref_no_data (GByteArray *array);

// GPtrArray -----------------------------------------------------------------
/*
 * g_ptr_array is used in:
 *   ColumnMetaData.path_in_schema
 *   RowGroup.columns
 *   FileMetaData.schema
 *   FileMetaData.row_groups
 *   Parquet reader list
 *   Parquet writer list
 *
 * Max. g_ptr_array count:
 *   C  := # of columns (max. 16)
 *   RG := # of row groups (max. 1)
 *   F  := # of in/out files (max. 32 input file + 16 output file = 48)
 *
 *   ColumnMetaData.path_in_schema: (C * RG * F)
 *   RowGroup.columns: (RG * F)
 *   FileMetaData.schema: (F)
 *   FileMetaData.row_groups: (F)
 *   Parquet reader list: (1)
 *   Parquet writer list: (1)
 *
 * So, we set the maximum g_ptr_array count to 914.  However, we can reduce it
 * to 626 since the maximum column count for CH-benCHmark is 10.
 */
// #define G_PTR_ARRAY_POOL_SIZE (914)
#define G_PTR_ARRAY_POOL_SIZE (10000)
/*
 * The length of the g_ptr_array never exceeds # of columns (i.e., 16).
 * BUF FIX: Might increase up to the total row goup # (# of repartition)
 */
// #define G_PTR_ARRAY_PDATA_SIZE (16)
#define G_PTR_ARRAY_PDATA_SIZE (400)

typedef struct _GPtrArray GPtrArray;
struct _GPtrArray
{
  gpointer pdata[G_PTR_ARRAY_PDATA_SIZE];
  guint	len;
};

#define g_ptr_array_index(array,index_) ((array)->pdata)[index_]

void g_ptr_array_add (GPtrArray *array, gpointer data);

void g_ptr_array_unref (GPtrArray *array);

GPtrArray* g_ptr_array_new (guint reserved_size,
                            GDestroyNotify element_free_func,
                            gboolean null_terminated);

GPtrArray* g_ptr_array_new_with_free_func (GDestroyNotify element_free_func);

#endif /* G_ARRAY_H */
