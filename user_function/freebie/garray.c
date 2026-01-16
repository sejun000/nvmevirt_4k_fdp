#include "c_standard_libs.h"

#include "gtypes.h"
#include "garray.h"
#include "gmem.h"

#define MIN_ARRAY_SIZE  16
#define g_array_elt_len(array,i) ((gsize)(array)->elt_size * (i))
#define g_array_elt_pos(array,i) ((array)->data + g_array_elt_len((array),(i)))
#define g_array_elt_zero(array, pos, len)                               \
  (memset (g_array_elt_pos ((array), pos), 0,  g_array_elt_len ((array), len)))
#define g_array_zero_terminate(array)                                        \
{                                                                            \
  if ((array)->zero_terminated)                                              \
    g_array_elt_zero ((array), (array)->len, 1);                             \
}
#define g_array_add_len(array, length) \
  (array->len + (length) + array->zero_terminated)

// GArray --------------------------------------------------------------------

/* Private */

typedef struct _GRealArray  GRealArray;
struct _GRealArray
{
  guint8 *data;
  guint len;
  guint elt_capacity;
  guint elt_size;
  guint zero_terminated : 1;
  guint clear : 1;
  guint ref_cnt;
};

void
g_array_maybe_expand (GArray *farray, guint len)
{
  GRealArray *array = (GRealArray*) farray;

  if (len > array->elt_capacity)
  {
    gsize want_alloc = g_nearest_pow (g_array_elt_len (array, len));
    want_alloc = MAX (want_alloc, MIN_ARRAY_SIZE);
    array->data = g_realloc (array->data, want_alloc);
    array->elt_capacity = MIN (want_alloc / array->elt_size, G_MAXUINT);
  }
}

/* Public */

GArray*
g_array_new (gboolean zero_terminated, gboolean clear, guint elt_size)
{
  return g_array_sized_new (zero_terminated, clear,
                            elt_size, 0);
}

GArray*
g_array_sized_new (gboolean zero_terminated, gboolean clear, guint elt_size,
                   guint reserved_size)
{
  GRealArray *array;

  array = g_new (GRealArray, 1);
  array->data = NULL;
  array->len = 0;
  array->elt_capacity = 0;
  array->zero_terminated = (zero_terminated ? 1 : 0);
  array->clear = (clear ? 1 : 0);
  array->elt_size = elt_size;
  array->ref_cnt = 1;

  if (array->zero_terminated || reserved_size != 0)
  {
    g_array_maybe_expand ((GArray *) array,
                          g_array_add_len (array, reserved_size));
    g_array_zero_terminate (array);
  }

  return (GArray*) array;
}

GArray *
g_array_new_take (gpointer data, gsize len, gboolean clear, gsize element_size)
{
  GRealArray *rarray;
  GArray *array;

  array = g_array_sized_new (FALSE, clear,
                             element_size, 0);
  rarray = (GRealArray *) array;
  rarray->data = (guint8 *) g_steal_pointer (&data);
  rarray->len = len;
  rarray->elt_capacity = len;

  return array;
}

GArray*
g_array_append_vals (GArray *farray, gconstpointer data, guint len)
{
  GRealArray *array = (GRealArray*) farray;

  if (len == 0)
    return farray;

  g_array_maybe_expand (farray, g_array_add_len (array, len));

  custom_memcpy (g_array_elt_pos (array, array->len), data,
  // memcpy (g_array_elt_pos (array, array->len), data,
          g_array_elt_len (array, len));
  array->len += len;

  g_array_zero_terminate (array);

  return farray;
}

GArray*
g_array_append_without_expand (GArray *farray, gconstpointer data, guint len)
{
  GRealArray *array = (GRealArray*) farray;

  if (len == 0)
    return farray;

  custom_memcpy (g_array_elt_pos (array, array->len), data,
  // memcpy (g_array_elt_pos (array, array->len), data,
          g_array_elt_len (array, len));
  array->len += len;

  g_array_zero_terminate (array);

  return farray;
}

void
g_array_unref (GArray *array)
{
  if (array == NULL)
    return;
  GRealArray *rarray = (GRealArray*) array;

  rarray->ref_cnt--;

  if (rarray->ref_cnt <= 0)
  {
    g_free (rarray->data);
    g_free (rarray);
  }
}

GArray *
g_array_ref (GArray *array)
{
  GRealArray *rarray = (GRealArray*) array;
  rarray->ref_cnt++;
  return (GArray *) rarray;
}

// GByteArray ----------------------------------------------------------------
GByteArray*
g_byte_array_new (void)
{
  return (GByteArray *) g_array_sized_new (FALSE, FALSE,
                                           1, 0);
}

GByteArray*
g_byte_array_sized_new (guint reserved_size)
{
  return (GByteArray *) g_array_sized_new (FALSE, FALSE,
                                           1, reserved_size);
}

void
g_byte_array_unref (GByteArray *array)
{
  if (array == NULL)
    return;
  g_array_unref((GArray *) array);
}

void g_byte_array_unref_no_data (GByteArray *array)
{
  if (array == NULL) {
    return;
  }
  GRealArray *rarray = (GRealArray *) array;
  g_free(rarray);
}

// GPtrArray -----------------------------------------------------------------

/* Private */
typedef struct _GRealPtrArray GRealPtrArray;
struct _GRealPtrArray
{
  gpointer pdata[G_PTR_ARRAY_PDATA_SIZE];
  guint len;
  guint alloc;
  guint8 null_terminated : 1;
  GDestroyNotify element_free_func;
  gint32 free_next;
  gint32 pool_idx;
};

static GRealPtrArray g_real_ptr_array_pool[G_PTR_ARRAY_POOL_SIZE];
static gint32 free_head = -1;
static gint32 free_cnt = G_PTR_ARRAY_POOL_SIZE;
spinlock_t garray_lock;

static void
ptr_array_maybe_null_terminate (GRealPtrArray *rarray)
{
  if (G_UNLIKELY (rarray->null_terminated))
    rarray->pdata[rarray->len] = NULL;
}

/* Public */

void
g_ptr_array_add (GPtrArray *array, gpointer data)
{
  GRealPtrArray *rarray = (GRealPtrArray *) array;
  g_assert (rarray->len < G_PTR_ARRAY_PDATA_SIZE, "pdata limit exceeded");
  rarray->pdata[rarray->len++] = data;
  ptr_array_maybe_null_terminate (rarray);
}

void check_g_array (void)
{
  spin_lock(&garray_lock);
  if (free_cnt == G_PTR_ARRAY_POOL_SIZE) {
    printk("G_ARRAY_POOL is all freed\n");
  } else {
    printk("There is a leak in G_ARRAY_POOL\n");
  }
  spin_unlock(&garray_lock);
}

void
g_array_init (void)
{
  printk("Size of g_array pool: %lu\n", sizeof(g_real_ptr_array_pool));
  spin_lock_init(&garray_lock);
  free_cnt = G_PTR_ARRAY_POOL_SIZE;
  for (gint i = 0; i < G_PTR_ARRAY_POOL_SIZE; i++) {
    g_real_ptr_array_pool[i].free_next = i + 1;
  }
  g_real_ptr_array_pool[G_PTR_ARRAY_POOL_SIZE - 1].free_next = -1;
  free_head = 0;
}

void
g_ptr_array_unref (GPtrArray *array)
{
  if (array == NULL)
    return;

  GRealPtrArray *rarray = (GRealPtrArray *) array;
  for (guint i = 0; i < rarray->len; i++)
    rarray->element_free_func (rarray->pdata[i]);

  memset (array->pdata, 0, G_PTR_ARRAY_PDATA_SIZE * sizeof (gpointer));

  spin_lock(&garray_lock);
  /* Push to free list */
  rarray->free_next = free_head;
  free_head = rarray->pool_idx;
  free_cnt++;
  spin_unlock(&garray_lock);
}

GPtrArray *
g_ptr_array_new (guint reserved_size, GDestroyNotify element_free_func,
                 gboolean null_terminated)
{
  GRealPtrArray *array;

  /* Prevent -Wall */
  ((void) reserved_size);

  spin_lock(&garray_lock);

  /* Pop from free list */
  g_assert (free_cnt > 0, "g_real_ptr_array_pool limit exceeded");
  array = &(g_real_ptr_array_pool[free_head]);
  array->pool_idx = free_head;
  free_head = array->free_next;
  array->free_next = -1;
  free_cnt--;

  array->pdata[0] = NULL;
  array->len = 0;
  array->alloc = 0;
  array->null_terminated = null_terminated ? 1 : 0;
  array->element_free_func = element_free_func;

  g_assert (reserved_size < G_PTR_ARRAY_PDATA_SIZE,
            "reserved_size limit exceeded");

  spin_unlock(&garray_lock);

  return (GPtrArray *) array;
}

GPtrArray*
g_ptr_array_new_with_free_func (GDestroyNotify element_free_func)
{
  return g_ptr_array_new (0, element_free_func,
                          FALSE);
}
