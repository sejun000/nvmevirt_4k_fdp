#ifndef PARQUET_H
#define PARQUET_H

#include "gerror.h"
#include "thrift_protocol.h"
#include "freebie_functions.h"

/*
 * If defined, reading dictionary page is enabled.
 * Note that we encode the value using plain encoding only; this is reserved for
 * future use.
 */
#define PARQUET_RLE_DICT_ENCODING

#define MAGIC (0x31524150) // PAR1
#define VECTOR_MAX_SIZE (16)

ThriftProtocol *
create_thrift_file_compact_protocol (gchar *filename, int file_size, int max_file_size, GError **error);
// Parquet value --------------------------------------------------------------
#define NULL_VALUE                    (NULL)
#define IS_NULL_VALUE(val)            ((val) == NULL)
#define IS_VALUE_TYPE_BOOLEAN(val)    ((val)->type == VALUE_TYPE_BOOLEAN)
#define IS_VALUE_TYPE_INT32(val)      ((val)->type == VALUE_TYPE_INT32)
#define IS_VALUE_TYPE_INT64(val)      ((val)->type == VALUE_TYPE_INT64)
#define IS_VALUE_TYPE_FLOAT(val)      ((val)->type == VALUE_TYPE_FLOAT)
#define IS_VALUE_TYPE_DOUBLE(val)     ((val)->type == VALUE_TYPE_DOUBLE)
#define IS_VALUE_TYPE_BYTE_ARRAY(val) ((val)->type == VALUE_TYPE_BYTE_ARRAY)

enum _ValueType
{
  VALUE_TYPE_BOOLEAN,
  VALUE_TYPE_INT32,
  VALUE_TYPE_INT64,
  VALUE_TYPE_FLOAT,
  VALUE_TYPE_DOUBLE,
  VALUE_TYPE_BYTE_ARRAY,
};
typedef enum _ValueType ValueType;

typedef struct _Value Value;
struct _Value
{
  union {
    gpointer ptr;
    guint32 literal;
  } data;
  ValueType type;
  guint32 len;
};

/*
 * Boolean type value: This value represents a single boolean value.
 *   `type` should be `VALUE_TYPE_BOOLEAN`
 *   `data.literal` should be either `0` (FALSE) or `1` (TRUE)
 *   `len` should be `0`
 */
#define GET_BOOLEAN(val) \
  ((gboolean) (val)->data.literal)

static inline Value *
value_set_bool (Value *v, gboolean b)
{
  v->type = VALUE_TYPE_BOOLEAN;
  v->data.literal = b;
  v->len = 0;

  return v;
}

/*
 * Int32 type value: This value represents a single 4-byte integer value.
 *   `type` should be `VALUE_TYPE_INT32`
 *   `data.ptr` should be an address for the 4-byte integer
 *   `len` should be `4`
 */
static inline Value *
value_set_i32 (Value *v, gpointer i32)
{
  v->type = VALUE_TYPE_INT32;
  v->data.ptr = i32;
  v->len = 4;

  return v;
}

/*
 * Int64 type value: This value represents a single 8-byte integer value.
 *   `type` should be `VALUE_TYPE_INT64`
 *   `data.ptr` should be an address for the 8-byte integer
 *   `len` should be `8`
 */
static inline Value *
value_set_i64 (Value *v, gpointer i64)
{
  v->type = VALUE_TYPE_INT64;
  v->data.ptr = i64;
  v->len = 8;

  return v;
}

/*
 * Float type value: This value represents a single 4-byte float value.
 *   `type` should be `VALUE_TYPE_FLOAT`
 *   `data.ptr` should be an address for the 4-byte float
 *   `len` should be `4`
 */
static inline Value *
value_set_float (Value *v, gpointer f)
{
  v->type = VALUE_TYPE_FLOAT;
  v->data.ptr = f;
  v->len = 4;

  return v;
}

/*
 * Double type value: This value represents a single 8-byte double value.
 *   `type` should be `VALUE_TYPE_DOUBLE`
 *   `data.ptr` should be an address for the 8-byte double
 *   `len` should be `8`
 */
static inline Value *
value_set_double (Value *v, gpointer d)
{
  v->type = VALUE_TYPE_DOUBLE;
  v->data.ptr = d;
  v->len = 8;

  return v;
}

/*
 * Byte array type value: This value represents a single byte array value.
 *   `type` should be `VALUE_TYPE_BYTE_ARRAY`
 *   `data.ptr` should be an address for the array
 *   `len` should be a length for the array
 * Note that a fixed-length byte array is converted to this type.
 */
static inline Value *
value_set_byte_array (Value *v, gpointer ptr, guint len)
{
  v->type = VALUE_TYPE_BYTE_ARRAY;
  v->data.ptr = ptr;
  v->len = len;

  return v;
}

/*
 * Utils
 */
static inline Value *
value_cpy (Value *dst, Value *src)
{
  dst->type = src->type;
  dst->data = src->data;
  dst->len = src->len;
  return dst;
}

Value *value_deepcpy (Value *dst, Value *src);

static inline Value *
value_new (void)
{
  return g_new0 (Value, 1);
}

static inline void
value_unref (gpointer val)
{
  if (val == NULL)
    return;

  g_free (val);
}

// Vector type ---------------------------------------------------------------

struct _Vector
{
  Value values[VECTOR_MAX_SIZE];
  guint32 null_map;
  guint16 value_cnt;
  guint16 total_cnt;
};
typedef struct _Vector Vector;

#define SET_VECTOR_ELEM_NULL(vec, idx, isnull) \
  ((vec)->null_map | ((isnull) << (idx)))

#define IS_VECTOR_ELEM_NULL(vec, idx) \
  (((vec)->null_map >> idx) & 0x1)

#endif /* PARQUET_H */
