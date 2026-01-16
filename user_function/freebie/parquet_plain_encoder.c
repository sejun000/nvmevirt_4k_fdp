#include "gobject-type.h"
#include "gobject.h"
#include "gtypes.h"
#include "parquet.h"
#include "thrift.h"
#include "thrift_file_transport.h"
#include "parquet_plain_encoder.h"
#include "utils.h"

#define COMBINE_MEMCPY(sizeoftype)                                            \
  {                                                                           \
    guint32 seq_len;                                                          \
                                                                              \
    if (encoder->seq_head_ptr == NULL)                                        \
    {                                                                         \
      encoder->seq_head_ptr = value->data.ptr;                                \
      encoder->seq_len = value->len;                                          \
                                                                              \
      return 0;                                                               \
    }                                                                         \
                                                                              \
    if (&(encoder->seq_head_ptr[encoder->seq_len]) == value->data.ptr)        \
    {                                                                         \
      encoder->seq_len += value->len;                                         \
      return 0;                                                               \
    }                                                                         \
    if (!thrift_transport_write (encoder->transport,                          \
                                 encoder->seq_head_ptr,                       \
                                 encoder->seq_len, error))                    \
      return -1;                                                              \
                                                                              \
    seq_len = encoder->seq_len;                                               \
    encoder->seq_head_ptr = value->data.ptr;                                  \
    encoder->seq_len = value->len;                                            \
                                                                              \
    return seq_len;                                                           \
  }

GType
parquet_plain_encoder_get_type (void)
{
  return G_TYPE_PARQUET_PLAIN_ENCODER;
}

void
parquet_plain_encoder_write_list_begin (ParquetPlainEncoder *encoder,
                                        gboolean is_fixed_length,
                                        gint32 fixed_length)
{
  encoder->bool_idx = 0;
  encoder->bool_w_byte = 0x0;
  encoder->is_fixed_length = is_fixed_length;
  encoder->fixed_length = fixed_length;
  encoder->seq_head_ptr = NULL;
  encoder->seq_len = 0;
}

gint32
parquet_plain_encoder_write_list_end (ParquetPlainEncoder *encoder,
                                      GError **error)
{
  gint32 ret = 0;

  /*
   * `ParquetPlainEncoder.bool_idx > 0` indicates that the list is the
   * boolean type and has a left byte to flush
   */
  if (encoder->bool_idx > 0) {
    if (!thrift_transport_write (encoder->transport,
                                 &(encoder->bool_w_byte),
                                 1, error))
      return -1;

    ret += 1;
  }

  /*
   * Flush deferred data
   */
  if (encoder->seq_head_ptr != NULL && encoder->seq_len != 0) {
    if (!thrift_transport_write (encoder->transport,
                                 encoder->seq_head_ptr,
                                 encoder->seq_len, error))
      return -1;

    ret += encoder->seq_len;
  }

  encoder->seq_head_ptr = NULL;
  encoder->seq_len = 0;

  return ret;
}

gint32
parquet_plain_encoder_write_bool (ParquetPlainEncoder *encoder,
                                  const Value *value, GError **error)
{
  gint32 ret = 0;
  gboolean b;

  g_assert (IS_VALUE_TYPE_BOOLEAN (value), "Value type mismatch");
  b = GET_BOOLEAN (value);

  /*
   * Boolean value is encoded in 1 bit, packed into a byte in LSB-first order.
   * @see https://parquet.apache.org/docs/file-format/data-pages/encodings
   */
  encoder->bool_w_byte |= (b ? 0x1 : 0x0) << (encoder->bool_idx++);

  if (encoder->bool_idx == 8)
  {
    if (!thrift_transport_write (encoder->transport,
                                 &(encoder->bool_w_byte),
                                 1, error))
      return -1;

    ret += 1;
    encoder->bool_idx = 0;
    encoder->bool_w_byte = 0x0;
  }

  return ret;
}

gint32
parquet_plain_encoder_write_i32 (ParquetPlainEncoder *encoder,
                                 const Value *value, GError **error)
{
  g_assert (IS_VALUE_TYPE_INT32 (value), "Value type mismatch");

  COMBINE_MEMCPY(4);
}

gint32
parquet_plain_encoder_write_i64 (ParquetPlainEncoder *encoder,
                                 const Value *value, GError **error)
{
  g_assert (IS_VALUE_TYPE_INT64 (value), "Value type mismatch");

  COMBINE_MEMCPY(8);
}

gint32
parquet_plain_encoder_write_float (ParquetPlainEncoder *encoder,
                                   const Value *value, GError **error)
{
  g_assert (IS_VALUE_TYPE_FLOAT (value), "Value type mismatch");

  COMBINE_MEMCPY(4);
}


gint32
parquet_plain_encoder_write_double (ParquetPlainEncoder *encoder,
                                    const Value *value, GError **error)
{
  g_assert (IS_VALUE_TYPE_DOUBLE (value), "Value type mismatch");

  COMBINE_MEMCPY(8);
}

gint32
parquet_plain_encoder_write_byte_array (ParquetPlainEncoder *encoder,
                                        const Value *value, GError **error)
{
  g_assert (IS_VALUE_TYPE_BYTE_ARRAY (value) && !encoder->is_fixed_length,
            "Value type mismatch");


  COMBINE_MEMCPY(value->len);
}

gint32
parquet_plain_encoder_write_fixed_len_byte_array (ParquetPlainEncoder *encoder,
                                                  const Value *value, GError **error)
{
  g_assert (IS_VALUE_TYPE_BYTE_ARRAY (value) && encoder->is_fixed_length,
            "Value type mismatch");
  g_assert (encoder->fixed_length != 0 && encoder->fixed_length == value->len,
            "wrong fixed length");

  COMBINE_MEMCPY(value->len);
}

gint32
parquet_plain_encoder_read_list_begin (ParquetPlainEncoder *encoder,
                                       gboolean is_fixed_length,
                                       gint32 fixed_length,
                                       GError **error)
{
  THRIFT_UNUSED_VAR (error);

  encoder->bool_idx = 0;
  encoder->bool_byte_ptr = NULL;
  encoder->is_fixed_length = is_fixed_length;
  encoder->fixed_length = fixed_length;

  return 0;
}

#ifdef PARQUET_RLE_DICT_ENCODING
gint32
parquet_plain_encoder_read_bool (ParquetPlainEncoder *encoder,
                                 Value *value)
{
  gint32 ret = 0;
  gboolean b;

  /*
   * Boolean value is encoded in 1 bit, packed into a byte in LSB-first order.
   * @see https://parquet.apache.org/docs/file-format/data-pages/encodings
   */
  if (encoder->bool_idx == 0)
  {
    encoder->bool_byte_ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                                                   1);
    ret++;
  }

  b = (*encoder->bool_byte_ptr >> encoder->bool_idx) & 0x1;
  value_set_bool (value, b);

  encoder->bool_idx = (encoder->bool_idx + 1) & 0x7;

  return ret;
}

gint32
parquet_plain_encoder_read_i32 (ParquetPlainEncoder *encoder, Value *value)
{
  gpointer ptr;

  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              4);

  value_set_i32 (value, ptr);
  return 4;
}

gint32
parquet_plain_encoder_read_i64 (ParquetPlainEncoder *encoder, Value *value)
{
  gpointer ptr;

  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              8);

  value_set_i64 (value, ptr);
  return 8;
}

gint32
parquet_plain_encoder_read_float (ParquetPlainEncoder *encoder, Value *value)
{
  gpointer ptr;

  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              4);

  value_set_float (value, ptr);
  return 4;
}

gint32
parquet_plain_encoder_read_double (ParquetPlainEncoder *encoder, Value *value)
{
  gpointer ptr;

  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              8);

  value_set_double (value, ptr);
  return 8;
}

gint32
parquet_plain_encoder_read_byte_array (ParquetPlainEncoder *encoder,
                                       Value *value)
{
  guint len;
  gpointer ptr;

  g_assert (!encoder->is_fixed_length, "encoder mode mismatch");

  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport, 4);
  len = le_to_i32 (ptr);
  /* Advance pointer */
  thrift_file_transport_get_unsafe_ptr (encoder->transport, len);
  /* Include 4-byte binary length */
  len += 4;

  value_set_byte_array (value, ptr, len);

  return len;
}

gint32
parquet_plain_encoder_read_fixed_len_byte_array (ParquetPlainEncoder *encoder,
                                                 Value *value)
{
  guint len;
  gpointer ptr;

  g_assert (encoder->is_fixed_length && encoder->fixed_length != 0,
            "encoder mode mismatch");

  len = encoder->fixed_length;
  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport, len);

  value_set_byte_array (value, ptr, len);

  return len;
}
#endif

gint32
parquet_plain_encoder_read_bool_vec (ParquetPlainEncoder *encoder,
                                     Vector *vector, gint cnt)
{
  gint32 ret = 0;
  gboolean b;

  vector->value_cnt = cnt;
  for (gint i = 0; i < cnt; i++)
  {
    /*
     * Boolean value is encoded in 1 bit, packed into a byte in LSB-first order.
     * @see https://parquet.apache.org/docs/file-format/data-pages/encodings
     */
    if (encoder->bool_idx == 0)
    {
      encoder->bool_byte_ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                                                     1);
      ret++;
    }

    b = (*encoder->bool_byte_ptr >> encoder->bool_idx) & 0x1;
    value_set_bool (&(vector->values[i]), b);

    encoder->bool_idx = (encoder->bool_idx + 1) & 0x7;
  }

  return ret;
}

gint32
parquet_plain_encoder_read_i32_vec (ParquetPlainEncoder *encoder,
                                    Vector *vector, gint cnt)
{
  guint32 *ptr;

  vector->value_cnt = cnt;
  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              4 * cnt);
  for (gint i = 0; i < cnt; i++)
    value_set_i32 (&(vector->values[i]), &(ptr[i]));

  return 4 * cnt;
}

gint32
parquet_plain_encoder_read_i64_vec (ParquetPlainEncoder *encoder,
                                    Vector *vector, gint cnt)
{
  guint64 *ptr;

  vector->value_cnt = cnt;
  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              8 * cnt);
  for (gint i = 0; i < cnt; i++)
    value_set_i64 (&(vector->values[i]), &(ptr[i]));

  return 8 * cnt;
}

gint32
parquet_plain_encoder_read_float_vec (ParquetPlainEncoder *encoder,
                                      Vector *vector, gint cnt)
{
  gfloat *ptr;

  vector->value_cnt = cnt;
  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              4 * cnt);
  for (gint i = 0; i < cnt; i++)
    value_set_float (&(vector->values[i]), &(ptr[i]));

  return 4 * cnt;
}

gint32
parquet_plain_encoder_read_double_vec (ParquetPlainEncoder *encoder,
                                       Vector *vector, gint cnt)
{
  gdouble *ptr;

  vector->value_cnt = cnt;
  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              8 * cnt);
  for (gint i = 0; i < cnt; i++)
    value_set_double (&(vector->values[i]), &(ptr[i]));

  return 8 * cnt;
}

gint32
parquet_plain_encoder_read_byte_array_vec (ParquetPlainEncoder *encoder,
                                           Vector *vector, gint cnt)
{
  gint32 ret = 0;
  guint8 *ptr;
  guint16 len;

  g_assert (!encoder->is_fixed_length, "encoder mode mismatch");

  vector->value_cnt = cnt;
  for (gint i = 0; i < cnt; i++)
  {
    ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                                4);
    ret += 4;
    len = le_to_i32 (ptr);
    /* Advance pointer  */
    thrift_file_transport_get_unsafe_ptr (encoder->transport, len);
    ret += len;
    value_set_byte_array (&(vector->values[i]), ptr, 4 + len);
  }

  return ret;
}

gint32
parquet_plain_encoder_read_fixed_len_byte_array_vec (ParquetPlainEncoder *encoder,
                                                     Vector *vector, gint cnt)
{
  gint32 ret = 0;
  guint8 *ptr;
  guint16 len;

  g_assert (encoder->is_fixed_length && encoder->fixed_length != 0,
            "encoder mode mismatch");

  vector->value_cnt = cnt;
  len = encoder->fixed_length;

  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              cnt * len);
  ret += cnt * len;

  for (gint i = 0; i < cnt; i++)
    value_set_byte_array (&(vector->values[i]), &(ptr[i * len]), len);

  return ret;
}

void
parquet_plain_encoder_instance_init (ParquetPlainEncoder *self)
{
  self->bool_idx = 0;
  self->bool_byte_ptr = NULL;
  self->bool_w_byte = 0x0;
  self->is_fixed_length = FALSE;
  self->fixed_length = 0;
}

void
parquet_plain_encoder_finalize (GObject *gobject)
{
  ParquetPlainEncoder *pp;

  pp = PARQUET_PLAIN_ENCODER (gobject);

  g_object_unref (pp->transport);
}
