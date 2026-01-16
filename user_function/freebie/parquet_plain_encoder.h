#ifndef PARQUET_PLAIN_ENCODER_H
#define PARQUET_PLAIN_ENCODER_H

#include "garray.h"
#include "gobject.h"
#include "gtypes.h"
#include "thrift_transport.h"
#include "parquet.h"

/* type macros */
#define PARQUET_TYPE_PLAIN_ENCODER \
  (parquet_plain_encoder_get_type ())
#define PARQUET_PLAIN_ENCODER(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), PARQUET_TYPE_PLAIN_ENCODER, \
   ParquetPlainEncoder))
#define PARQUET_IS_PLAIN_ENCODER(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), PARQUET_TYPE_PLAIN_ENCODER))

/*!
 * Parquet Plain Encoder instance.
 */
typedef struct _ParquetPlainEncoder ParquetPlainEncoder;
struct _ParquetPlainEncoder
{
  GObject parent;

  /* Input */
  ThriftTransport *transport;

  /* Boolean */
  gint8 bool_idx;
  guint8 *bool_byte_ptr;

  /* Byte array */
  gboolean is_fixed_length;
  guint32 fixed_length;

  /*
   * Combined memcpy:
   * Combining multiple memcpy operations that copy contiguous memory regions
   * into a single large memcpy can reduce overhead.  However, if we buffers
   * reads at page granularity, the write head pointer (seq_head_ptr) may become
   * invalid because buffering the next page overwrites the current page’s
   * buffer space.  So, to combine memcpy, buffering granularity should be a
   * whole file or at least all column chunks for a column.
   */
  guint8 *seq_head_ptr;
  guint32 seq_len;

  /* Write buffer */
  guint8 bool_w_byte;
};

/* used by PARQUET_TYPE_PLAIN_ENCODER */
GType parquet_plain_encoder_get_type (void);

/*
 * `*_list_begin` must be called before read or write the plain-encoded value.
 * `*_write_list_end` must be called after writing all values to flush the
 * buffered data.
 */

void parquet_plain_encoder_write_list_begin (ParquetPlainEncoder *encoder,
                                             gboolean is_fixed_length,
                                             gint32 fixed_length);

gint32 parquet_plain_encoder_write_list_end (ParquetPlainEncoder *encoder,
                                             GError **error);

gint32 parquet_plain_encoder_write_bool (ParquetPlainEncoder *encoder,
                                         const Value *value, GError **error);

gint32 parquet_plain_encoder_write_i32 (ParquetPlainEncoder *encoder,
                                        const Value *value, GError **error);

gint32 parquet_plain_encoder_write_i64 (ParquetPlainEncoder *encoder,
                                        const Value *value, GError **error);

gint32 parquet_plain_encoder_write_float (ParquetPlainEncoder *encoder,
                                          const Value *value, GError **error);

gint32 parquet_plain_encoder_write_double (ParquetPlainEncoder *encoder,
                                           const Value *value, GError **error);

gint32 parquet_plain_encoder_write_byte_array (ParquetPlainEncoder *encoder,
                                               const Value *value, GError **error);

gint32 parquet_plain_encoder_write_fixed_len_byte_array (ParquetPlainEncoder *encoder,
                                                         const Value *value, GError **error);

gint32 parquet_plain_encoder_read_list_begin (ParquetPlainEncoder *encoder,
                                              gboolean is_fixed_length,
                                              gint32 fixed_length,
                                              GError **error);

#ifdef PARQUET_RLE_DICT_ENCODING
gint32 parquet_plain_encoder_read_bool (ParquetPlainEncoder *encoder,
                                        Value *value);

gint32 parquet_plain_encoder_read_i32 (ParquetPlainEncoder *encoder,
                                       Value *value);

gint32 parquet_plain_encoder_read_i64 (ParquetPlainEncoder *encoder,
                                       Value *value);

gint32 parquet_plain_encoder_read_float (ParquetPlainEncoder *encoder,
                                         Value *value);

gint32 parquet_plain_encoder_read_double (ParquetPlainEncoder *encoder,
                                          Value *value);

gint32 parquet_plain_encoder_read_byte_array (ParquetPlainEncoder *encoder,
                                              Value *value);

gint32 parquet_plain_encoder_read_fixed_len_byte_array (ParquetPlainEncoder *encoder,
                                                        Value *value);
#endif

gint32 parquet_plain_encoder_read_bool_vec (ParquetPlainEncoder *encoder,
                                            Vector *vector, gint cnt);

gint32 parquet_plain_encoder_read_i32_vec (ParquetPlainEncoder *encoder,
                                           Vector *vector, gint cnt);

gint32 parquet_plain_encoder_read_i64_vec (ParquetPlainEncoder *encoder,
                                           Vector *vector, gint cnt);

gint32 parquet_plain_encoder_read_float_vec (ParquetPlainEncoder *encoder,
                                             Vector *vector, gint cnt);

gint32 parquet_plain_encoder_read_double_vec (ParquetPlainEncoder *encoder,
                                              Vector *vector, gint cnt);

gint32 parquet_plain_encoder_read_byte_array_vec (ParquetPlainEncoder *encoder,
                                                  Vector *vector, gint cnt);

gint32 parquet_plain_encoder_read_fixed_len_byte_array_vec (ParquetPlainEncoder *encoder,
                                                            Vector *vector,
                                                            gint cnt);

void parquet_plain_encoder_instance_init (ParquetPlainEncoder *self);

void parquet_plain_encoder_finalize (GObject *gobject);

#endif /* PARQUET_PLAIN_ENCODER_H */
