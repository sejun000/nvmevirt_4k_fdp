#ifndef PARQUET_RLE_BP_ENCODER_H
#define PARQUET_RLE_BP_ENCODER_H

#include "gobject.h"
#include "gtypes.h"
#include "garray.h"
#include "gerror.h"
#include "thrift_transport.h"
#include "parquet.h"

/* type macros */
#define PARQUET_TYPE_RLE_BP_ENCODER \
  (parquet_rle_bp_encoder_get_type ())
#define PARQUET_RLE_BP_ENCODER(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), PARQUET_TYPE_RLE_BP_ENCODER, \
   ParquetRleBpEncoder))
#define PARQUET_IS_RLE_BP_ENCODER(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), PARQUET_TYPE_RLE_BP_ENCODER))

/*!
 * Parquet Rle Bp Encoder instance.
 */
typedef struct _ParquetRleBpEncoder ParquetRleBpEncoder;
struct _ParquetRleBpEncoder
{
  GObject parent;

  /* Input */
  ThriftTransport *transport;
  guint32 total_bytes;
  guint8 bit_width;

  /* Context */
  gboolean is_total_bytes_init;
  guint32 run_len;
  guint32 run_bytes;
  gboolean is_bp;

  /* BP */
  guint8 *cur_bytes;
  guint8 cur_byte_idx;
  guint8 cur_bit_off;

  /* RLE */
  gint32 run_value;

  /* Buffer */
  /*
   * While writes are buffered within the RLE-BP encoder itself, reads simply
   * reference an external buffer for buffering.
   */
  guint8 *r_buf_ptr;
  GByteArray *w_buf;
};

/* used by PARQUET_TYPE_RLE_BP_ENCODER */
GType parquet_rle_bp_encoder_get_type (void);

/*
 * RLE-BP encoder only supports reading or writing the i32 values as RLE-BP
 * encoding is only used in encoding repetition/definition levels or dictionary
 * indices when the encoding type is RLE_DICTIONARY.
 */

/*
 * `*_list_begin` must be called before read or write the RLE-BP encoded value.
 * `*_write_list_end` must be called after writing all values to flush the
 * buffered data.
 */

/*
 * Setting `total_bytes` to 0 is considered as auto-detect (by reading
 * `total_bytes` from the transport).  This is because `total_bytes` is not
 * always prepended.
 * @see https://parquet.apache.org/docs/file-format/data-pages/encodings
 */
#define PARQUET_RLE_BP_AUTO_TOTAL_LEN (0)
gint32 parquet_rle_bp_encoder_read_list_begin (ParquetRleBpEncoder *encoder,
                                               guint32 total_bytes,
                                               gint8 bit_width);

gint32 parquet_rle_bp_encoder_read_i32 (ParquetRleBpEncoder *encoder,
                                        gint32 *value);

/*
 * Special function for reading definition levels as a batch and convert it to
 * the null bitmap.
 */
#ifndef PARQUET_RLE_DICT_ENCODING
gint32 parquet_rle_bp_encoder_read_nullmap (ParquetRleBpEncoder *encoder,
                                            guint32 *null_map, guint32 *notnull_cnt,
                                            guint32 cnt);
#endif

void parquet_rle_bp_encoder_write_list_begin (ParquetRleBpEncoder *encoder);

gint32 parquet_rle_bp_encoder_write_list_end (ParquetRleBpEncoder *encoder,
                                              GError **error);

gint32 parquet_rle_bp_encoder_write_i32 (ParquetRleBpEncoder *encoder,
                                         const gint32 value);

void parquet_rle_bp_encoder_instance_init (ParquetRleBpEncoder *self);

void parquet_rle_bp_encoder_finalize (GObject *gobject);

#endif /* PARQUET_RLE_BP_ENCODER_H */
