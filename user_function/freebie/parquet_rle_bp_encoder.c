#include "parquet_rle_bp_encoder.h"
#include "garray.h"
#include "gerror.h"
#include "gobject-type.h"
#include "gobject.h"
#include "gtypes.h"
#include "utils.h"
#include "thrift_file_transport.h"

/* Private */
static const guint32 bp_mask[33] = {
  0x00000000, /* 0000 0000 0000 0000 0000 0000 0000 0000 */
  0x00000001, /* 0000 0000 0000 0000 0000 0000 0000 0001 */
  0x00000003, /* 0000 0000 0000 0000 0000 0000 0000 0011 */
  0x00000007, /* 0000 0000 0000 0000 0000 0000 0000 0111 */
  0x0000000F, /* 0000 0000 0000 0000 0000 0000 0000 1111 */
  0x0000001F, /* 0000 0000 0000 0000 0000 0000 0001 1111 */
  0x0000003F, /* 0000 0000 0000 0000 0000 0000 0011 1111 */
  0x0000007F, /* 0000 0000 0000 0000 0000 0000 0111 1111 */
  0x000000FF, /* 0000 0000 0000 0000 0000 0000 1111 1111 */
  0x000001FF, /* 0000 0000 0000 0000 0000 0001 1111 1111 */
  0x000003FF, /* 0000 0000 0000 0000 0000 0011 1111 1111 */
  0x000007FF, /* 0000 0000 0000 0000 0000 0111 1111 1111 */
  0x00000FFF, /* 0000 0000 0000 0000 0000 1111 1111 1111 */
  0x00001FFF, /* 0000 0000 0000 0000 0001 1111 1111 1111 */
  0x00003FFF, /* 0000 0000 0000 0000 0011 1111 1111 1111 */
  0x00007FFF, /* 0000 0000 0000 0000 0111 1111 1111 1111 */
  0x0000FFFF, /* 0000 0000 0000 0000 1111 1111 1111 1111 */
  0x0001FFFF, /* 0000 0000 0000 0001 1111 1111 1111 1111 */
  0x0003FFFF, /* 0000 0000 0000 0011 1111 1111 1111 1111 */
  0x0007FFFF, /* 0000 0000 0000 0111 1111 1111 1111 1111 */
  0x000FFFFF, /* 0000 0000 0000 1111 1111 1111 1111 1111 */
  0x001FFFFF, /* 0000 0000 0001 1111 1111 1111 1111 1111 */
  0x003FFFFF, /* 0000 0000 0011 1111 1111 1111 1111 1111 */
  0x007FFFFF, /* 0000 0000 0111 1111 1111 1111 1111 1111 */
  0x00FFFFFF, /* 0000 0000 1111 1111 1111 1111 1111 1111 */
  0x01FFFFFF, /* 0000 0001 1111 1111 1111 1111 1111 1111 */
  0x03FFFFFF, /* 0000 0011 1111 1111 1111 1111 1111 1111 */
  0x07FFFFFF, /* 0000 0111 1111 1111 1111 1111 1111 1111 */
  0x0FFFFFFF, /* 0000 1111 1111 1111 1111 1111 1111 1111 */
  0x1FFFFFFF, /* 0001 1111 1111 1111 1111 1111 1111 1111 */
  0x3FFFFFFF, /* 0011 1111 1111 1111 1111 1111 1111 1111 */
  0x7FFFFFFF, /* 0111 1111 1111 1111 1111 1111 1111 1111 */
  0xFFFFFFFF, /* 1111 1111 1111 1111 1111 1111 1111 1111 */
};

/*
 * Imported from DuckDB:
 * @see https://github.com/duckdb/duckdb/blob/v1.1.3/extension/parquet/include/decode_utils.hpp#L18-L38
 */
static gint32
parquet_rle_bp_encoder_bit_unpack_i32 (ParquetRleBpEncoder *encoder,
                                       gint32 *value)
{
  gint32 val = 0;
  guint32 mask = 0;
  gint ret = 0;
  guint8 cur_byte;

  mask = bp_mask[encoder->bit_width];
  cur_byte = encoder->cur_bytes[encoder->cur_byte_idx];

  val |= (cur_byte >> encoder->cur_bit_off) & mask;
  encoder->cur_bit_off += encoder->bit_width;

  while (encoder->cur_bit_off > 8)
  {
    encoder->cur_byte_idx++;

    if (encoder->cur_byte_idx >= encoder->bit_width)
    {
      encoder->cur_bytes = encoder->r_buf_ptr;
      encoder->r_buf_ptr += encoder->bit_width;
      ret += encoder->bit_width;
      encoder->cur_byte_idx = 0;
    }

    cur_byte = encoder->cur_bytes[encoder->cur_byte_idx];
    val |= (cur_byte << (8 - (encoder->cur_bit_off - encoder->bit_width))) & mask;
    encoder->cur_bit_off -= 8;
  }

  *value = val;

  return ret;
}

/*
 * TODO: Write BP
 *
 * static void
 * bit_pack (guint8 *buf, GPtrArray *unpacked, guint32 total_bytes, guint8 width)
 * {
 *   Value *val = NULL;
 *   gint32 bp_pos = 0;
 *   guint32 idx = 0;
 *
 *   for (guint i = 0; i < unpacked->len; i++)
 *   {
 *     val = unpacked->pdata[i];
 *     // XXX: Only supports UINT32
 *     buf[idx] |= (val->data.v_int32 << bp_pos) & 0xFF;
 *     bp_pos += width;
 *
 *     while (bp_pos > 8)
 *     {
 *       idx++;
 *       buf[idx] |= val->data.v_int32 >> (8 - (bp_pos - width)) & 0xFF;
 *       bp_pos -= 8;
 *     }
 *   }
 *
 *   g_assert(idx == total_bytes - 1, "byte size mismatched");
 * }
 */

static gint32
parquet_rle_bp_encoder_write_end_run (ParquetRleBpEncoder *encoder)
{
  gint32 varint_size = 0;
  gint32 ret = 0;
  guint8 run_header_bytes[5] = {0};
  guint8 run_value_bytes[4] = {0};

  /*
   * Write run header
   */
  varint_size = int32_to_varint ((encoder->run_len << 1),
                                 run_header_bytes);
  g_byte_array_append (encoder->w_buf, run_header_bytes, varint_size);
  ret += varint_size;

  /*
   * Write run value
   */
  i32_to_le (encoder->run_value, run_value_bytes);
  g_byte_array_append (encoder->w_buf, run_value_bytes, encoder->run_bytes);
  ret += encoder->run_bytes;

  return ret;
}

static gint32
parquet_rle_bp_encoder_read_start_run (ParquetRleBpEncoder *encoder)
{
  gint32 xfer;
  gint32 ret = 0;
  gint32 header;

  g_assert (encoder->run_bytes == 0,
            "run_bytes should be 0 to start a new run");

  xfer = varint_to_int32 (encoder->r_buf_ptr, &header);
  ret += xfer;
  encoder->r_buf_ptr += xfer;
  encoder->total_bytes -= xfer;

  /*
   * Prepare reading RLE only
   */
#ifndef PARQUET_RLE_DICT_ENCODING
  g_assert ((header & 0x1) == 0x0, "reading prohibited encoding BP");

  encoder->run_len = header >> 1;
  encoder->run_bytes = (encoder->bit_width + 7) / 8;
  g_assert (encoder->run_bytes == 1, "encoded level value should be 1byte");

  encoder->run_value = *encoder->r_buf_ptr;

  ret += encoder->run_bytes;
  encoder->r_buf_ptr += encoder->run_bytes;
  encoder->total_bytes -= encoder->run_bytes;
  encoder->run_bytes -= encoder->run_bytes;

  /*
   * Prepare reading both RLE/BP
   */
#else
  /*
   * LSB determines the run is encoded in BP or RLE.
   */
  encoder->is_bp = (header & 0x1) == 0x1;

  /*
   * Prepare reading a BP run
   */
  if (encoder->is_bp)
  {
    encoder->run_len = (header >> 1) * 8;
    encoder->run_bytes = encoder->run_len * encoder->bit_width / 8;

    /*
     * BP always packs 8 values at a time, so consume `encoder->bit_width` bytes
     * as a batch (width * 8 = width-bytes).
     */
    encoder->cur_bytes = encoder->r_buf_ptr;
    encoder->r_buf_ptr += encoder->bit_width;
    ret += encoder->bit_width;
    encoder->total_bytes -= encoder->bit_width;
    encoder->run_bytes -= encoder->bit_width;

    encoder->cur_byte_idx = 0;
    encoder->cur_bit_off = 0;
  }

  /*
   * Prepare reading a RLE run
   */
  else
  {
    guint8 run_value[4] = {0};

    encoder->run_len = header >> 1;
    encoder->run_bytes = (encoder->bit_width + 7) / 8;

    memcpy (run_value, encoder->r_buf_ptr, encoder->run_bytes);

    ret += encoder->run_bytes;
    encoder->r_buf_ptr += encoder->run_bytes;
    encoder->total_bytes -= encoder->run_bytes;
    encoder->run_bytes -= encoder->run_bytes;

    encoder->run_value = le_to_i32 (run_value);
  }
#endif

  return ret;
}


/* Public */

GType
parquet_rle_bp_encoder_get_type (void)
{
  return G_TYPE_PARQUET_RLE_BP_ENCODER;
}

gint32
parquet_rle_bp_encoder_read_list_begin (ParquetRleBpEncoder *encoder,
                                        guint32 total_bytes,
                                        gint8 bit_width)
{
  gint32 ret = 0;
  guint8 *ptr;

  /*
   * When RLE-BP enc/decoder is used only for definition level, total_bytes is
   * always prepended.
   */
#ifndef PARQUET_RLE_DICT_ENCODING
  ((void) total_bytes); // prevent -Wall
  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport, 4);
  ret += 4;
  encoder->total_bytes = le_to_i32 (ptr);

  /*
   * Read the data length if total_bytes is 0.
   */
#else
  if (total_bytes == 0)
  {
    ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport, 4);
    ret += 4;
    encoder->total_bytes = le_to_i32 (ptr);
  }
  else
  {
    encoder->total_bytes = total_bytes;
  }
#endif

  ptr = thrift_file_transport_get_unsafe_ptr (encoder->transport,
                                              encoder->total_bytes);
  encoder->r_buf_ptr = ptr;
  encoder->is_total_bytes_init = TRUE;

  encoder->bit_width = bit_width;
  encoder->run_len = 0;
  encoder->run_bytes = 0;
  encoder->is_bp = FALSE;
  encoder->cur_bit_off = 0;
  encoder->run_value = 0;

  return ret;
}

gint32
parquet_rle_bp_encoder_read_i32 (ParquetRleBpEncoder *encoder,
                                 gint32 *value)
{
  gint xfer = 0;
  gint32 ret = 0;

  g_assert (encoder->is_total_bytes_init,
            "total_bytes should be initiated");
  g_assert (encoder->bit_width != 0,
            "bit_width should not be 0");

  /*
   * Prepare reading a run
   */
  if (encoder->run_len < 1)
  {
    /*
     * Start a new run
     */
    xfer = parquet_rle_bp_encoder_read_start_run (encoder);
    if (xfer < 0)
      return -1;
    ret += xfer;
  }

  g_assert (encoder->run_len > 0, "wrong run length");

  /*
   * Read BP run
   */
  if (encoder->is_bp)
  {
    xfer = parquet_rle_bp_encoder_bit_unpack_i32 (encoder, value);
    if (xfer < 0)
      return -1;
    ret += xfer;
    encoder->total_bytes -= xfer;
    encoder->run_bytes -= xfer;
  }

  /*
   * Read RLE run
   */
  else
    *value = encoder->run_value;

  encoder->run_len--;

  return ret;
}

#ifndef PARQUET_RLE_DICT_ENCODING
gint32
parquet_rle_bp_encoder_read_nullmap (ParquetRleBpEncoder *encoder,
                                     guint32 *null_map, guint32 *notnull_cnt,
                                     guint32 cnt)
{
  gint xfer = 0;
  gint32 ret = 0;
  guint mask;
  guint bits;
  guint left_cnt;

  g_assert (encoder->is_total_bytes_init,
            "total_bytes should be initiated");
  g_assert (encoder->bit_width != 0,
            "bit_width should not be 0");

  *null_map = 0;
  *notnull_cnt = 0;

  for (guint i = 0; i < cnt;)
  {
    /*
     * Prepare reading a run
     */
    if (encoder->run_len < 1)
    {
      /*
       * Start a new run
       */
      xfer = parquet_rle_bp_encoder_read_start_run (encoder);
      if (xfer < 0)
        return -1;
      ret += xfer;
    }

    g_assert (encoder->run_len > 0, "wrong run length");

    left_cnt = MIN (encoder->run_len, cnt - i);

    /*
     * Convert run to the null bitmap
     */
    mask = (0x1 << left_cnt) - 1;
    bits = (encoder->run_value - 1) & mask;
    *null_map |= bits << i;

    *notnull_cnt += encoder->run_value * left_cnt;
    encoder->run_len -= left_cnt;
    i += left_cnt;
  }

  return ret;
}
#endif

void
parquet_rle_bp_encoder_write_list_begin (ParquetRleBpEncoder *encoder)
{
  encoder->total_bytes = 0;
  encoder->run_len = 0;

  g_assert (encoder->bit_width != 0, "bit width not provided");
  encoder->run_bytes = (encoder->bit_width + 7) / 8;

  encoder->run_value = 0;

  /* Reset w_buf */
  encoder->w_buf->len = 0;
}

gint32
parquet_rle_bp_encoder_write_list_end (ParquetRleBpEncoder *encoder,
                                       GError **error)
{
  gint32 ret = 0;
  gint32 xfer = 0;
  guint8 le[4];

  /*
   * Finalize the RLE run
   */
  xfer = parquet_rle_bp_encoder_write_end_run (encoder);
  if (xfer < 0)
    return -1;
  encoder->total_bytes += xfer;
  g_assert (encoder->w_buf->len == encoder->total_bytes, "data size mismatch");

  /*
   * Write data length when required
   */
  if (!encoder->is_total_bytes_init)
  {
    i32_to_le (encoder->total_bytes, le);
    if (!thrift_transport_write (encoder->transport, le,
                                 4, error))
      return -1;
    ret += 4;
  }

  /*
   * Flush all buffered data
   */
  if (!thrift_transport_write (encoder->transport,
                               encoder->w_buf->data,
                               encoder->total_bytes, error))
    return -1;
  ret += xfer;

  /* Reset w_buf */
  encoder->w_buf->len = 0;

  return ret;
}

/*
 * TODO: Use both RLE and BP for encoding (RLE encoding only for now)
 */
gint32
parquet_rle_bp_encoder_write_i32 (ParquetRleBpEncoder *encoder,
                                  const gint32 value)
{
  gint32 xfer = 0;
  gint32 ret = 0;

  /* First run */
  if (encoder->run_len == 0)
  {
    encoder->run_value = value;
    encoder->run_len++;
  }

  /* Continue run */
  else if (encoder->run_value == value)
  {
    encoder->run_len++;
  }

  /* Start new run */
  else
  {
    xfer = parquet_rle_bp_encoder_write_end_run (encoder);
    if (xfer < 0)
      return -1;
    ret += xfer;
    encoder->total_bytes += xfer;

    /* New run */
    encoder->run_value = value;
    encoder->run_len = 1;
  }

  return ret;
}

void
parquet_rle_bp_encoder_instance_init (ParquetRleBpEncoder *self)
{
  self->total_bytes = 0;
  self->is_total_bytes_init = FALSE;
  self->bit_width = 0;
  self->run_len = 0;
  self->run_bytes = 0;
  self->is_bp = FALSE;
  self->cur_bit_off = 0;
  self->run_value = 0;
  self->w_buf = g_byte_array_new ();
}

void
parquet_rle_bp_encoder_finalize (GObject *gobject)
{
  ParquetRleBpEncoder *encoder;

  g_assert (PARQUET_IS_RLE_BP_ENCODER (gobject),
            "not a rle-bp protocol");
  encoder = PARQUET_RLE_BP_ENCODER (gobject);
  g_byte_array_unref (encoder->w_buf);

  g_object_unref (encoder->transport);
}
