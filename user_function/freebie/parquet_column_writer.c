#include "garray.h"
#include "gobject-type.h"
#include "gtypes.h"
#include "parquet.h"
#include "parquet_plain_encoder.h"
#include "parquet_rle_bp_encoder.h"
#include "parquet_column_writer.h"
#include "parquet_types.h"
#include "thrift_protocol.h"
#include "thrift_transport.h"
#include "thrift_file_transport.h"
#include "utils.h"

// Init headers --------------------------------------------------------------

static void
statistics_init (Statistics *stat)
{
  // Accumulator
  stat->null_count = 0;
  stat->__isset_null_count = TRUE;

  // Cleanup unused
  stat->max = NULL;
  stat->__isset_max = FALSE;
  stat->min = NULL;
  stat->__isset_min = FALSE;
  stat->max_value = NULL;
  stat->__isset_max_value = FALSE;
  stat->min_value = NULL;
  stat->__isset_min_value = FALSE;
  stat->distinct_count = 0;
  stat->__isset_distinct_count = FALSE;
  stat->is_max_value_exact = FALSE;
  stat->__isset_is_max_value_exact = FALSE;
  stat->is_min_value_exact = FALSE;
  stat->__isset_is_min_value_exact = FALSE;
}
static void
data_page_header_init (DataPageHeader *data_phdr, Statistics *stat)
{
  // Set required
  data_phdr->encoding = ENCODING_PLAIN;
  data_phdr->definition_level_encoding = ENCODING_RLE;
  data_phdr->repetition_level_encoding = ENCODING_RLE;

  // Accumulator
  data_phdr->num_values = 0;
  data_phdr->statistics = stat;
  statistics_init (stat);
  data_phdr->__isset_statistics = TRUE;
}

static void
page_header_init (PageHeader* phdr, DataPageHeader *data_phdr, Statistics *stat)
{
  // Set required
  phdr->type = PAGE_TYPE_DATA_PAGE;
  phdr->data_page_header = data_phdr;
  data_page_header_init (data_phdr, stat);
  phdr->__isset_data_page_header = TRUE;

  // Determined later
  phdr->uncompressed_page_size = -1;
  phdr->compressed_page_size = -1;

  // Cleanup unused
  phdr->crc = 0;
  phdr->__isset_crc = FALSE;

  g_object_unref (phdr->index_page_header);
  phdr->index_page_header = NULL;
  phdr->__isset_index_page_header = FALSE;

  g_object_unref (phdr->dictionary_page_header);
  phdr->dictionary_page_header = NULL;
  phdr->__isset_dictionary_page_header = FALSE;

  g_object_unref (phdr->data_page_header_v2);
  phdr->data_page_header_v2 = NULL;
  phdr->__isset_data_page_header_v2 = FALSE;
}

static void
column_meta_data_init (ColumnMetaData *column_meta_data)
{
  // Set required
  /* .codec */
  column_meta_data->codec = COMPRESSION_CODEC_UNCOMPRESSED;

  // Determined in `*_prepare`
  /* .type */
  column_meta_data->type = -1;
  /* .path_in_schema */
  if (column_meta_data->path_in_schema == NULL)
    column_meta_data->path_in_schema = g_ptr_array_new_with_free_func (g_free);
  /* .data_page_offset */
  column_meta_data->data_page_offset = -1;

  // Accumulator
  /* .num_values */
  column_meta_data->num_values = 0;
  /* .total_size */
  column_meta_data->total_uncompressed_size = 0;
  column_meta_data->total_compressed_size = 0;
  /* .statistics */
  if (column_meta_data->statistics == NULL)
    column_meta_data->statistics = g_object_new (TYPE_STATISTICS,
                                     NULL);
  statistics_init (column_meta_data->statistics);
  column_meta_data->__isset_statistics = TRUE;

  // Cleanup unused
  /* .key_value_metadata */
  g_array_unref (column_meta_data->encodings);
  column_meta_data->encodings = NULL;
  g_ptr_array_unref (column_meta_data->key_value_metadata);
  column_meta_data->key_value_metadata = NULL;
  column_meta_data->__isset_key_value_metadata = FALSE;
  /* .index_page_offset */
  column_meta_data->index_page_offset = 0;
  column_meta_data->__isset_index_page_offset = FALSE;
  /* .dictionary_page_offset */
  column_meta_data->dictionary_page_offset = 0;
  column_meta_data->__isset_dictionary_page_offset = FALSE;
  /* .encoding_stats */
  g_ptr_array_unref (column_meta_data->encoding_stats);
  column_meta_data->encoding_stats = NULL;
  column_meta_data->__isset_encoding_stats = FALSE;
  /* .bloom_filter_offset */
  column_meta_data->bloom_filter_offset = 0;
  column_meta_data->__isset_bloom_filter_offset = FALSE;
  /* .bloom_filter_length */
  column_meta_data->bloom_filter_length = 0;
  column_meta_data->__isset_bloom_filter_length = FALSE;
  /* .size_statistics */
  g_object_unref (column_meta_data->size_statistics);
  column_meta_data->size_statistics = NULL;
  column_meta_data->__isset_size_statistics = FALSE;
}

static void
column_chunk_init (ColumnChunk *column_chunk)
{
  // Set required
  /* .meta_data */
  if (column_chunk->meta_data == NULL)
    column_chunk->meta_data = g_object_new (TYPE_COLUMN_META_DATA,
                                            NULL);
  column_meta_data_init (column_chunk->meta_data);
  column_chunk->__isset_meta_data = TRUE;

  // Cleanup unused
  /* .file_path */
  column_chunk->file_path = NULL;
  column_chunk->__isset_file_path = FALSE;
  /*
   * .file_offset
   * this field in column chunk is deprecated
   */
  column_chunk->file_offset = 0;
  /* .offset_index_offset */
  column_chunk->offset_index_offset = 0;
  column_chunk->__isset_offset_index_offset = FALSE;
  /* .offset_index_length */
  column_chunk->offset_index_length = 0;
  column_chunk->__isset_offset_index_length = FALSE;
  /* .column_index_offset */
  column_chunk->column_index_offset = 0;
  column_chunk->__isset_column_index_offset = FALSE;
  /* .column_index_length */
  column_chunk->column_index_length = 0;
  column_chunk->__isset_column_index_length = FALSE;
  /* .crypto_metadata */
  g_object_unref (column_chunk->crypto_metadata);
  column_chunk->crypto_metadata = NULL;
  column_chunk->__isset_crypto_metadata = FALSE;
  /* .encrypted_column_metadata */
  column_chunk->encrypted_column_metadata = NULL;
  column_chunk->__isset_encrypted_column_metadata = FALSE;
}

// Data page writer ----------------------------------------------------------

static void
data_page_writer_prepare (ColumnWriter *writer)
{
  ParquetPlainEncoder *plain;
  ParquetRleBpEncoder *rle;
  gboolean is_fixed_length;
  gint32 fixed_length;

  plain = &writer->plain_encoder;
  rle = &writer->rle_bp_encoder;

  is_fixed_length = (writer->schema->type == TYPE_FIXED_LEN_BYTE_ARRAY);
  fixed_length = writer->schema->type_length;

  /*
   * Init encoders:
   * The write buffer size is allocated in powers of 2, which means it will
   * likely expand to MAX_PAGE_LIMIT. To accommodate this, we pre-allocate the
   * maximum required memory.
   */
  rle->is_total_bytes_init = FALSE;
  rle->bit_width = 1;
  parquet_rle_bp_encoder_write_list_begin (rle);
  parquet_plain_encoder_write_list_begin (plain, is_fixed_length,
                                          fixed_length);

  /*
   * Init page header
   */
  page_header_init (&writer->page_hdr,
                    &writer->data_page_hdr,
                    &writer->data_page_stat);

  /*
   * Init counters
   * `writer->def_bytes` is initiated to `4` for prepended 4-byte little endian
   * RLE-BP length
   */
  writer->phdr_bytes = 0;
  writer->def_bytes = 4;
  writer->data_bytes = 0;
}

static gint32
data_page_writer_write (ColumnWriter *writer, Value *value, GError **error)
{
  ParquetPlainEncoder *plain;
  ParquetRleBpEncoder *rle;
  gint32 xfer = 0;
  gint32 ret = 0;

  plain = &writer->plain_encoder;
  rle = &writer->rle_bp_encoder;

  if (IS_NULL_VALUE (value))
  {
    /*
     * Write definition level only
     */
    xfer = parquet_rle_bp_encoder_write_i32 (rle, 0);
    if (xfer < 0)
      return -1;
    ret += xfer;

    writer->def_bytes += xfer;
    writer->data_page_stat.null_count++;
    writer->data_page_hdr.num_values++;

    return ret;
  }

  /*
   * Write definition level
   */
  xfer = parquet_rle_bp_encoder_write_i32 (rle, 1);
  if (xfer < 0)
    return -1;
  ret += xfer;
  writer->def_bytes += xfer;

  /*
   * Write data
   */
  switch (writer->schema->type)
  {
  case TYPE_BOOLEAN:
    {
      xfer = parquet_plain_encoder_write_bool (plain, value, error);
    }
    break;
  case TYPE_INT32:
    {
      xfer = parquet_plain_encoder_write_i32 (plain, value, error);
    }
    break;
  case TYPE_INT64:
    {
      xfer = parquet_plain_encoder_write_i64 (plain, value, error);
    }
    break;
  case TYPE_FLOAT:
    {
      xfer = parquet_plain_encoder_write_float (plain, value, error);
    }
    break;
  case TYPE_DOUBLE:
    {
      xfer = parquet_plain_encoder_write_double (plain, value, error);
    }
    break;
  case TYPE_BYTE_ARRAY:
    {
      xfer = parquet_plain_encoder_write_byte_array (plain, value, error);
    }
    break;
  case TYPE_FIXED_LEN_BYTE_ARRAY:
    {
      xfer = parquet_plain_encoder_write_fixed_len_byte_array (plain, value, error);
    }
    break;
  default:
    {
      g_set_error (error,
                   THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                   "unknown data type (type='%d')",
                   writer->schema->type);
      return -1;
    }
  }
  if (xfer < 0)
    return -1;
  ret += xfer;
  writer->data_bytes += xfer;

  writer->data_page_hdr.num_values++;
  return ret;
}

static gint32
data_page_writer_end (ColumnWriter *writer, GError **error)
{
  ParquetRleBpEncoder *rle;
  ParquetPlainEncoder *plain;
  gint32 xfer = 0;
  gint32 ret = 0;
  gint64 total_bytes;

  rle = &writer->rle_bp_encoder;
  plain = &writer->plain_encoder;

  /*
   * Estimate and set total data size
   */
  // Last run
  writer->def_bytes += rle->run_bytes + get_varint_size (rle->run_len << 1);
  // Last boolean
  writer->data_bytes += plain->bool_idx > 0 ? 1 : 0;
  // Defereed data
  writer->data_bytes += plain->seq_len;
  // Set total size
  total_bytes = writer->def_bytes + writer->data_bytes;
  writer->page_hdr.uncompressed_page_size = total_bytes;
  writer->page_hdr.compressed_page_size = total_bytes;

  /*
   * Write
   */
  // Data
  xfer = parquet_plain_encoder_write_list_end (plain, error);
  if (xfer < 0)
    return -1;
  ret += xfer;
  // Sanity check
  {
    gint64 begin, end;

    begin = column_writer_get_begin_offset (writer);
    end = thrift_file_transport_get_location (writer->compact_protocol->transport);
    g_assert (writer->data_bytes == end - begin, "Wrong data_bytes estimation");

    // Prevent -Wall
    ((void) begin);
    ((void) end);
  }

  // Page header
  xfer = thrift_struct_write (THRIFT_STRUCT (&(writer->page_hdr)),
                              writer->compact_protocol,
                              error);
  if (xfer < 0)
    return -1;
  ret += xfer;
  writer->phdr_bytes = xfer; // Memoize
  // Definition level
  xfer = parquet_rle_bp_encoder_write_list_end (rle, error);
  if (xfer < 0)
    return -1;
  ret += xfer;

  return ret;
}

// Column writer -------------------------------------------------------------

GType
column_writer_get_type (void)
{
  return G_TYPE_COLUMN_WRITER;
}

void
column_writer_prepare (ColumnWriter *writer, SchemaElement *schema)
{
  ThriftTransport *transport;
  ColumnChunk *column_chunk;
  ColumnMetaData *column_meta;
  gchar *buf;

  g_assert (writer->compact_protocol != NULL,
            "input not provided");

  transport = writer->compact_protocol->transport;

  if (writer->plain_encoder.transport == NULL)
    writer->plain_encoder.transport = g_object_ref (transport);
  if (writer->rle_bp_encoder.transport == NULL)
    writer->rle_bp_encoder.transport = g_object_ref (transport);

  /*
   * Init header and metadata
   */
  g_object_unref (writer->schema);
  writer->schema = g_object_ref (schema);

  g_object_unref (writer->column_chunk);
  column_chunk = g_object_new (TYPE_COLUMN_CHUNK,
                               NULL);
  column_chunk_init (column_chunk);
  column_meta = column_chunk->meta_data;

  column_meta->type = schema->type;

  /*
   * In relational data model that do not support nested schema, `path_in_schema`
   * contains only the column name.
   */
  string_copy (&buf, schema->name);
  g_ptr_array_add (column_meta->path_in_schema, buf);

  column_meta->data_page_offset = thrift_file_transport_get_location (transport);

  writer->column_chunk = column_chunk;
  writer->is_init = TRUE;

  /*
   * Prepare data pare writer
   */
  data_page_writer_prepare (writer);
}

gint32
column_writer_write (ColumnWriter *writer, Value *value, GError **error)
{
  g_assert (writer->is_init, "ColumnWriter not initiated");
  /*
   * Return the actual I/O amount.
   */
  return data_page_writer_write (writer, value, error);
}

gint32
column_writer_end (ColumnWriter *writer, GError **error)
{
  ColumnMetaData *column_meta;
  gint32 ret = 0;
  gint64 total_bytes = 0;

  g_assert (writer->is_init, "ColumnWriter not initiated");

  /*
   * Finalize the data page writer
   */
  ret = data_page_writer_end (writer, error);
  if (ret < 0)
    return -1;
  column_meta = writer->column_chunk->meta_data;

  /*
   * Finalize the column chunk metadata
   */
  column_meta->num_values += writer->data_page_hdr.num_values;
  column_meta->statistics->null_count += writer->data_page_stat.null_count;

  if (column_meta->num_values)
  {
    total_bytes = writer->phdr_bytes +
                  writer->def_bytes +
                  writer->data_bytes;
  }

  column_meta->total_compressed_size = total_bytes;
  column_meta->total_uncompressed_size = total_bytes;

  writer->is_init = FALSE;

  return ret;
}

void
column_writer_finalize (GObject *object)
{
  ColumnWriter *writer;

  writer = COLUMN_WRITER (object);

  g_object_unref (writer->compact_protocol);
  parquet_plain_encoder_finalize ((GObject *) &writer->plain_encoder);
  parquet_rle_bp_encoder_finalize ((GObject *) &writer->rle_bp_encoder);
  g_object_unref (writer->schema);
  g_object_unref (writer->column_chunk);
}

void
column_writer_instance_init (ColumnWriter * object)
{
  object->compact_protocol = NULL;

  parquet_plain_encoder_instance_init (&object->plain_encoder);
  G_TYPE_FROM_INSTANCE (&object->plain_encoder) = G_TYPE_PARQUET_PLAIN_ENCODER;
  object->plain_encoder.transport = NULL;

  parquet_rle_bp_encoder_instance_init (&object->rle_bp_encoder);
  G_TYPE_FROM_INSTANCE (&object->rle_bp_encoder) = G_TYPE_PARQUET_RLE_BP_ENCODER;
  object->rle_bp_encoder.transport = NULL;

  object->schema = NULL;
  object->column_chunk = NULL;
  object->is_init = FALSE;

  object->phdr_bytes = 0;
  object->def_bytes = 0;
  object->data_bytes = 0;

  page_header_instance_init (&object->page_hdr);
  G_TYPE_FROM_INSTANCE (&object->page_hdr) = G_TYPE_PAGE_HEADER;

  data_page_header_instance_init (&object->data_page_hdr);
  G_TYPE_FROM_INSTANCE (&object->data_page_hdr) = G_TYPE_DATA_PAGE_HEADER;

  statistics_instance_init (&object->data_page_stat);
  G_TYPE_FROM_INSTANCE (&object->data_page_stat) = G_TYPE_STATISTICS;

  dictionary_page_header_instance_init (&object->delta_page_hdr);
  G_TYPE_FROM_INSTANCE (&object->delta_page_hdr) = G_TYPE_DICTIONARY_PAGE_HEADER;
}
