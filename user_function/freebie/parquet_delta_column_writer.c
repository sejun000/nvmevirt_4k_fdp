#include "garray.h"
#include "gtypes.h"
#include "parquet.h"
#include "parquet_plain_encoder.h"
#include "parquet_rle_bp_encoder.h"
#include "parquet_delta_column_writer.h"
#include "parquet_types.h"
#include "thrift_protocol.h"
#include "thrift_transport.h"
#include "thrift_file_transport.h"
#include "utils.h"

#define DELTA_PAGE_RESERVE_SIZE (1 << 14) // 16Ki

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
dict_page_header_init (DictionaryPageHeader *dict_phdr)
{
  // Set required
  dict_phdr->encoding = ENCODING_PLAIN;

  // Not use
  dict_phdr->num_values = 0;
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
delta_page_header_init (PageHeader* phdr, DictionaryPageHeader *delta_phdr)
{
  // Set required
  phdr->type = PAGE_TYPE_DICTIONARY_PAGE;
  phdr->dictionary_page_header = delta_phdr;
  dict_page_header_init (delta_phdr);
  phdr->__isset_dictionary_page_header = TRUE;

  // Determined later
  phdr->uncompressed_page_size = -1;
  phdr->compressed_page_size = -1;

  // Cleanup unused
  phdr->crc = 0;
  phdr->__isset_crc = FALSE;

  phdr->data_page_header = NULL;
  phdr->__isset_data_page_header = FALSE;

  g_object_unref (phdr->index_page_header);
  phdr->index_page_header = NULL;
  phdr->__isset_index_page_header = FALSE;

  g_object_unref (phdr->data_page_header_v2);
  phdr->data_page_header_v2 = NULL;
  phdr->__isset_data_page_header_v2 = FALSE;
}

static void
null_page_header_init (PageHeader* phdr, DataPageHeader *data_phdr,
                       Statistics *stat)
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

  phdr->dictionary_page_header = NULL;
  phdr->__isset_dictionary_page_header = FALSE;

  g_object_unref (phdr->index_page_header);
  phdr->index_page_header = NULL;
  phdr->__isset_index_page_header = FALSE;

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
  /* .dictionary_page_offset */
  column_meta_data->dictionary_page_offset = -1;
  column_meta_data->__isset_dictionary_page_offset = TRUE;

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
delta_page_writer_prepare (ColumnWriter *writer)
{
  ParquetPlainEncoder *plain;

  plain = &writer->plain_encoder;

  /*
   * Init encoder
   */
  parquet_plain_encoder_write_list_begin (plain, FALSE,
                                          0);

  /*
   * Init page header
   */
  delta_page_header_init (&writer->page_hdr,
                          &writer->delta_page_hdr);

  /*
   * Init counter
   */
  writer->phdr_bytes = 0;
  writer->data_bytes = 0;
}

static void
null_page_writer_prepare (ColumnWriter *writer)
{
  ParquetRleBpEncoder *rle;

  rle = &writer->rle_bp_encoder;

  /*
   * Init encoders:
   * The write buffer size is allocated in powers of 2, which means it will
   * likely expand to MAX_PAGE_LIMIT. To accommodate this, we pre-allocate the
   * maximum required memory.
   */
  rle->is_total_bytes_init = FALSE;
  rle->bit_width = 1;
  parquet_rle_bp_encoder_write_list_begin (rle);

  /*
   * Init page header
   */
  null_page_header_init (&writer->page_hdr,
                         &writer->data_page_hdr,
                         &writer->data_page_stat);

  /*
   * Init counters
   * `writer->def_bytes` is initiated to `4` for prepended 4-byte little endian
   * RLE-BP length
   */
  writer->def_bytes = 4;
}

static gint32
delta_page_writer_write (ColumnWriter *writer, Value *value, GError **error)
{
  ParquetPlainEncoder *plain;
  gint32 xfer = 0;
  gint32 ret = 0;

  plain = &writer->plain_encoder;

  /*
   * Write delta
   */
  switch (writer->schema->type)
  {
  case TYPE_BYTE_ARRAY:
    {
      xfer = parquet_plain_encoder_write_byte_array (plain, value, error);
    }
    break;
  default:
    {
      g_set_error (error,
                   THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                   "Delta column must be BYTE_ARRAY type (type='%d')",
                   writer->schema->type);
      return -1;
    }
  }

  if (xfer < 0)
    return -1;
  ret += xfer;
  writer->data_bytes += xfer;

  return ret;
}

static gint32
null_page_writer_write (ColumnWriter *writer)
{
  ParquetRleBpEncoder *rle;
  gint32 xfer = 0;
  gint32 ret = 0;

  rle = &writer->rle_bp_encoder;

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

static gint32
delta_page_writer_end (ColumnWriter *writer, GError **error)
{
  ParquetPlainEncoder *plain;
  gint32 xfer = 0;
  gint32 ret = 0;

  plain = &writer->plain_encoder;

  /*
   * Set total data size
   */
  // Deferred data
  writer->data_bytes += plain->seq_len;
  writer->page_hdr.uncompressed_page_size = writer->data_bytes;
  writer->page_hdr.compressed_page_size = writer->data_bytes;

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
    end =
      thrift_file_transport_get_location (writer->compact_protocol->transport);
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

  return ret;
}

static gint32
null_page_writer_end (ColumnWriter *writer, GError **error)
{
  ParquetRleBpEncoder *rle;
  gint32 xfer = 0;
  gint32 ret = 0;

  rle = &writer->rle_bp_encoder;

  /*
   * Set total data size
   */
  // Last run
  writer->def_bytes += rle->run_bytes + get_varint_size (rle->run_len << 1);
  // Set total size
  writer->page_hdr.uncompressed_page_size = writer->def_bytes;
  writer->page_hdr.compressed_page_size = writer->def_bytes;

  /*
   * Write
   */
  // Page header
  xfer = thrift_struct_write (THRIFT_STRUCT (&(writer->page_hdr)),
                              writer->compact_protocol,
                              error);
  if (xfer < 0)
    return -1;
  ret += xfer;
  /*
   * Since `writer->phdr_bytes` is already used for the delta page header, we
   * use `writer->def_bytes` as the sum of the page header size and the
   * definition size for the null page (@see freebie_repartition.c).
   */
  writer->def_bytes += xfer;

  // Definition level
  xfer = parquet_rle_bp_encoder_write_list_end (rle, error);
  if (xfer < 0)
    return -1;
  ret += xfer;

  return ret;
}

// Column writer -------------------------------------------------------------

static void
delta_column_writer_prepare (ColumnWriter *writer, SchemaElement *schema)
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

  /*
   * Set delta page offset
   */
  column_meta->dictionary_page_offset =
      thrift_file_transport_get_location (transport);
  column_meta->data_page_offset = -1;

  writer->column_chunk = column_chunk;
  writer->is_init = TRUE;

  /*
   * Prepare delta page writer
   */
  delta_page_writer_prepare (writer);
}

gint32
delta_column_writer_prepare_null (ColumnWriter *writer, GError **error)
{
  ThriftTransport *transport;
  ColumnMetaData *column_meta;
  gint32 ret = 0;

  g_assert (writer->is_init,
            "ColumnWriter not initiated");

  /*
   * Finalize the delta page writer
   */
  ret = delta_page_writer_end (writer, error);
  if (ret < 0)
    return -1;

  /*
   * Update the column chunk metadata
   */
  column_meta = writer->column_chunk->meta_data;
  column_meta->total_compressed_size += ret;
  column_meta->total_uncompressed_size += ret;

  /*
   * Set null page offset
   */
  transport = writer->compact_protocol->transport;
  column_meta->data_page_offset = thrift_file_transport_get_location (transport);

  /*
   * Prepare null page writer
   */
  null_page_writer_prepare (writer);

  return ret;
}

gint32
delta_column_writer_write (ColumnWriter *writer, Value *value, GError **error)
{
  g_assert (writer->is_init, "ColumnWriter not initiated");

  /*
   * Return the actual I/O amount.
   */
  return delta_page_writer_write (writer, value, error);
}

gint32
delta_column_writer_write_null (ColumnWriter *writer)
{
  /*
   * Return the actual I/O amount.
   */
  if (null_page_writer_write (writer) < 0)
    return -1;

  return 0;
}

static gint32
delta_column_writer_end (ColumnWriter *writer, GError **error)
{
  ColumnMetaData *column_meta;
  gint32 ret = 0;
  gint64 total_bytes = 0;

  g_assert (writer->is_init, "ColumnWriter not initiated");

  /*
   * Finalize the data page writer
   */
  ret = null_page_writer_end (writer, error);
  if (ret < 0)
    return -1;
  column_meta = writer->column_chunk->meta_data;

  /*
   * Finalize the column chunk metadata
   */
  column_meta->num_values += writer->data_page_hdr.num_values;
  column_meta->statistics->null_count += writer->data_page_stat.null_count;

  if (writer->data_bytes)
  {
    total_bytes = writer->phdr_bytes +
                  writer->data_bytes +
                  writer->def_bytes;
  }

  column_meta->total_compressed_size = total_bytes;
  column_meta->total_uncompressed_size = total_bytes;

  writer->is_init = FALSE;

  return ret;
}

ColumnWriter *
parquet_writer_write_delta_column_prepare (ParquetWriter *writer)
{
  GPtrArray *schema_arr;
  SchemaElement *schema;

  g_assert (writer->column_writer != NULL,
            "column writer not created");

  schema_arr = writer->file_meta_data->schema;
  schema = g_ptr_array_index (schema_arr, schema_arr->len - 1);
  delta_column_writer_prepare (writer->column_writer, schema);

  return writer->column_writer;
}

gboolean
parquet_writer_write_delta_column_end (ParquetWriter *writer, GError **error)
{
  RowGroup *row_group;
  ColumnChunk *column_chunk;
  ColumnMetaData *column_meta_data;
  gint32 ret;

  g_assert (writer->cur_row_group != NULL,
            "row group writer not prepared");
  row_group = writer->cur_row_group;

  g_assert (writer->column_writer != NULL,
            "column writer not created");

  ret = delta_column_writer_end (writer->column_writer, error);
  if (ret < 0)
    return FALSE;

  writer->written_amount += ret;
  column_chunk = writer->column_writer->column_chunk;
  column_meta_data = column_chunk->meta_data;

  g_ptr_array_add (row_group->columns,
                   g_object_ref (column_chunk));

  row_group->num_rows = column_meta_data->num_values;
  row_group->total_byte_size += column_meta_data->total_uncompressed_size;
  row_group->total_compressed_size += column_meta_data->total_compressed_size;

  return TRUE;
}
