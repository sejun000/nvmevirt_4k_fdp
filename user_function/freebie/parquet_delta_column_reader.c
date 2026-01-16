#include "parquet_delta_column_reader.h"
#include "gerror.h"
#include "gobject.h"
#include "gtypes.h"
#include "parquet.h"
#include "parquet_plain_encoder.h"
#include "parquet_rle_bp_encoder.h"
#include "parquet_types.h"
#include "thrift_protocol.h"
#include "thrift_struct.h"
#include "thrift_file_transport.h"
#include "thrift_transport.h"

// Data page reader -----------------------------------------------------------

static void
delta_page_header_init (PageHeader* page_hdr,
                        DictionaryPageHeader *delta_page_hdr)
{
  page_header_instance_init (page_hdr);
  dictionary_page_header_instance_init (delta_page_hdr);

  page_hdr->dictionary_page_header = delta_page_hdr;
  page_hdr->__isset_dictionary_page_header = TRUE;
}

static gint32
delta_page_reader_prepare (ColumnReader *reader, GError **error)
{
  gint32 xfer = 0;
  gint32 ret = 0;

  /*
   * Read and check data page header
   */
  delta_page_header_init (&reader->page_hdr,
                          &reader->delta_page_hdr);

  xfer = thrift_struct_read (THRIFT_STRUCT (&reader->page_hdr),
                             reader->compact_protocol,
                             error);
  if (xfer < 0)
    return -1;
  ret += xfer;

  if (!reader->page_hdr.__isset_dictionary_page_header)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Not dictionary page header for delta page");
    return -1;
  }

  if (reader->page_hdr.compressed_page_size
      != reader->page_hdr.uncompressed_page_size)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Data page must not be compressed");
    return -1;
  }

  /*
   * Init read status
   * `num_values` in page header is not used for delta page
   */
  reader->page_left_values = 0;
  reader->page_left_bytes = reader->page_hdr.uncompressed_page_size;

  /*
   * Repetition level:
   * As repetition level is used to represent nested columns, this is not used
   * in relational data. So, the repetition type must be set to optional and not
   * exist on the data page.
   */
  if (reader->schema->__isset_repetition_type != FIELD_REPETITION_TYPE_OPTIONAL)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Repetition must be set to optional");
    return -1;
  }

  /*
   * Preapre reading delta page
   */
  switch (reader->delta_page_hdr.encoding)
  {
  case ENCODING_PLAIN:
    {
      xfer = parquet_plain_encoder_read_list_begin (&reader->plain_encoder,
                                                    FALSE, 0, error);
      if (xfer < 0)
        return -1;
      ret += xfer;
      reader->page_left_bytes -= xfer;
    }
    break;
  default:
    {
      g_set_error (error,
                   THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                   "Delta page must be plain-encoded (type='%d')",
                   reader->data_page_hdr.encoding);
      return -1;
    }
  }

  return ret;
}

static gint32
delta_page_reader_read (ColumnReader *reader, Vector *vector, GError **error)
{
  gint32 xfer = 0;
  gint32 ret = 0;
  guint cnt = 0;

  for (guint i = 0; i < VECTOR_MAX_SIZE; i++)
  {
    Value *value = vector->values + i;

    /*
     * Page is definitely exhausted
     */
    if (reader->page_left_bytes < 4)
      break;

    if (reader->delta_page_hdr.encoding == ENCODING_PLAIN)
    {
      ParquetPlainEncoder *encoder = &reader->plain_encoder;

      switch (reader->column_chunk->meta_data->type)
      {
      case TYPE_BYTE_ARRAY:
        xfer = parquet_plain_encoder_read_byte_array (encoder, value);
        break;
      default:
        {
          g_set_error (error,
                       THRIFT_PROTOCOL_ERROR,
                       THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                       "Delta column must be BYTE_ARRAY type (type='%d')",
                       reader->column_chunk->meta_data->type);
          return -1;
        }
      }

      if (xfer < 0)
        return -1;
      ret += xfer;
      reader->page_left_bytes -= xfer;

      /*
       * No more delta in a page; "4" means that we read only the prepended
       * byte array size, which is 0.
       */
      if (value->len == 4)
        break;

      cnt++;
    }
    else
    {
      g_set_error (error,
                   THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                   "Delta page must be plain-encoded (type='%d')",
                   reader->data_page_hdr.encoding);
      return -1;
    }
  }

  vector->null_map = 0;
  vector->total_cnt = cnt;
  vector->value_cnt = cnt;

  return ret;
}

// Column reader --------------------------------------------------------------

gint32
delta_column_reader_prepare (ColumnReader *reader, SchemaElement *schema,
                             ColumnChunk *column_chunk, GError **error)
{
  ColumnMetaData *col_meta = NULL;
  ThriftTransport *transport = NULL;
  goffset offset = 0;
  gint32 ret = 0;
  gint32 xfer = 0;

  /*
   * Check input values
   */
  g_assert (!reader->is_init, "must not be initiated");

  g_assert (reader->compact_protocol != NULL,
            "required input not provided");

  if (!column_chunk->__isset_meta_data)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Column meta data not set");
    return -1;
  }

  col_meta = column_chunk->meta_data;

  g_assert (col_meta->type == schema->type,
            "wrong schema for the column");

  if (column_chunk->__isset_file_path)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Column chunk should be in same file");
    return -1;
  }

  if (column_chunk->file_offset != 0)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Column chunk file offset must be set to 0 as"
                 "this field is deprecated");
    return -1;
  }

  if (col_meta->codec != COMPRESSION_CODEC_UNCOMPRESSED ||
      col_meta->total_uncompressed_size != col_meta->total_compressed_size)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Column must not be compressed");
    return -1;
  }

  if (!col_meta->__isset_dictionary_page_offset)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Delta column must be dictionary-plain encoded.");
    return -1;
  }

  /*
   * Initiate status
   */
  g_object_unref (reader->schema);
  g_object_unref (reader->column_chunk);
  reader->schema = g_object_ref (schema);
  reader->column_chunk = g_object_ref (column_chunk);

  /*
   * Init read status
   * `num_values` in column chunk is not used for delta column
   */
  reader->column_left_values = 0;
  reader->column_left_bytes = col_meta->total_uncompressed_size;
  reader->page_left_values = 0;
  reader->page_left_bytes = 0;

  /*
   * Set offset to dictionary page offset as the deltas are stored in
   * dictionary page
   */
  offset = col_meta->dictionary_page_offset;

  transport = reader->compact_protocol->transport;
  if (reader->plain_encoder.transport == NULL)
    reader->plain_encoder.transport = g_object_ref (transport);
  if (reader->def_encoder.transport == NULL)
    reader->def_encoder.transport = g_object_ref (transport);

  thrift_file_transport_set_location (transport, offset);

  /*
   * Prepare reading the first delta page
   */
  xfer = delta_page_reader_prepare (reader, error);
  if (xfer < 0)
    return -1;

  ret += xfer;
  reader->column_left_bytes -= xfer;
  reader->is_init = TRUE;

  return ret;
}

gint32
delta_column_reader_read (ColumnReader *reader, Vector *vector, GError **error)
{
  gint32 xfer = 0;
  gint32 ret = 0;

  g_assert (reader->is_init, "reader not initiated");

  /*
   * Try reading the vector.
   */
  xfer = delta_page_reader_read (reader, vector, error);
  if (xfer < 0)
    return -1;
  ret += xfer;
  reader->column_left_bytes -= xfer;

  /*
   * All values in the delta page are exhausted. Stop reading.
   */
  if (vector->total_cnt == 0)
  {
    reader->is_init = FALSE;
    return ret;
  }

  return ret;
}

ColumnReader *
parquet_reader_read_delta_column_prepare (ParquetReader *reader,
                                          GError **error)
{
  ColumnReader *column_reader;
  FileMetaData *file_meta_data;
  guint num_cols;
  guint column_idx;
  SchemaElement *schema;
  RowGroup *row_group;
  ColumnChunk *column_chunk;
  gint32 xfer;

  /*
   * Sanity check
   */
  g_assert (reader->column_reader != NULL, "column reader not prepared");
  column_reader = reader->column_reader;

  g_assert (reader->file_meta_data != NULL, "file_meta_data not prepared");
  file_meta_data = reader->file_meta_data;
  num_cols = file_meta_data->schema->len - 1;

  /*
   * Delta column is always the last column
   */
  column_idx = num_cols - 1;

  g_assert (column_idx < num_cols, "wrong column_idx");
  schema = file_meta_data->schema->pdata[column_idx + 1];

  g_assert ((guint) reader->cur_row_group_idx < file_meta_data->row_groups->len,
            "wrong row_group_idx");
  row_group = file_meta_data->row_groups->pdata[reader->cur_row_group_idx];

  g_assert (column_idx < row_group->columns->len,
            "wrong column_idx");
  column_chunk = row_group->columns->pdata[column_idx];

  /*
   * Prepare column reader
   */
  xfer = delta_column_reader_prepare (column_reader, schema, column_chunk, error);
  if (xfer < 0)
    return NULL;

  return column_reader;
}
