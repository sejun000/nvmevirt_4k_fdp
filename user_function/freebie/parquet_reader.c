#include "gerror.h"
#include "gobject-type.h"
#include "parquet.h"
#include "parquet_reader.h"
#include "parquet_column_reader.h"
#include "parquet_types.h"
#include "thrift_file_transport.h"
#include "thrift_protocol.h"
#include "thrift_transport.h"
#include "utils.h"

static FileMetaData *
parquet_reader_get_file_meta_data (ThriftProtocol *protocol, GError **error);

GType
parquet_reader_get_type (void)
{
  return G_TYPE_PARQUET_READER;
}

gboolean
parquet_reader_prepare (ParquetReader *reader, gchar *filename, int filesize, GError **error)
{
  ThriftProtocol *protocol;
  FileMetaData *file_meta_data;
  ColumnReader *column_reader;

  /*
   * Create ThriftTransport and ThriftProtocol
   */
  protocol = create_thrift_file_compact_protocol (filename, filesize, filesize, error);
  if (protocol == NULL)
    return FALSE;
  reader->compact_protocol = protocol;

  /*
   * Read FileMetaData
   */
  file_meta_data = parquet_reader_get_file_meta_data (protocol, error);
  if (file_meta_data == NULL)
    return FALSE;
  reader->file_meta_data = file_meta_data;

  /*
   * Prepare ColumnReader
   */
  column_reader = g_object_new (G_TYPE_COLUMN_READER,
                                NULL);
  column_reader->compact_protocol = g_object_ref (protocol);
  reader->column_reader = column_reader;

  return TRUE;
}

gint32
parquet_reader_end (ParquetReader *reader, GError **error)
{
  g_assert (reader->compact_protocol != NULL,
            "compact_protocol not prepared");

  /*
   * Close the transport
   */
  if (!thrift_transport_close (reader->compact_protocol->transport,
                               error))
    return -1;

  return 0;
}

gboolean
parquet_reader_read_row_group_prepare (ParquetReader *reader)
{
  g_assert (reader->file_meta_data != NULL,
            "file_meta_data not prepared");

  reader->cur_row_group_idx++;

  return (guint) reader->cur_row_group_idx <
         reader->file_meta_data->row_groups->len;
}

ColumnReader *
parquet_reader_read_column_chunk_prepare (ParquetReader *reader,
                                          guint column_idx,
                                          GError **error)
{
  ColumnReader *column_reader;
  SchemaElement *schema;
  FileMetaData *file_meta_data;
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

  g_assert (column_idx + 1 < file_meta_data->schema->len,
            "wrong column_idx");
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
  xfer = column_reader_prepare (column_reader, schema,
                                column_chunk, error);
  if (xfer < 0)
    return NULL;

  return column_reader;
}

static FileMetaData *
parquet_reader_get_file_meta_data (ThriftProtocol *protocol, GError **error)
{
  FileMetaData *file_meta_data;
  gssize file_size;
  gint footer_size;
  guint8 _footer_size[4];
  guint8 magic[4];
  goffset footer_offset;
  gint32 xfer;

  /*
   * Validate file size
   */
  file_size = thrift_file_transport_get_size (protocol->transport, error);
  if (file_size < 0)
    return NULL;

  if (file_size < 12)
  {
    g_set_error (error, THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "File size too small");
    return NULL;
  }

  /*
   * Read all
   */
  if (!thrift_file_transport_prefetch (protocol->transport, 0,
                                       file_size, error))
    return NULL;

  thrift_file_transport_set_location (protocol->transport, file_size - 8);

  /*
   * Get footer size
   */
  xfer = thrift_transport_read_all (protocol->transport, _footer_size, 4, error);
  if (xfer < 0)
    return NULL;
  footer_size = le_to_i32 (_footer_size);
  footer_offset = file_size - footer_size - 8;

  /*
   * Validate footer magic
   */
  xfer = thrift_transport_read_all (protocol->transport, magic, 4, error);
  if (xfer < 0)
    return NULL;
  if (le_to_i32 (magic) != MAGIC)
  {
    g_set_error (error, THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Invalid footer magic");
    return NULL;
  }

  /*
   * Read file meta data
   */
  thrift_file_transport_set_location (protocol->transport, footer_offset);
  file_meta_data = g_object_new (TYPE_FILE_META_DATA,
                                 NULL);
  xfer = thrift_struct_read (THRIFT_STRUCT(file_meta_data),
                             protocol, error);
  if (xfer < 0)
    return NULL;

  /*
   * Validate header magic
   */
  thrift_file_transport_set_location (protocol->transport, 0);
  xfer = thrift_transport_read_all (protocol->transport, magic, 4, error);
  if (xfer < 0)
    return NULL;
  if (le_to_i32 (magic) != MAGIC)
  {
    g_set_error (error, THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Invalid header magic");
    return NULL;
  }

  return file_meta_data;
}

void
parquet_reader_instance_init (ParquetReader * object)
{
  object->compact_protocol = NULL;
  object->file_meta_data = NULL;
  object->cur_row_group_idx = -1;
  object->column_reader = NULL;
}

void
parquet_reader_finalize (GObject *object)
{
  ParquetReader *reader;

  reader = PARQUET_READER (object);

  g_object_unref (reader->compact_protocol);
  g_object_unref (reader->file_meta_data);
  g_object_unref (reader->column_reader);
}
