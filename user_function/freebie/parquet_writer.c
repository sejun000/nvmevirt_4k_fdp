#include "garray.h"
#include "gerror.h"
#include "gobject.h"
#include "gtypes.h"
#include "gobject-type.h"
#include "parquet_types.h"
#include "thrift_protocol.h"
#include "thrift_transport.h"
#include "thrift_file_transport.h"
#include "parquet.h"
#include "parquet_writer.h"
#include "parquet_column_writer.h"
#include "utils.h"

#define OPPORTUNISTIC_FOOTER_READ_SIZE (4 << 10) // 4Ki

/* Private */

static gboolean
parquet_writer_import_row_groups (ParquetWriter *writer, GError **error);

static void
file_meta_data_init (FileMetaData *file_meta_data)
{
  const gchar *created_by = "FreeBIE version v0.0.1 (build 0123456789)";

  // Set required
  /* .version */
  file_meta_data->version = 0;
  /* .created_by */
  string_copy (&file_meta_data->created_by, created_by);
  file_meta_data->__isset_created_by = TRUE;

  // Determined in `*_prepare`
  /* .schema */
  if (file_meta_data->schema == NULL)
    file_meta_data->schema = g_ptr_array_new_with_free_func (g_object_unref);

  // Accumulator
  /* .num_rows */
  file_meta_data->num_rows = 0;
  /* .row_groups */
  if (file_meta_data->row_groups == NULL)
    file_meta_data->row_groups = g_ptr_array_new_with_free_func (g_object_unref);

  // Not used
  /* .key_value_metadata */
  g_ptr_array_unref (file_meta_data->key_value_metadata);
  file_meta_data->key_value_metadata = NULL;
  file_meta_data->__isset_key_value_metadata = FALSE;
  /* .column_orders */
  g_ptr_array_unref (file_meta_data->column_orders);
  file_meta_data->column_orders = NULL;
  file_meta_data->__isset_column_orders = FALSE;
  /* .encryption_algorithm */
  g_object_unref (file_meta_data->encryption_algorithm);
  file_meta_data->encryption_algorithm = NULL;
  file_meta_data->__isset_encryption_algorithm = FALSE;
  /* .footer_signing_key_metadata */
  g_byte_array_unref (file_meta_data->footer_signing_key_metadata);
  file_meta_data->footer_signing_key_metadata = NULL;
  file_meta_data->__isset_footer_signing_key_metadata = FALSE;
}

static void
row_group_init (RowGroup *row_group)
{
  // Determined in `*_prepare`
  /* .file_offset */
  row_group->file_offset = -1;
  row_group->__isset_file_offset = TRUE;
  /* .ordinal */
  row_group->ordinal = -1;
  row_group->__isset_ordinal = TRUE;

  // Accumulator
  /* .columns */
  if (row_group->columns == NULL)
    row_group->columns = g_ptr_array_new_with_free_func (g_object_unref);
  /* .num_rows */
  row_group->num_rows = 0;
  /* .total_byte_size */
  row_group->total_byte_size = 0; 
  /* .total_compressed_size */
  row_group->total_compressed_size = 0;
  row_group->__isset_total_compressed_size = TRUE;

  // Cleanup unused
  /* .sorting_columns */
  g_ptr_array_unref (row_group->sorting_columns);
  row_group->sorting_columns = NULL;
  row_group->__isset_sorting_columns = FALSE;
}

/* Public */

GType
parquet_writer_get_type (void)
{
  return G_TYPE_PARQUET_WRITER;
}

gboolean
parquet_writer_prepare (ParquetWriter *writer, gchar *filename, int filesize, int max_file_size,
                        GPtrArray *schema_src, GError **error)
{
  ThriftProtocol *protocol;
  SchemaElement *root_schema;

  /*
   * Create Thrift Protocol and Transport.
   */
  protocol = create_thrift_file_compact_protocol (filename, filesize, max_file_size, error);
  if (protocol == NULL)
    return FALSE;
  writer->compact_protocol = protocol;

  /*
   * Import the row groups if any.  If the file is empty, write the header magic
   * first.
   */
  if (!parquet_writer_import_row_groups (writer, error))
    return FALSE;

  /*
   * Parquet writer doesn't handle the schema generation; just import the
   * provided one.
   *
   * First, validate the schema.
   */
  if (schema_src->len <= 0)
  {
    g_set_error (error, THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Invalid schema");
    return FALSE;
  }

  root_schema = schema_src->pdata[0];
  if (schema_src->len != (guint) root_schema->num_children + 1)
  {
    g_set_error (error, THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Invalid schema");
    return FALSE;
  }

  /*
   * Import the schema.
   */
  for (guint i = 0; i < schema_src->len; i++)
    g_ptr_array_add (writer->file_meta_data->schema,
                     g_object_ref (schema_src->pdata[i]));

  writer->column_writer->compact_protocol = g_object_ref (protocol);

  return TRUE;
}

gint32
parquet_writer_end (ParquetWriter *writer, GError **error)
{
  ThriftProtocol *protocol;
  ThriftTransport *transport;
  FileMetaData *file_meta_data;
  guint8 le[4];
  gint32 meta_len = 0;
  gint32 ret = 0;
  GArray *encodings;
  Encoding encoding[1] = {ENCODING_PLAIN};

  g_assert (writer->compact_protocol != NULL,
            "compact_protocol not prepared");
  protocol = writer->compact_protocol;
  transport = protocol->transport;

  g_assert (writer->file_meta_data != NULL,
            "file meta data writer not prepared");
  file_meta_data = writer->file_meta_data;

  /*
   * Use PLAIN encoding (for now)
   */
  encodings = g_array_sized_new (FALSE, TRUE,
                                 sizeof(Encoding), 1);
  g_array_append_vals (encodings, encoding, 1);
  for (guint rg_idx = 0; rg_idx < file_meta_data->row_groups->len; rg_idx++)
  {
    RowGroup *rg = file_meta_data->row_groups->pdata[rg_idx];
    for (guint cc_idx = 0; cc_idx < rg->columns->len; cc_idx++)
    {
      ColumnChunk *cc = rg->columns->pdata[cc_idx];
      cc->meta_data->encodings = g_array_ref (encodings);
    }
  }

  if (writer->row_groups_binary == NULL && writer->num_row_groups == 0)
  {
    meta_len = thrift_struct_write (THRIFT_STRUCT (file_meta_data),
                                    protocol, error);
    if (meta_len < 0)
      return -1;
    ret += meta_len;
  }
  else
  {
    meta_len = file_meta_data_write_inject_row_group_binary (THRIFT_STRUCT (file_meta_data),
                                                             writer->row_groups_binary,
                                                             writer->num_row_groups,
                                                             protocol, error);
    if (meta_len < 0)
      return -1;
    ret += meta_len;
  }

  g_array_unref (encodings);

  i32_to_le (meta_len, le);
  if (!thrift_transport_write (transport, le, 4, error))
    return -1;
  ret += 4;

  i32_to_le (MAGIC, le);
  if (!thrift_transport_write (transport, le, 4, error))
    return -1;
  ret += 4;

  if (!thrift_transport_close (transport, error))
    return -1;

  return ret;
}

void
parquet_writer_write_row_group_prepare (ParquetWriter *writer)
{
  ThriftTransport *transport;
  RowGroup *row_group;

  g_assert (writer->cur_row_group == NULL,
            "row group writer already prepared");
  row_group = g_object_new (TYPE_ROW_GROUP,
                            NULL);
  row_group_init (row_group);

  transport = writer->compact_protocol->transport;
  row_group->file_offset = thrift_file_transport_get_location (transport);

  row_group->ordinal = writer->file_meta_data->row_groups->len;

  writer->cur_row_group = row_group;
}

void
parquet_writer_write_row_group_end (ParquetWriter *writer)
{
  FileMetaData *file_meta_data;
  RowGroup *row_group;

  g_assert (writer->file_meta_data != NULL,
            "file meta data writer not prepared");
  file_meta_data = writer->file_meta_data;

  g_assert (writer->cur_row_group != NULL,
            "row group writer not prepared");
  row_group = writer->cur_row_group;

  g_assert (row_group->total_byte_size == row_group->total_compressed_size,
            "invalid row group size");

  // Add to file meta data iff it contains data
  if (row_group->total_byte_size)
  {
    g_ptr_array_add (file_meta_data->row_groups, row_group);
    file_meta_data->num_rows += row_group->num_rows;
  }

  else
  {
    g_object_unref (row_group);
  }

  writer->cur_row_group = NULL;
}

ColumnWriter *
parquet_writer_write_column_chunk_prepare (ParquetWriter *writer,
                                           guint column_idx)
{
  SchemaElement *schema;

  g_assert (writer->column_writer != NULL,
            "column writer not created");

  schema = writer->file_meta_data->schema->pdata[column_idx + 1];
  column_writer_prepare (writer->column_writer, schema);

  return writer->column_writer;
}

gboolean
parquet_writer_write_column_chunk_end (ParquetWriter *writer, GError **error)
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

  ret = column_writer_end (writer->column_writer, error);
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

gboolean
parquet_merge_writer(ParquetWriter *dst_writer, ParquetWriter *src_writer,
                                                            GError **error)
{
  RowGroup *src_row_group;
  RowGroup *dst_row_group;
  ColumnChunk *column_chunk;
  ColumnMetaData *column_meta_data;
  gint32 ret;

  g_assert (dst_writer->cur_row_group != NULL, "row group writer not prepared");
  dst_writer->written_amount += src_writer->written_amount - 4;

  dst_row_group = dst_writer->cur_row_group;
  src_row_group = src_writer->cur_row_group;
  // For each column chunk in the source writer
  for (int i = 0; i < src_row_group->columns->len; i++) {
    column_chunk = src_row_group->columns->pdata[i];
    column_meta_data = column_chunk->meta_data;

    column_meta_data->data_page_offset = dst_row_group->file_offset + dst_row_group->total_compressed_size;

    g_ptr_array_add (dst_row_group->columns, g_object_ref (column_chunk));
    dst_row_group->num_rows = column_meta_data->num_values;
    dst_row_group->total_byte_size += column_meta_data->total_uncompressed_size;
    dst_row_group->total_compressed_size += column_meta_data->total_compressed_size;
  }
  return TRUE;
}

static gboolean
parquet_writer_import_row_groups (ParquetWriter *writer, GError **error)
{
  ThriftProtocol *protocol;
  GByteArray *row_groups_binary;
  gint32 num_row_groups;
  gint64 num_rows;
  gssize file_size;
  gint32 footer_size;
  guint8 _footer_size[4];
  goffset footer_offset;
  guint8 magic[4];
  gint32 xfer;

  protocol = writer->compact_protocol;

  file_size = thrift_file_transport_get_size (protocol->transport, error);
  if (file_size < 0)
    return FALSE;

  /*
   * If the file is empty, write the header magic and exit.
   */
  else if (file_size == 0)
  {
    guint8 le[4];
    i32_to_le (MAGIC, le);
    if (!thrift_transport_write (protocol->transport, le, 4, error))
      return FALSE;
    file_size += 4;
    writer->written_amount += 4;
  }

  /*
   * Parquet file size must be bigger than 12.
   */
  else if (file_size < 12)
  {
    g_set_error (error, THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "File size too small");
    return FALSE;
  }

  else
  {
    /*
     * Do an opportunistic read of up to OPPORTUNISTIC_FOOTER_READ_SIZE
     */
    footer_offset = MAX (file_size - OPPORTUNISTIC_FOOTER_READ_SIZE, 0);
    footer_size = MIN (file_size, OPPORTUNISTIC_FOOTER_READ_SIZE);
    if (!thrift_file_transport_prefetch (protocol->transport, footer_offset,
                                         footer_size, error))
      return FALSE;

    thrift_file_transport_set_location (protocol->transport, file_size - 8);

    /*
     * Get footer size
     */
    xfer = thrift_transport_read_all (protocol->transport, _footer_size, 4, error);
    if (xfer < 0)
      return FALSE;
    footer_size = le_to_i32 (_footer_size);
    footer_offset = file_size - footer_size - 8;

    /*
     * Validate footer magic
     */
    xfer = thrift_transport_read_all (protocol->transport, magic, 4, error);
    if (xfer < 0)
      return FALSE;
    if (le_to_i32 (magic) != MAGIC)
    {
      g_set_error (error, THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                   "Invalid footer magic");
      return FALSE;
    }

    /*
     * If the footer size is bigger than OPPORTUNISTIC_FOOTER_READ_SIZE, read more.
     */
    if (G_UNLIKELY (footer_size + 8 > OPPORTUNISTIC_FOOTER_READ_SIZE))
    {
      if (!thrift_file_transport_prefetch (protocol->transport, footer_offset,
                                           footer_size, error))
        return FALSE;
    }

    /*
     * Read file meta data
     */
    thrift_file_transport_set_location (protocol->transport, footer_offset);
    xfer = file_meta_data_read_extract_row_groups_binary (&row_groups_binary,
                                                          &num_row_groups,
                                                          &num_rows,
                                                          protocol, error);
    if (xfer < 0)
      return FALSE;

    writer->row_groups_binary = row_groups_binary;
    writer->num_row_groups = num_row_groups;
    writer->file_meta_data->num_rows += num_rows;
  }

  /*
   * Set write cursor to the end of the file.
   */
  thrift_file_transport_set_location (protocol->transport, file_size);

  return TRUE;
}

void
parquet_writer_finalize (GObject *object)
{
  ParquetWriter *writer;

  writer = PARQUET_WRITER (object);

  g_object_unref (writer->compact_protocol);
  g_object_unref (writer->file_meta_data);
  g_byte_array_unref (writer->row_groups_binary);
  g_object_unref (writer->cur_row_group);
  g_object_unref (writer->column_writer);
}

void
parquet_writer_instance_init (ParquetWriter * object)
{
  object->row_groups_binary = NULL;
  object->num_row_groups = 0;
  object->written_amount = 0;
  object->compact_protocol = NULL;

  object->file_meta_data = g_object_new (TYPE_FILE_META_DATA,
                                         NULL);
  file_meta_data_init (object->file_meta_data);

  object->cur_row_group = NULL;

  object->column_writer = g_object_new (G_TYPE_COLUMN_WRITER,
                                        NULL);
}
