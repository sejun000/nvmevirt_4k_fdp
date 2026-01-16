#include "parquet_column_reader.h"
#include "garray.h"
#include "gerror.h"
#include "gobject-type.h"
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
data_page_header_init (PageHeader* page_hdr, DataPageHeader *data_page_hdr,
                       Statistics *data_page_stat)
{
  page_header_instance_init (page_hdr);
  data_page_header_instance_init (data_page_hdr);
  statistics_instance_init (data_page_stat);

  page_hdr->data_page_header = data_page_hdr;
  page_hdr->__isset_data_page_header = TRUE;

  data_page_hdr->statistics = data_page_stat;
  data_page_hdr->__isset_statistics = TRUE;
}

static gint32
data_page_reader_prepare (ColumnReader *reader, GError **error)
{
  gint32 xfer = 0;
  gint32 ret = 0;

  /*
   * Read and check data page header
   */
  data_page_header_init (&reader->page_hdr,
                         &reader->data_page_hdr,
                         &reader->data_page_stat);

  /* Ignore any remaining bytes from the previous page */
  if (reader->page_left_bytes != 0)
  {
    ThriftTransport *transport;
    goffset offset;
    gsize left_bytes;

    transport = reader->compact_protocol->transport;
    offset = thrift_file_transport_get_location(transport);

    left_bytes = reader->page_left_bytes;
    /*
     * If the definition level from the previous page is not fully consumed,
     * exclude it from left_bytes as it has already been included in the offset
     */
    left_bytes -= reader->def_encoder.total_bytes;

    thrift_file_transport_set_location(transport, offset + left_bytes);
  }

  xfer = thrift_struct_read (THRIFT_STRUCT (&reader->page_hdr),
                             reader->compact_protocol,
                             error);
  if (xfer < 0)
    return -1;
  ret += xfer;

  if (!reader->page_hdr.__isset_data_page_header)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Not data header for data page");
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
   */
  reader->page_left_values = reader->data_page_hdr.num_values;
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
   * Prepare definition level:
   * Definition level must be encoded with RLE/BP hybrid encoding.
   * @see https://parquet.apache.org/docs/file-format/nulls
   */
  if (reader->data_page_hdr.definition_level_encoding != ENCODING_RLE)
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                 "Definition level must be encoded with RLE-BP");
    return -1;
  }

  xfer = parquet_rle_bp_encoder_read_list_begin (&reader->def_encoder,
                                                 PARQUET_RLE_BP_AUTO_TOTAL_LEN,
                                                 1);
  if (xfer < 0)
    return -1;
  ret += xfer;
  reader->page_left_bytes -= xfer;

  /*
   * Preapre reading data page
   */
  switch (reader->data_page_hdr.encoding)
  {
  case ENCODING_PLAIN:
    {
      gboolean is_fixed_length;
      gint32 fixed_length;

      is_fixed_length = (reader->schema->type == TYPE_FIXED_LEN_BYTE_ARRAY);
      fixed_length = reader->schema->type_length;

      xfer = parquet_plain_encoder_read_list_begin (&reader->plain_encoder,
                                                    is_fixed_length, fixed_length,
                                                    error);
      if (xfer < 0)
        return -1;
      ret += xfer;
      reader->page_left_bytes -= xfer;
    }
    break;
#ifdef PARQUET_RLE_DICT_ENCODING
  case ENCODING_RLE_DICTIONARY:
    {
      gint8 bit_width;
      guint32 total_bytes;

      /*
       * For the RLE_DICTIONARY encoded data page, read 1 byte for the
       * bit-pack width.
       * @see https://parquet.apache.org/docs/file-format/data-pages/encodings
       */
      xfer = thrift_transport_read_all (reader->dict_encoder->transport,
                                        &bit_width, 1, error);
      if (xfer < 0)
        return -1;
      ret += xfer;
      reader->page_left_bytes -= xfer;

      total_bytes = reader->page_left_bytes - reader->def_encoder.total_bytes;
      xfer = parquet_rle_bp_encoder_read_list_begin (reader->dict_encoder,
                                                     total_bytes, bit_width);
      if (xfer < 0)
        return -1;
      ret += xfer;
      reader->page_left_bytes -= xfer;
    }
    break;
#endif
  default:
    {
      g_set_error (error,
                   THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                   "Not implemented encoding type (type='%d')",
                   reader->data_page_hdr.encoding);
      return -1;
    }
  }

  return ret;
}

static gint32
data_page_reader_read (ColumnReader *reader, Vector *vector, GError **error)
{
  gint32 xfer = 0;
  gint32 ret = 0;
  guint cnt = 0;

  g_assert (reader->page_left_values != 0,
            "data_page_reader_prepare should be called beforehand");

  vector->total_cnt = MIN (reader->page_left_values, VECTOR_MAX_SIZE);
  vector->null_map = 0;

  /*
   * Read definition level
   */
#ifndef PARQUET_RLE_DICT_ENCODING
  xfer = parquet_rle_bp_encoder_read_nullmap (&reader->def_encoder,
                                              &(vector->null_map),
                                              &cnt, vector->total_cnt);
  if (xfer < 0)
    return -1;
  ret += xfer;
  reader->page_left_bytes -= xfer;
#else
  gint32 def_level;
  for (gint32 i = 0; i < vector->total_cnt; i++)
  {
    xfer = parquet_rle_bp_encoder_read_i32 (&reader->def_encoder,
                                            &def_level);
    if (xfer < 0)
      return -1;
    ret += xfer;
    reader->page_left_bytes -= xfer;

    /* 0 is null for definition level */
    vector->null_map = SET_VECTOR_ELEM_NULL (vector, i, def_level ^ 0x1);
    cnt += def_level;
  }
#endif

  /*
   * Read whole vector
   */
  if (reader->data_page_hdr.encoding == ENCODING_PLAIN)
  {
    switch (reader->column_chunk->meta_data->type)
    {
    case TYPE_BOOLEAN:
      {
        xfer = parquet_plain_encoder_read_bool_vec (&reader->plain_encoder,
                                                    vector, cnt);
      }
      break;
    case TYPE_INT32:
      {
        xfer = parquet_plain_encoder_read_i32_vec (&reader->plain_encoder,
                                                   vector, cnt);
      }
      break;
    case TYPE_INT64:
      {
        xfer = parquet_plain_encoder_read_i64_vec (&reader->plain_encoder,
                                                   vector, cnt);
      }
      break;
    case TYPE_FLOAT:
      {
        xfer = parquet_plain_encoder_read_float_vec (&reader->plain_encoder,
                                                     vector, cnt);
      }
      break;
    case TYPE_DOUBLE:
      {
        xfer = parquet_plain_encoder_read_double_vec (&reader->plain_encoder,
                                                      vector, cnt);
      }
      break;
    case TYPE_FIXED_LEN_BYTE_ARRAY:
      {
        xfer = parquet_plain_encoder_read_fixed_len_byte_array_vec (&reader->plain_encoder,
                                                                    vector, cnt);
      }
      break;
    case TYPE_BYTE_ARRAY:
      {
        xfer = parquet_plain_encoder_read_byte_array_vec (&reader->plain_encoder,
                                                          vector, cnt);
      }
      break;
    default:
      {
        g_set_error (error,
                     THRIFT_PROTOCOL_ERROR,
                     THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                     "unknown data type (type='%d')",
                     reader->column_chunk->meta_data->type);
        return -1;
      }
    }
    if (xfer < 0)
      return -1;
    ret += xfer;
    reader->page_left_bytes -= xfer;
  }
#ifdef PARQUET_RLE_DICT_ENCODING
  else if (reader->data_page_hdr.encoding == ENCODING_RLE_DICTIONARY)
  {
    gint32 dict_idx;

    vector->value_cnt = cnt;
    for (guint32 i = 0; i < cnt; i++)
    {
      xfer = parquet_rle_bp_encoder_read_i32 (reader->dict_encoder,
                                              &dict_idx);
      if (xfer < 0)
        return -1;
      ret += xfer;
      reader->page_left_bytes -= xfer;

      value_cpy (&(vector->values[i]),
                 &(g_array_index (reader->dict, Value, dict_idx)));
    }
  }
#endif
  else
  {
    g_set_error (error,
                 THRIFT_PROTOCOL_ERROR,
                 THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                 "Not implemented encoding type (type='%d')",
                 reader->data_page_hdr.encoding);
    return -1;
  }

  reader->page_left_values -= vector->total_cnt;

  return ret;
}

// Column reader --------------------------------------------------------------

GType
column_reader_get_type (void)
{
  return G_TYPE_COLUMN_READER;
}

gint32
column_reader_prepare (ColumnReader *reader, SchemaElement *schema,
                       ColumnChunk *column_chunk, GError **error)
{
  ThriftTransport *transport = NULL;
  ColumnMetaData *col_meta = NULL;
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

  /*
   * Initiate status
   */
  g_object_unref (reader->schema);
  g_object_unref (reader->column_chunk);
  reader->schema = g_object_ref (schema);
  reader->column_chunk = g_object_ref (column_chunk);

  /* Skipping empty column */
  if (!col_meta->num_values || !col_meta->total_uncompressed_size)
  {
    reader->is_init = TRUE;
    return ret;
  }

  reader->column_left_values = col_meta->num_values;
  reader->column_left_bytes = col_meta->total_uncompressed_size;

  /* Reset page read status */
  reader->page_left_values = 0;
  reader->page_left_bytes = 0;

#ifdef PARQUET_RLE_DICT_ENCODING
  offset = col_meta->__isset_dictionary_page_offset
           ? col_meta->dictionary_page_offset
           : col_meta->data_page_offset;
#else
  g_assert (!col_meta->__isset_dictionary_page_offset,
            "reading dictionary page is prohibited");
  offset = col_meta->data_page_offset;
#endif

  transport = reader->compact_protocol->transport;
  if (reader->plain_encoder.transport == NULL)
    reader->plain_encoder.transport = g_object_ref (transport);
  if (reader->def_encoder.transport == NULL)
    reader->def_encoder.transport = g_object_ref (transport);

  thrift_file_transport_set_location (transport, offset);

#ifdef PARQUET_RLE_DICT_ENCODING
  /*
   * Read dictionary page if exists
   * Note that we do not use dictionary encoding.  The code below has not been
   * removed for future use.
   */
  if (G_UNLIKELY (col_meta->__isset_dictionary_page_offset))
  {
    DictionaryPageHeader *dict_phdr = NULL;
    Value dict_val_tmp;
    Value *dict_val;
    gint64 dict_size;
    gboolean is_fixed_length;
    gint32 fixed_length;

    /*
     * As a dictionary page exists only once in the front of a column chunk,
     * we can determine the page size by subtracting the page offsets.
     */
    dict_size = col_meta->data_page_offset - col_meta->dictionary_page_offset;

    /*
     * Read dictionary page header and check
     */
    g_object_unref (reader->dict_phdr);
    reader->dict_phdr = g_object_new (TYPE_PAGE_HEADER,
                                      NULL);
    xfer = thrift_struct_read (THRIFT_STRUCT (reader->dict_phdr),
                               reader->compact_protocol,
                               error);
    if (xfer < 0)
      return -1;
    ret += xfer;
    dict_size -= xfer;
    reader->column_left_bytes -= xfer;

    if (!reader->dict_phdr->__isset_dictionary_page_header)
    {
      g_set_error (error,
                   THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                   "Not dictionary header for dictionary page");
      return -1;
    }

    dict_phdr = reader->dict_phdr->dictionary_page_header;

    if (dict_phdr->encoding != ENCODING_PLAIN)
    {
      g_set_error (error,
                   THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                   "Dictionary page should be plain encoded");
      return -1;
    }

    if (reader->dict_phdr->compressed_page_size
        != reader->dict_phdr->uncompressed_page_size)
    {
      g_set_error (error,
                   THRIFT_PROTOCOL_ERROR,
                   THRIFT_PROTOCOL_ERROR_INVALID_DATA,
                   "Dictionary page should not be compressed");
      return -1;
    }

    g_assert (dict_size == reader->dict_phdr->uncompressed_page_size,
              "wrong size estimation");

    /*
     * Read dictionary data
     * Note that the dictionary data is always encoded in plain encoding, we use
     * plain encoder only.
     */
    g_array_unref (reader->dict);
    reader->dict = g_array_sized_new (FALSE, TRUE,
                                      sizeof (Value),
                                      dict_phdr->num_values);

    is_fixed_length = (col_meta->type == TYPE_FIXED_LEN_BYTE_ARRAY);
    fixed_length = schema->type_length;
    xfer = parquet_plain_encoder_read_list_begin (&reader->plain_encoder,
                                                  is_fixed_length, fixed_length,
                                                  error);
    if (xfer < 0)
      return -1;
    ret += xfer;
    dict_size -= xfer;
    reader->column_left_bytes -= xfer;

    switch (col_meta->type)
    {
    case TYPE_BOOLEAN:
      {
        for (gint i = 0; i < dict_phdr->num_values; i++)
        {
          xfer = parquet_plain_encoder_read_bool (&reader->plain_encoder,
                                                  &dict_val_tmp);
          if (xfer < 0)
            return -1;
          ret += xfer;
          dict_size -= xfer;
          reader->column_left_bytes -= xfer;

          dict_val = value_new ();
          value_deepcpy (dict_val, &dict_val_tmp);
          g_array_append_vals (reader->dict, dict_val, 1);
        }
      }
      break;
    case TYPE_INT32:
      {
        for (gint i = 0; i < dict_phdr->num_values; i++)
        {
          xfer = parquet_plain_encoder_read_i32 (&reader->plain_encoder,
                                                 &dict_val_tmp);
          if (xfer < 0)
            return -1;
          ret += xfer;
          dict_size -= xfer;
          reader->column_left_bytes -= xfer;

          dict_val = value_new ();
          value_deepcpy (dict_val, &dict_val_tmp);
          g_array_append_vals (reader->dict, dict_val, 1);
        }
      }
      break;
    case TYPE_INT64:
      {
        for (gint i = 0; i < dict_phdr->num_values; i++)
        {
          xfer = parquet_plain_encoder_read_i64 (&reader->plain_encoder,
                                                 &dict_val_tmp);
          if (xfer < 0)
            return -1;
          ret += xfer;
          dict_size -= xfer;
          reader->column_left_bytes -= xfer;

          dict_val = value_new ();
          value_deepcpy (dict_val, &dict_val_tmp);
          g_array_append_vals (reader->dict, dict_val, 1);
        }
      }
      break;
    case TYPE_FLOAT:
      {
        for (gint i = 0; i < dict_phdr->num_values; i++)
        {
          xfer = parquet_plain_encoder_read_float (&reader->plain_encoder,
                                                   &dict_val_tmp);
          if (xfer < 0)
            return -1;
          ret += xfer;
          dict_size -= xfer;
          reader->column_left_bytes -= xfer;

          dict_val = value_new();
          value_deepcpy (dict_val, &dict_val_tmp);
          g_array_append_vals (reader->dict, dict_val, 1);
        }
      }
      break;
    case TYPE_DOUBLE:
      {
        for (gint i = 0; i < dict_phdr->num_values; i++)
        {
          xfer = parquet_plain_encoder_read_double (&reader->plain_encoder,
                                                    &dict_val_tmp);
          if (xfer < 0)
            return -1;
          ret += xfer;
          dict_size -= xfer;
          reader->column_left_bytes -= xfer;

          dict_val = value_new();
          value_deepcpy (dict_val, &dict_val_tmp);
          g_array_append_vals (reader->dict, dict_val, 1);
        }
      }
      break;
    case TYPE_FIXED_LEN_BYTE_ARRAY:
      {
        for (gint i = 0; i < dict_phdr->num_values; i++)
        {
          xfer = parquet_plain_encoder_read_fixed_len_byte_array (&reader->plain_encoder,
                                                                  &dict_val_tmp);
          if (xfer < 0)
            return -1;
          ret += xfer;
          dict_size -= xfer;
          reader->column_left_bytes -= xfer;

          dict_val = value_new();
          value_deepcpy (dict_val, &dict_val_tmp);
          g_array_append_vals (reader->dict, dict_val, 1);
        }
      }
      break;
    case TYPE_BYTE_ARRAY:
      {
        for (gint i = 0; i < dict_phdr->num_values; i++)
        {
          xfer = parquet_plain_encoder_read_byte_array (&reader->plain_encoder,
                                                        &dict_val_tmp);
          if (xfer < 0)
            return -1;
          ret += xfer;
          dict_size -= xfer;
          reader->column_left_bytes -= xfer;

          dict_val = value_new();
          value_deepcpy (dict_val, &dict_val_tmp);
          g_array_append_vals (reader->dict, dict_val, 1);
        }
      }
      break;
    default:
      {
        g_set_error (error,
                     THRIFT_PROTOCOL_ERROR,
                     THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                     "not implemented data type (type='%d')",
                     col_meta->type);
        return -1;
      }
    }

    g_assert (dict_phdr->num_values == (gint) reader->dict->len,
              "dictionary num values mismatch");

    g_assert (dict_size == 0,
              "dictionary page size mismatch");

    /*
     * Prepare RLE-BP decoder for the RLE-DICT page
     */
    reader->dict_encoder = g_object_new (PARQUET_TYPE_RLE_BP_ENCODER,
                                         NULL);
    reader->dict_encoder->transport = g_object_ref (transport);
  }
#endif

  /*
   * Prepare reading the first data page
   */
  xfer = data_page_reader_prepare (reader, error);
  if (xfer < 0)
    return -1;
  ret += xfer;
  reader->column_left_bytes -= xfer;
  reader->is_init = TRUE;

  return ret;
}

gint32
column_reader_read (ColumnReader *reader, Vector *vector, GError **error)
{
  gint32 xfer = 0;
  gint32 ret = 0;

  g_assert (reader->is_init, "reader not initiated");

  /*
   * All values in the column are exhausted.  Stop reading the column.
   */
  if (reader->column_left_values == 0)
  {
    reader->is_init = FALSE;
    return 0;
  }

  /*
   * All values in the data page are exhausted.  Prepare reading the next page.
   */
  if (reader->page_left_values == 0)
  {
    xfer = data_page_reader_prepare (reader, error);
    if (xfer < 0)
      return -1;
    ret += xfer;
    reader->column_left_bytes -= xfer;
  }

  xfer = data_page_reader_read (reader, vector, error);
  if (xfer < 0)
    return -1;
  ret += xfer;
  reader->column_left_bytes -= xfer;
  reader->column_left_values -= vector->total_cnt;

  return ret;
}

void
column_reader_finalize (GObject *object)
{
  ColumnReader *reader;

  reader = COLUMN_READER (object);

  g_object_unref (reader->schema);
  g_object_unref (reader->column_chunk);
  g_object_unref (reader->compact_protocol);
  parquet_plain_encoder_finalize ((GObject *)&reader->plain_encoder);
  parquet_rle_bp_encoder_finalize ((GObject *)&reader->def_encoder);
  g_object_unref (reader->dict_encoder);
  g_object_unref (reader->dict_phdr);
  g_array_unref (reader->dict);
}

void
column_reader_instance_init (ColumnReader * object)
{
  object->schema = NULL;
  object->column_chunk = NULL;
  object->compact_protocol = NULL;

  parquet_plain_encoder_instance_init (&object->plain_encoder);
  G_TYPE_FROM_INSTANCE (&object->plain_encoder) = G_TYPE_PARQUET_PLAIN_ENCODER;
  object->plain_encoder.transport = NULL;

  parquet_rle_bp_encoder_instance_init (&object->def_encoder);
  G_TYPE_FROM_INSTANCE (&object->def_encoder) = G_TYPE_PARQUET_RLE_BP_ENCODER;
  object->def_encoder.transport = NULL;

  object->dict_encoder = NULL;

  object->is_init = FALSE;
  object->dict_phdr = NULL;
  object->dict = NULL;
  object->column_left_bytes = 0;
  object->column_left_values = 0;

  object->page_left_values = 0;
  object->page_left_bytes = 0;

  page_header_instance_init (&object->page_hdr);
  G_TYPE_FROM_INSTANCE (&object->page_hdr) = G_TYPE_PAGE_HEADER;

  data_page_header_instance_init (&object->data_page_hdr);
  G_TYPE_FROM_INSTANCE (&object->data_page_hdr) = G_TYPE_DATA_PAGE_HEADER;

  statistics_instance_init (&object->data_page_stat);
  G_TYPE_FROM_INSTANCE (&object->data_page_stat) = G_TYPE_STATISTICS;

  dictionary_page_header_instance_init (&object->delta_page_hdr);
  G_TYPE_FROM_INSTANCE (&object->delta_page_hdr) = G_TYPE_DICTIONARY_PAGE_HEADER;
}
