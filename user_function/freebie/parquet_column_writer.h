#ifndef PARQUET_COLUMN_WRITER_H
#define PARQUET_COLUMN_WRITER_H

#include "gobject.h"
#include "gtypes.h"
#include "parquet_types.h"
#include "parquet_plain_encoder.h"
#include "parquet_rle_bp_encoder.h"

typedef struct _ColumnWriter ColumnWriter;
struct _ColumnWriter
{
  GObject parent;

  /* Input */
  ThriftProtocol *compact_protocol;
  SchemaElement *schema;

  /* Encoders */
  ParquetPlainEncoder plain_encoder;
  ParquetRleBpEncoder rle_bp_encoder;

  /* Column */
  ColumnChunk *column_chunk;
  gboolean is_init;

  /* Data page */
  PageHeader page_hdr;
  DataPageHeader data_page_hdr;
  Statistics data_page_stat;
  gint64 phdr_bytes;
  gint64 def_bytes;
  gint64 data_bytes;

  /* Delta page */
  DictionaryPageHeader delta_page_hdr;
};

#define column_writer_get_begin_offset(writer) \
  ((writer)->column_chunk->meta_data->__isset_dictionary_page_offset \
    ? (writer)->column_chunk->meta_data->dictionary_page_offset \
    : (writer)->column_chunk->meta_data->data_page_offset)

GType column_writer_get_type (void);

void column_writer_prepare (ColumnWriter *writer, SchemaElement *schema);

gint32 column_writer_write (ColumnWriter *writer, Value *value, GError **error);

gint32 column_writer_end (ColumnWriter *writer, GError **error);

void column_writer_finalize (GObject *object);

void column_writer_instance_init (ColumnWriter * object);

#define TYPE_COLUMN_WRITER \
  (column_writer_get_type())
#define COLUMN_WRITER(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), TYPE_COLUMN_WRITER, ColumnWriter))
#define IS_COLUMN_WRITER(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), TYPE_COLUMN_WRITER))

#endif /* PARQUET_COLUMN_WRITER_H */
