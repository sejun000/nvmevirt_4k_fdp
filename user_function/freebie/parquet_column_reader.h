#ifndef PARQUET_COLUMN_READER_H
#define PARQUET_COLUMN_READER_H

#include "garray.h"
#include "gobject.h"
#include "gtypes.h"
#include "parquet.h"
#include "parquet_types.h"
#include "parquet_plain_encoder.h"
#include "parquet_rle_bp_encoder.h"

typedef struct _ColumnReader ColumnReader;
struct _ColumnReader
{
  GObject parent;

  /* Input */
  ThriftProtocol *compact_protocol;
  SchemaElement *schema;
  ColumnChunk *column_chunk;

  /* Encoders */
  ParquetPlainEncoder plain_encoder;
  ParquetRleBpEncoder def_encoder;

  /* Columnn */
  gboolean is_init;
  gint32 column_left_values;
  gint32 column_left_bytes;

  /* Dictionary page */
  ParquetRleBpEncoder *dict_encoder;
  PageHeader *dict_phdr;
  GArray *dict;

  /* Data page */
  PageHeader page_hdr;
  DataPageHeader data_page_hdr;
  Statistics data_page_stat;
  gint32 page_left_values;
  gint32 page_left_bytes;

    /* Delta page */
  DictionaryPageHeader delta_page_hdr;
};

GType column_reader_get_type (void);

gint32 column_reader_prepare (ColumnReader *reader, SchemaElement *schema,
                              ColumnChunk *column_chunk, GError **error);

gint32 column_reader_read (ColumnReader *reader, Vector *value, GError **error);

void column_reader_finalize (GObject *object);

void column_reader_instance_init (ColumnReader * object);

#define TYPE_COLUMN_READER \
  (column_reader_get_type())
#define COLUMN_READER(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), TYPE_COLUMN_READER, ColumnReader))
#define IS_COLUMN_READER(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), TYPE_COLUMN_READER))

#endif /* PARQUET_COLUMN_READER_H */
