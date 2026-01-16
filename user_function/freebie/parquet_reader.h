#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include "gerror.h"
#include "gobject.h"
#include "gtypes.h"
#include "parquet_types.h"
#include "parquet_column_reader.h"
#include "thrift_protocol.h"

typedef struct _ParquetReader ParquetReader;
struct _ParquetReader
{
  GObject parent;

  ThriftProtocol *compact_protocol;

  FileMetaData *file_meta_data;

  gint cur_row_group_idx;
  ColumnReader *column_reader;
};

GType parquet_reader_get_type (void);

gboolean parquet_reader_prepare (ParquetReader *reader, gchar *filename, int filesize,
                                 GError **error);

gint32 parquet_reader_end (ParquetReader *reader, GError **error);

gboolean parquet_reader_read_row_group_prepare (ParquetReader *reader);

ColumnReader *parquet_reader_read_column_chunk_prepare (ParquetReader *reader,
                                                        guint column_idx,
                                                        GError **error);

void parquet_reader_instance_init (ParquetReader * object);

void parquet_reader_finalize (GObject *object);

#define PARQUET_TYPE_READER \
  (parquet_reader_get_type())
#define PARQUET_READER(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), PARQUET_TYPE_READER, ParquetReader))
#define PARQUET_IS_READER(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), PARQUET_TYPE_READER))

#endif /* PARQUET_READER_H */
