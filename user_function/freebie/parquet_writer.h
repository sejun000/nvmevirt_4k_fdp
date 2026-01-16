#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include "garray.h"
#include "gerror.h"
#include "gobject.h"
#include "gtypes.h"
#include "parquet_types.h"
#include "parquet_column_writer.h"

typedef struct _ParquetWriter ParquetWriter;
struct _ParquetWriter
{
  GObject parent;

  ThriftProtocol *compact_protocol;

  FileMetaData *file_meta_data;
  GByteArray *row_groups_binary;
  gint32 num_row_groups;
  guint32 written_amount;

  RowGroup *cur_row_group;
  ColumnWriter *column_writer;
};

GType parquet_writer_get_type (void);

gboolean parquet_writer_prepare (ParquetWriter *writer, gchar *filename, int filesize, int max_file_size,
                        GPtrArray *schema_src, GError **error);

gint32 parquet_writer_end (ParquetWriter *writer, GError **error);

void parquet_writer_write_row_group_prepare (ParquetWriter *writer);

void parquet_writer_write_row_group_end (ParquetWriter *writer);

ColumnWriter *parquet_writer_write_column_chunk_prepare (ParquetWriter *writer,
                                                         guint column_idx);

gboolean parquet_writer_write_column_chunk_end (ParquetWriter *writer,
                                                GError **error);

gboolean parquet_merge_writer(ParquetWriter *dest_writer, ParquetWriter *src_writer,
                                                            GError **error);

void parquet_writer_instance_init (ParquetWriter * object);

void parquet_writer_finalize (GObject *object);

#define PARQUET_TYPE_WRITER \
  (parquet_writer_get_type())
#define PARQUET_WRITER(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), PARQUET_TYPE_WRITER, ParquetWriter))
#define PARQUET_IS_WRITER(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), PARQUET_TYPE_WRITER))

#endif /* PARQUET_WRITER_H */
