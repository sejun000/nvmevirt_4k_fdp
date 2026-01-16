#ifndef PARQUET_DELTA_COLUMN_WRITER_H
#define PARQUET_DELTA_COLUMN_WRITER_H

#include "parquet_writer.h"
#include "parquet_column_writer.h"

/* `parquet_writer_write_column_chunk_prepare` for delta column */
ColumnWriter *parquet_writer_write_delta_column_prepare (ParquetWriter *writer);

/* `parquet_writer_write_column_chunk_end` for delta column */
gboolean parquet_writer_write_delta_column_end (ParquetWriter *writer,
                                                GError **error);

/* `column_writer_write` for delta value */
gint32 delta_column_writer_write (ColumnWriter *writer, Value *value,
                                  GError **error);

/* `column_writer_prepare` for NULL value in delta column */
gint32 delta_column_writer_prepare_null (ColumnWriter *writer, GError **error);

/* `column_writer_write` for NULL value in delta column */
gint32 delta_column_writer_write_null (ColumnWriter *writer);

#endif /* PARQUET_DELTA_COLUMN_WRITER_H */
