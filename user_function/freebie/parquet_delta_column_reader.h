#ifndef PARQUET_DELTA_COLUMN_READER_H
#define PARQUET_DELTA_COLUMN_READER_H

#include "parquet_column_reader.h"
#include "parquet_reader.h"

/* `parquet_reader_read_column_chunk_prepare` for delta column */
ColumnReader *parquet_reader_read_delta_column_prepare (ParquetReader *reader,
                                                        GError **error);

/* `column_reader_prepare` for delta column */
gint32 delta_column_reader_prepare (ColumnReader *reader, SchemaElement *schema,
                                    ColumnChunk *column_chunk, GError **error);

/* `column_reader_read` for delta column */
gint32 delta_column_reader_read (ColumnReader *reader, Vector *value,
                                 GError **error);

#endif /* PARQUET_DELTA_COLUMN_READER_H */
