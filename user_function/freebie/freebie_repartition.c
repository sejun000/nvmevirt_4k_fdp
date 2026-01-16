#include "c_standard_libs.h"

#include "garray.h"
#include "gerror.h"
#include "gobject.h"
#include "gtypes.h"
#include "parquet.h"
#include "parquet_types.h"
#include "parquet_column_reader.h"
#include "parquet_delta_column_reader.h"
#include "parquet_column_writer.h"
#include "parquet_delta_column_writer.h"
#include "parquet_reader.h"
#include "parquet_writer.h"
#include "freebie_repartition.h"
#include "freebie_delta_mgr.h"
#include "utils.h"
#include "thrift_file_transport.h"

#include "../../csd_slm.h"
#include "../../csd_user_func.h"
#include "../../csd_dispatcher.h"
#include "freebie_functions.h"

static void gather_reverse_order_info (ReverseOrderInfo *dst, ParquetWriter *src,
                                       gint column_idx)
{
  dst[column_idx].offset = column_writer_get_begin_offset (src->column_writer);
  dst[column_idx].data_bytes = src->column_writer->data_bytes;
  dst[column_idx].phdr_bytes = src->column_writer->phdr_bytes;
  dst[column_idx].def_bytes = src->column_writer->def_bytes;
}

static gint calc_coverage (struct freebie_params *freebie_params)
{
  return (gint) power (freebie_params->spread_factor,
                        freebie_params->last_level - (freebie_params->current_level + 1));
}

#define calc_child_idx(child_idx, coverage, spread_factor) \
  ((guint8) ((child_idx / coverage) % spread_factor))

#define append_child_idx(arr, child_idx, row_idx) \
  arr[row_idx >> 1] |= child_idx << (4 * (row_idx & 0x1))

#define get_child_idx(arr, row_idx) \
  ((child_idxs[row_idx >> 1] >> (4 * (row_idx & 0x1))) & 0xF)

gboolean freebie_helper_repartition (void *_task)
{
  GError *error_t = NULL;
  GError **error = &error_t;
  struct ccsd_task_info *task = (struct ccsd_task_info *)_task;
  struct freebie_params *freebie_params = (struct freebie_params *)(task->params); 

  ParquetReader *reader = NULL;
  GPtrArray *schema;
  GPtrArray *readers;
  GPtrArray *writers = NULL;
  ParquetWriter *writer;
  guint8 *child_idxs;
  guint64 child_idx = 0;
  Vector vector;
  ColumnReader *column_reader;
  guint num_cols;
  gint32 xfer;
  guint32 total_rows = 0;

  guint32 spread_factor = freebie_params->spread_factor;
  guint32 input_file_cnt = freebie_params->input_file_cnt;
  guint32 partition_no = freebie_params->partition_no;
  guint32 relation_id = freebie_params->relation_id; 
  guint32 current_level = freebie_params->current_level;
  guint32 row_id_map = freebie_params->row_id_map;

  // Delta column
  FreeBIEDeltaMgr *delta_mgr = NULL;
  DeltaHT *delta_ht = NULL;
  DeltaList *delta_list = NULL;

  guint start_column = 0;
  guint end_column = 0;
	FreeBIEShared *shared_data = (FreeBIEShared *)task->shared; 

  ReverseOrderInfo **reverse_order_info;

  g_assert(task->has_master == TRUE, "This function should be called only by helper tasks");
  NVMEV_FREEBIE_DEBUG("(HELPER) Repartition start (relation_id: %d) (current level: %d) (partition_no: %d)\n",
                                    relation_id, current_level, partition_no);
  
  if (input_file_cnt < 1) {
    return TRUE;
  }

  readers = g_ptr_array_new_with_free_func (g_object_unref);
  for (guint i = 0; i < input_file_cnt; i++) {
    reader = g_object_new (PARQUET_TYPE_READER, NULL);
    if (!parquet_reader_prepare (reader, (gchar *) freebie_params->input_buf[i],
                                  freebie_params->input_buf_size[i], error)) {
      return FALSE;
    }

    if (reader->file_meta_data == NULL) {
      return FALSE;
    }

    total_rows += reader->file_meta_data->num_rows;
    g_ptr_array_add (readers, reader);
  }

  schema = reader->file_meta_data->schema;
  num_cols = schema->len - 1;

  start_column = (num_cols / 2) + 1;
  end_column = num_cols - 2;

  // NVMEV_FREEBIE_DEBUG("(HELPER) start_column: %d, end_column: %d\n",
  //                     start_column, end_column);

  writers = g_ptr_array_new_with_free_func (g_object_unref);
  for (guint i = 0; i < spread_factor; i++) {
    writer = g_object_new (PARQUET_TYPE_WRITER, NULL);
    if (!parquet_writer_prepare (writer, (gchar *) freebie_params->output_buf[i],
                                  freebie_params->output_buf_size[i], freebie_params->output_buf_alloc_size[i], schema, error)) {
        return FALSE;
    }
    parquet_writer_write_row_group_prepare (writer);
    g_ptr_array_add (writers, writer);
  }

  // NVMEV_FREEBIE_DEBUG("(HELPER) Scanning Delta Columns\n");
  {
    delta_mgr = freebie_delta_mgr_init (schema, row_id_map);
    freebie_delta_mgr_list_init (&delta_list, delta_mgr->delta_list_entry_pool_idx);
    freebie_delta_mgr_ht_init (&delta_ht, delta_mgr->delta_list_entry_pool_idx);

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_delta_column_prepare (reader, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          xfer = delta_column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          g_assert (vector.total_cnt == vector.value_cnt, "delta entry should not be null");

          // Put all deltas into the hashtable.
          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            freebie_delta_mgr_ht_put (delta_mgr, delta_ht, vector.values + vec_idx);
          }
        }
      }
    }
  }

  /* --------------------------------------------------------------------------
   * ************* Synchronize with master task *************
   * ------------------------------------------------------------------------ */
  shared_data->delta_list = delta_list;
  shared_data->delta_ht = delta_ht;
  shared_data->delta_mgr = delta_mgr;
  // NVMEV_FREEBIE_DEBUG("(HELPER) Wait for master task to process Partition key column\n");
  clear_bit(task->task_id, task->master_task->helper_bitmap);
  while (!bitmap_empty(task->helper_bitmap, MAX_TASK_COUNT)) {
	  cond_resched();
  }
  // Get partition key data structures from main task
  reverse_order_info = shared_data->reverse_order_info;
  child_idxs = shared_data->child_idxs;

   for (guint column_idx = start_column; column_idx <= end_column; column_idx++) {
    gint row_idx = 0;
    FreeBIEDeltaListEntry *delta_cursor;
    delta_cursor = freebie_delta_mgr_list_get_next (delta_list, NULL);

    // NVMEV_FREEBIE_DEBUG("(HELPER) Processing column chunk (%d)\n", column_idx);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_column_chunk_prepare (writers->pdata[i], column_idx);
    }

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_column_chunk_prepare (reader, column_idx, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          guint16 val_idx = 0;
          xfer = column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            Value *value = NULL_VALUE;
            Value delta = {0};

            child_idx = get_child_idx (child_idxs, row_idx);
            writer = writers->pdata[child_idx];

            /* Is this normal value? */
            if (G_LIKELY (!IS_VECTOR_ELEM_NULL (&vector, vec_idx))) {
              value = &(vector.values[val_idx++]);
            }

            /* Should we use delta instead? */
            if (delta_cursor && delta_cursor->row_idx == row_idx) {
              if (freebie_delta_mgr_apply_entry (delta_mgr, &delta, delta_cursor, column_idx)) {
                value = &delta;
              }
              delta_cursor = freebie_delta_mgr_list_get_next (delta_list, delta_cursor);
            }

            xfer = column_writer_write (writer->column_writer, value, error);
            if (xfer < 0)
              return FALSE;

            writer->written_amount += xfer;
            row_idx++;
          }
        }
      }
    }

    for (guint i = 0; i < spread_factor; i++) {
      if (!parquet_writer_write_column_chunk_end (writers->pdata[i], error))
        return FALSE;

      gather_reverse_order_info (reverse_order_info[i], writers->pdata[i], column_idx);
    }
  }
  
  // Clean-up readers
  for (guint i = 0; i < freebie_params->input_file_cnt; i++) {
    reader = readers->pdata[i];

    xfer = parquet_reader_end (reader, error);
    if (xfer < 0) {
      return FALSE;
    }
  }
  // BUG_FIX don't free readers here, it's used for detla_apply in master task
  // g_ptr_array_unref (readers);

  // Helper functions should notify their masters
  // They can terminate after the master task copied the intermediate buffer
  shared_data->helper_readers = readers;
  shared_data->helper_writers = writers;

  task->task_step = CCSD_TASK_WAIT;
	clear_bit(task->task_id, task->master_task->helper_bitmap);

  // The master task will g_ptr_array_unref the writers of helper tasks
  NVMEV_FREEBIE_DEBUG("(HELPER) Done\n");

  return FALSE;
}

gboolean freebie_repartition (void *_task)
{
  GError *error_t = NULL;
  GError **error = &error_t;
  struct ccsd_task_info *task = (struct ccsd_task_info *)_task;
  struct freebie_params *freebie_params = (struct freebie_params *)(task->params);

  ParquetReader *reader = NULL;
  GPtrArray *schema;
  GPtrArray *readers;
  GPtrArray *writers = NULL;
  ParquetWriter *writer;
  guint8 *child_idxs;
  guint64 child_idx = 0;
  gint coverage;
  Vector vector;
  ColumnReader *column_reader;
  guint num_cols;
  gint32 xfer;
  guint32 total_rows = 0;

  // Delta column
  FreeBIEDeltaMgr *delta_mgr = NULL;
  DeltaHT *delta_ht = NULL;
  DeltaList *delta_list = NULL;

  // Freebie parameters
  guint32 spread_factor = freebie_params->spread_factor;
  guint32 input_file_cnt = freebie_params->input_file_cnt;
  guint32 partition_no = freebie_params->partition_no;
  guint32 relation_id = freebie_params->relation_id; 
  guint32 current_level = freebie_params->current_level;
  guint32 row_id_map = freebie_params->row_id_map;

  // Metadata related data structures
  freebie_source_range_entry freebie_sre[32]; // TODO: This size should be modified later
  guint first_child_partition_no = partition_no * spread_factor;
  guint parent_offset = 0;
  guint child_offset = 0;
  guint nentry = 0;

  // For helper tasks
  guint start_column = 0;
  guint end_column = 0;
	FreeBIEShared *shared_data = (FreeBIEShared *)task->shared; 

  // Reverse order info for all columns in all output files
  // reverse_order_info[child_idx][column_idx]
  ReverseOrderInfo **reverse_order_info;

  // For Metadata update
  parent_offset += sizeof(guint) * 3; // Logical Metadata header
  if (current_level == 0) {
    child_offset = parent_offset + (sizeof(guint) * spread_factor);
  }
  else /* config->current_level > 0 */ {
    parent_offset += (sizeof(guint) * spread_factor);
    for (guint i = 1; i < current_level; i++) {
      parent_offset += (sizeof(FreeBIESGPair) * power(spread_factor, i + 1));
    }
    child_offset = parent_offset + (sizeof(FreeBIESGPair) * power(spread_factor, current_level + 1));
  }

  NVMEV_FREEBIE_DEBUG("FREEBIE MASTER - Repartition start (relation_id: %d) (current_level: %d) (partition no: %d)\n",
                                    relation_id, current_level, partition_no);

  /* --------------------------------------------------------------------------
   * STEP 1. Run Helper tasks if there are any 
   * ------------------------------------------------------------------------ */
#ifdef ENABLE_HELPER_TASK
  task->exec_helper_task->task_step = CCSD_TASK_SCHEDULE;
  set_bit(task->exec_helper_task->task_id, task->helper_bitmap);
  set_bit(task->task_id, task->exec_helper_task->helper_bitmap);
#endif

  if (input_file_cnt < 1) {
    return TRUE;
  }

  /* --------------------------------------------------------------------------
   * STEP 2. Read input files STEP 2. Read input files STEP 2. Read input files STEP 2. Read input files 
   * ------------------------------------------------------------------------ */
  readers = g_ptr_array_new_with_free_func (g_object_unref);
  for (guint i = 0; i < input_file_cnt; i++) {
    reader = g_object_new (PARQUET_TYPE_READER, NULL);
    NVMEV_FREEBIE_DEBUG("FREEBIE MASTER - Repartition target file size is %lu\n", freebie_params->input_buf_size[i]);
    if (!parquet_reader_prepare (reader, (gchar *) freebie_params->input_buf[i],
                                  freebie_params->input_buf_size[i], error)) {
      return FALSE;
    }

    if (reader->file_meta_data == NULL) {
      return FALSE;
    }

    total_rows += reader->file_meta_data->num_rows;
    g_ptr_array_add (readers, reader);
  }

  schema = reader->file_meta_data->schema;
  num_cols = schema->len - 1;

  /* Init reverse order infos */
  {
    reverse_order_info = g_new (ReverseOrderInfo *, spread_factor);
    for (guint i = 0; i < spread_factor; i++)
      reverse_order_info[i] = g_new (ReverseOrderInfo, num_cols);
  }

  /* --------------------------------------------------------------------------
   * STEP 3. Determine the start & end column
   * (first 2 is for partition key, row_id_map) (last is for delta)
   * ------------------------------------------------------------------------ */
#ifdef ENABLE_HELPER_TASK
  start_column = 2;
  end_column = (num_cols / 2);
#else
  // 0 : repartition key
  // 1 : delta row
  // last : delta column
  start_column = 2;
  end_column = num_cols - 2;
#endif

  // NVMEV_FREEBIE_DEBUG("(MASTER) start_column: %d, end_column: %d\n",
  //                     start_column, end_column);

  /* --------------------------------------------------------------------------
   * STEP 4. Read Output Files
   * ------------------------------------------------------------------------ */
  writers = g_ptr_array_new_with_free_func (g_object_unref);
  for (guint i = 0; i < spread_factor; i++) {
    writer = g_object_new (PARQUET_TYPE_WRITER, NULL);
    if (!parquet_writer_prepare (writer, (gchar *) freebie_params->output_buf[i],
                                  freebie_params->output_buf_size[i], freebie_params->output_buf_alloc_size[i], schema, error)) {
        return FALSE;
    }
    parquet_writer_write_row_group_prepare (writer);
    g_ptr_array_add (writers, writer);
  }

  child_idxs = g_new0 (guint8, (total_rows + 1) / 2);
  coverage = calc_coverage (freebie_params);

  /* --------------------------------------------------------------------------
   * STEP 5. Process Partition key (First column)
   * ------------------------------------------------------------------------ */
  {
    const gint column_idx = 0;
    guint row_idx = 0;

    // NVMEV_FREEBIE_DEBUG ("(MASTER) Processing partition key column chunk (%d)\n", column_idx);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_column_chunk_prepare (writers->pdata[i], column_idx);
    }

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_column_chunk_prepare (reader, column_idx, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          xfer = column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          g_assert (vector.total_cnt == vector.value_cnt, "partition key should not be null");

          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            Value *value = &(vector.values[vec_idx]);
            child_idx = IS_VALUE_TYPE_INT32 (value)
                        ? le_to_i32 (value->data.ptr)
                        : le_to_i64 (value->data.ptr);
            child_idx = calc_child_idx (child_idx, coverage, spread_factor);
            append_child_idx (child_idxs, child_idx, row_idx);

            writer = writers->pdata[child_idx];
            xfer = column_writer_write (writer->column_writer, value, error);
            if (xfer < 0)
              return FALSE;

            writer->written_amount += xfer;
            row_idx++;
          }
        }
      }
    }

    for (guint i = 0; i < spread_factor; i++) {
      if (!parquet_writer_write_column_chunk_end (writers->pdata[i], error)) {
        return FALSE;
      }

      gather_reverse_order_info (reverse_order_info[i], writers->pdata[i], column_idx);
    }
  }

  /* --------------------------------------------------------------------------
   * ************* Synchronize with helper task *************
   * ------------------------------------------------------------------------ */
#ifdef ENABLE_HELPER_TASK
  // NVMEV_FREEBIE_DEBUG("(MASTER) Wait for helper task to process Delta column\n");
  while (!bitmap_empty(task->helper_bitmap, MAX_TASK_COUNT)) {
	  cond_resched();
  }
  shared_data->child_idxs = child_idxs;
  shared_data->reverse_order_info = reverse_order_info;
  // Get delta data structures from helper task
  delta_mgr = shared_data->delta_mgr;
  delta_list = shared_data->delta_list;
  delta_ht = shared_data->delta_ht;

#else
  /* --------------------------------------------------------------------------
   * STEP 6. Process Delta Columns (Last column)
   * ------------------------------------------------------------------------ */
  {
    // NVMEV_FREEBIE_DEBUG ("(MASTER) Processing Delta column\n");

    delta_mgr = freebie_delta_mgr_init (schema, row_id_map);
    freebie_delta_mgr_list_init (&delta_list, delta_mgr->delta_list_entry_pool_idx);
    freebie_delta_mgr_ht_init (&delta_ht, delta_mgr->delta_list_entry_pool_idx);

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_delta_column_prepare (reader, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          xfer = delta_column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          g_assert (vector.total_cnt == vector.value_cnt, "delta entry should not be null");

          // Put all deltas into the hashtable.
          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            freebie_delta_mgr_ht_put (delta_mgr, delta_ht, vector.values + vec_idx);
          }
        }
      }
    }
  }
#endif

  /* --------------------------------------------------------------------------
   * STEP 7. Process Row ID Map column (Second column for now)
   * ------------------------------------------------------------------------ */

  if (row_id_map != 0x00000002) {
    g_set_error (error, THRIFT_PROTOCOL_ERROR, THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                 "Not implemented row ID handling");
    return FALSE;
  }

  /* --------------------------------------------------------------------------
   * Row ID
   *
   * TODO: Currently, we assume the row id is always the second column.
   *       Note that the primary key may not reside in the second column and
   *       may also span multiple columns; however, these cases are not yet
   *       implemented.
   * ------------------------------------------------------------------------ */
  {
    const gint column_idx = 1; // TODO: fix this
    guint row_idx = 0;

    // NVMEV_FREEBIE_DEBUG ("(MASTER) Processing row id column chunk (%d)\n", column_idx);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_column_chunk_prepare (writers->pdata[i], column_idx);
    }

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_column_chunk_prepare (reader, column_idx, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          xfer = column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          g_assert (vector.total_cnt == vector.value_cnt, "Row id should not be null");

          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            Value *value = &(vector.values[vec_idx]);
            FreeBIEDeltaListEntry *delta;

            child_idx = get_child_idx (child_idxs, row_idx);
            writer = writers->pdata[child_idx];

            /*
             * When a delta is found for this row id, move from hashtable
             * to list with row_idx.
             */
            if ((delta = freebie_delta_mgr_ht_find_and_extract (delta_mgr, delta_ht, value)))
              freebie_delta_mgr_list_put (delta_list, delta, row_idx);

            /*
             * Write value to the appropriate child destination file.
             */
            xfer = column_writer_write (writer->column_writer, value, error);
            if (xfer < 0)
              return FALSE;

            writer->written_amount += xfer;
            row_idx++;
          }
        }
      }
    }

    for (guint i = 0; i < spread_factor; i++) {
      if (!parquet_writer_write_column_chunk_end (writers->pdata[i], error)) {
        return FALSE;
      }
      
      gather_reverse_order_info (reverse_order_info[i], writers->pdata[i], column_idx);
    }
  }

  /* --------------------------------------------------------------------------
   * ************* Synchronize with helper task *************
   * ------------------------------------------------------------------------ */
#ifdef ENABLE_HELPER_TASK
  // Set bit to sync later again
  set_bit(task->exec_helper_task->task_id, task->helper_bitmap);
  // Now lets helper tasks to run as well
  clear_bit(task->task_id, task->exec_helper_task->helper_bitmap);
#endif

  /* --------------------------------------------------------------------------
   * STEP 8. Process Left columns (3 ~ n-1) 
   * ------------------------------------------------------------------------ */

  for (guint column_idx = start_column; column_idx <= end_column; column_idx++) {
    gint row_idx = 0;
    FreeBIEDeltaListEntry *delta_cursor;
    delta_cursor = freebie_delta_mgr_list_get_next (delta_list, NULL);

    // NVMEV_FREEBIE_DEBUG("(MASTER) Processing column chunk (%d)\n", column_idx);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_column_chunk_prepare (writers->pdata[i], column_idx);
    }

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_column_chunk_prepare (reader, column_idx, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          guint16 val_idx = 0;
          xfer = column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            Value *value = NULL_VALUE;
            Value delta = {0};

            child_idx = get_child_idx (child_idxs, row_idx);
            writer = writers->pdata[child_idx];

            /* Is this normal value? */
            if (G_LIKELY (!IS_VECTOR_ELEM_NULL (&vector, vec_idx))) {
              value = &(vector.values[val_idx++]);
            }

            /* Should we use delta instead? */
            if (delta_cursor && delta_cursor->row_idx == row_idx) {
              if (freebie_delta_mgr_apply_entry (delta_mgr, &delta, delta_cursor, column_idx)) {
                value = &delta;
              }
              delta_cursor = freebie_delta_mgr_list_get_next (delta_list, delta_cursor);
            }

            xfer = column_writer_write (writer->column_writer, value, error);
            if (xfer < 0)
              return FALSE;

            writer->written_amount += xfer;
            row_idx++;
          }
        }
      }
    }

    for (guint i = 0; i < spread_factor; i++) {
      if (!parquet_writer_write_column_chunk_end (writers->pdata[i], error))
        return FALSE;
      
      gather_reverse_order_info (reverse_order_info[i], writers->pdata[i], column_idx);
    }
  }

#ifdef ENABLE_HELPER_TASK
  /* --------------------------------------------------------------------------
   * STEP 9. Wait for helper tasks to finish 
   * ------------------------------------------------------------------------ */
  /* --------------------------------------------------------------------------
   * ************* Synchronize with helper task *************
   * ------------------------------------------------------------------------ */
  // NVMEV_FREEBIE_DEBUG("(MASTER) Wait for parallel helper task\n");
  while (!bitmap_empty(task->helper_bitmap, MAX_TASK_COUNT)) {
	  cond_resched();
  }
#endif

  /* --------------------------------------------------------------------------
   * STEP 10. Copy intermediate buffer from helper tasks to output buffer
   * ------------------------------------------------------------------------ */
#ifdef ENABLE_HELPER_TASK
  NVMEV_FREEBIE_DEBUG("(MASTER) Copy intermediate -> Output bufffer start\n");
	struct ccsd_task_info *helper_task = task->exec_helper_task;
  struct freebie_params *helper_params = (struct freebie_params *)(helper_task->params);
  GPtrArray *helper_writers = shared_data->helper_writers;
  ParquetWriter *helper_writer = NULL;

  for (int buffer_idx = 0; buffer_idx < spread_factor; buffer_idx++) {
    size_t helper_output_buffer = helper_params->output_buf[buffer_idx];
    helper_writer = helper_writers->pdata[buffer_idx];
    writer = writers->pdata[buffer_idx];

    // Update reverse_order_info offset
    for (guint column_idx = end_column + 1; column_idx <= num_cols - 2; column_idx++) {
      reverse_order_info[buffer_idx][column_idx].offset -= 4;
      reverse_order_info[buffer_idx][column_idx].offset += thrift_file_transport_get_location (writer->compact_protocol->transport);
    }

    // The 4 bytes are for the MAGIC
    thrift_transport_write(writer->compact_protocol->transport,
                           (gpointer) (helper_output_buffer + 4), helper_writer->written_amount - 4, error);

    // Fill in the metadata in "writer"
    // NVMEV_FREEBIE_DEBUG("Master Writer %d Written amount %u\n", buffer_idx, writer->written_amount);
    // NVMEV_FREEBIE_DEBUG("Helper Writer %d Written amount %u\n", buffer_idx, helper_writer->written_amount);
    parquet_merge_writer(writer, helper_writer, error);
    // NVMEV_FREEBIE_DEBUG("Total Writer %d Written amount %u\n", buffer_idx, writer->written_amount);
  }
  g_ptr_array_unref (helper_writers);
  NVMEV_FREEBIE_DEBUG("(MASTER) Copy intermediate -> Output bufffer done\n");

  // Helper tasks can now terminate
  task->exec_helper_task->task_step = CCSD_TASK_END;
#endif

  /* --------------------------------------------------------------------------
   * STEP 11. Write Delta Column (Last column)
   * 
   * <------- Delta column chunk ------->
   * +-----------------+----------------+
   * | Dictionary page | Data pages ... |
   * | (Delta page)    | (Null pages)   |
   * +-----------------+----------------+
   *
   * In practice, the delta column is organized into a dictionary page and the
   * data pages: the dictionary page stores the actual deltas, while the data
   * page contains only NULLs. This design ensures that the number of values
   * in each column chunk remains consistent across all column chunks within
   * a row group, since the number of deltas does not necessarily match the
   * number of rows. By placing all deltas in the dictionary page, we avoid
   * affecting the value count of the column chunk, and by filling the data page
   * with NULLs, we maintain alignment with the other column chunks.
   * ------------------------------------------------------------------------ */
  {
    FreeBIEDeltaListEntry *iter = NULL;

    // NVMEV_FREEBIE_DEBUG("(MASTER) Writing Delta Column (%d)\n", num_cols - 1);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_delta_column_prepare (writers->pdata[i]);
    }

    /*
     * Write remaining deltas
     */
    iter = freebie_delta_mgr_ht_get_next (delta_ht, iter);
    while (iter) {
      FreeBIEDeltaEntry *delta;
      Value delta_value = {0};

      g_assert (delta_mgr->base_ptr, "Base pointer not initiated");
      delta = freebie_delta_mgr_get_entry (delta_mgr, iter);

      child_idx = calc_child_idx (delta->part_key, coverage, spread_factor);
      writer = writers->pdata[child_idx];

      /*
       * Delta is always stored in byte array type.
       */
      value_set_byte_array (&delta_value, delta, delta->len + sizeof (delta->len));

      xfer = delta_column_writer_write (writer->column_writer, &delta_value, error);
      if (xfer < 0) {
        return FALSE;
      }

      writer->written_amount += xfer;

      iter = freebie_delta_mgr_ht_get_next (delta_ht, iter);
    }

    // NVMEV_FREEBIE_DEBUG("(MASTER) Fill NULL Delta (%d)\n", num_cols - 1);

    /*
     * Fill NULLs
     */
    for (guint i = 0; i < spread_factor; i++) {
      writer = writers->pdata[i];

      /*
       * Finalize delta page (i.e., dictionary page) writing
       */
      xfer = delta_column_writer_prepare_null (writer->column_writer, error);
      if (xfer < 0) {
        return FALSE;
      }

      writer->written_amount += xfer;

      /*
       * Fill NULLs for the delta column
       */
      for (guint j = 0; j < writer->cur_row_group->num_rows; j++) {
        xfer = delta_column_writer_write_null (writer->column_writer);
        if (xfer < 0) {
          return FALSE;
        }
      }

      /*
       * End writing destination column chunks
       */
      if (!parquet_writer_write_delta_column_end (writer, error)) {
        return FALSE;
      }
      gather_reverse_order_info (reverse_order_info[i], writer, num_cols - 1);
    }

    // NVMEV_FREEBIE_DEBUG("(MASTER) Finalize Delta (%d)\n", num_cols - 1);
  }

  freebie_delta_mgr_list_close (&delta_list);
  freebie_delta_mgr_ht_close (&delta_ht);
  freebie_delta_mgr_finish(delta_mgr);
  g_free (child_idxs);

  // Clean-up readers
  for (guint i = 0; i < input_file_cnt; i++) {
    reader = readers->pdata[i];

    xfer = parquet_reader_end (reader, error);
    if (xfer < 0) {
      return FALSE;
    }
  }
#ifdef ENABLE_HELPER_TASK
  g_ptr_array_unref (shared_data->helper_readers);
  kfree(task->shared);
#endif
  g_ptr_array_unref (readers);

  // NVMEV_FREEBIE_DEBUG("(MASTER) Start Reorder\n");
  /* (Example) reordering w/ reverse info */
  {
    for (guint i = 0; i < spread_factor; i++) {
      guint offset;

      /* Validate */
      offset = reverse_order_info[i][0].offset;
      for (guint j = 0; j < num_cols; j++)
      {
        g_assert (offset == reverse_order_info[i][j].offset, "Validation failed");
        offset += reverse_order_info[i][j].data_bytes +
                  reverse_order_info[i][j].phdr_bytes +
                  reverse_order_info[i][j].def_bytes;
      }

      gchar *input_buf;
      input_buf = (gchar *)freebie_params->output_buf[i];

      /* Reorder */
      offset = reverse_order_info[i][0].offset;
      for (guint j = 0; j < num_cols - 1; j++) {
        ReverseOrderInfo *info;
        guint col_size;
        gchar *col_buf;
        guint r_offset = 0;

        info = &reverse_order_info[i][j];
        col_size = info->data_bytes + info->phdr_bytes + info->def_bytes;
        col_buf = g_new (gchar, col_size);
        if (col_buf == NULL) {
          printk("NO!!!!!\n");
          BUG();
        }     

        // (1) Page header
        memcpy(col_buf + r_offset, input_buf + offset + info->data_bytes, info->phdr_bytes);
        r_offset += info->phdr_bytes;

        // (2) Definition level
        memcpy(col_buf + r_offset, input_buf + offset + info->data_bytes + info->phdr_bytes, info->def_bytes);
        r_offset += info->def_bytes;

        // (3) Data
        memcpy(col_buf + r_offset, input_buf + offset, info->data_bytes);

        // (4) Flush reordered
        memcpy (input_buf + offset, col_buf, col_size);
        offset += col_size;

        g_free (col_buf);
      }

      /* Reorder delta column */
      {
        ReverseOrderInfo *info;
        guint col_size;
        gchar *col_buf;
        guint r_offset = 0;

        info = &reverse_order_info[i][num_cols - 1];
        col_size = info->data_bytes + info->phdr_bytes + info->def_bytes;
        col_buf = g_new (gchar, col_size);
        if (col_buf == NULL) {
          printk("NO!!!!!\n");
          BUG();
        }

        // (1) Delta page header
        memcpy(col_buf + r_offset, input_buf + offset + info->data_bytes, info->phdr_bytes);
        r_offset += info->phdr_bytes;

        // (2) Delta page data
        memcpy(col_buf + r_offset, input_buf + offset, info->data_bytes);
        r_offset += info->data_bytes;

        // (3) Null page header + Null page definition level
        memcpy(col_buf + r_offset, input_buf + offset + info->data_bytes + info->phdr_bytes, info->def_bytes);

        // (4) Flush reordered
        memcpy (input_buf + offset, col_buf, col_size);

        offset += col_size;
        g_free (col_buf);
      }
    }
  }
  // NVMEV_FREEBIE_DEBUG("(MASTER) End Reorder\n");

  /* --------------------------------------------------------------------------
   * STEP 12. Write footers & Persist output files 
   * ------------------------------------------------------------------------ */
  for (guint i = 0; i < spread_factor; i++) {
    writer = writers->pdata[i];
    parquet_writer_write_row_group_end (writer);

    xfer = parquet_writer_end (writer, error);
    if (xfer < 0) {
      return FALSE;
    }
    writer->written_amount += xfer;

    // Modify metadata
    
    if (freebie_params->last_level_generated_bitmap & (1 <<i)) {
      // This only happens in last level output cases
      BUG_ON(freebie_params->last_level != freebie_params->current_level + 1);

      freebie_add_sre(freebie_sre, &nentry, child_offset + (first_child_partition_no + i) * sizeof(FreeBIESGPair),
                    writer->written_amount, FREEBIE_OPERATION_WRITE);

      freebie_add_sre(freebie_sre, &nentry, child_offset + (first_child_partition_no + i) * sizeof(FreeBIESGPair) + sizeof(guint),
                      1, FREEBIE_OPERATION_ADD);
    } else {
      freebie_add_sre(freebie_sre, &nentry, child_offset + (first_child_partition_no + i) * sizeof(FreeBIESGPair),
                    writer->written_amount, FREEBIE_OPERATION_ADD);
    }


    // Use this field to store the increased size of the output file
    freebie_params->output_buf_alloc_size[i] = writer->written_amount;

    // NVMEV_FREEBIE_DEBUG("(MASTER) Output file[%d] increased size (%d + %d --> %d)\n", i,
    //                     freebie_params->output_buf_size[i], writer->written_amount,
    //                     freebie_params->output_buf_size[i] + writer->written_amount);
  }

  // Modify metadata
  if (current_level == 0) {
    freebie_add_sre(freebie_sre, &nentry, parent_offset + partition_no * sizeof(guint),
                    input_file_cnt, FREEBIE_OPERATION_ADD);
  }
  else {
    freebie_add_sre(freebie_sre, &nentry, parent_offset + partition_no * sizeof(FreeBIESGPair),
                    0, FREEBIE_OPERATION_WRITE);
    freebie_add_sre(freebie_sre, &nentry, parent_offset + partition_no * sizeof(FreeBIESGPair) + sizeof(guint),
                    1, FREEBIE_OPERATION_ADD);
  }

  // Write the output parquet files to NVM (SLM -> NAND)
  for (int task_idx = 0; task_idx < task->copy_helper_count; task_idx++) {
    struct ccsd_task_info *copy_helper_task = task->copy_helper_task[task_idx];

    copy_helper_task->done_size = freebie_params->output_buf_size[task_idx];              // Old size of the output file
    copy_helper_task->requested_io_offset = freebie_params->output_buf_size[task_idx];    // Old size of the output file
    copy_helper_task->total_size = freebie_params->output_buf_alloc_size[task_idx] + copy_helper_task->done_size;
   
    copy_helper_task->program_idx = COPY_FROM_SLM_PROGRAM_INDEX; // Copy to NAND
    copy_helper_task->can_terminate = true;

    // Wait for helpers to write to NVM
		set_bit(copy_helper_task->task_id, task->helper_bitmap);

    copy_helper_task->task_step = CCSD_TASK_SCHEDULE;
  }

  NVMEV_FREEBIE_DEBUG("(MASTER) Start Output File Copy\n");
  while (!bitmap_empty(task->helper_bitmap, MAX_TASK_COUNT)) {
			cond_resched();
  }
  NVMEV_FREEBIE_DEBUG("(MASTER) Output File Copy done\n");

  /* --------------------------------------------------------------------------
   * STEP 13. Update Partition Map 
   * ------------------------------------------------------------------------ */

  freebie_update_metadata(task, relation_id, freebie_sre, nentry);
  NVMEV_FREEBIE_DEBUG("(MASTER) Update Metadata done\n");
  g_ptr_array_unref (writers);

  /* Free reverse order infos */
  {
    for (guint i = 0; i < spread_factor; i++)
      g_free (reverse_order_info[i]);
    g_free (reverse_order_info);
  }

  return TRUE;
}

gboolean freebie_repartition_with_delete (void *_task)
{
  GError *error_t = NULL;
  GError **error = &error_t;
  struct ccsd_task_info *task = (struct ccsd_task_info *)_task;
  struct freebie_params *freebie_params = (struct freebie_params *)(task->params);

  ParquetReader *reader = NULL;
  GPtrArray *schema;
  GPtrArray *readers;
  GPtrArray *writers = NULL;
  ParquetWriter *writer;
  guint8 *child_idxs;
  guint64 child_idx = 0;
  gint coverage;
  Vector vector;
  ColumnReader *column_reader;
  guint num_cols;
  gint32 xfer;
  guint32 total_rows = 0;

  // Delta column
  FreeBIEDeltaMgr *delta_mgr = NULL;
  DeltaHT *delta_ht = NULL;
  DeltaList *delta_list = NULL;

  // Freebie parameters
  guint32 spread_factor = freebie_params->spread_factor;
  guint32 input_file_cnt = freebie_params->input_file_cnt;
  guint32 partition_no = freebie_params->partition_no;
  guint32 relation_id = freebie_params->relation_id; 
  guint32 current_level = freebie_params->current_level;
  guint32 row_id_map = freebie_params->row_id_map;

  // Metadata related data structures
  freebie_source_range_entry freebie_sre[32]; // TODO: This size should be modified later
  guint first_child_partition_no = partition_no * spread_factor;
  guint parent_offset = 0;
  guint child_offset = 0;
  guint nentry = 0;

  // For helper tasks
  guint start_column = 0;
  guint end_column = 0;
	FreeBIEShared *shared_data = (FreeBIEShared *)task->shared; 

  // Reverse order info for all columns in all output files
  ReverseOrderInfo **reverse_order_info;

  // For Metadata update
  parent_offset += sizeof(guint) * 3; // Logical Metadata header
  if (current_level == 0) {
    child_offset = parent_offset + (sizeof(guint) * spread_factor);
  }
  else /* config->current_level > 0 */ {
    parent_offset += (sizeof(guint) * spread_factor);
    for (guint i = 1; i < current_level; i++) {
      parent_offset += (sizeof(FreeBIESGPair) * power(spread_factor, i + 1));
    }
    child_offset = parent_offset + (sizeof(FreeBIESGPair) * power(spread_factor, current_level + 1));
  }

  NVMEV_FREEBIE_DEBUG("FREEBIE MASTER - Repartition start (with DELETE) (relation_id: %d) (current_level: %d) (partition no: %d)\n",
                                    relation_id, current_level, partition_no);

  if (input_file_cnt < 1) {
    return TRUE;
  }

  /* --------------------------------------------------------------------------
   * STEP 1. Read input files
   * ------------------------------------------------------------------------ */
  readers = g_ptr_array_new_with_free_func (g_object_unref);
  for (guint i = 0; i < input_file_cnt; i++) {
    reader = g_object_new (PARQUET_TYPE_READER, NULL);
    if (!parquet_reader_prepare (reader, (gchar *) freebie_params->input_buf[i],
                                  freebie_params->input_buf_size[i], error)) {
      return FALSE;
    }

    if (reader->file_meta_data == NULL) {
      return FALSE;
    }

    total_rows += reader->file_meta_data->num_rows;
    g_ptr_array_add (readers, reader);
  }

  schema = reader->file_meta_data->schema;
  num_cols = schema->len - 1;

  /* Init reverse order infos */
  {
    reverse_order_info = g_new (ReverseOrderInfo *, spread_factor);
    for (guint i = 0; i < spread_factor; i++)
      reverse_order_info[i] = g_new (ReverseOrderInfo, num_cols);
  }

  /* --------------------------------------------------------------------------
   * STEP 2. Determine the start & end column
   * (first 2 is for partition key, row_id_map) (last is for delta)
   * ------------------------------------------------------------------------ */
  start_column = 2;
  end_column = num_cols - 2;

  // NVMEV_FREEBIE_DEBUG("(MASTER) start_column: %d, end_column: %d\n",
  //                     start_column, end_column);

  /* --------------------------------------------------------------------------
   * STEP 3. Read Output Files
   * ------------------------------------------------------------------------ */
  writers = g_ptr_array_new_with_free_func (g_object_unref);
  for (guint i = 0; i < spread_factor; i++) {
    writer = g_object_new (PARQUET_TYPE_WRITER, NULL);
    if (!parquet_writer_prepare (writer, (gchar *) freebie_params->output_buf[i],
                                  freebie_params->output_buf_size[i], freebie_params->output_buf_alloc_size[i], schema, error)) {
        return FALSE;
    }
    parquet_writer_write_row_group_prepare (writer);
    g_ptr_array_add (writers, writer);
  }

  child_idxs = g_new0 (guint8, (total_rows + 1) / 2);
  coverage = calc_coverage (freebie_params);

  /* --------------------------------------------------------------------------
   * STEP 4. Read Delta Columns (Last column)
   * ------------------------------------------------------------------------ */
  {
    // NVMEV_FREEBIE_DEBUG ("(MASTER) Read Delta column\n");

    delta_mgr = freebie_delta_mgr_init (schema, row_id_map);
    freebie_delta_mgr_list_init (&delta_list, delta_mgr->delta_list_entry_pool_idx);
    freebie_delta_mgr_ht_init (&delta_ht, delta_mgr->delta_list_entry_pool_idx);

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_delta_column_prepare (reader, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          xfer = delta_column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          g_assert (vector.total_cnt == vector.value_cnt, "delta entry should not be null");

          // Put all deltas into the hashtable.
          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            freebie_delta_mgr_ht_put (delta_mgr, delta_ht, vector.values + vec_idx);
          }
        }
      }
    }
  }

  /* --------------------------------------------------------------------------
   * STEP 5. Read Row ID Map column (Second column for now)
   * ------------------------------------------------------------------------ */

  if (row_id_map != 0x00000002) {
    g_set_error (error, THRIFT_PROTOCOL_ERROR, THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
                 "Not implemented row ID handling");
    return FALSE;
  }
  {
    const gint column_idx = 1;
    guint row_idx = 0;

    // NVMEV_FREEBIE_DEBUG ("(MASTER) Read row id column chunk (%d)\n", column_idx);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_column_chunk_prepare (writers->pdata[i], column_idx);
    }

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_column_chunk_prepare (reader, column_idx, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          xfer = column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          g_assert (vector.total_cnt == vector.value_cnt, "Row id should not be null");

          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            Value *value = &(vector.values[vec_idx]);
            FreeBIEDeltaListEntry *delta;

            if ((delta = freebie_delta_mgr_ht_find_and_extract (delta_mgr, delta_ht, value)))
              freebie_delta_mgr_list_put (delta_list, delta, row_idx);

            row_idx++;
          }
        }
      }
    }
  }

  /* --------------------------------------------------------------------------
   * STEP 6. Read & Write Partition key (First column)
   * ------------------------------------------------------------------------ */
  {
    const gint column_idx = 0;
    guint row_idx = 0;
    FreeBIEDeltaListEntry *delta_cursor;

    delta_cursor = freebie_delta_mgr_list_get_next (delta_list, NULL);

    // NVMEV_FREEBIE_DEBUG ("(MASTER) Read & Write partition key column chunk (%d)\n", column_idx);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_column_chunk_prepare (writers->pdata[i], column_idx);
    }

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_column_chunk_prepare (reader, column_idx, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          xfer = column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          g_assert (vector.total_cnt == vector.value_cnt, "partition key should not be null");

          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            Value *value = &(vector.values[vec_idx]);
            gboolean tombstone = FALSE;
            child_idx = IS_VALUE_TYPE_INT32 (value)
                        ? le_to_i32 (value->data.ptr)
                        : le_to_i64 (value->data.ptr);
            child_idx = calc_child_idx (child_idx, coverage, spread_factor);
            append_child_idx (child_idxs, child_idx, row_idx);

            writer = writers->pdata[child_idx];
            /* Is this record alive ? */
            if (delta_cursor && delta_cursor->row_idx == row_idx) {
              tombstone = freebie_delta_mgr_is_tombstone(delta_mgr, delta_cursor);
              delta_cursor = freebie_delta_mgr_list_get_next (delta_list, delta_cursor);
            }

            if (!tombstone) {
              xfer = column_writer_write (writer->column_writer, value, error);
              if (xfer < 0)
                return FALSE;
              writer->written_amount += xfer;
            }

            row_idx++;
          }
        }
      }
    }

    for (guint i = 0; i < spread_factor; i++) {
      if (!parquet_writer_write_column_chunk_end (writers->pdata[i], error)) {
        return FALSE;
      }

      gather_reverse_order_info (reverse_order_info[i], writers->pdata[i], column_idx);
    }
  }
  
  /* --------------------------------------------------------------------------
   * STEP 7. Read & Write Row ID Map column (Second column for now)
   * ------------------------------------------------------------------------ */
  {
    const gint column_idx = 1;
    guint row_idx = 0;
    FreeBIEDeltaListEntry *delta_cursor;

    delta_cursor = freebie_delta_mgr_list_get_next (delta_list, NULL);

    // NVMEV_FREEBIE_DEBUG ("(MASTER) Read & Write row id column chunk (%d)\n", column_idx);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_column_chunk_prepare (writers->pdata[i], column_idx);
    }

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_column_chunk_prepare (reader, column_idx, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          xfer = column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          g_assert (vector.total_cnt == vector.value_cnt, "Row id should not be null");

          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            Value *value = &(vector.values[vec_idx]);
            gboolean tombstone = FALSE;

            child_idx = get_child_idx (child_idxs, row_idx);
            writer = writers->pdata[child_idx];

            if (delta_cursor && delta_cursor->row_idx == row_idx) {
              tombstone = freebie_delta_mgr_is_tombstone(delta_mgr, delta_cursor);
              delta_cursor = freebie_delta_mgr_list_get_next (delta_list, delta_cursor);
            }

            if (!tombstone) {
              xfer = column_writer_write (writer->column_writer, value, error);
              if (xfer < 0)
                return FALSE;

              writer->written_amount += xfer;
            }

            row_idx++;
          }
        }
      }
    }

    for (guint i = 0; i < spread_factor; i++) {
      if (!parquet_writer_write_column_chunk_end (writers->pdata[i], error)) {
        return FALSE;
      }
    
      gather_reverse_order_info (reverse_order_info[i], writers->pdata[i], column_idx);
    }
  }

  /* --------------------------------------------------------------------------
   * STEP 8. Process Left columns (3 ~ n-1) 
   * ------------------------------------------------------------------------ */

  for (guint column_idx = start_column; column_idx <= end_column; column_idx++) {
    gint row_idx = 0;
    FreeBIEDeltaListEntry *delta_cursor;
    delta_cursor = freebie_delta_mgr_list_get_next (delta_list, NULL);

    // NVMEV_FREEBIE_DEBUG("(MASTER) Processing column chunk (%d)\n", column_idx);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_column_chunk_prepare (writers->pdata[i], column_idx);
    }

    for (guint input_file_idx = 0; input_file_idx < input_file_cnt; input_file_idx++) {
      reader = readers->pdata[input_file_idx];
      reader->cur_row_group_idx = -1;

      while (parquet_reader_read_row_group_prepare (reader)) {
        column_reader = parquet_reader_read_column_chunk_prepare (reader, column_idx, error);
        if (column_reader == NULL)
          return FALSE;

        while (TRUE) {
          guint16 val_idx = 0;
          xfer = column_reader_read (column_reader, &vector, error);
          if (xfer < 0)
            return FALSE;

          if (!column_reader->is_init)
            break;

          for (guint16 vec_idx = 0; vec_idx < vector.total_cnt; vec_idx++) {
            Value *value = NULL_VALUE;
            Value delta = {0};
            gboolean tombstone = FALSE;

            child_idx = get_child_idx (child_idxs, row_idx);
            writer = writers->pdata[child_idx];

            /* Is this normal value? */
            if (G_LIKELY (!IS_VECTOR_ELEM_NULL (&vector, vec_idx))) {
              value = &(vector.values[val_idx++]);
            }

            /* Should we use delta instead? */
            if (delta_cursor && delta_cursor->row_idx == row_idx) {
              if (freebie_delta_mgr_is_tombstone(delta_mgr, delta_cursor)) {
                tombstone = TRUE;
              } else if (freebie_delta_mgr_apply_entry (delta_mgr, &delta, delta_cursor, column_idx)) {
                value = &delta;
              }

              delta_cursor = freebie_delta_mgr_list_get_next (delta_list, delta_cursor);
            }

            if (!tombstone) {
              xfer = column_writer_write (writer->column_writer, value, error);
              if (xfer < 0)
                return FALSE;
              writer->written_amount += xfer;
            }

            row_idx++;
          }
        }
      }
    }

    for (guint i = 0; i < spread_factor; i++) {
      if (!parquet_writer_write_column_chunk_end (writers->pdata[i], error))
        return FALSE;
      
      gather_reverse_order_info (reverse_order_info[i], writers->pdata[i], column_idx);
    }
  }

  /* --------------------------------------------------------------------------
   * STEP 9. Write Delta Column (Last column)
   * 
   * <------- Delta column chunk ------->
   * +-----------------+----------------+
   * | Dictionary page | Data pages ... |
   * | (Delta page)    | (Null pages)   |
   * +-----------------+----------------+
   *
   * In practice, the delta column is organized into a dictionary page and the
   * data pages: the dictionary page stores the actual deltas, while the data
   * page contains only NULLs. This design ensures that the number of values
   * in each column chunk remains consistent across all column chunks within
   * a row group, since the number of deltas does not necessarily match the
   * number of rows. By placing all deltas in the dictionary page, we avoid
   * affecting the value count of the column chunk, and by filling the data page
   * with NULLs, we maintain alignment with the other column chunks.
   * ------------------------------------------------------------------------ */
  {
    FreeBIEDeltaListEntry *iter = NULL;

    // NVMEV_FREEBIE_DEBUG("(MASTER) Writing Delta Column (%d)\n", num_cols - 1);

    for (guint i = 0; i < spread_factor; i++) {
      parquet_writer_write_delta_column_prepare (writers->pdata[i]);
    }

    /*
     * Write remaining deltas
     */
    iter = freebie_delta_mgr_ht_get_next (delta_ht, iter);
    while (iter) {
      FreeBIEDeltaEntry *delta;
      Value delta_value = {0};

      g_assert (delta_mgr->base_ptr, "Base pointer not initiated");
      delta = freebie_delta_mgr_get_entry (delta_mgr, iter);

      child_idx = calc_child_idx (delta->part_key, coverage, spread_factor);
      writer = writers->pdata[child_idx];

      /*
       * Delta is always stored in byte array type.
       */
      value_set_byte_array (&delta_value, delta, delta->len + sizeof (delta->len));

      xfer = delta_column_writer_write (writer->column_writer, &delta_value, error);
      if (xfer < 0) {
        return FALSE;
      }

      writer->written_amount += xfer;

      iter = freebie_delta_mgr_ht_get_next (delta_ht, iter);
    }

    // NVMEV_FREEBIE_DEBUG("(MASTER) Fill NULL Delta (%d)\n", num_cols - 1);

    /*
     * Fill NULLs
     */
    for (guint i = 0; i < spread_factor; i++) {
      writer = writers->pdata[i];

      /*
       * Finalize delta page (i.e., dictionary page) writing
       */
      xfer = delta_column_writer_prepare_null (writer->column_writer, error);
      if (xfer < 0) {
        return FALSE;
      }

      writer->written_amount += xfer;

      /*
       * Fill NULLs for the delta column
       */
      for (guint j = 0; j < writer->cur_row_group->num_rows; j++) {
        xfer = delta_column_writer_write_null (writer->column_writer);
        if (xfer < 0) {
          return FALSE;
        }
      }

      /*
       * End writing destination column chunks
       */
      if (!parquet_writer_write_delta_column_end (writer, error)) {
        return FALSE;
      }
      gather_reverse_order_info (reverse_order_info[i], writer, num_cols - 1);
    }

    // NVMEV_FREEBIE_DEBUG("(MASTER) Finalize Delta (%d)\n", num_cols - 1);
  }

  freebie_delta_mgr_list_close (&delta_list);
  freebie_delta_mgr_ht_close (&delta_ht);
  freebie_delta_mgr_finish(delta_mgr);
  g_free (child_idxs);

  // Clean-up readers
  for (guint i = 0; i < input_file_cnt; i++) {
    reader = readers->pdata[i];

    xfer = parquet_reader_end (reader, error);
    if (xfer < 0) {
      return FALSE;
    }
  }
  g_ptr_array_unref (readers);

  /* --------------------------------------------------------------------------
   * STEP 10. Reorder output buffers 
   * ------------------------------------------------------------------------ */

  // NVMEV_FREEBIE_DEBUG("(MASTER) Start Reorder\n");
  /* (Example) reordering w/ reverse info */
  {
    for (guint i = 0; i < spread_factor; i++) {
      guint offset;

      /* Validate */
      offset = reverse_order_info[i][0].offset;
      for (guint j = 0; j < num_cols; j++)
      {
        g_assert (offset == reverse_order_info[i][j].offset, "Validation failed");
        offset += reverse_order_info[i][j].data_bytes +
                  reverse_order_info[i][j].phdr_bytes +
                  reverse_order_info[i][j].def_bytes;
      }

      gchar *input_buf;
      input_buf = (gchar *)freebie_params->output_buf[i];

      /* Reorder */
      offset = reverse_order_info[i][0].offset;
      for (guint j = 0; j < num_cols - 1; j++) {
        ReverseOrderInfo *info;
        guint col_size;
        gchar *col_buf;
        guint r_offset = 0;

        info = &reverse_order_info[i][j];
        col_size = info->data_bytes + info->phdr_bytes + info->def_bytes;
        col_buf = g_new (gchar, col_size);
        if (col_buf == NULL) {
          BUG();
        }     

        // (1) Page header
        memcpy(col_buf + r_offset, input_buf + offset + info->data_bytes, info->phdr_bytes);
        r_offset += info->phdr_bytes;

        // (2) Definition level
        memcpy(col_buf + r_offset, input_buf + offset + info->data_bytes + info->phdr_bytes, info->def_bytes);
        r_offset += info->def_bytes;

        // (3) Data
        memcpy(col_buf + r_offset, input_buf + offset, info->data_bytes);

        // (4) Flush reordered
        memcpy (input_buf + offset, col_buf, col_size);
        offset += col_size;

        g_free (col_buf);
      }

      /* Reorder delta column */
      {
        ReverseOrderInfo *info;
        guint col_size;
        gchar *col_buf;
        guint r_offset = 0;

        info = &reverse_order_info[i][num_cols - 1];
        col_size = info->data_bytes + info->phdr_bytes + info->def_bytes;
        col_buf = g_new (gchar, col_size);
        if (col_buf == NULL) {
          BUG();
        }

        // (1) Delta page header
        memcpy(col_buf + r_offset, input_buf + offset + info->data_bytes, info->phdr_bytes);
        r_offset += info->phdr_bytes;

        // (2) Delta page data
        memcpy(col_buf + r_offset, input_buf + offset, info->data_bytes);
        r_offset += info->data_bytes;

        // (3) Null page header + Null page definition level
        memcpy(col_buf + r_offset, input_buf + offset + info->data_bytes + info->phdr_bytes, info->def_bytes);

        // (4) Flush reordered
        memcpy (input_buf + offset, col_buf, col_size);

        offset += col_size;
        g_free (col_buf);
      }
    }
  }
  // NVMEV_FREEBIE_DEBUG("(MASTER) End Reorder\n");

  /* --------------------------------------------------------------------------
   * STEP 11. Write footers & Persist output files 
   * ------------------------------------------------------------------------ */
  for (guint i = 0; i < spread_factor; i++) {
    writer = writers->pdata[i];
    parquet_writer_write_row_group_end (writer);

    xfer = parquet_writer_end (writer, error);
    if (xfer < 0) {
      return FALSE;
    }
    writer->written_amount += xfer;

    // Modify metadata
    // Add support for last level output files.
    // The output file in the last level can incrase over the predefined threshold
    // Here we receive hints from the host to increase the generation of output files.
    // For the last level output files, The generation increase is initiated by the host.
    // if (freebie_params->last_level_generated_bitmap & (1 << i)) {
    //   freebie_add_sre(freebie_sre, &nentry, child_offset + (first_child_partition_no + i) * sizeof(FreeBIESGPair) + sizeof(guint),
    //                   1, FREEBIE_OPERATION_ADD);
    // }
    if (freebie_params->last_level_generated_bitmap & (1 <<i)) {
      // This only happens in last level output cases
      BUG_ON(freebie_params->last_level != freebie_params->current_level + 1);

      freebie_add_sre(freebie_sre, &nentry, child_offset + (first_child_partition_no + i) * sizeof(FreeBIESGPair),
                    writer->written_amount, FREEBIE_OPERATION_WRITE);

      freebie_add_sre(freebie_sre, &nentry, child_offset + (first_child_partition_no + i) * sizeof(FreeBIESGPair) + sizeof(guint),
                      1, FREEBIE_OPERATION_ADD);
    } else {
      freebie_add_sre(freebie_sre, &nentry, child_offset + (first_child_partition_no + i) * sizeof(FreeBIESGPair),
                    writer->written_amount, FREEBIE_OPERATION_ADD);
    }

    // Use this field to store the increased size of the output file
    freebie_params->output_buf_alloc_size[i] = writer->written_amount;

    // NVMEV_FREEBIE_DEBUG("(MASTER) Output file[%d] increased size (%d + %d --> %d)\n", i,
    //                     freebie_params->output_buf_size[i], writer->written_amount,
    //                     freebie_params->output_buf_size[i] + writer->written_amount);
  }

  // Modify metadata
  if (current_level == 0) {
    freebie_add_sre(freebie_sre, &nentry, parent_offset + partition_no * sizeof(guint),
                    input_file_cnt, FREEBIE_OPERATION_ADD);
  }
  else {
    freebie_add_sre(freebie_sre, &nentry, parent_offset + partition_no * sizeof(FreeBIESGPair),
                    0, FREEBIE_OPERATION_WRITE);
    freebie_add_sre(freebie_sre, &nentry, parent_offset + partition_no * sizeof(FreeBIESGPair) + sizeof(guint),
                    1, FREEBIE_OPERATION_ADD);
  }

  // Write the output parquet files to NVM (SLM -> NAND)
  for (int task_idx = 0; task_idx < task->copy_helper_count; task_idx++) {
    struct ccsd_task_info *copy_helper_task = task->copy_helper_task[task_idx];

    copy_helper_task->done_size = freebie_params->output_buf_size[task_idx];              // Old size of the output file
    copy_helper_task->requested_io_offset = freebie_params->output_buf_size[task_idx];    // Old size of the output file
    copy_helper_task->total_size = freebie_params->output_buf_alloc_size[task_idx] + copy_helper_task->done_size;
   
    copy_helper_task->program_idx = COPY_FROM_SLM_PROGRAM_INDEX; // Copy to NAND
    copy_helper_task->can_terminate = true;

    // Wait for helpers to write to NVM
		set_bit(copy_helper_task->task_id, task->helper_bitmap);

    copy_helper_task->task_step = CCSD_TASK_SCHEDULE;
  }

  NVMEV_FREEBIE_DEBUG("(MASTER) Start Output File Copy\n");
  while (!bitmap_empty(task->helper_bitmap, MAX_TASK_COUNT)) {
			cond_resched();
  }
  NVMEV_FREEBIE_DEBUG("(MASTER) Output File Copy done\n");

  /* --------------------------------------------------------------------------
   * STEP 12. Update Partition Map 
   * ------------------------------------------------------------------------ */

  freebie_update_metadata(task, relation_id, freebie_sre, nentry);
  NVMEV_FREEBIE_DEBUG("(MASTER) Update Metadata done\n");
  g_ptr_array_unref (writers);

  /* Free reverse order infos */
  {
    for (guint i = 0; i < spread_factor; i++)
      g_free (reverse_order_info[i]);
    g_free (reverse_order_info);
  }

  return TRUE;
}
