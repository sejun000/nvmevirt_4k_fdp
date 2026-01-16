#include "gobject.h"
#include "gobject-type.h"
#include "gtypes.h"
#include "gmem.h"
#include "parquet_types.h"
#include "thrift.h"
#include "thrift_protocol.h"
#include "thrift_compact_protocol.h"
#include "thrift_file_transport.h"
#include "parquet_plain_encoder.h"
#include "parquet_rle_bp_encoder.h"
#include "parquet_column_reader.h"
#include "parquet_column_writer.h"
#include "parquet_reader.h"
#include "parquet_writer.h"

static Statistics *statistics_pool_pop (void);
static void statistics_pool_push (Statistics *stat);
static ColumnChunk *column_chunk_pool_pop (void);
static void column_chunk_pool_push (ColumnChunk *column_chunk);
static ColumnMetaData *column_meta_data_pool_pop (void);
static void column_meta_data_pool_push (ColumnMetaData *column_meta_data);

gpointer
g_object_new (GType object_type, const gchar *first_property_name, ...)
{
  gpointer obj;

  THRIFT_UNUSED_VAR (first_property_name);

  switch (object_type)
  {
  /* Parquet */
  case G_TYPE_SIZE_STATISTICS:
    obj = g_new (SizeStatistics, 1);
    size_statistics_instance_init (SIZE_STATISTICS(obj));
    break;
  case G_TYPE_STATISTICS:
    obj = statistics_pool_pop ();
    statistics_instance_init (STATISTICS(obj));
    break;
  case G_TYPE_STRING_TYPE:
    obj = g_new (StringType, 1);
    string_type_instance_init (STRING_TYPE(obj));
    break;
  case G_TYPE_U_U_I_D_TYPE:
    obj = g_new (UUIDType, 1);
    u_u_i_d_type_instance_init (U_U_I_D_TYPE(obj));
    break;
  case G_TYPE_MAP_TYPE:
    obj = g_new (MapType, 1);
    map_type_instance_init (MAP_TYPE(obj));
    break;
  case G_TYPE_LIST_TYPE:
    obj = g_new (ListType, 1);
    list_type_instance_init (LIST_TYPE(obj));
    break;
  case G_TYPE_ENUM_TYPE:
    obj = g_new (EnumType, 1);
    enum_type_instance_init (ENUM_TYPE(obj));
    break;
  case G_TYPE_DATE_TYPE:
    obj = g_new (DateType, 1);
    date_type_instance_init (DATE_TYPE(obj));
    break;
  case G_TYPE_FLOAT16_TYPE:
    obj = g_new (Float16Type, 1);
    float16_type_instance_init (FLOAT16_TYPE(obj));
    break;
  case G_TYPE_NULL_TYPE:
    obj = g_new (NullType, 1);
    null_type_instance_init (NULL_TYPE(obj));
    break;
  case G_TYPE_DECIMAL_TYPE:
    obj = g_new (DecimalType, 1);
    decimal_type_instance_init (DECIMAL_TYPE(obj));
    break;
  case G_TYPE_MILLI_SECONDS:
    obj = g_new (MilliSeconds, 1);
    milli_seconds_instance_init (MILLI_SECONDS(obj));
    break;
  case G_TYPE_MICRO_SECONDS:
    obj = g_new (MicroSeconds, 1);
    micro_seconds_instance_init (MICRO_SECONDS(obj));
    break;
  case G_TYPE_NANO_SECONDS:
    obj = g_new (NanoSeconds, 1);
    nano_seconds_instance_init (NANO_SECONDS(obj));
    break;
  case G_TYPE_TIME_UNIT:
    obj = g_new (TimeUnit, 1);
    time_unit_instance_init (TIME_UNIT(obj));
    break;
  case G_TYPE_TIMESTAMP_TYPE:
    obj = g_new (TimestampType, 1);
    timestamp_type_instance_init (TIMESTAMP_TYPE(obj));
    break;
  case G_TYPE_TIME_TYPE:
    obj = g_new (TimeType, 1);
    time_type_instance_init (TIME_TYPE(obj));
    break;
  case G_TYPE_INT_TYPE:
    obj = g_new (IntType, 1);
    int_type_instance_init (INT_TYPE(obj));
    break;
  case G_TYPE_JSON_TYPE:
    obj = g_new (JsonType, 1);
    json_type_instance_init (JSON_TYPE(obj));
    break;
  case G_TYPE_BSON_TYPE:
    obj = g_new (BsonType, 1);
    bson_type_instance_init (BSON_TYPE(obj));
    break;
  case G_TYPE_VARIANT_TYPE:
    obj = g_new (VariantType, 1);
    variant_type_instance_init (VARIANT_TYPE(obj));
    break;
  case G_TYPE_LOGICAL_TYPE:
    obj = g_new (LogicalType, 1);
    logical_type_instance_init (LOGICAL_TYPE(obj));
    break;
  case G_TYPE_SCHEMA_ELEMENT:
    obj = g_new (SchemaElement, 1);
    schema_element_instance_init (SCHEMA_ELEMENT(obj));
    break;
  case G_TYPE_DATA_PAGE_HEADER:
    obj = g_new (DataPageHeader, 1);
    data_page_header_instance_init (DATA_PAGE_HEADER(obj));
    break;
  case G_TYPE_INDEX_PAGE_HEADER:
    obj = g_new (IndexPageHeader, 1);
    index_page_header_instance_init (INDEX_PAGE_HEADER(obj));
    break;
  case G_TYPE_DICTIONARY_PAGE_HEADER:
    obj = g_new (DictionaryPageHeader, 1);
    dictionary_page_header_instance_init (DICTIONARY_PAGE_HEADER(obj));
    break;
  case G_TYPE_DATA_PAGE_HEADER_V2:
    obj = g_new (DataPageHeaderV2, 1);
    data_page_header_v2_instance_init (DATA_PAGE_HEADER_V2(obj));
    break;
  case G_TYPE_SPLIT_BLOCK_ALGORITHM:
    obj = g_new (SplitBlockAlgorithm, 1);
    split_block_algorithm_instance_init (SPLIT_BLOCK_ALGORITHM(obj));
    break;
  case G_TYPE_BLOOM_FILTER_ALGORITHM:
    obj = g_new (BloomFilterAlgorithm, 1);
    bloom_filter_algorithm_instance_init (BLOOM_FILTER_ALGORITHM(obj));
    break;
  case G_TYPE_XX_HASH:
    obj = g_new (XxHash, 1);
    xx_hash_instance_init (XX_HASH(obj));
    break;
  case G_TYPE_BLOOM_FILTER_HASH:
    obj = g_new (BloomFilterHash, 1);
    bloom_filter_hash_instance_init (BLOOM_FILTER_HASH(obj));
    break;
  case G_TYPE_UNCOMPRESSED:
    obj = g_new (Uncompressed, 1);
    uncompressed_instance_init (UNCOMPRESSED(obj));
    break;
  case G_TYPE_BLOOM_FILTER_COMPRESSION:
    obj = g_new (BloomFilterCompression, 1);
    bloom_filter_compression_instance_init (BLOOM_FILTER_COMPRESSION(obj));
    break;
  case G_TYPE_BLOOM_FILTER_HEADER:
    obj = g_new (BloomFilterHeader, 1);
    bloom_filter_header_instance_init (BLOOM_FILTER_HEADER(obj));
    break;
  case G_TYPE_PAGE_HEADER:
    obj = g_new (PageHeader, 1);
    page_header_instance_init (PAGE_HEADER(obj));
    break;
  case G_TYPE_KEY_VALUE:
    obj = g_new (KeyValue, 1);
    key_value_instance_init (KEY_VALUE(obj));
    break;
  case G_TYPE_SORTING_COLUMN:
    obj = g_new (SortingColumn, 1);
    sorting_column_instance_init (SORTING_COLUMN(obj));
    break;
  case G_TYPE_PAGE_ENCODING_STATS:
    obj = g_new (PageEncodingStats, 1);
    page_encoding_stats_instance_init (PAGE_ENCODING_STATS(obj));
    break;
  case G_TYPE_COLUMN_META_DATA:
    obj = column_meta_data_pool_pop();
    column_meta_data_instance_init (COLUMN_META_DATA(obj));
    break;
  case G_TYPE_ENCRYPTION_WITH_FOOTER_KEY:
    obj = g_new (EncryptionWithFooterKey, 1);
    encryption_with_footer_key_instance_init (ENCRYPTION_WITH_FOOTER_KEY(obj));
    break;
  case G_TYPE_ENCRYPTION_WITH_COLUMN_KEY:
    obj = g_new (EncryptionWithColumnKey, 1);
    encryption_with_column_key_instance_init (ENCRYPTION_WITH_COLUMN_KEY(obj));
    break;
  case G_TYPE_COLUMN_CRYPTO_META_DATA:
    obj = g_new (ColumnCryptoMetaData, 1);
    column_crypto_meta_data_instance_init (COLUMN_CRYPTO_META_DATA(obj));
    break;
  case G_TYPE_COLUMN_CHUNK:
    obj = column_chunk_pool_pop();
    column_chunk_instance_init (COLUMN_CHUNK(obj));
    break;
  case G_TYPE_ROW_GROUP:
    obj = g_new (RowGroup, 1);
    row_group_instance_init (ROW_GROUP(obj));
    break;
  case G_TYPE_TYPE_DEFINED_ORDER:
    obj = g_new (TypeDefinedOrder, 1);
    type_defined_order_instance_init (TYPE_DEFINED_ORDER(obj));
    break;
  case G_TYPE_COLUMN_ORDER:
    obj = g_new (ColumnOrder, 1);
    column_order_instance_init (COLUMN_ORDER(obj));
    break;
  case G_TYPE_PAGE_LOCATION:
    obj = g_new (PageLocation, 1);
    page_location_instance_init (PAGE_LOCATION(obj));
    break;
  case G_TYPE_OFFSET_INDEX:
    obj = g_new (OffsetIndex, 1);
    offset_index_instance_init (OFFSET_INDEX(obj));
    break;
  case G_TYPE_COLUMN_INDEX:
    obj = g_new (ColumnIndex, 1);
    column_index_instance_init (COLUMN_INDEX(obj));
    break;
  case G_TYPE_AES_GCM_V1:
    obj = g_new (AesGcmV1, 1);
    aes_gcm_v1_instance_init (AES_GCM_V1(obj));
    break;
  case G_TYPE_AES_GCM_CTR_V1:
    obj = g_new (AesGcmCtrV1, 1);
    aes_gcm_ctr_v1_instance_init (AES_GCM_CTR_V1(obj));
    break;
  case G_TYPE_ENCRYPTION_ALGORITHM:
    obj = g_new (EncryptionAlgorithm, 1);
    encryption_algorithm_instance_init (ENCRYPTION_ALGORITHM(obj));
    break;
  case G_TYPE_FILE_META_DATA:
    obj = g_new (FileMetaData, 1);
    file_meta_data_instance_init (FILE_META_DATA(obj));
    break;
  case G_TYPE_FILE_CRYPTO_META_DATA:
    obj = g_new (FileCryptoMetaData, 1);
    file_crypto_meta_data_instance_init (FILE_CRYPTO_META_DATA(obj));
    break;

  /* Thrift */
  case G_TYPE_THRIFT_TRANSPORT:
    obj = g_new (ThriftTransport, 1);
    thrift_transport_init (THRIFT_TRANSPORT(obj));
    break;
  case G_TYPE_THRIFT_FILE_TRANSPORT:
    obj = g_new (ThriftFileTransport, 1);
    thrift_file_transport_init (THRIFT_FILE_TRANSPORT(obj));
    break;
  case G_TYPE_THRIFT_PROTOCOL:
    obj = g_new (ThriftProtocol, 1);
    thrift_protocol_init (THRIFT_PROTOCOL(obj));
    break;
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    obj = g_new (ThriftCompactProtocol, 1);
    thrift_compact_protocol_init (THRIFT_COMPACT_PROTOCOL(obj));
    break;

  /* Parquet */
  case G_TYPE_PARQUET_PLAIN_ENCODER:
    obj = g_new (ParquetPlainEncoder, 1);
    parquet_plain_encoder_instance_init (PARQUET_PLAIN_ENCODER(obj));
    break;
  case G_TYPE_PARQUET_RLE_BP_ENCODER:
    obj = g_new (ParquetRleBpEncoder, 1);
    parquet_rle_bp_encoder_instance_init (PARQUET_RLE_BP_ENCODER(obj));
    break;
  case G_TYPE_COLUMN_READER:
    obj = g_new (ColumnReader, 1);
    column_reader_instance_init (COLUMN_READER(obj));
    break;
  case G_TYPE_COLUMN_WRITER:
    obj = g_new (ColumnWriter, 1);
    column_writer_instance_init (COLUMN_WRITER(obj));
    break;
  case G_TYPE_PARQUET_READER:
    obj = g_new (ParquetReader, 1);
    parquet_reader_instance_init (PARQUET_READER(obj));
    break;
  case G_TYPE_PARQUET_WRITER:
    obj = g_new (ParquetWriter, 1);
    parquet_writer_instance_init (PARQUET_WRITER(obj));
    break;

  default:
    g_assert (FALSE, "not implented gobject type");
  }

  G_OBJECT (obj)->ref_count = 1;
  G_TYPE_FROM_INSTANCE (obj) = object_type;

  return obj;
}

void
g_object_unref (gpointer object)
{
  GObject *gobj;
  GType obejct_type;
  gboolean should_free = TRUE;

  if (object == NULL)
    return;

  gobj = G_OBJECT (object);
  gobj->ref_count--;
  if (gobj->ref_count > 0)
    return;

  obejct_type = G_TYPE_FROM_INSTANCE (object);
  switch (obejct_type)
  {
  /* Parquet */
  case G_TYPE_SIZE_STATISTICS:
    size_statistics_finalize (object);
    break;
  case G_TYPE_STATISTICS:
    statistics_finalize (object);
    statistics_pool_push (STATISTICS (object));
    should_free = FALSE;
    break;
  case G_TYPE_STRING_TYPE:
    string_type_finalize (object);
    break;
  case G_TYPE_U_U_I_D_TYPE:
    u_u_i_d_type_finalize (object);
    break;
  case G_TYPE_MAP_TYPE:
    map_type_finalize (object);
    break;
  case G_TYPE_LIST_TYPE:
    list_type_finalize (object);
    break;
  case G_TYPE_ENUM_TYPE:
    enum_type_finalize (object);
    break;
  case G_TYPE_DATE_TYPE:
    date_type_finalize (object);
    break;
  case G_TYPE_FLOAT16_TYPE:
    float16_type_finalize (object);
    break;
  case G_TYPE_NULL_TYPE:
    null_type_finalize (object);
    break;
  case G_TYPE_DECIMAL_TYPE:
    decimal_type_finalize (object);
    break;
  case G_TYPE_MILLI_SECONDS:
    milli_seconds_finalize (object);
    break;
  case G_TYPE_MICRO_SECONDS:
    micro_seconds_finalize (object);
    break;
  case G_TYPE_NANO_SECONDS:
    nano_seconds_finalize (object);
    break;
  case G_TYPE_TIME_UNIT:
    time_unit_finalize (object);
    break;
  case G_TYPE_TIMESTAMP_TYPE:
    timestamp_type_finalize (object);
    break;
  case G_TYPE_TIME_TYPE:
    time_type_finalize (object);
    break;
  case G_TYPE_INT_TYPE:
    int_type_finalize (object);
    break;
  case G_TYPE_JSON_TYPE:
    json_type_finalize (object);
    break;
  case G_TYPE_BSON_TYPE:
    bson_type_finalize (object);
    break;
  case G_TYPE_VARIANT_TYPE:
    variant_type_finalize (object);
    break;
  case G_TYPE_LOGICAL_TYPE:
    logical_type_finalize (object);
    break;
  case G_TYPE_SCHEMA_ELEMENT:
    schema_element_finalize (object);
    break;
  case G_TYPE_DATA_PAGE_HEADER:
    data_page_header_finalize (object);
    break;
  case G_TYPE_INDEX_PAGE_HEADER:
    index_page_header_finalize (object);
    break;
  case G_TYPE_DICTIONARY_PAGE_HEADER:
    dictionary_page_header_finalize (object);
    break;
  case G_TYPE_DATA_PAGE_HEADER_V2:
    data_page_header_v2_finalize (object);
    break;
  case G_TYPE_SPLIT_BLOCK_ALGORITHM:
    split_block_algorithm_finalize (object);
    break;
  case G_TYPE_BLOOM_FILTER_ALGORITHM:
    bloom_filter_algorithm_finalize (object);
    break;
  case G_TYPE_XX_HASH:
    xx_hash_finalize (object);
    break;
  case G_TYPE_BLOOM_FILTER_HASH:
    bloom_filter_hash_finalize (object);
    break;
  case G_TYPE_UNCOMPRESSED:
    uncompressed_finalize (object);
    break;
  case G_TYPE_BLOOM_FILTER_COMPRESSION:
    bloom_filter_compression_finalize (object);
    break;
  case G_TYPE_BLOOM_FILTER_HEADER:
    bloom_filter_header_finalize (object);
    break;
  case G_TYPE_PAGE_HEADER:
    page_header_finalize (object);
    break;
  case G_TYPE_KEY_VALUE:
    key_value_finalize (object);
    break;
  case G_TYPE_SORTING_COLUMN:
    sorting_column_finalize (object);
    break;
  case G_TYPE_PAGE_ENCODING_STATS:
    page_encoding_stats_finalize (object);
    break;
  case G_TYPE_COLUMN_META_DATA:
    column_meta_data_finalize (object);
    column_meta_data_pool_push (COLUMN_META_DATA (object));
    should_free = FALSE;
    break;
  case G_TYPE_ENCRYPTION_WITH_FOOTER_KEY:
    encryption_with_footer_key_finalize (object);
    break;
  case G_TYPE_ENCRYPTION_WITH_COLUMN_KEY:
    encryption_with_column_key_finalize (object);
    break;
  case G_TYPE_COLUMN_CRYPTO_META_DATA:
    column_crypto_meta_data_finalize (object);
    break;
  case G_TYPE_COLUMN_CHUNK:
    column_chunk_finalize (object);
    column_chunk_pool_push (COLUMN_CHUNK (object));
    should_free = FALSE;
    break;
  case G_TYPE_ROW_GROUP:
    row_group_finalize (object);
    break;
  case G_TYPE_TYPE_DEFINED_ORDER:
    type_defined_order_finalize (object);
    break;
  case G_TYPE_COLUMN_ORDER:
    column_order_finalize (object);
    break;
  case G_TYPE_PAGE_LOCATION:
    page_location_finalize (object);
    break;
  case G_TYPE_OFFSET_INDEX:
    offset_index_finalize (object);
    break;
  case G_TYPE_COLUMN_INDEX:
    column_index_finalize (object);
    break;
  case G_TYPE_AES_GCM_V1:
    aes_gcm_v1_finalize (object);
    break;
  case G_TYPE_AES_GCM_CTR_V1:
    aes_gcm_ctr_v1_finalize (object);
    break;
  case G_TYPE_ENCRYPTION_ALGORITHM:
    encryption_algorithm_finalize (object);
    break;
  case G_TYPE_FILE_META_DATA:
    file_meta_data_finalize (object);
    break;
  case G_TYPE_FILE_CRYPTO_META_DATA:
    file_crypto_meta_data_finalize (object);
    break;

  /* Thrift */
  case G_TYPE_THRIFT_TRANSPORT:
    thrift_transport_dispose (object);
    break;
  case G_TYPE_THRIFT_FILE_TRANSPORT:
    thrift_file_transport_finalize (object);
    break;
  case G_TYPE_THRIFT_PROTOCOL:
    thrift_protocol_dispose (object);
    break;
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    thrift_compact_protocol_dispose (object);
    break;

  /* Parquet */
  case G_TYPE_PARQUET_PLAIN_ENCODER:
    parquet_plain_encoder_finalize (object);
    break;
  case G_TYPE_PARQUET_RLE_BP_ENCODER:
    parquet_rle_bp_encoder_finalize (object);
    break;
  case G_TYPE_COLUMN_READER:
    column_reader_finalize (object);
    break;
  case G_TYPE_COLUMN_WRITER:
    column_writer_finalize (object);
    break;
  case G_TYPE_PARQUET_READER:
    parquet_reader_finalize (object);
    break;
  case G_TYPE_PARQUET_WRITER:
    parquet_writer_finalize (object);
    break;

  default:
    g_assert (FALSE, "not implented gobject type");
  }

  if (should_free)
    g_free (object);
}

gpointer
g_object_ref (gpointer object)
{
  GObject *gobj;

  if (object == NULL)
    return NULL;

  gobj = G_OBJECT (object);
  g_assert (gobj->ref_count > 0, "ref_count should not be 0");
  gobj->ref_count++;

  return object;
}


// Statistics allocator ------------------------------------------------------
struct _StatisticsPoolElem
{
  Statistics elem;
  gint32 free_next;
  gint32 pool_idx;
};
typedef struct _StatisticsPoolElem StatisticsPoolElem;

static StatisticsPoolElem statistics_pool[STATISTICS_POOL_SIZE];
static gint32 statistics_pool_free_head = -1;
static gint32 statistics_pool_free_cnt = STATISTICS_POOL_SIZE;
spinlock_t statistics_pool_lock;

static Statistics *
statistics_pool_pop (void)
{
  gint32 pool_idx;

  g_assert (statistics_pool_free_cnt > 0,
            "statistics_pool limit exceeded");

  spin_lock(&statistics_pool_lock);
  pool_idx = statistics_pool_free_head;
  statistics_pool_free_head = statistics_pool[pool_idx].free_next;
  statistics_pool[pool_idx].free_next = -1;
  statistics_pool[pool_idx].pool_idx = pool_idx;
  statistics_pool_free_cnt--;
  spin_unlock(&statistics_pool_lock);

  return &(statistics_pool[pool_idx].elem);
}

static void
statistics_pool_push (Statistics *stat)
{
  StatisticsPoolElem *elem;

  spin_lock(&statistics_pool_lock);
  elem = (StatisticsPoolElem *) stat;
  elem->free_next = statistics_pool_free_head;
  statistics_pool_free_head = elem->pool_idx;
  statistics_pool_free_cnt++;
  spin_unlock(&statistics_pool_lock);
}

// ColumnChunk allocator -----------------------------------------------------
struct _ColumnChunkPoolElem
{
  ColumnChunk elem;
  gint32 free_next;
  gint32 pool_idx;
};
typedef struct _ColumnChunkPoolElem ColumnChunkPoolElem;
static ColumnChunkPoolElem column_chunk_pool[COLUMN_CHUNK_POOL_SIZE];
static gint32 column_chunk_pool_free_head = -1;
static gint32 column_chunk_pool_free_cnt = COLUMN_CHUNK_POOL_SIZE;
spinlock_t column_chunk_pool_lock;

static ColumnChunk *
column_chunk_pool_pop (void)
{
  gint32 pool_idx;

  g_assert (column_chunk_pool_free_cnt > 0,
            "column_chunk pool limit exceeded");

  spin_lock(&column_chunk_pool_lock);
  pool_idx = column_chunk_pool_free_head;
  column_chunk_pool_free_head = column_chunk_pool[pool_idx].free_next;
  column_chunk_pool[pool_idx].free_next = -1;
  column_chunk_pool[pool_idx].pool_idx = pool_idx;
  column_chunk_pool_free_cnt--;
  spin_unlock(&column_chunk_pool_lock);

  return &(column_chunk_pool[pool_idx].elem);
}

static void
column_chunk_pool_push (ColumnChunk *column_chunk)
{
  ColumnChunkPoolElem *elem;

  spin_lock(&column_chunk_pool_lock);
  elem = (ColumnChunkPoolElem *) column_chunk;
  elem->free_next = column_chunk_pool_free_head;
  column_chunk_pool_free_head = elem->pool_idx;
  column_chunk_pool_free_cnt++;
  spin_unlock(&column_chunk_pool_lock);
}

// ColumnMetaData allocator --------------------------------------------------
struct _ColumnMetaDataPoolElem
{
  ColumnMetaData elem;
  gint32 free_next;
  gint32 pool_idx;
};
typedef struct _ColumnMetaDataPoolElem ColumnMetaDataPoolElem;
static ColumnMetaDataPoolElem column_meta_data_pool[COLUMN_META_DATA_POOL_SIZE];
static gint32 column_meta_data_pool_free_head = -1;
static gint32 column_meta_data_pool_free_cnt = COLUMN_META_DATA_POOL_SIZE;
spinlock_t column_meta_data_pool_lock;

static ColumnMetaData *
column_meta_data_pool_pop (void)
{
  gint32 pool_idx;

  g_assert (column_meta_data_pool_free_cnt > 0,
            "column_meta_data pool limit exceeded");

  spin_lock(&column_meta_data_pool_lock);
  pool_idx = column_meta_data_pool_free_head;
  column_meta_data_pool_free_head = column_meta_data_pool[pool_idx].free_next;
  column_meta_data_pool[pool_idx].free_next = -1;
  column_meta_data_pool[pool_idx].pool_idx = pool_idx;
  column_meta_data_pool_free_cnt--;
  spin_unlock(&column_meta_data_pool_lock);

  return &(column_meta_data_pool[pool_idx].elem);
}

static void
column_meta_data_pool_push (ColumnMetaData *column_meta_data)
{
  ColumnMetaDataPoolElem *elem;

  spin_lock(&column_meta_data_pool_lock);
  elem = (ColumnMetaDataPoolElem *) column_meta_data;
  elem->free_next = column_meta_data_pool_free_head;
  column_meta_data_pool_free_head = elem->pool_idx;
  column_meta_data_pool_free_cnt++;
  spin_unlock(&column_meta_data_pool_lock);
}

void check_g_object (void)
{
  spin_lock(&statistics_pool_lock);
  if (statistics_pool_free_cnt == STATISTICS_POOL_SIZE) {
    printk("STATISTICS_POOL is all freed\n");
  } else {
    printk("There is a leak in STATISTICS_POOL\n");
  }
  spin_unlock(&statistics_pool_lock);

  spin_lock(&column_chunk_pool_lock);
  if (column_chunk_pool_free_cnt == COLUMN_CHUNK_POOL_SIZE) {
    printk("COLUMN_CHUNK_POOL is all freed\n");
  } else {
    printk("There is a leak in COLUMN_CHUNK_POOL\n");
  }
  spin_unlock(&column_chunk_pool_lock);

  spin_lock(&column_meta_data_pool_lock);
  if (column_meta_data_pool_free_cnt == COLUMN_META_DATA_POOL_SIZE) {
    printk("COLUMN_META_DATA_POOL is all freed\n");
  } else {
    printk("There is a leak in COLUMN_META_DATA_POOL\n");
  }
  spin_unlock(&column_meta_data_pool_lock);
}

void g_object_init (void)
{
  printk("Size of statistics pool: %lu\n", sizeof(statistics_pool));
  printk("Size of column_chunk pool: %lu\n", sizeof(column_chunk_pool));
  printk("Size of column_meta_data pool: %lu\n", sizeof(column_meta_data_pool));

  spin_lock_init(&statistics_pool_lock);
  spin_lock_init(&column_chunk_pool_lock);
  spin_lock_init(&column_meta_data_pool_lock);

  statistics_pool_free_head = -1;
  statistics_pool_free_cnt = STATISTICS_POOL_SIZE;

  if (statistics_pool_free_head == -1 &&
      statistics_pool_free_cnt == STATISTICS_POOL_SIZE)
  {
    for (gint i = 0; i < STATISTICS_POOL_SIZE; i++)
      statistics_pool[i].free_next = i + 1;
    statistics_pool[STATISTICS_POOL_SIZE - 1].free_next = -1;
    statistics_pool_free_head = 0;
  }

  column_chunk_pool_free_head = -1;
  column_chunk_pool_free_cnt = COLUMN_CHUNK_POOL_SIZE;

  if (column_chunk_pool_free_head == -1 &&
      statistics_pool_free_cnt == STATISTICS_POOL_SIZE)
  {
    for (gint i = 0; i < COLUMN_CHUNK_POOL_SIZE; i++)
      column_chunk_pool[i].free_next = i + 1;
    column_chunk_pool[COLUMN_CHUNK_POOL_SIZE - 1].free_next = -1;
    column_chunk_pool_free_head = 0;
  }

  column_meta_data_pool_free_head = -1;
  column_meta_data_pool_free_cnt = COLUMN_META_DATA_POOL_SIZE;
  if (column_meta_data_pool_free_head == -1 &&
      statistics_pool_free_cnt == STATISTICS_POOL_SIZE)
  {
    for (gint i = 0; i < COLUMN_META_DATA_POOL_SIZE; i++)
      column_meta_data_pool[i].free_next = i + 1;
    column_meta_data_pool[COLUMN_META_DATA_POOL_SIZE - 1].free_next = -1;
    column_meta_data_pool_free_head = 0;
  }
}