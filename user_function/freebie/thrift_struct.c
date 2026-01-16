#include "thrift_struct.h"
#include "gobject-type.h"
#include "parquet_types.h"

gint32
thrift_struct_read (ThriftStruct *object, ThriftProtocol *protocol,
                    GError **error)
{
  GType object_type = G_TYPE_FROM_INSTANCE (object);
  gint32 result = 0;

  switch (object_type) {
  case G_TYPE_SIZE_STATISTICS:
    result = size_statistics_read (object, protocol, error);
    break;
  case G_TYPE_STATISTICS:
    result = statistics_read (object, protocol, error);
    break;
  case G_TYPE_STRING_TYPE:
    result = string_type_read (object, protocol, error);
    break;
  case G_TYPE_U_U_I_D_TYPE:
    result = u_u_i_d_type_read (object, protocol, error);
    break;
  case G_TYPE_MAP_TYPE:
    result = map_type_read (object, protocol, error);
    break;
  case G_TYPE_LIST_TYPE:
    result = list_type_read (object, protocol, error);
    break;
  case G_TYPE_ENUM_TYPE:
    result = enum_type_read (object, protocol, error);
    break;
  case G_TYPE_DATE_TYPE:
    result = date_type_read (object, protocol, error);
    break;
  case G_TYPE_FLOAT16_TYPE:
    result = float16_type_read (object, protocol, error);
    break;
  case G_TYPE_NULL_TYPE:
    result = null_type_read (object, protocol, error);
    break;
  case G_TYPE_DECIMAL_TYPE:
    result = decimal_type_read (object, protocol, error);
    break;
  case G_TYPE_MILLI_SECONDS:
    result = milli_seconds_read (object, protocol, error);
    break;
  case G_TYPE_MICRO_SECONDS:
    result = micro_seconds_read (object, protocol, error);
    break;
  case G_TYPE_NANO_SECONDS:
    result = nano_seconds_read (object, protocol, error);
    break;
  case G_TYPE_TIME_UNIT:
    result = time_unit_read (object, protocol, error);
    break;
  case G_TYPE_TIMESTAMP_TYPE:
    result = timestamp_type_read (object, protocol, error);
    break;
  case G_TYPE_TIME_TYPE:
    result = time_type_read (object, protocol, error);
    break;
  case G_TYPE_INT_TYPE:
    result = int_type_read (object, protocol, error);
    break;
  case G_TYPE_JSON_TYPE:
    result = json_type_read (object, protocol, error);
    break;
  case G_TYPE_BSON_TYPE:
    result = bson_type_read (object, protocol, error);
    break;
  case G_TYPE_VARIANT_TYPE:
    result = variant_type_read (object, protocol, error);
    break;
  case G_TYPE_LOGICAL_TYPE:
    result = logical_type_read (object, protocol, error);
    break;
  case G_TYPE_SCHEMA_ELEMENT:
    result = schema_element_read (object, protocol, error);
    break;
  case G_TYPE_DATA_PAGE_HEADER:
    result = data_page_header_read (object, protocol, error);
    break;
  case G_TYPE_INDEX_PAGE_HEADER:
    result = index_page_header_read (object, protocol, error);
    break;
  case G_TYPE_DICTIONARY_PAGE_HEADER:
    result = dictionary_page_header_read (object, protocol, error);
    break;
  case G_TYPE_DATA_PAGE_HEADER_V2:
    result = data_page_header_v2_read (object, protocol, error);
    break;
  case G_TYPE_SPLIT_BLOCK_ALGORITHM:
    result = split_block_algorithm_read (object, protocol, error);
    break;
  case G_TYPE_BLOOM_FILTER_ALGORITHM:
    result = bloom_filter_algorithm_read (object, protocol, error);
    break;
  case G_TYPE_XX_HASH:
    result = xx_hash_read (object, protocol, error);
    break;
  case G_TYPE_BLOOM_FILTER_HASH:
    result = bloom_filter_hash_read (object, protocol, error);
    break;
  case G_TYPE_UNCOMPRESSED:
    result = uncompressed_read (object, protocol, error);
    break;
  case G_TYPE_BLOOM_FILTER_COMPRESSION:
    result = bloom_filter_compression_read (object, protocol, error);
    break;
  case G_TYPE_BLOOM_FILTER_HEADER:
    result = bloom_filter_header_read (object, protocol, error);
    break;
  case G_TYPE_PAGE_HEADER:
    result = page_header_read (object, protocol, error);
    break;
  case G_TYPE_KEY_VALUE:
    result = key_value_read (object, protocol, error);
    break;
  case G_TYPE_SORTING_COLUMN:
    result = sorting_column_read (object, protocol, error);
    break;
  case G_TYPE_PAGE_ENCODING_STATS:
    result = page_encoding_stats_read (object, protocol, error);
    break;
  case G_TYPE_COLUMN_META_DATA:
    result = column_meta_data_read (object, protocol, error);
    break;
  case G_TYPE_ENCRYPTION_WITH_FOOTER_KEY:
    result = encryption_with_footer_key_read (object, protocol, error);
    break;
  case G_TYPE_ENCRYPTION_WITH_COLUMN_KEY:
    result = encryption_with_column_key_read (object, protocol, error);
    break;
  case G_TYPE_COLUMN_CRYPTO_META_DATA:
    result = column_crypto_meta_data_read (object, protocol, error);
    break;
  case G_TYPE_COLUMN_CHUNK:
    result = column_chunk_read (object, protocol, error);
    break;
  case G_TYPE_ROW_GROUP:
    result = row_group_read (object, protocol, error);
    break;
  case G_TYPE_TYPE_DEFINED_ORDER:
    result = type_defined_order_read (object, protocol, error);
    break;
  case G_TYPE_COLUMN_ORDER:
    result = column_order_read (object, protocol, error);
    break;
  case G_TYPE_PAGE_LOCATION:
    result = page_location_read (object, protocol, error);
    break;
  case G_TYPE_OFFSET_INDEX:
    result = offset_index_read (object, protocol, error);
    break;
  case G_TYPE_COLUMN_INDEX:
    result = column_index_read (object, protocol, error);
    break;
  case G_TYPE_AES_GCM_V1:
    result = aes_gcm_v1_read (object, protocol, error);
    break;
  case G_TYPE_AES_GCM_CTR_V1:
    result = aes_gcm_ctr_v1_read (object, protocol, error);
    break;
  case G_TYPE_ENCRYPTION_ALGORITHM:
    result = encryption_algorithm_read (object, protocol, error);
    break;
  case G_TYPE_FILE_META_DATA:
    result = file_meta_data_read (object, protocol, error);
    break;
  case G_TYPE_FILE_CRYPTO_META_DATA:
    result = file_crypto_meta_data_read (object, protocol, error);
    break;

  default:
    result = -1;
  }

  return result;
}

gint32 thrift_struct_write (ThriftStruct *object, ThriftProtocol *protocol,
                            GError **error)
{
  GType object_type = G_TYPE_FROM_INSTANCE (object);
  gint32 result = 0;

  switch (object_type) {
  case G_TYPE_SIZE_STATISTICS:
    result = size_statistics_write (object, protocol, error);
    break;
  case G_TYPE_STATISTICS:
    result = statistics_write (object, protocol, error);
    break;
  case G_TYPE_STRING_TYPE:
    result = string_type_write (object, protocol, error);
    break;
  case G_TYPE_U_U_I_D_TYPE:
    result = u_u_i_d_type_write (object, protocol, error);
    break;
  case G_TYPE_MAP_TYPE:
    result = map_type_write (object, protocol, error);
    break;
  case G_TYPE_LIST_TYPE:
    result = list_type_write (object, protocol, error);
    break;
  case G_TYPE_ENUM_TYPE:
    result = enum_type_write (object, protocol, error);
    break;
  case G_TYPE_DATE_TYPE:
    result = date_type_write (object, protocol, error);
    break;
  case G_TYPE_FLOAT16_TYPE:
    result = float16_type_write (object, protocol, error);
    break;
  case G_TYPE_NULL_TYPE:
    result = null_type_write (object, protocol, error);
    break;
  case G_TYPE_DECIMAL_TYPE:
    result = decimal_type_write (object, protocol, error);
    break;
  case G_TYPE_MILLI_SECONDS:
    result = milli_seconds_write (object, protocol, error);
    break;
  case G_TYPE_MICRO_SECONDS:
    result = micro_seconds_write (object, protocol, error);
    break;
  case G_TYPE_NANO_SECONDS:
    result = nano_seconds_write (object, protocol, error);
    break;
  case G_TYPE_TIME_UNIT:
    result = time_unit_write (object, protocol, error);
    break;
  case G_TYPE_TIMESTAMP_TYPE:
    result = timestamp_type_write (object, protocol, error);
    break;
  case G_TYPE_TIME_TYPE:
    result = time_type_write (object, protocol, error);
    break;
  case G_TYPE_INT_TYPE:
    result = int_type_write (object, protocol, error);
    break;
  case G_TYPE_JSON_TYPE:
    result = json_type_write (object, protocol, error);
    break;
  case G_TYPE_BSON_TYPE:
    result = bson_type_write (object, protocol, error);
    break;
  case G_TYPE_VARIANT_TYPE:
    result = variant_type_write (object, protocol, error);
    break;
  case G_TYPE_LOGICAL_TYPE:
    result = logical_type_write (object, protocol, error);
    break;
  case G_TYPE_SCHEMA_ELEMENT:
    result = schema_element_write (object, protocol, error);
    break;
  case G_TYPE_DATA_PAGE_HEADER:
    result = data_page_header_write (object, protocol, error);
    break;
  case G_TYPE_INDEX_PAGE_HEADER:
    result = index_page_header_write (object, protocol, error);
    break;
  case G_TYPE_DICTIONARY_PAGE_HEADER:
    result = dictionary_page_header_write (object, protocol, error);
    break;
  case G_TYPE_DATA_PAGE_HEADER_V2:
    result = data_page_header_v2_write (object, protocol, error);
    break;
  case G_TYPE_SPLIT_BLOCK_ALGORITHM:
    result = split_block_algorithm_write (object, protocol, error);
    break;
  case G_TYPE_BLOOM_FILTER_ALGORITHM:
    result = bloom_filter_algorithm_write (object, protocol, error);
    break;
  case G_TYPE_XX_HASH:
    result = xx_hash_write (object, protocol, error);
    break;
  case G_TYPE_BLOOM_FILTER_HASH:
    result = bloom_filter_hash_write (object, protocol, error);
    break;
  case G_TYPE_UNCOMPRESSED:
    result = uncompressed_write (object, protocol, error);
    break;
  case G_TYPE_BLOOM_FILTER_COMPRESSION:
    result = bloom_filter_compression_write (object, protocol, error);
    break;
  case G_TYPE_BLOOM_FILTER_HEADER:
    result = bloom_filter_header_write (object, protocol, error);
    break;
  case G_TYPE_PAGE_HEADER:
    result = page_header_write (object, protocol, error);
    break;
  case G_TYPE_KEY_VALUE:
    result = key_value_write (object, protocol, error);
    break;
  case G_TYPE_SORTING_COLUMN:
    result = sorting_column_write (object, protocol, error);
    break;
  case G_TYPE_PAGE_ENCODING_STATS:
    result = page_encoding_stats_write (object, protocol, error);
    break;
  case G_TYPE_COLUMN_META_DATA:
    result = column_meta_data_write (object, protocol, error);
    break;
  case G_TYPE_ENCRYPTION_WITH_FOOTER_KEY:
    result = encryption_with_footer_key_write (object, protocol, error);
    break;
  case G_TYPE_ENCRYPTION_WITH_COLUMN_KEY:
    result = encryption_with_column_key_write (object, protocol, error);
    break;
  case G_TYPE_COLUMN_CRYPTO_META_DATA:
    result = column_crypto_meta_data_write (object, protocol, error);
    break;
  case G_TYPE_COLUMN_CHUNK:
    result = column_chunk_write (object, protocol, error);
    break;
  case G_TYPE_ROW_GROUP:
    result = row_group_write (object, protocol, error);
    break;
  case G_TYPE_TYPE_DEFINED_ORDER:
    result = type_defined_order_write (object, protocol, error);
    break;
  case G_TYPE_COLUMN_ORDER:
    result = column_order_write (object, protocol, error);
    break;
  case G_TYPE_PAGE_LOCATION:
    result = page_location_write (object, protocol, error);
    break;
  case G_TYPE_OFFSET_INDEX:
    result = offset_index_write (object, protocol, error);
    break;
  case G_TYPE_COLUMN_INDEX:
    result = column_index_write (object, protocol, error);
    break;
  case G_TYPE_AES_GCM_V1:
    result = aes_gcm_v1_write (object, protocol, error);
    break;
  case G_TYPE_AES_GCM_CTR_V1:
    result = aes_gcm_ctr_v1_write (object, protocol, error);
    break;
  case G_TYPE_ENCRYPTION_ALGORITHM:
    result = encryption_algorithm_write (object, protocol, error);
    break;
  case G_TYPE_FILE_META_DATA:
    result = file_meta_data_write (object, protocol, error);
    break;
  case G_TYPE_FILE_CRYPTO_META_DATA:
    result = file_crypto_meta_data_write (object, protocol, error);
    break;

  default:
    result = -1;
  }

  return result;
}
