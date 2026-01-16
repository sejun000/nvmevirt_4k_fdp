#ifndef _CSD_USER_FUNC_H
#define _CSD_USER_FUNC_H

#undef CONFIG_CSD_FUNC_VERBOSE

#ifdef CONFIG_CSD_FUNC_VERBOSE
#define CSD_DEBUG(string, args...) printk(KERN_INFO string, ##args)
#else
#define CSD_DEBUG(string, args...)
#endif

#define ALIGNED_UP(value, align) (((value) + (align)-1) & ~((align)-1))

enum CSD_PROGRAM_INDEX {
	COPY_TO_SLM_PROGRAM_INDEX = 0,
	COPY_FROM_SLM_PROGRAM_INDEX,

	STREAM_TYPE_PROGRAM_INDEX_START = 2,
	KEY_FINDER_PROGRAM_INDEX, // 2
	KNN_PROGRAM_INDEX, // 3
	GREP_PROGRAM_INDEX, // 4
	TPCH_FILTER_INDEX, // 5
	DECOMPRESSION_INDEX, // 6
	TPCH_FILTER2_INDEX,
	STATIC_OUTPUT_TYPE_PROGRAM_INDEX_START,
	STAT32_PROGRAM_INDEX, // 9
	TPCH_STAT_PROGRAM_INDEX, // 10
	FILTERED_STAT32_PROGRAM_INDEX, // 11
	STATIC_OUTPUT_TYPE_PROGRAM_INDEX_END,
	STREAM_TYPE_PROGRAM_INDEX_END,

	RANDOM_ACCESS_TYPE_PROGRAM_INDEX_START = 0x100,
	BTREE_PROGRAM_INDEX, // 257
	BTREE_PROGRAM_INDEX_TEMP, // 258
	QUICK_SORT_INDEX, // 6
	RANDOM_ACCESS_TYPE_PROGRAM_INDEX_END,

	STORE_TYPE_PROGRAM_INDEX_START = 0x200,
	COMPRESSION_INDEX, // 513
	STORE_TYPE_PROGRAM_INDEX_END,

	MULTI_STREAM_TYPE_PROGRAM_INDEX_START = 0x300,
	COMPACTION_PROGRAM_INDEX,
	LEVELDB_COMPACTION_PROGRAM_INDEX,
	LEVELDB_CRC_PROGRAM_INDEX,
	ROCKSDB_COMPACTION_PROGRAM_INDEX,
	ROCKSDB_CRC_PROGRAM_INDEX,
	HYBRID_ENCODING_PROGRAM_INDEX,
	HYBRID_DECODING_PROGRAM_INDEX,
	MULTI_STREAM_TYPE_PROGRAM_INDEX_END,

	FREEBIE_REPART_INDEX = 0x500,
	FREEBIE_SETUP_INDEX,
	FREEBIE_RELEASE_CAT_INDEX,
	FREEBIE_TERMINATE_INDEX,
	FREEBIE_GARBAGE_COLLECT_INDEX,
    FREEBIE_DEBUG_INDEX,

	eBPF_PROGRAM_INDEX = 0x10000,
};

#define IS_STREAM_TYPE_PROGRAM_INDEX(idx) \
	(((idx) > STREAM_TYPE_PROGRAM_INDEX_START) && ((idx) < STREAM_TYPE_PROGRAM_INDEX_END))
#define IS_STATIC_OUTPUT_TYPE_PROGRAM_INDEX(idx) \
	(((idx) > STATIC_OUTPUT_TYPE_PROGRAM_INDEX_START) && ((idx) < STATIC_OUTPUT_TYPE_PROGRAM_INDEX_END))
#define IS_RANDOM_ACCESS_PROGRAM_INDEX(idx) \
	(((idx) > RANDOM_ACCESS_TYPE_PROGRAM_INDEX_START) && ((idx) < RANDOM_ACCESS_TYPE_PROGRAM_INDEX_END))
#define IS_STORE_PROGRAM_INDEX(idx) (((idx) > STORE_TYPE_PROGRAM_INDEX_START) && ((idx) < STORE_TYPE_PROGRAM_INDEX_END))
#define IS_MULTI_STREAM_PROGRAM_INDEX(idx) \
	(((idx) > MULTI_STREAM_TYPE_PROGRAM_INDEX_START) && ((idx) < MULTI_STREAM_TYPE_PROGRAM_INDEX_END))

// Parameter
struct ebpf_context {
	void *buf_o;
	void *buf_i;
	size_t size_i;
};

struct key_finder_params {
	unsigned int field;
	unsigned int value;
};

struct key_finder2_params {
	unsigned int field1;
	unsigned int value1;
	unsigned int field2;
	unsigned int value2;
};

struct tpch_filter_params {
	unsigned int value1;
	unsigned int value2;
};

struct knn_params {
	unsigned int row_length;
	unsigned int item_size;
};

struct grep_params {
	unsigned int column_size;
	unsigned int str_len;
	unsigned char str[16];
};

struct btree_params {
	size_t key;
};

struct compression_params {
	int compress_type;
};

struct decompression_params {
	int decompress_type;
};

struct tpch_params {
	int tpch_num;
};

struct compaction_params {
	unsigned int nInput;
	unsigned int key_size;
	unsigned int value_size;
};

struct leveldb_compaction_params {
	size_t first_level_size;
	size_t second_level_start;
	size_t second_level_size;
	size_t sstable_size;
	size_t datablock_threshold;
	bool crc_flag; // crc flag for datablock crc calculation
};

struct leveldb_crc_params {
	size_t sstable_size;
	size_t datablock_threshold;
	int num_cores;
	int core_id;
};

struct rocksdb_host_properties_param {
	uint64_t file_number;
	uint64_t creation_time;
	uint64_t file_creation_time;
	uint64_t oldest_key_time;
	char db_id[37];
	char db_session_id[21];
};

struct rocksdb_compaction_params {
	size_t first_level_size;
	size_t second_level_start;
	size_t second_level_size;
	size_t sstable_size;
	size_t datablock_threshold;
	bool crc_flag; // crc flag for datablock crc calculation
	struct rocksdb_host_properties_param host_properties;
};

struct rocksdb_crc_params {
	size_t sstable_size;
	size_t datablock_threshold;
	int num_cores;
	int core_id;
};

struct decoding_params {
	size_t original_size;
	int max_value;
};

// Magic Parameters

struct leveldb_magic_compaction_params {
	size_t first_level_start;
	size_t first_level_size;
	size_t second_level_start;
	size_t second_level_size;
	size_t output_buf;
	size_t output_second_buf;
	size_t sstable_size;
	size_t datablock_threshold;
	bool crc_flag; // crc flag for datablock crc calculation
};
struct leveldb_magic_crc_params {
	size_t sstable_size;
	size_t datablock_threshold;
	size_t input_buf;
	size_t output_buf;
	size_t output_buf_size;
	int num_cores;
	int core_id;
};
struct rocksdb_magic_compaction_params {
	size_t first_level_start;
	size_t first_level_size;
	size_t second_level_start;
	size_t second_level_size;
	size_t output_buf;
	size_t output_second_buf;
	size_t sstable_size;
	size_t datablock_threshold;
	bool crc_flag; // crc flag for datablock crc calculation
	struct rocksdb_host_properties_param host_properties;
};

struct rocksdb_magic_crc_params {
	size_t sstable_size;
	size_t datablock_threshold;
	size_t input_buf;
	size_t output_buf;
	size_t output_buf_size;
	int num_cores;
	int core_id;
};

#define FREEBIE_MAX_SPAN_OUT 	(16)
#define FREEBIE_MAX_INPUT_FILE 	(16)
struct freebie_params {
	uint32_t spread_factor;	// Number of output files
	uint32_t input_file_cnt;  // Number of input files

	size_t input_buf[FREEBIE_MAX_INPUT_FILE];	// Input file
	int input_buf_size[FREEBIE_MAX_INPUT_FILE]; 

	size_t output_buf[FREEBIE_MAX_SPAN_OUT];
	int output_buf_size[FREEBIE_MAX_SPAN_OUT];  		// Output file size before Fallocate
	int output_buf_alloc_size[FREEBIE_MAX_SPAN_OUT];  	// Output file size after Fallocate

	// FreeBie Metadata
	uint32_t last_level;
	uint32_t current_level;
	uint32_t partition_no;
	uint32_t generation_no;
	uint32_t relation_id;
	uint32_t row_id_map;

	// For last level Generation
	uint32_t last_level_generated_bitmap;
};

struct CSD_PARAMS {
	unsigned int delay;
	unsigned int finish;
	size_t compressed_size; // used for both compresssion and encoding
	union {
		struct key_finder_params key_finder_params;
		struct key_finder2_params key_finder2_params;
		struct tpch_filter_params tpch_filter_params;
		struct knn_params knn_params;
		struct grep_params grep_params;
		struct btree_params btree_params;
		struct compression_params compression_params;
		struct decompression_params decompression_params;
		struct tpch_params tpch_params;
		struct compaction_params compaction_params;
		struct leveldb_compaction_params leveldb_compaction_params;
		struct leveldb_crc_params leveldb_crc_params;
		struct rocksdb_compaction_params rocksdb_compaction_params;
		struct rocksdb_crc_params rocksdb_crc_params;
		struct decoding_params decoding_params;

		struct leveldb_magic_compaction_params leveldb_magic_compaction_params;
		struct leveldb_magic_crc_params leveldb_magic_crc_params;
		struct rocksdb_magic_compaction_params rocksdb_magic_compaction_params;
		struct rocksdb_magic_crc_params rocksdb_magic_crc_params;
	};
	struct profile_info {
		int pid;
		int host_id;
	} profile_info; // used to print real compute time for DCSD

	struct magic_info {
		size_t output_buf;
		size_t output_second_buf;
		size_t output_buf_size;
		size_t output_second_buf_size;
	} magic_info;
};
// GREP
struct key_finder_format {
	unsigned int data[8];
};

struct tpch_filter_format {
	unsigned int l_orderkey;
	unsigned int l_partkey;
	unsigned int l_suppkey;
	unsigned int l_linenumber;
	size_t l_quantity;
	size_t l_extendedprice;
	int l_shipdate;
	int l_commitdate;
	int l_receiptdate;
	unsigned char l_discount[5];
	unsigned char l_tax[5];
	unsigned char l_returnflag;
	unsigned char l_linestatus;
	unsigned char l_shipinstruct[18];
	unsigned char l_shipmode[10];
	unsigned char l_comment[44];
};

struct compaction_format {
	unsigned char key[16];
	unsigned char value[4080];
};

size_t __csdvirt_run_program(int program_idx, void *buf_in, void *buf_out, size_t size, void *param);
size_t __key_finder(void *buf_in, void *buf_out, size_t size, void *param);
size_t __magic_key_finder(void *buf_in, void *buf_out, size_t size, void *param);
size_t __tpch_filter(void *buf_in, void *buf_out, size_t size, void *param);

// Statistics
struct statistic_format {
	size_t sum;
	unsigned int max;
	unsigned int min;
};

size_t __statistic_int32(void *buf_in, void *buf_out, size_t size, void *param);
size_t __magic_statistic_int32(void *buf_in, void *buf_out, size_t size, void *param);
size_t __filtered_statistic_int32(void *buf_in, void *buf_out, size_t size, void *param);
size_t __tpch_statistic(void *buf_in, void *buf_out, size_t size, void *param);
size_t __knn(void *buf_in, void *buf_out, size_t size, void *param);
size_t __magic_knn(void *buf_in, void *buf_out, size_t size, void *param);
size_t __grep(void *buf_in, void *buf_out, size_t size, void *param);
size_t __magic_grep(void *buf_in, void *buf_out, size_t size, void *param);

// Btree
// Node-level information
#define INTERNAL 0
#define LEAF 1
#define BTREE_BLK_SIZE (4096)

typedef unsigned char val__t[64];
#define VAL_SIZE sizeof(val__t)

#define NODE_CAPACITY ((BTREE_BLK_SIZE - 2 * sizeof(unsigned long)) / (2 * sizeof(unsigned long)))
#define LOG_CAPACITY (BTREE_BLK_SIZE / VAL_SIZE)

static const size_t FANOUT = NODE_CAPACITY;
struct BtreeNode {
	unsigned long next;
	unsigned long type;
	unsigned long key[NODE_CAPACITY];
	unsigned long ptr[NODE_CAPACITY];
};

struct BtreeLog {
	val__t val[LOG_CAPACITY];
};

struct btree_output {
	size_t key;
	val__t val;
};

size_t __btree(void *buf_in, void *buf_out, size_t size, void *param);
size_t __btree_demand(void *buf_in, void *buf_out, size_t size, void *param);

size_t __compression(void *buf_in, void *buf_out, size_t size, void *param);
size_t __decompression(void *buf_in, void *buf_out, size_t size, void *param);

size_t __quickSort(void *buf_in, void *buf_out, size_t size, void *param);

size_t __compaction(void *buf_in, void *buf_out, size_t size, void *param);

#define FRONT_METADATA_SIZE (16 * 1024)
#define MAX_OUTPUT_TABLES (100)

// TODO: change name to sstable_properties
struct compaction_properties {
	uint64_t raw_key_size;
	uint64_t raw_value_size;
	uint64_t data_size;
	uint64_t index_size;
	uint64_t num_entries;
	uint64_t num_data_blocks;
	uint64_t tail_start_offset;
};

// This struct is kept here since user also use it
struct compacted_file_metadata {
	char smallest_key[24];
	char largest_key[24];
	uint32_t smallest_klen;
	uint32_t largest_klen;
	uint64_t smallest_seqno;
	uint64_t largest_seqno;
	uint64_t tail_size;
	struct compaction_properties properties;
};

struct level_compacted_file_metadata {
	uint64_t datablock_size;
	char smallest_key[24];
	char largest_key[24];
	uint32_t smallest_klen;
	uint32_t largest_klen;
};

struct compaction_stats {
	uint64_t num_input_entries;
	uint64_t raw_input_key_size;
	uint64_t raw_input_value_size;
	uint64_t num_output_entries;
};

size_t __leveldb_compaction(void *buf_in, void *buf_out, size_t size, void *param);
size_t __leveldb_magic_compaction(void *buf_in, void *buf_out, size_t size, void *param);
size_t __leveldb_crc_calculation(void *buf_in, void *buf_out, size_t size, void *param);
size_t __leveldb_magic_crc_calculation(void *buf_in, void *buf_out, size_t size, void *param);
size_t __rocksdb_compaction(void *buf_in, void *buf_out, size_t size, void *param);
size_t __rocksdb_magic_compaction(void *buf_in, void *buf_out, size_t size, void *param);
size_t __rocksdb_crc_calculation(void *buf_in, void *buf_out, size_t size, void *param);
size_t __rocksdb_magic_crc_calculation(void *buf_in, void *buf_out, size_t size, void *param);

size_t __hybrid_encoding(void *buf_in, void *buf_out, size_t size, void *param);

enum TPCHFields {
	tpch_field_orderkey,
	tpch_field_partkey,
	tpch_field_suppkey,
	tpch_field_linenumber,
	tpch_field_quantity,
	tpch_field_extendedprice,
	tpch_field_discount,
	tpch_field_tax,
	tpch_field_returnflag,
	tpch_field_linestatus,
	tpch_field_shipdate,
	tpch_field_commitdate,
	tpch_field_receiptdate,
	tpch_field_shipinstruct,
	tpch_field_shipmode,
	tpch_field_comment,
	tpch_field_count // 필드 개수
};

size_t __tpch_filter2(void *buf_in, void *buf_out, size_t size, void *param);

// Utils
void *get_data_from_ptr(size_t ptr, size_t size);
int check_data_using_ptr(size_t ptr, size_t size, int pid, int host_id);
#endif
