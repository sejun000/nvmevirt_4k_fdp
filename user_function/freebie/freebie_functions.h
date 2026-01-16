#ifndef FREEBIE_FUNCTIONS_H
#define FREEBIE_FUNCTIONS_H

#define _FREEBIE_DEBUG_ (1)

#if (_FREEBIE_DEBUG_ == 1)
// #define NVMEV_FREEBIE_DEBUG(string, args...) printk("[FREEBIE %u] - %s : " smp_processor_id(), string, __func__, ##args)
#define NVMEV_FREEBIE_DEBUG(fmt, ...)                                    \
        printk("[FREEBIE][CPU%u] %s: " fmt,                               \
               smp_processor_id(), __func__, ##__VA_ARGS__)
#else
#define NVMEV_FREEBIE_DEBUG(string, args...)
#endif


#define FREEBIE_MAX_TABLE_COUNT (5)

#define FREEBIE_GC_THRESHOLD (0)

#define FREEBIE_NO_CQ_ENTRY(idx)				\
	((idx) == COPY_TO_SLM_PROGRAM_INDEX) || 	\
	((idx) == COPY_FROM_SLM_PROGRAM_INDEX) ||	\
	((idx) == FREEBIE_GARBAGE_COLLECT_INDEX) || \
	((idx) == FREEBIE_REPART_INDEX)

#define FREEBIE_MAX_PARTITION_MAP_SIZE 		(16 * 1024 * 1024)
#define FREEBIE_PARTITION_MAP_SIZE 			(9 * 1024 * 1024)
#define FREEBIE_PARTITION_MAP_FILE_SIZE 	(64 * 1024 * 1024)
#define FREEBIE_DATA_CHUNK_SIZE 			(4096)
#define FREEBIE_ROOT_ENTRY_SIZE 			(sizeof(uint32_t) / 2)
#define FREEBIE_MAX_VALID_ROOT_INDEX		(FREEBIE_PARTITION_MAP_SIZE / FREEBIE_DATA_CHUNK_SIZE)
#define FREEBIE_MAX_ROOT_INDEX				(FREEBIE_MAX_PARTITION_MAP_SIZE / FREEBIE_DATA_CHUNK_SIZE)
#define FREEBIE_ROOT_CHUNK_SIZE 			(FREEBIE_MAX_ROOT_INDEX * FREEBIE_ROOT_ENTRY_SIZE)

// For get map command
#define FREEBIE_MDTS_SIZE					(256 * 1024)
#define FREEBIE_ROOT_ENTRY_COUNT_PER_MDTS	(FREEBIE_MDTS_SIZE / FREEBIE_DATA_CHUNK_SIZE)

// Maximum ROOT Index (includding the padding)
#define FREEBIE_ROOT_CHUNK_COUNT 			((FREEBIE_PARTITION_MAP_FILE_SIZE / 2) / FREEBIE_ROOT_CHUNK_SIZE)
#define FREEBIE_DATA_CHUNK_COUNT 			((FREEBIE_PARTITION_MAP_FILE_SIZE / 2) / FREEBIE_DATA_CHUNK_SIZE)

enum {
	FREEBIE_ROOT_FIRST_SLOT = 0,
	FREEBIE_ROOT_SECOND_SLOT = 1,
	FREEBIE_ROOT_SLOT_COUNT,
};

enum {
	FREEBIE_OPERATION_WRITE = 0,
	FREEBIE_OPERATION_ADD = 1,
};

enum {
	ROOT_FREE = -1,
	ROOT_ALLOCATED = 0,
	ROOT_IN_USE = 1,
};

// Data structure for Internal metadata mapping
typedef struct __freebie_internal_map {
	bool is_valid;									// Is this data structure valid?
	uint32_t relation_id;							// The relation id of the corresponding table
	size_t root_buffer_addr;						// The SLM address of the root buffer
	uint8_t root_id[FREEBIE_ROOT_SLOT_COUNT]; 		// ID of the root saved in SLM each buffer
	int map_version[FREEBIE_ROOT_SLOT_COUNT];		// version of the root

#ifdef KERNEL_MODE 
	atomic_t root_reader_count[FREEBIE_ROOT_SLOT_COUNT]; 			// Reader count for each root buffer

	atomic_t valid_root_buffer;						// 0 ~ FREEBIE_ROOT_SLOT_COUNT - 1
	atomic_t root_rc[FREEBIE_ROOT_CHUNK_COUNT]; 	// Reference count of the root chunk
	atomic_t data_rc[FREEBIE_DATA_CHUNK_COUNT]; 	// Reference count of the data chunk

	atomic_t gc_count;								// Repartition count		

	atomic_t partition_map_version;

	// Lock to avoid multiple repartitioning metadata update
	spinlock_t repartition_lock;
#endif

	struct slm_lba_info *slm_lba_info;				// SLM LBA info
	struct ccsd_task_info *gc_task;					// Garbage collection task	
} freebie_internal_map;

typedef struct __freebie_source_range_entry {
	uint32_t offset; 		// File offset
	// uint32_t size; 		// Size of the source range (it's always 4 bytes, deprecated)
	uint32_t value;	 		// Value to be written/added to the source
	uint8_t operation;		// Operation type (0: Write, 1: Add)
	bool processed;			// Is this entry processed?
} freebie_source_range_entry;

size_t freebie_setup(struct ccsd_task_info *task, uint32_t relation_id);
size_t freebie_release_root(uint32_t relation_id, uint32_t root_id);
size_t freebie_get_root_buffer(uint32_t relation_id, uint8_t *root_id);
void freebie_add_sre(freebie_source_range_entry *sre, uint32_t *nentry,
						uint32_t offset, uint32_t value, uint8_t operation);
struct slm_lba_info *freebie_get_slm_lba_info(uint32_t relation_id);
bool freebie_check_root_setup(uint32_t relation_id);
void freebie_update_metadata(struct ccsd_task_info * task, uint32_t relation_id,
								freebie_source_range_entry *sre, uint32_t nentry);
bool freebie_garbage_collection(struct ccsd_task_info *task, uint32_t relation_id);
size_t freebie_terminate(uint32_t relation_id);
void init_freebie_map(void);
void check_freebie_map(void);

uint32_t freebie_start_read_partition_map(uint32_t relation_id, uint32_t root_index);
uint32_t freebie_end_read_partition_map(uint32_t relation_id, uint32_t valid_root_buffer);
size_t freebie_get_lba_of_map_block(uint32_t relation_id, uint32_t valid_root_buffer, uint32_t root_index);

#endif
