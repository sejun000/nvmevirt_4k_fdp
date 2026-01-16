#include <linux/kthread.h>
#include <linux/ktime.h>
#include <linux/highmem.h>
#include <linux/sched/clock.h>
#include <linux/delay.h>

#include "buddy.h"

#ifndef _NVMEVIRT_CSD_SLM_H
#define _NVMEVIRT_CSD_SLM_H

#define ALIGNED_DOWN(value, align) ((value) & ~((align)-1))
#define _SLM_DEBUG_ (0)
#define _VSLM_DEBUG_ (0)

static const unsigned int SLM_PAGE_SIZE = 16 * 1024;

typedef int(init_fn)(size_t size);
typedef size_t(allocate_fn)(size_t length, void *args);
typedef int(deallocate_fn)(size_t mem_offset);
typedef size_t(size_fn)(size_t mem_offset);
typedef void(status_fn)(void);
typedef void(kill_fn)(void);

struct allocator_ops {
	init_fn *init;
	allocate_fn *allocate;
	deallocate_fn *deallocate;
	size_fn *size;
	status_fn *status;
	kill_fn *kill;
};

struct slm_physical_resource {
	uint64_t start_addr;
	struct allocator_ops allocator;
};

size_t alloc_slm_range(size_t size);
void free_slm_range(size_t addr);

void copy_to_slm(size_t dest, void *src, size_t size);
void copy_from_slm(void *dest, size_t src, size_t size);

void init_slm_memory(void *start, size_t slm_size);
void final_slm_memory(void);

size_t get_slm_offset(size_t addr);
size_t get_slm_addr(size_t offset);

// Async Computational Commands
struct slm_lba_info {
	size_t start_addr;			// SLM start addr
	size_t slm_len;				// Length of the SLM
	size_t input_len;			// Length of the Request
	int nentry;
	struct source_range_entry *sre;

	size_t next_offset;			// Next offset for "compute" to access
	size_t compute_access_physical_page_offset; // Latest SLM page offset compute have accessed

	uint32_t task_id;
	bool is_output;
	size_t final_offset;
	size_t final_leftovers;
};

struct slm_data_ready_info {
	struct slm_lba_info *slm_lba_info;
	uint8_t state;
};

void reserve_slm_memory(size_t start_addr, size_t len, bool is_output);
void set_slm_memory_lba_info(size_t start_addr, size_t len, struct slm_lba_info *slm_lba_info_ptr);
struct slm_lba_info *get_slm_memory_lba_info(size_t addr);
size_t get_slm_last_page_leftover(size_t start_addr);
size_t get_allocated_slm_size(size_t addr);

bool check_slm_data_consumed(size_t start_sddr, size_t len, bool log);
bool check_input_slm_data_ready(size_t start_sddr, size_t len, bool log);
bool check_output_slm_data_ready(size_t start_sddr, size_t *cmd_len, bool log);
bool check_output_slm_data_finalized(size_t start_addr);

void print_check_output_slm_data_ready(size_t start_addr, size_t len);

void finalize_slm_data_ready(size_t start_addr, size_t output_len);

struct slm_lba_info *alloc_slm_lba_info(size_t start_addr, size_t slm_len, size_t input_len, uint32_t task_id,
										struct source_range_entry *sre, int nentry);
void free_slm_lba_info(size_t slm_offset, size_t slm_size);

uint32_t get_io_task_from_slm_addr(size_t addr);
size_t get_continuous_size(size_t addr, size_t len);

void notify_slm_data_ready(size_t start_addr, size_t len);
void notify_slm_data_consumed(size_t start_addr, size_t len);
void notify_misaligned_slm_data_ready(size_t start_sddr, size_t len);

void check_slm_allocated_status(void);
#endif
