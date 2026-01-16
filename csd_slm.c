#include <linux/kthread.h>
#include <linux/ktime.h>
#include <linux/highmem.h>
#include <linux/sched/clock.h>
#include <linux/delay.h>

#include "nvmev.h"
#include "nvme_csd.h"
#include "csd_slm.h"
#include "csd_dispatcher.h"

#if (_SLM_DEBUG_ == 1)
#define NVMEV_SLM_INFO(string, args...) printk("[SLM_DEBUG] - %s : " string, __func__, ##args)
#else
#define NVMEV_SLM_INFO(string, args...)
#endif

#if (_VSLM_DEBUG_ == 1)
#define NVMEV_VSLM_INFO(string, args...) printk("[VSLM_DEBUG] - %s : " string, __func__, ##args)
#else
#define NVMEV_VSLM_INFO(string, args...)
#endif

#define DIVIDE_UP(value, divisor) (((value) + (divisor)-1) / (divisor))

static struct slm_physical_resource slm_physical_manager;
static struct slm_data_ready_info *slm_data_ready_info;

// SLM states should not be modified or stalled when sync mode is on
// If you're not using dCSD, SLM state doesn't mean anything
// This could... be buggy? but, since the original code of a normal csd
// didn't aim to modify the slm state, I bet it should work find
enum slm_status {
	SLM_STATE_DEFAULT = 0,
	SLM_STATE_RESERVED = SLM_STATE_DEFAULT,
	SLM_STATE_READY = SLM_STATE_DEFAULT,
	SLM_STATE_CONSUMED = SLM_STATE_DEFAULT,
};

static const struct allocator_ops buddy_allocator_ops = {
	.init = buddy_init,
	.allocate = buddy_alloc,
	.deallocate = buddy_free,
	.size = buddy_size,
	.status = buddy_print,
	.kill = buddy_kill,
};

char *__print_state_name(uint8_t state)
{
	switch (state) {
		case SLM_STATE_DEFAULT:
			return "SLM_STATE_DEFAULT";
		default:
			return "SOMETHING_WRONG";
	}
	BUG();
	return NULL;
}

size_t get_allocated_slm_size(size_t addr) {
	size_t slm_offset = get_slm_offset(addr) / SLM_PAGE_SIZE;

	if (slm_data_ready_info[slm_offset].slm_lba_info == NULL) {
		printk("Trying to read the slm_lba_info of a NULL area %lu\n", slm_offset);
		BUG();
	}

	return slm_data_ready_info[slm_offset].slm_lba_info->slm_len;
}

void check_slm_allocated_status(void)
{
    slm_physical_manager.allocator.status();
}

// Allocate a slm range (memory management cmd)
size_t alloc_slm_range(size_t size)
{
	size_t slm_offset = -1;

	slm_offset = slm_physical_manager.allocator.allocate(size, NULL);
	if (slm_offset == -1) {
		return -1;
	}

	NVMEV_SLM_INFO("Alloc SLM - SLM offset: %lu (%lu), SLM PageOffset: %lu(Count:%lu)", slm_offset,
				   get_slm_addr(slm_offset), slm_offset / SLM_PAGE_SIZE, DIVIDE_UP(size, SLM_PAGE_SIZE));
	return get_slm_addr(slm_offset);
}

void free_slm_range(size_t addr)
{
	unsigned int ret = 0;
	size_t slm_offset = 0;

	slm_offset = get_slm_offset(addr);
	size_t slm_size = slm_physical_manager.allocator.size(slm_offset);

	free_slm_lba_info(slm_offset, slm_size);

	NVMEV_SLM_INFO("Free SLM - SLM offset: %lu, SLM size: %lu\n", slm_offset, slm_size);

	if (slm_physical_manager.allocator.deallocate(slm_offset) != 0) {
		NVMEV_ERROR("Error when freeing SLM page\n");
	}
}

void copy_to_slm(size_t dest, void *src, size_t size)
{
	if (dest < slm_physical_manager.start_addr) {
		NVMEV_ERROR("[%s] INVALID SLM ADDR:%lu", __FUNCTION__, dest);
	}

	// TODO: additional checking of range
	memcpy((void *)dest, src, size);
}

void copy_from_slm(void *dest, size_t src, size_t size)
{
	if (src < slm_physical_manager.start_addr) {
		NVMEV_ERROR("[%s] INVALID SLM ADDR:%lu", __FUNCTION__, src);
	}

	// TODO: additional checking of range
	memcpy(dest, (void *)src, size);
}

size_t get_slm_offset(size_t addr)
{
	return (addr - slm_physical_manager.start_addr);
}

size_t get_slm_addr(size_t offset)
{
	return (slm_physical_manager.start_addr + offset);
}

void init_slm_memory(void *start, size_t slm_size)
{
	size_t i = 0;
	size_t page_count = slm_size / SLM_PAGE_SIZE;

	NVMEV_SLM_INFO("SLM Init Info: start: %lu, size: %lu, Page: %lu", (size_t)start, slm_size, page_count);

	// slm_data_ready_info = kzalloc_node(sizeof(struct slm_data_ready_info) * page_count, GFP_KERNEL);
	slm_data_ready_info = vmalloc_node(sizeof(struct slm_data_ready_info) * page_count, 1);
	for (i = 0; i < page_count; i++) {
		slm_data_ready_info[i].slm_lba_info = NULL;
		slm_data_ready_info[i].state = SLM_STATE_DEFAULT;
	}

	slm_physical_manager.start_addr = (size_t)start;
	slm_physical_manager.allocator = buddy_allocator_ops;
	if (slm_physical_manager.allocator.init(slm_size) != 0) {
		NVMEV_ERROR("Allocator init failed!\n");
	}
}

void final_slm_memory(void)
{
	// kfree(slm_data_ready_info);
	vfree(slm_data_ready_info);
	slm_physical_manager.allocator.kill();
}

void set_slm_memory_lba_info(size_t start_addr, size_t len, struct slm_lba_info *slm_lba_info_ptr)
{
	size_t i = 0;
	size_t page_count;
	size_t start_offset = get_slm_offset(start_addr);
	start_offset = start_offset / SLM_PAGE_SIZE;

	page_count = DIVIDE_UP(len, SLM_PAGE_SIZE);

	for (i = start_offset; i < start_offset + page_count; i++) {
		slm_data_ready_info[i].slm_lba_info = slm_lba_info_ptr;
	}

	NVMEV_SLM_INFO("Set slm_lba_info - start offset: %lu, page count: %lu\n", start_offset, page_count);
}

struct slm_lba_info *get_slm_memory_lba_info(size_t addr)
{
	return slm_data_ready_info[get_slm_offset(addr) / SLM_PAGE_SIZE].slm_lba_info;
}

inline bool __check_slm_memory(size_t start, size_t page_count, uint8_t state, bool log)
{
	size_t i = 0;
	for (i = start; i <= start + page_count; i++) {
		if (slm_data_ready_info[i].state != state) {
			if (log == true) {
				NVMEV_SLM_INFO("Check failed on offset: %lu, state is : %s, expected: %s\n",
								i, __print_state_name(slm_data_ready_info[i].state), __print_state_name(state));
			}
			return false;
		}
	}
	return true;
}

inline void __set_slm_memory(size_t start, size_t page_count, uint8_t state)
{
	size_t i = 0;
	for (i = start; i < start + page_count; i++) {
		slm_data_ready_info[i].state = state;
	}
}

void reserve_slm_memory(size_t start_addr, size_t len, bool is_output)
{
	size_t start_offset = get_slm_offset(start_addr) / SLM_PAGE_SIZE;
	size_t end_offset = get_slm_offset(start_addr + len) / SLM_PAGE_SIZE;
	size_t page_count = end_offset - start_offset;
	uint8_t state = SLM_STATE_RESERVED;

	if (is_output) {
		state = SLM_STATE_CONSUMED;
	}

	__set_slm_memory(start_offset, page_count, state);

	NVMEV_SLM_INFO("Reserved SLM - start_offset: %lu, count: %lu, state: %s", start_offset, page_count,
								__print_state_name(state));
}

// Check functions

// Check input SLM buffer - Compute
bool check_input_slm_data_ready(size_t start_addr, size_t len, bool log)
{
	bool ret = true;
	size_t start_offset = get_slm_offset(start_addr);
	size_t end_offset = get_slm_offset(start_addr + len);
	size_t start_page_offset = start_offset / SLM_PAGE_SIZE;
	size_t end_page_offset = end_offset / SLM_PAGE_SIZE;
	size_t page_count;
	struct slm_lba_info *info = slm_data_ready_info[start_page_offset].slm_lba_info;

	page_count = end_page_offset - start_page_offset;

	if (page_count != 1) {
		NVMEV_ERROR("Invalid page count:%lu in %s", page_count, __FUNCTION__);
	}

	ret = __check_slm_memory(start_page_offset, 0, SLM_STATE_READY, log);

	// Request demand loading for random access
	if ((info != NULL) && (info->is_output == false) && (ret == true)) {
		size_t read_offset = start_addr - info->start_addr;
		info->next_offset = read_offset + len;
	}

	return ret;
}

bool check_slm_data_consumed(size_t start_addr, size_t len, bool log)
{
	bool ret = false;
	size_t start_offset = get_slm_offset(start_addr);
	size_t end_offset = get_slm_offset(start_addr + len);
	size_t start_page_offset = start_offset / SLM_PAGE_SIZE;
	size_t end_page_offset = end_offset / SLM_PAGE_SIZE;

	if ((end_offset % SLM_PAGE_SIZE == 0) && (start_page_offset != end_page_offset)) {
		end_page_offset -= 1;
	}

	ret = __check_slm_memory(start_page_offset, end_page_offset - start_page_offset, SLM_STATE_CONSUMED, log);

	if (log == true) {
		NVMEV_SLM_INFO("Check CONSUMED (start_offset: %lu, end_offset: %lu) - Ready: %u\n",
					start_page_offset, end_page_offset, ret);
	}

	return ret;
}

// Check SLM range - Read 
bool check_output_slm_data_ready(size_t start_addr, size_t *cmd_len, bool log)
{
	bool ret = false;
	size_t len = *cmd_len;
	size_t start_offset = get_slm_offset(start_addr);
	size_t end_offset = get_slm_offset(start_addr + len);
	size_t start_page_offset = start_offset / SLM_PAGE_SIZE;
	size_t end_page_offset = end_offset / SLM_PAGE_SIZE;
	size_t page_count = 0;

	if ((end_offset % SLM_PAGE_SIZE == 0) && (start_page_offset != end_page_offset)) {
		end_page_offset -= 1;
	}

	page_count = end_page_offset - start_page_offset;

	struct slm_lba_info *info = slm_data_ready_info[start_page_offset].slm_lba_info;

	if (info != NULL && info->is_output == true && info->final_offset != -1) {
		if ((end_page_offset >= info->final_offset) && (start_page_offset <= info->final_offset)) {
			page_count = (info->final_offset - start_page_offset);
			*cmd_len = SLM_PAGE_SIZE * (info->final_offset - start_page_offset) + info->final_leftovers;
			NVMEV_SLM_INFO("[READ] End of a computation has been detected, page start: %lu, page count: %lu, cmd_len: %lu\n",
								start_page_offset, page_count, *cmd_len);
			if (*cmd_len == 0) {
				return true;
			}
		}
	}

	ret =  __check_slm_memory(start_page_offset, page_count, SLM_STATE_READY, log);

	if (info != NULL && info->is_output == true && info->final_offset != -1) {
		if ((end_page_offset >= info->final_offset) && (start_page_offset <= info->final_offset)) {
			*cmd_len = SLM_PAGE_SIZE * (info->final_offset - start_page_offset) + info->final_leftovers;
		}
	}

	if (log == true) {
		NVMEV_SLM_INFO("[READ] Check output SLM_STATE_READY (start offset: %lu, count: %lu, size: %lu) - %d\n",
								start_page_offset, page_count + 1, *cmd_len, ret);
	}

	return ret;
}

// Check output SLM finalized - Read
bool check_output_slm_data_finalized(size_t slm_addr)
{
	size_t slm_offset = get_slm_offset(slm_addr) / SLM_PAGE_SIZE;
	struct slm_lba_info *info = slm_data_ready_info[slm_offset].slm_lba_info;

	if (info != NULL && info->is_output == true && info->final_offset != -1) {
		if (slm_offset == info->final_offset) {
			NVMEV_SLM_INFO("[FINALIZE DETECTED] slm_offset: %lu\n", slm_offset);
			return true;
		}
	}

	return false;
}

// This print function is seperated just for debugging purpose.
// Sometimes the internal buffer may be full, resulting the dispatcher to check_output_slm_data_ready multiple times
void print_check_output_slm_data_ready(size_t start_addr, size_t len)
{
	size_t end_offset = get_slm_offset(start_addr + len) / SLM_PAGE_SIZE;
	size_t start_offset = get_slm_offset(start_addr) / SLM_PAGE_SIZE;
	size_t page_count = end_offset - start_offset;

	NVMEV_SLM_INFO("[READ] Check output SLM_STATE_READY(start offset: %lu, count: %lu) - Success", start_offset, page_count);
}

// Notify functions

void notify_slm_data_consumed(size_t start_addr, size_t len)
{
	size_t page_count = 0;
	size_t remain = 0;
	size_t start_offset = get_slm_offset(start_addr);
	size_t end_offset = get_slm_offset(start_addr + len);
	struct slm_lba_info *info;
	remain = (start_offset + len) % SLM_PAGE_SIZE;
	start_offset = start_offset / SLM_PAGE_SIZE;
	end_offset = end_offset / SLM_PAGE_SIZE;
	page_count = end_offset - start_offset;
	info = slm_data_ready_info[start_offset].slm_lba_info;

	__set_slm_memory(start_offset, page_count, SLM_STATE_CONSUMED);
}

void notify_slm_data_ready(size_t start_addr, size_t len)
{
	size_t page_count = 0;
	size_t remain = 0;
	size_t start_offset = get_slm_offset(start_addr);
	size_t end_offset = get_slm_offset(start_addr + len);
	remain = (start_offset + len) % SLM_PAGE_SIZE;
	start_offset = start_offset / SLM_PAGE_SIZE;
	end_offset = end_offset / SLM_PAGE_SIZE;
	page_count = end_offset - start_offset;

	__set_slm_memory(start_offset, page_count, SLM_STATE_READY);
}

void notify_misaligned_slm_data_ready(size_t start_addr, size_t len)
{
	size_t page_count = 0;
	size_t start_offset = get_slm_offset(start_addr);
	size_t end_offset = get_slm_offset(start_addr + len);
	start_offset = start_offset / SLM_PAGE_SIZE;

	if ((end_offset / SLM_PAGE_SIZE) * SLM_PAGE_SIZE < end_offset) {
		end_offset = (end_offset / SLM_PAGE_SIZE) + 1;
	} else {
		end_offset = end_offset / SLM_PAGE_SIZE;
	}

	page_count = end_offset - start_offset;

	NVMEV_SLM_INFO("SLM READY - start_offset: %lu, page_count: %lu(misaligned)",
					start_offset, page_count);

	__set_slm_memory(start_offset, page_count, SLM_STATE_READY);
}

void finalize_slm_data_ready(size_t output_addr, size_t output_len)
{
	// For OOM Buffers
	size_t output_buf_size = get_allocated_slm_size((size_t)output_addr); 
	output_addr = output_addr + (output_len % output_buf_size);
	size_t output_offset = get_slm_offset(output_addr) / SLM_PAGE_SIZE;
	size_t remain = output_len - (output_len / SLM_PAGE_SIZE) * SLM_PAGE_SIZE;
	bool log = true;

	// Should check if the output_offset is SLM_STATE_CONSUMED
	while (check_slm_data_consumed(output_addr, 0, log) == false) {
		log = false;
		cond_resched();
	}

	if (slm_data_ready_info[output_offset].slm_lba_info != NULL) {
		slm_data_ready_info[output_offset].slm_lba_info->final_offset = output_offset;
		slm_data_ready_info[output_offset].slm_lba_info->final_leftovers = remain;

		NVMEV_SLM_INFO("[FINALIZED] Final page offset: %lu, leftovers: %lu\n", output_offset, remain);
	}

	notify_slm_data_ready(output_addr, SLM_PAGE_SIZE);
}

size_t get_slm_last_page_leftover(size_t slm_addr)
{
	size_t slm_offset = get_slm_offset(slm_addr) / SLM_PAGE_SIZE;
	struct slm_lba_info *info = slm_data_ready_info[slm_offset].slm_lba_info;

	if (info != NULL && info->is_output == true && info->final_offset != -1) {
		if (slm_offset == info->final_offset) {
			NVMEV_SLM_INFO("[INTERMEDIATE] offset: %lu, leftover: %lu\n", info->final_offset, info->final_leftovers);
			return info->final_leftovers;
		} else {
			printk("WRONG LAST PAGE ADDRESS %lu %lu\n", slm_offset, info->final_offset);
			BUG();
		}
	}

	BUG();

	return 0;
}

struct slm_lba_info *alloc_slm_lba_info(size_t start_addr, size_t slm_len, size_t input_len,
										uint32_t task_id, struct source_range_entry *sre, int nentry)
{
	struct slm_lba_info *info = vmalloc_node(sizeof(struct slm_lba_info), 1);
	if (info == NULL) {
		NVMEV_ERROR("slm_lba_info alloc fail!");
		BUG();
	}
	info->start_addr = start_addr;
	info->slm_len = slm_len; 								// Length of SLM
	info->input_len = input_len;							// Length of Input
	info->task_id = task_id;
	info->next_offset = 0;
	info->compute_access_physical_page_offset = -1;
	info->is_output = false;
	info->final_offset = -1;
	info->final_leftovers = 0;

	info->nentry = nentry;
	info->sre = sre;

	NVMEV_SLM_INFO("alloc slm lba info (page_offset:%lu, size:%lu, page_count, %lu)",
							get_slm_offset(info->start_addr) / SLM_PAGE_SIZE, info->slm_len, (info->slm_len) / SLM_PAGE_SIZE);

	return info;
}

void free_slm_lba_info(size_t slm_offset, size_t slm_size)
{
	size_t i = 0;
	size_t page_count = DIVIDE_UP(slm_size, SLM_PAGE_SIZE);
	size_t start_offset = slm_offset / SLM_PAGE_SIZE;
	size_t end_offset = start_offset + page_count;
	size_t prev = 0;

	while (start_offset < end_offset) {
		if (slm_data_ready_info[start_offset].slm_lba_info == NULL) {
			break;
		}

		if (slm_data_ready_info[start_offset].slm_lba_info != NULL) {
			prev = start_offset;
			start_offset += (slm_data_ready_info[start_offset].slm_lba_info->slm_len / SLM_PAGE_SIZE) + 1;
			if (((struct slm_lba_info *)slm_data_ready_info[prev].slm_lba_info)->sre != NULL) {
				vfree(((struct slm_lba_info *)slm_data_ready_info[prev].slm_lba_info)->sre);
			}
			vfree(slm_data_ready_info[prev].slm_lba_info);
			printk("Freed slm_lba_info (page_offset:%lu)\n", prev);
		}
	}

	start_offset = slm_offset / SLM_PAGE_SIZE;
	for (i = start_offset; i < start_offset + page_count; i++) {
		slm_data_ready_info[i].slm_lba_info = NULL;
		slm_data_ready_info[i].state = SLM_STATE_DEFAULT;
	}
}

// ======= Demand Read =======

// To avoid Kernel panic in case of early SLM free
uint32_t get_io_task_from_slm_addr(size_t addr)
{
	uint64_t offset = get_slm_offset(addr) / SLM_PAGE_SIZE;
	struct slm_lba_info *info = slm_data_ready_info[offset].slm_lba_info;
	if (info != NULL && info->is_output == false) {
		return info->task_id;
	}

	return -1;
}

size_t get_continuous_size(size_t addr, size_t len)
{
	size_t i = 0;
	uint64_t start_offset = get_slm_offset(addr) / SLM_PAGE_SIZE;
	size_t page_count = len / SLM_PAGE_SIZE;
	uint32_t *buf = (uint32_t *)addr;

	for (i = 0; i < page_count; i++) {
		if (slm_data_ready_info[start_offset + i].state != SLM_STATE_READY) {
			break;
		}
	}

	return i * SLM_PAGE_SIZE;
}
