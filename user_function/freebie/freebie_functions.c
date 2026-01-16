#include "../../nvme_csd.h"
#include "../../nvmev.h"
#include "../../csd_dispatcher.h"
#include "../../csd_user_func.h"
#include "../../csd_slm.h"

#define KERNEL_MODE
#include "freebie_functions.h"

freebie_internal_map freebie_map[FREEBIE_MAX_TABLE_COUNT];

static uint32_t __allocate_new_data_block(freebie_internal_map *freebie_map_entry)
{
	for (int i = 0; i < FREEBIE_DATA_CHUNK_COUNT; i++) {
		if (atomic_cmpxchg(&freebie_map_entry->data_rc[i], 0, 1) == 0) {
			return i;
		}
	}
	return -1;
}

static uint32_t __allocate_new_root_block(freebie_internal_map *freebie_map_entry)
{
	for (int i = 1; i < FREEBIE_ROOT_CHUNK_COUNT; i++) {
		if (atomic_cmpxchg(&freebie_map_entry->root_rc[i], ROOT_FREE, ROOT_ALLOCATED) == ROOT_FREE) {
			return i;
		}
	}
	return -1;
}

static freebie_internal_map *__get_freebie_map_entry(uint32_t relation_id)
{
	for (int i = 0; i < FREEBIE_MAX_TABLE_COUNT; i++) {
		if ((freebie_map[i].is_valid == true) && (freebie_map[i].relation_id == relation_id)) {
			return &freebie_map[i];
		}
	}
	return NULL;
}

static freebie_internal_map *__find_empty_freebie_map_entry(void)
{
	for (int i = 0; i < FREEBIE_MAX_TABLE_COUNT; i++) {
		if (!freebie_map[i].is_valid) {
			freebie_map[i].is_valid = true;
			return &freebie_map[i];
		}
	}
	return NULL;
}

static void __freebie_request_io(struct ccsd_task_info *task, size_t slm_addr, size_t size,
											size_t file_offset, bool is_read)
{
	struct ccsd_task_info *buddy_task = task->buddy_task;
	if (buddy_task->task_step == CCSD_TASK_WAIT) {
		if (is_read) {
			// NVMEV_FREEBIE_DEBUG("[READ] file_offset: %lu, size: %lu\n", file_offset, size);
			buddy_task->program_idx = COPY_TO_SLM_PROGRAM_INDEX;
		}
		else {
			// NVMEV_FREEBIE_DEBUG("[WRITE] file_offset: %lu, size: %lu\n", file_offset, size);
			buddy_task->program_idx = COPY_FROM_SLM_PROGRAM_INDEX;
		}

		buddy_task->input_buf_addr = slm_addr;
		buddy_task->total_size = size;
        buddy_task->done_size = 0;
		buddy_task->requested_io_offset = file_offset;
		task->task_step = CCSD_TASK_WAIT;
		buddy_task->task_step = CCSD_TASK_SCHEDULE;
		while (task->task_step == CCSD_TASK_WAIT) {
			cond_resched();
		}
		// NVMEV_FREEBIE_DEBUG("[DONE]\n");
	}
	else {
		NVMEV_FREEBIE_DEBUG("Buddy task is not in wait state\n");
		BUG();
	}
}

#define __freebie_request_read(task, slm_addr, size, file_offset) \
	__freebie_request_io(task, slm_addr, size, file_offset, true)

#define __freebie_request_write(task, slm_addr, size, file_offset) \
	__freebie_request_io(task, slm_addr, size, file_offset, false)

void check_freebie_map(void)
{
	for (int i = 0; i < FREEBIE_MAX_TABLE_COUNT; i++) {
		if (freebie_map[i].is_valid == true) {
			printk("Freebie map(%d) is not freed\n", i);
		}
	}
}

void clear_freebie_map(freebie_internal_map *p)
{
	p->is_valid = false;
	p->relation_id = 0;
	p->root_buffer_addr = 0;
	memset(p->root_id, -1, sizeof(p->root_id));
	memset(p->map_version, 0, sizeof(p->map_version));
	p->slm_lba_info = NULL;

	atomic_set(&p->valid_root_buffer, 0);
	atomic_set(&p->root_reader_count[0], 0);
	atomic_set(&p->root_reader_count[1], 0);

	for (int j = 0; j < FREEBIE_ROOT_CHUNK_COUNT; j++)
		atomic_set(&p->root_rc[j], ROOT_FREE);
	for (int j = 0; j < FREEBIE_DATA_CHUNK_COUNT; j++)
		atomic_set(&p->data_rc[j], 0);

	spin_lock_init(&p->repartition_lock);

	atomic_set(&p->gc_count, 0);
	atomic_set(&p->partition_map_version, 0);

	p->gc_task = NULL;
}

void init_freebie_map(void)
{
	for (int i = 0; i < FREEBIE_MAX_TABLE_COUNT; i++) {
		clear_freebie_map(&freebie_map[i]);
	}
}

bool freebie_check_root_setup(uint32_t relation_id)
{
	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry == NULL) {
		NVMEV_FREEBIE_DEBUG("ROOT %u is not allocated", relation_id);
		return false;
	}
	return true;
}

size_t freebie_get_root_buffer(uint32_t relation_id, uint8_t *root_id)
{
	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry == NULL) {
		NVMEV_FREEBIE_DEBUG("ROOT %u is not allocated", relation_id);
		return - 1;
	}

	uint32_t valid_catal = atomic_read(&freebie_map_entry->valid_root_buffer);

	*root_id = freebie_map_entry->root_id[valid_catal];
	atomic_inc(&freebie_map_entry->root_rc[*root_id]);

	return freebie_map_entry->root_buffer_addr + (valid_catal * FREEBIE_ROOT_CHUNK_SIZE);
}

uint32_t freebie_start_read_partition_map(uint32_t relation_id, uint32_t root_index)
{
	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	uint32_t valid_root_buffer = -1;
	if (freebie_map_entry == NULL) {
		NVMEV_FREEBIE_DEBUG("ROOT %u is not allocated", relation_id);
		return - 1;
	}	

	if (root_index == 0) {
		// spin_lock(&freebie_map_entry->repartition_lock);

		valid_root_buffer = atomic_read(&freebie_map_entry->valid_root_buffer);
		atomic_inc(&freebie_map_entry->root_reader_count[valid_root_buffer]);

		// spin_unlock(&freebie_map_entry->repartition_lock);

		// printk("Freebie: Start read partition map, valid_root_buffer=%u, reader_count=%d\n",
		// 		valid_root_buffer, atomic_read(&freebie_map_entry->root_reader_count[valid_root_buffer]));
	} else {
		BUG();
	}

	return valid_root_buffer;
}

uint32_t freebie_end_read_partition_map(uint32_t relation_id, uint32_t valid_root_buffer)
{
	uint32_t ret;
	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry == NULL) {
		NVMEV_FREEBIE_DEBUG("ROOT %u is not allocated", relation_id);
		BUG();
	}	

	ret = freebie_map_entry->map_version[valid_root_buffer];

	// If this is the last request
	atomic_dec(&freebie_map_entry->root_reader_count[valid_root_buffer]);
	// printk("Freebie: End read partition map, valid_root_buffer=%u, reader_count=%d\n",
	// 		valid_root_buffer, atomic_read(&freebie_map_entry->root_reader_count[valid_root_buffer]));

	return ret;
}

size_t freebie_get_lba_of_map_block(uint32_t relation_id, uint32_t valid_root_buffer, uint32_t root_index)
{
	uint32_t io_offset;
	uint16_t *root;
	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry == NULL) {
		NVMEV_FREEBIE_DEBUG("ROOT %u is not allocated", relation_id);
		return -1;
	}	
	root = (uint16_t *) (freebie_map_entry->root_buffer_addr + (valid_root_buffer * FREEBIE_ROOT_CHUNK_SIZE));
	io_offset = (FREEBIE_PARTITION_MAP_FILE_SIZE / 2) + (root[root_index] * FREEBIE_DATA_CHUNK_SIZE);
	struct slm_lba_info *slm_info = freebie_map_entry->slm_lba_info;
	size_t base_addr = 0;
	size_t local_offset = io_offset;

	if (io_offset == 0) {
		return slm_info->sre[0].saddr;
	}

	for (int i = 0; i < slm_info->nentry; i++) {
		base_addr = slm_info->sre[i].saddr;
		if (local_offset < slm_info->sre[i].nByte) {
			break;
		}
		local_offset -= slm_info->sre[i].nByte;
	}

	return base_addr + (local_offset / 512);
}

struct slm_lba_info *freebie_get_slm_lba_info(uint32_t relation_id)
{
	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry != NULL) {
		return freebie_map_entry->slm_lba_info;
	}
	return NULL;
}

void freebie_add_sre(freebie_source_range_entry *sre, uint32_t *nentry,
	uint32_t offset, uint32_t value, uint8_t operation)
{
	sre[*nentry].offset = offset;
	sre[*nentry].value = value;
	sre[*nentry].operation = operation;
	sre[*nentry].processed = false;
	(*nentry)++;
}

size_t freebie_terminate(uint32_t relation_id)
{
	NVMEV_FREEBIE_DEBUG("Freebie Terminate Start %d\n", relation_id);

	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry == NULL) {
		return true;
	}

	// BUG FIX, Terminate should come in when GC will never, I say never ran

	// Check if GC task is running, wait for it to finish
	BUG_ON(freebie_map_entry->gc_task->task_step == CCSD_TASK_SCHEDULE); 
	struct ccsd_task_info *gc_buddy_task = freebie_map_entry->gc_task->buddy_task;
	BUG_ON(gc_buddy_task == NULL);
	BUG_ON(gc_buddy_task->task_step == CCSD_TASK_SCHEDULE);

	NVMEV_FREEBIE_DEBUG("Terminate GC Task\n");
	freebie_map_entry->gc_task->task_step = CCSD_TASK_END; 
	gc_buddy_task->task_step = CCSD_TASK_END; 
	freebie_map_entry->gc_task = NULL;

	NVMEV_FREEBIE_DEBUG("Free ROOT Buffer adress\n");
	free_slm_range(freebie_map_entry->root_buffer_addr);
	if (((struct slm_lba_info *)freebie_map_entry->slm_lba_info)->sre != NULL) {
		vfree(((struct slm_lba_info *)freebie_map_entry->slm_lba_info)->sre);
	}
	vfree(freebie_map_entry->slm_lba_info);

	freebie_map_entry->relation_id = 0;
	freebie_map_entry->root_buffer_addr = 0;
	memset(freebie_map_entry->root_id, -1, sizeof(freebie_map_entry->root_id));
	freebie_map_entry->slm_lba_info = NULL;
	atomic_set(&freebie_map_entry->valid_root_buffer, 0);
	for (int j = 0; j < FREEBIE_ROOT_CHUNK_COUNT; j++)
		atomic_set(&freebie_map_entry->root_rc[j], ROOT_FREE);
	for (int j = 0; j < FREEBIE_DATA_CHUNK_COUNT; j++)
		atomic_set(&freebie_map_entry->data_rc[j], 0);

	freebie_map_entry->is_valid = false;

	NVMEV_FREEBIE_DEBUG("Freebie Terminate Done\n");
	return true;
}

size_t freebie_setup(struct ccsd_task_info *task, uint32_t user_relation_id)
{
	struct slm_lba_info *slm_lba_info = task->slm_lba_info;
	size_t root_buffer = slm_lba_info->start_addr;

	uint32_t *latest_root_chunk_index;
	uint32_t *relation_id;
	uint16_t *root;
	freebie_internal_map *freebie_map_entry = __find_empty_freebie_map_entry();

	NVMEV_FREEBIE_DEBUG("Start Freebie Setup\n");

	if (freebie_map_entry == NULL) {
		NVMEV_FREEBIE_DEBUG("Freebie map is full\n");
		BUG();
	}

	__freebie_request_read(task, (size_t)root_buffer, 2 * sizeof(uint32_t), 0);
	relation_id = (uint32_t *)root_buffer;
	latest_root_chunk_index	= (uint32_t *)(root_buffer + sizeof(uint32_t));
	BUG_ON(*latest_root_chunk_index >= FREEBIE_ROOT_CHUNK_COUNT);
	// Check if the relation id is valid
	if (*relation_id != user_relation_id) {
		NVMEV_FREEBIE_DEBUG("Relation ID is not identical: file: %u user: %u\n",
									*relation_id, user_relation_id);
		return true;
	} else {
		NVMEV_FREEBIE_DEBUG("Valid Relation ID: %u\n", *relation_id);
		NVMEV_FREEBIE_DEBUG("Latest ROOT chunk index is: %u\n", *latest_root_chunk_index);
	}

	clear_freebie_map(freebie_map_entry);

	freebie_map_entry->is_valid = true;
	freebie_map_entry->relation_id = *relation_id;
	freebie_map_entry->root_id[0] = *latest_root_chunk_index;
	freebie_map_entry->root_buffer_addr = root_buffer;
	freebie_map_entry->slm_lba_info = task->slm_lba_info;

	for (int i = 0; i < FREEBIE_ROOT_CHUNK_COUNT; i++)
		atomic_set(&freebie_map_entry->root_rc[i], ROOT_FREE);
	for (int i = 0; i < FREEBIE_DATA_CHUNK_COUNT; i++)
		atomic_set(&freebie_map_entry->data_rc[i], 0);

	atomic_set(&freebie_map_entry->valid_root_buffer, 0);
	atomic_set(&freebie_map_entry->root_rc[0], 1); // META

	atomic_inc(&freebie_map_entry->root_rc[*latest_root_chunk_index]);

	__freebie_request_read(task, (size_t)root_buffer, FREEBIE_ROOT_CHUNK_SIZE,
								(*latest_root_chunk_index * FREEBIE_ROOT_CHUNK_SIZE));
	root = (uint16_t *)(root_buffer);
	for (int i = 0 ; i < FREEBIE_MAX_VALID_ROOT_INDEX; i++) {
		if (root[i] >= FREEBIE_DATA_CHUNK_COUNT) {
			NVMEV_FREEBIE_DEBUG("Invalid data block index in root: %d %u\n", i, root[i]);
			printk("freebie setup start address: %lu\n", root_buffer);
			BUG();
		}
		atomic_set(&freebie_map_entry->data_rc[root[i]], 1);
	}
	NVMEV_FREEBIE_DEBUG("Finish Freebie Setup");

	return true;
}

size_t freebie_release_root(uint32_t relation_id, uint32_t root_id)
{
	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry == NULL) {
		NVMEV_FREEBIE_DEBUG("Freebie map entry is NULL\n");
		return true;
	}

	// NVMEV_FREEBIE_DEBUG("Freebie Release ROOT: %u\n", root_id);
	atomic_dec(&freebie_map_entry->root_rc[root_id]);
	return true;
}

void freebie_update_metadata(struct ccsd_task_info *task, uint32_t relation_id,
									freebie_source_range_entry *freebie_sre, uint32_t nentry)
{
	freebie_internal_map *freebie_map_entry = NULL;

	uint16_t old_data_block_index = -1;
	uint16_t new_data_block_index = -1;
	uint32_t logical_file_offset = -1;
	size_t hidden_slm_addr = -1;
	uint32_t old_root_buffer_index;
	uint32_t new_root_buffer_index;
	uint32_t new_root_id;
	uint32_t *hidden_slm;
	uint16_t *root;
	uint32_t *meta;

	// 0. Get the freebie map entry
	freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry == NULL) {
		NVMEV_FREEBIE_DEBUG("Freebie map entry is NULL might have been terminated\n");
		// Do nothing for terminated relation
		return;
	}

	BUG_ON(nentry == 0);
	BUG_ON(freebie_sre == NULL);

	spin_lock(&freebie_map_entry->repartition_lock);

	// Copy the root buffer to the empty slm buffer
	old_root_buffer_index = atomic_read(&freebie_map_entry->valid_root_buffer);
	new_root_buffer_index = (old_root_buffer_index + 1) % FREEBIE_ROOT_SLOT_COUNT;
	while (atomic_read(&freebie_map_entry->root_reader_count[new_root_buffer_index]) > 0) {
		cond_resched();
	}

	memcpy((void *)(freebie_map_entry->root_buffer_addr + (new_root_buffer_index * FREEBIE_ROOT_CHUNK_SIZE)),
			(void *)(freebie_map_entry->root_buffer_addr + (old_root_buffer_index * FREEBIE_ROOT_CHUNK_SIZE)),
			FREEBIE_ROOT_CHUNK_SIZE);
	root = (uint16_t*)(freebie_map_entry->root_buffer_addr + (new_root_buffer_index * FREEBIE_ROOT_CHUNK_SIZE));

	// Increase all RC of the data blocks in the root
	// The modified data block's RC will be decreased later
	for (int i = 0 ; i < FREEBIE_MAX_VALID_ROOT_INDEX; i++) {
		atomic_inc(&freebie_map_entry->data_rc[root[i]]);
	}

	hidden_slm_addr = freebie_map_entry->root_buffer_addr + (FREEBIE_ROOT_SLOT_COUNT * FREEBIE_ROOT_CHUNK_SIZE);
	hidden_slm = (uint32_t *)hidden_slm_addr;

	for (uint i = 0; i < nentry; i++) {
		if (freebie_sre[i].processed == true) {
			continue;
		}
		logical_file_offset = freebie_sre[i].offset / FREEBIE_DATA_CHUNK_SIZE;
		old_data_block_index = root[logical_file_offset];
		BUG_ON(old_data_block_index >= FREEBIE_DATA_CHUNK_COUNT);

		new_data_block_index = __allocate_new_data_block(freebie_map_entry);

		// NVMEV_FREEBIE_DEBUG("logical_file_offset: %u, old_data_block_index: %u, new_data_block_index: %u\n",
		// 		logical_file_offset, old_data_block_index, new_data_block_index);

		if (new_data_block_index == -1) {
			NVMEV_FREEBIE_DEBUG("No free data block available\n");
			BUG();
		}

		// Read the old data block to the data buffer
		// NVMEV_FREEBIE_DEBUG("Read old data block %u\n", old_data_block_index);
		__freebie_request_read(task, hidden_slm_addr, FREEBIE_DATA_CHUNK_SIZE, (FREEBIE_PARTITION_MAP_FILE_SIZE / 2) + old_data_block_index * FREEBIE_DATA_CHUNK_SIZE);

		// Modify the data buffer
		for (uint j = 0; j < nentry; j++) {
			if (freebie_sre[j].processed == true) {
				continue;
			}
			if ((freebie_sre[j].offset / FREEBIE_DATA_CHUNK_SIZE) == logical_file_offset) {
				uint32_t internal_offset = (freebie_sre[j].offset % FREEBIE_DATA_CHUNK_SIZE) / sizeof(uint32_t);
				if (freebie_sre[j].operation == FREEBIE_OPERATION_WRITE) {
					hidden_slm[internal_offset] = freebie_sre[j].value;
				} else if (freebie_sre[j].operation == FREEBIE_OPERATION_ADD) {
					hidden_slm[internal_offset] += freebie_sre[j].value;
				}
				freebie_sre[j].processed = true;
			}
		}

		// Write the data buffer to new data block
		// NVMEV_FREEBIE_DEBUG("Write new data block %u\n", new_data_block_index);
		__freebie_request_write(task, hidden_slm_addr, FREEBIE_DATA_CHUNK_SIZE, (FREEBIE_PARTITION_MAP_FILE_SIZE / 2) + new_data_block_index * FREEBIE_DATA_CHUNK_SIZE);

		// Update the root buffer with the new data block addressses
		atomic_dec(&freebie_map_entry->data_rc[old_data_block_index]);
		root[logical_file_offset] = new_data_block_index;
	}

	// Allocate new root block
	// Write the root buffer to the root block
	new_root_id = __allocate_new_root_block(freebie_map_entry);
	if (new_root_id == -1) {
		NVMEV_FREEBIE_DEBUG("No free root block available\n");
		BUG();
	}

	// NVMEV_FREEBIE_DEBUG("Write new root block %u\n", new_root_id);
	__freebie_request_write(task, (size_t)root, FREEBIE_ROOT_CHUNK_SIZE, new_root_id * FREEBIE_ROOT_CHUNK_SIZE);

	freebie_map_entry->root_id[new_root_buffer_index] = new_root_id;

	// Update the Meta (latest root chunk index)
	meta = (uint32_t *)hidden_slm_addr;
	meta[0] = freebie_map_entry->relation_id;
	meta[1] = new_root_id;

	// NVMEV_FREEBIE_DEBUG("Write ROOT Meta\n");
	__freebie_request_write(task, hidden_slm_addr, 2 * sizeof(uint32_t), 0);

	atomic_inc(&freebie_map_entry->partition_map_version);

	freebie_map_entry->map_version[new_root_buffer_index] = atomic_read(&freebie_map_entry->partition_map_version);

	// Now, read transactions will read the new root buffer
	atomic_set(&freebie_map_entry->valid_root_buffer, new_root_buffer_index);

	// Check GC condition
	if (atomic_read(&freebie_map_entry->gc_count) == 0) {
        // BUG (0813) - When GC is already running, it might mix up the task_step
		// freebie_map_entry->gc_task->task_step = CCSD_TASK_SCHEDULE;
        // BUG FIX - if GC is already running, just pass
        atomic_inc(&freebie_map_entry->gc_count);
		freebie_map_entry->gc_task->task_step = CCSD_TASK_SCHEDULE;

		// NVMEV_FREEBIE_DEBUG("GC required, wake up GC thread\n");
	}

	spin_unlock(&freebie_map_entry->repartition_lock);
}

bool freebie_garbage_collection(struct ccsd_task_info *task, uint32_t relation_id)
{
	uint32_t skip_root_id[FREEBIE_ROOT_SLOT_COUNT];
	size_t gc_slm_addr = -1;
	uint16_t *root;
	size_t root_id;

	freebie_internal_map *freebie_map_entry = __get_freebie_map_entry(relation_id);
	if (freebie_map_entry == NULL) {
		// NVMEV_FREEBIE_DEBUG("Freebie map entry is NULL\n");
		return false;
	}

	if (freebie_map_entry->gc_task == NULL) {
		freebie_map_entry->gc_task = task;
		task->task_step = CCSD_TASK_WAIT;
		NVMEV_FREEBIE_DEBUG("Freebie GC Thread Created (relation_id: %u), going to sleep... \n", relation_id);
		return false;
	}

	// NVMEV_FREEBIE_DEBUG("Freebie GC Woke up (relation_id: %u) (id: %u)\n", relation_id, task->task_id);

	skip_root_id[0] = freebie_map_entry->root_id[0];
	skip_root_id[1] = freebie_map_entry->root_id[1];
	gc_slm_addr = freebie_map_entry->root_buffer_addr + ((FREEBIE_ROOT_SLOT_COUNT + 1) * FREEBIE_ROOT_CHUNK_SIZE);

	for (int i = 1; i < FREEBIE_ROOT_CHUNK_COUNT; i++) {
		if (i == skip_root_id[0] || i == skip_root_id[1]) {
			continue;
		}
		if (atomic_read(&freebie_map_entry->root_rc[i]) == ROOT_ALLOCATED) {
			root_id = i;
			// NVMEV_FREEBIE_DEBUG("Freebie GC: %lu\n", root_id);
			__freebie_request_read(task, gc_slm_addr, FREEBIE_ROOT_CHUNK_SIZE,
							root_id * FREEBIE_ROOT_CHUNK_SIZE);
			root = (uint16_t *)(gc_slm_addr);
			for (int j = 0; j < FREEBIE_MAX_VALID_ROOT_INDEX; j++) {
				if (root[j] != -1) {
					atomic_dec(&freebie_map_entry->data_rc[root[j]]);
				}
			}
			atomic_dec(&freebie_map_entry->root_rc[root_id]);
		}
	}

	// NVMEV_FREEBIE_DEBUG("Freebie GC done Going to sleep...\n");

	// Clear repartition count
	task->task_step = CCSD_TASK_WAIT;
    atomic_dec(&freebie_map_entry->gc_count);

	return false;
}
