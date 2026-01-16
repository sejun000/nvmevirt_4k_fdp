/**********************************************************************
 * Copyright (c) 2020-2021
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTIABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * *********************************************************************/

#include <linux/kthread.h>
#include <linux/ktime.h>
#include <linux/highmem.h>
#include <linux/sched/clock.h>
#include <linux/delay.h>
#include <linux/crypto.h>
#include <linux/vmalloc.h>

#include <linux/version.h>
#include <linux/bpf.h>
#include <linux/filter.h>

#include "nvmev.h"
#include "nvme_csd.h"
#include "csd_slm.h"
#include "csd_dispatcher.h"
#include "csd_user_func.h"
#include "csd_ftl.h"

#include "user_function/freebie/freebie_repartition.h"
#include "user_function/freebie/freebie_delta_mgr.h"
#include "user_function/freebie/freebie_functions.h"
#include "user_function/freebie/gobject.h"
#include "user_function/freebie/garray.h"

#undef PERF_DEBUG
#define PRP_PFN(x) ((unsigned long)((x) >> PAGE_SHIFT))

#define sq_entry(entry_id) sq->sq[SQ_ENTRY_TO_PAGE_NUM(entry_id)][SQ_ENTRY_TO_PAGE_OFFSET(entry_id)]
#define cq_entry(entry_id) cq->cq[CQ_ENTRY_TO_PAGE_NUM(entry_id)][CQ_ENTRY_TO_PAGE_OFFSET(entry_id)]

extern struct nvmev_dev *vdev;

static struct csd_io_req_table io_req_table;
static struct ccsd_task_table task_table;

static struct task_struct *csd_compute_workder[32];
static struct task_struct *csd_slm_workder[32];

static struct task_struct *csd_dispatcher_helper;

// For handling rocksdb magic command
static struct ccsd_magic_parameter magic_param;

// For handling freebie command
static struct ccsd_freebie_parameter freebie_param;

static inline unsigned long long __get_wallclock(void)
{
	return cpu_clock(vdev->config.cpu_nr_dispatcher[0]);
}

static void __copy_prp_data(int sqid, int sq_entry, void *buf, size_t size, bool from_host)
{
	struct nvmev_submission_queue *sq = vdev->sqes[sqid];
	u64 paddr;
	u64 *paddr_list = NULL;
	int prp_offs = 0;
	int prp2_offs = 0;

	size_t offset = 0;
	size_t remaining = size;

	while (remaining) {
		size_t mem_offs = 0;
		size_t io_size;
		void *vaddr;

		prp_offs++;
		if (prp_offs == 1) {
			paddr = sq_entry(sq_entry).rw.prp1;
		} else if (prp_offs == 2) {
			paddr = sq_entry(sq_entry).rw.prp2;
			if (remaining > PAGE_SIZE) {
				paddr_list = kmap_atomic_pfn(PRP_PFN(paddr)) + (paddr & PAGE_OFFSET_MASK);
				paddr = paddr_list[prp2_offs++];
			}
		} else {
			paddr = paddr_list[prp2_offs++];
		}

		vaddr = kmap_atomic_pfn(PRP_PFN(paddr));

		io_size = min_t(size_t, remaining, PAGE_SIZE);

		if (paddr & PAGE_OFFSET_MASK) {
			mem_offs = paddr & PAGE_OFFSET_MASK;
			if (io_size + mem_offs > PAGE_SIZE)
				io_size = PAGE_SIZE - mem_offs;
		}

		if (from_host == true) {
			memcpy(buf + offset, vaddr + mem_offs, io_size);
		} else {
			memcpy(vaddr + mem_offs, buf + offset, io_size);
		}
		kunmap_atomic(vaddr);

		remaining -= io_size;
		offset += io_size;
	}

	if (paddr_list != NULL)
		kunmap_atomic(paddr_list);
}

static size_t __calculate_lba(struct slm_lba_info *slm_info, size_t io_offset)
{
	int i = 0;
	size_t base_addr = 0;
	size_t local_offset = io_offset;

	if (io_offset == 0) {
		return slm_info->sre[0].saddr;
	}

	for (i = 0; i < slm_info->nentry; i++) {
		base_addr = slm_info->sre[i].saddr;
		if (local_offset < slm_info->sre[i].nByte) {
			break;
		}
		local_offset -= slm_info->sre[i].nByte;
	}

	NVMEV_DEBUG("__calculate_lba: i: %d, io_offset: %llu, base_addr: %llu, local_offset: %llu\n", i, io_offset,
				base_addr, local_offset);

	return base_addr + (local_offset / 512);
}

static size_t __calculate_local_offset(struct slm_lba_info *sre_info, size_t io_offset)
{
	int i = 0;
	size_t base_addr = 0;
	size_t local_offset = io_offset;

	for (i = 0; i < sre_info->nentry; i++) {
		base_addr = sre_info->sre[i].saddr;
		if (local_offset < sre_info->sre[i].nByte) {
			break;
		}
		local_offset -= sre_info->sre[i].nByte;
	}

	return (local_offset % 512);
}

static size_t __calculate_io_size(struct slm_lba_info *slm_info, size_t io_offset, size_t io_size)
{
	int i = 0;
	size_t base_addr = 0;
	size_t local_offset = io_offset;

	if (io_offset == 0) {
		if (io_size < slm_info->sre[0].nByte) {
			return io_size;
		}
		return slm_info->sre[0].nByte;
	}

	for (i = 0; i < slm_info->nentry; i++) {
		base_addr = slm_info->sre[i].saddr;
		if (local_offset < slm_info->sre[i].nByte) {
			break;
		}
		local_offset -= slm_info->sre[i].nByte;
	}

	if (local_offset + io_size > slm_info->sre[i].nByte) {
		NVMEV_DEBUG("__calculate_io_size: i: %d, io_offset: %llu, base_addr: %llu, local_offset: %llu, io_size: %llu\n",
					i, io_offset, base_addr, local_offset, io_size);
		return slm_info->sre[i].nByte - local_offset;
	}

	NVMEV_DEBUG("__calculate_io_size: i: %d, io_offset: %llu, base_addr: %llu, local_offset: %llu, io_size: %llu\n", i,
				io_offset, base_addr, local_offset, io_size);
	return io_size;
}

static void dump_sre_info(struct slm_lba_info *slm_info)
{
  printk("====== Dumping SRE Info ======\n");
  printk("Nentry: %d\n", slm_info->nentry);
  for (int i = 0; i < slm_info->nentry; i++) {
    printk("Saddr: %llu, Bytes: %u\n", slm_info->sre[i].saddr, slm_info->sre[i].nByte);
  }
  printk("====== ================ ======\n");
}

static void __enqueue_slm_work(unsigned int io_req_id)
{
	int i = 0;
	struct csd_internal_io_req *io_req = &(io_req_table.io_req[io_req_id]);
	int slm_turn = 0;
	struct ccsd_list *slm_list = NULL;
	int min = 0xFFFF;
	int min_index = 0;

	slm_turn = io_req_table.slm_turn;
	min_index = slm_turn;
	for (i = 0; i < task_table.num_slm_resources; i++) {
		slm_list = &(io_req_table.slm_list[slm_turn]);
		NVMEV_DEBUG("i:%d,slm_turn:%d,min:%d,min_index:%d,running:%d", i, slm_turn, min, min_index, slm_list->running);
		if (slm_list->running == 0)
			break;
		if (min > slm_list->running) {
			min = slm_list->running;
			min_index = slm_turn;
		}
		slm_turn = (slm_turn + 1) % task_table.num_slm_resources;
	}
	slm_list = &(io_req_table.slm_list[min_index]);

	if (slm_list->head == -1) {
		slm_list->head = io_req_id;
	} else {
		unsigned int tail = slm_list->tail;
		BUG_ON(tail == -1);

		io_req->prev = tail;
		io_req_table.io_req[tail].next = io_req_id;
	}
	slm_list->tail = io_req_id;
	slm_list->running = slm_list->running + 1;

	io_req_table.slm_turn = (io_req_table.slm_turn + 1) % task_table.num_slm_resources;
}

static void __enqueue_task_multi(struct ccsd_list *list, unsigned int start, unsigned int end, unsigned int count)
{
	struct ccsd_task_info *task_start = &(task_table.task[start]);
	struct ccsd_task_info *task_end = &(task_table.task[end]);

	task_start->prev = -1;
	task_end->next = -1;

	spin_lock(&list->lock);
	if (list->head == -1) {
		list->head = start;
	} else {
		unsigned int tail = list->tail;
		BUG_ON(tail == -1);

		task_start->prev = tail;
		task_table.task[tail].next = start;
	}
	list->tail = end;
	list->running = list->running + count;
	spin_unlock(&list->lock);
}

static void __dequeue_task_multi(struct ccsd_list *list, unsigned int start, unsigned int end, unsigned int count)
{
	struct ccsd_task_info *task_start = &(task_table.task[start]);
	struct ccsd_task_info *task_end = &(task_table.task[end]);
	unsigned int prev = task_start->prev;
	unsigned int next = task_end->next;

	task_start->prev = -1;
	task_end->next = -1;

	spin_lock(&list->lock);
	if (list->head == start) {
		list->head = next;
	}

	if (list->tail == end) {
		list->tail = prev;
	}

	if (next != -1) {
		task_table.task[next].prev = prev;
	}

	if (prev != -1) {
		task_table.task[prev].next = next;
	}

	if (list->running < count) {
		NVMEV_ERROR("QUEUE_COUNT_ERROR : %d %d", list->running, count);
	}
	list->running = list->running - count;
	spin_unlock(&list->lock);
}

static void __enqueue_task(struct ccsd_list *list, unsigned int task_id)
{
	__enqueue_task_multi(list, task_id, task_id, 1);
}

static void __dequeue_task(struct ccsd_list *list, unsigned int task_id)
{
	__dequeue_task_multi(list, task_id, task_id, 1);
}

static int __pop_head_task(struct ccsd_list *list)
{
	int task_id;
	struct ccsd_task_info *task;

	spin_lock(&list->lock);

	task_id = list->head;
	if (task_id == -1) {
		spin_unlock(&list->lock);
		return -1;
	}

	task = &(task_table.task[task_id]);
	list->head = task->next;

	if (list->head == -1) {
		list->tail = -1;
	} else {
		task_table.task[list->head].prev = -1;
	}

	task->prev = -1;
	task->next = -1;

	if (list->running < 1) {
		NVMEV_ERROR("QUEUE_COUNT_ERROR : %d %d", list->running, 1);
	}
	list->running = list->running - 1;

	spin_unlock(&list->lock);

	return task_id;
}

static void __enqueue_compute_work(unsigned int task_id, unsigned int compute_turn)
{
	struct ccsd_list *comp_list = NULL;
	struct ccsd_task_info *task = &(task_table.task[task_id]);

	comp_list = &(task_table.comp_list[compute_turn]);
	__enqueue_task(comp_list, task_id);
}

static bool __find_idle_compute_core(unsigned int *compute_core_id)
{
	struct ccsd_list *comp_list = NULL;

	for (int i = 0; i < task_table.num_cpu_resources; i++) {
		comp_list = &(task_table.comp_list[i]);
		if (comp_list->running == 0) {
			*compute_core_id = i;
			return true;
		}
	}

	return false;
}

static void __enqueue_io_req_free_list(unsigned int start, unsigned int end)
{
	struct csd_internal_io_req *io_req = &(io_req_table.io_req[end]);
	struct ccsd_list *free_list = &(io_req_table.free_list);

	if (io_req->next != -1) {
		io_req_table.io_req[io_req->next].prev = -1;
	}
	io_req->next = -1;

	io_req = &(io_req_table.io_req[start]);

	if (free_list->head == -1) {
		io_req->prev = -1;
		free_list->head = start;
	} else {
		unsigned int tail = free_list->tail;
		BUG_ON(tail == -1);

		io_req->prev = tail;
		io_req_table.io_req[tail].next = start;
	}
	free_list->tail = end;
}

static void __dump_all_task_status(void)
{
    for (int i = 0; i < MAX_TASK_COUNT; i++) {
        struct ccsd_task_info *task = &(task_table.task[i]);
        if (task->task_step != CCSD_TASK_FREE) {
			printk("[=============================]");
            printk("[DEBUG] task id: %u\n", task->task_id);
            printk("[DEBUG] task name (if any) %s\n", task->task_name);
            printk("[DEBUG] task step: %u\n", task->task_step);
            printk("[DEBUG] task program index: %u\n", task->program_idx);
            printk("[DEBUG] task input buf addr: %lu\n", task->input_buf_addr);
            printk("[DEBUG] task has master: %u\n", task->has_master);
            printk("[DEBUG] task has buddy: %u\n", task->has_buddy);
            printk("[DEBUG] task proc: %u\n", task->proc_idx);
			printk("[=============================]");
        }
    }
}

static bool __do_perform_program_management(int sqid, int sq_entry, unsigned int *status)
{
#ifdef CSD_eBPF_ENABLE
#if (CSD_eBPF_ENABLE == 1)
	struct nvmev_submission_queue *sq = vdev->sqes[sqid];
	struct nvme_command_csd *cmd = (struct nvme_command_csd *)(&sq_entry(sq_entry));
	unsigned int ebpf_fd = cmd->load_program.pind;

	struct bpf_prog *bpf_prog = NULL;
	bpf_prog = bpf_prog_get_type(ebpf_fd, BPF_PROG_TYPE_CSLCSD);
	NVMEV_CSD_INFO("COM", "ebpf_fd:%u, BPF_PROG_TYPE_CSLCSD:%u, bpf_prog:%p", ebpf_fd, BPF_PROG_TYPE_CSLCSD, bpf_prog);
	if (IS_ERR(bpf_prog)) {
		pr_err("err: bpf_prog_get: %ld\n", PTR_ERR(bpf_prog));
		return PTR_ERR(bpf_prog);
	}
#endif
#endif
	return true;
}

static bool __do_perform_memory_management(int sqid, int sq_entry, unsigned int *status)
{
	struct nvmev_submission_queue *sq = vdev->sqes[sqid];
	struct nvme_command_csd *cmd = (struct nvme_command_csd *)(&sq_entry(sq_entry));
	struct memory_region_entry entries;
	unsigned int i = 0;
	unsigned int type = cmd->memory_management.sel;
	unsigned int nEntry = cmd->memory_management.numr;

	if (type == nvme_memory_range_create) {
	} else if (type == nvme_memory_range_delete) {
	} else {
		NVMEV_ERROR("[%s] invalid memory management parameter (%d)", __FUNCTION__, type);
	}

	if (nEntry != 1) {
		NVMEV_ERROR("[%s] invalid memory management parameter (%d, %d)", __FUNCTION__, type, nEntry);
		BUG();
	}

	__copy_prp_data(sqid, sq_entry, &entries, sizeof(struct memory_region_entry), true);
	for (i = 0; i < nEntry; i++) {
		if (type == nvme_memory_range_create) {
			size_t len = entries.nByte;
			size_t slm_offset = alloc_slm_range(len);

			if (slm_offset == -1) {
				NVMEV_ERROR("[%s] SLM Allocation failed\n", __FUNCTION__);
				*status = (NVME_SCT_CMD_SPECIFIC_STATUS << 8) | NVME_SC_MAX_MEM_RANGE_EXCEEDED;
				return false;
			}

			entries.saddr = get_slm_offset(slm_offset);
			NVMEV_CSD_INFO("SLM", "Allocate buffer:%llu, %lu", entries.saddr, len);
			__copy_prp_data(sqid, sq_entry, &entries, sizeof(struct memory_region_entry), false);
		} else if (type == nvme_memory_range_delete) {
			size_t addr = get_slm_addr(entries.saddr);
			NVMEV_CSD_INFO("SLM", "Release host buffer:%llu", entries.saddr);

			// If there is a task using this slm_lba_info,
			// we should also set the 'task->slm_lba_info' to NULL as well
			// Or else, the 'task->slm_lba_info' will point to some random kernel address
			// This cannot be distinguished anywhere else
			uint32_t task_id = get_io_task_from_slm_addr(addr);
			if (task_id != -1) {
				NVMEV_CSD_INFO("SLM", "Set SLM_LBA_INFO to NULL for task : %llu", task_id);
				struct ccsd_task_info *task = NULL;
				task = &(task_table.task[task_id]);
				task->slm_lba_info = NULL;
			}

			free_slm_range(addr);
		}
	}
	return true;
}

static inline int allocate_new_task(struct ccsd_task_info **task)
{
	int task_id;
	task_id = task_table.free_list.head;
	if (task_id == -1) {
		printk("Task allocation failed\n");
		return -1;
	}
	BUG_ON(task_id > MAX_TASK_COUNT);

	*task = &(task_table.task[task_id]);
	if ((*task)->task_step != CCSD_TASK_FREE) {
		NVMEV_ERROR("Free list is corrupted (Task id:%u, Task name: %s, Task step:%u, Program Index: %u, Input Buf Addr: %lu, Output Buf Addr: %lu)",
								task_id, (*task)->task_name, (*task)->task_step, (*task)->program_idx,
								(*task)->input_buf_addr, (*task)->output_buf_addr);
		BUG();
	}
	__dequeue_task(&task_table.free_list, task_id);

	(*task)->task_id = task_id;
	// (*task)->task_step = CCSD_TASK_SCHEDULE;
	(*task)->requested_io_offset = 0;
	(*task)->total_size = 0;
	(*task)->done_size = 0;
	(*task)->input_buf_addr = -1;
	(*task)->output_buf_addr = -1;
	(*task)->result = 0;
	(*task)->status = (NVME_SCT_GENERIC_CMD_STATUS << 8) | NVME_SC_SUCCESS;
	(*task)->internal_io_count = 0;
	(*task)->ruh = 0;
	(*task)->prev = -1;
	(*task)->next = -1;
	(*task)->task_name[0] = '\0';
	(*task)->program_idx = -1;

	(*task)->buddy_task = false;
	(*task)->has_master = false;
	(*task)->is_delete = false;
	(*task)->is_input_helper = false;
	(*task)->has_buddy = false;
	(*task)->has_helper = false;
	(*task)->can_terminate = false;
	(*task)->copy_helper_count = 0;
	(*task)->shared = NULL;

	return task_id;
}

// We are going to limit the maximum repartition running # to the # of CSD cores
int currently_running_repartition = 0;

static bool __do_perform_freebie_command(int sqid, int sq_entry, unsigned int proc_idx, unsigned int *status, size_t *result)
{
	struct nvmev_submission_queue *sq = vdev->sqes[sqid];
	struct nvme_command_csd *cmd = (struct nvme_command_csd *)(&sq_entry(sq_entry));
	int compute_core_id = 0;
	int program_idx = cmd->execute_program.pind;
	int opcode = cmd->common.opcode;
	struct ccsd_task_info *task = NULL;
	struct ccsd_task_info *slm_buddy_task = NULL;
	struct slm_lba_info *slm_lba_info = NULL;
	size_t task_id = 0;
	size_t slm_buddy_task_id = 0;

	// Don't copy PRP if you can't run it! Reduce latency cost
	if (currently_running_repartition > MAX_PARALLEL_REPARTITION) {
		// NVMEV_FREEBIE_DEBUG("[ERROR] Currently more than maximum parallel repartition tasks are running\n");
		return false;
	}

	task_id = allocate_new_task(&task);
	if (task_id == -1) {
		return false;
	}
	task->host_id = cmd->memory.cdw14;
	task->program_idx = program_idx;
	task->proc_idx = proc_idx;
	task->opcode = opcode;

	switch (program_idx) {
		case FREEBIE_REPART_INDEX:	
		{
			int task_idx = 0;
			struct source_range_entry *sre= NULL;

			size_t copy_helper_task_id[FREEBIE_MAX_INPUT_FILE + FREEBIE_MAX_SPAN_OUT];
			struct ccsd_task_info *copy_helper_task[FREEBIE_MAX_INPUT_FILE + FREEBIE_MAX_SPAN_OUT];
			size_t copy_helper_slm_range[FREEBIE_MAX_INPUT_FILE + FREEBIE_MAX_SPAN_OUT];

			size_t exec_helper_task_id;
			struct ccsd_task_info *exec_helper_task;
			size_t exec_helper_slm_range[FREEBIE_MAX_SPAN_OUT];
			int nr_helper_task = NR_HELPER_TASK;
			BUG_ON(nr_helper_task > 1);

			__copy_prp_data(sqid, sq_entry, &freebie_param, sizeof(struct ccsd_freebie_parameter), true);

			// Parse the freebie_param
			struct freebie_params *freebie_params = (struct freebie_params *)(freebie_param.param);
			int input_file_cnt = freebie_params->input_file_cnt;
			int output_file_cnt = freebie_params->spread_factor;
			int output_file_level = freebie_params->current_level + 1;

			if (freebie_params->relation_id == DELETE_RELATION_ID) {
				// Don't use helper task for Relation that contains tombstone
				nr_helper_task = 0;
				task->is_delete = true;
			}

			NVMEV_FREEBIE_DEBUG("FreeBie Repartition Start (Relation ID: %d) (current Level: %d)"
										"(partition no: %d) (input_file_cnt: %d)"
                                        "(output_file_cnt: %d) (Helper Tasks: %d)\n",
										freebie_params->relation_id, freebie_params->current_level,
                                        freebie_params->partition_no, input_file_cnt, output_file_cnt,
										nr_helper_task);

			// if (currently_running_repartition > MAX_PARALLEL_REPARTITION) {
			// 	NVMEV_FREEBIE_DEBUG("[ERROR on Relation ID %d] There is no resource left\n",
			// 						freebie_params->relation_id);
			// 	__enqueue_task(&task_table.free_list, task_id);
			// 	return false;
			// }

			// Get the slm_lba_info from setup cmd
			slm_lba_info = freebie_get_slm_lba_info(freebie_params->relation_id);
			if (slm_lba_info == NULL) {
				NVMEV_FREEBIE_DEBUG("[ERROR on Relation ID %d] There is no SLM LBA Info "
										"are you sure you've sent setup first?\n",
										freebie_params->relation_id);
				__enqueue_task(&task_table.free_list, task_id);
				return false;
			}
			
			// Allocate all the required tasks & SLMs
			// Return failure when there are not enough tasks or SLM space
			slm_buddy_task_id = allocate_new_task(&slm_buddy_task);
			if (slm_buddy_task_id == -1) {
				NVMEV_FREEBIE_DEBUG("[ERROR on Relation ID %d] SLM BUDDY Task allocation failed\n",
										freebie_params->relation_id);
				__enqueue_task(&task_table.free_list, task_id);
				return false;
			}

			// Create copy helper tasks
			for (task_idx = 0 ; task_idx < input_file_cnt + output_file_cnt; task_idx++) {
				copy_helper_task_id[task_idx] = allocate_new_task(&copy_helper_task[task_idx]);
				if (copy_helper_task_id[task_idx] == -1) {
					NVMEV_FREEBIE_DEBUG("[ERROR on Relation ID %d] COPY HELPER Task allocation failed\n",
										freebie_params->relation_id);
					__enqueue_task(&task_table.free_list, task_id);
					__enqueue_task(&task_table.free_list, slm_buddy_task_id);
					for (int i = 0; i < task_idx; i++) {
						__enqueue_task(&task_table.free_list, copy_helper_task_id[i]);
					}
					return false;
				}
			}

			// Create exec helper tasks
			if (nr_helper_task != 0) {
				exec_helper_task_id = allocate_new_task(&exec_helper_task);
				if (exec_helper_task_id == -1) {
					NVMEV_FREEBIE_DEBUG("[ERROR on Relation ID %d] EXEC HELPER Task allocation failed\n",
										freebie_params->relation_id);
					__enqueue_task(&task_table.free_list, task_id);
					__enqueue_task(&task_table.free_list, slm_buddy_task_id);
					for (int i = 0; i < input_file_cnt + output_file_cnt; i++) {
						__enqueue_task(&task_table.free_list, copy_helper_task_id[i]);
					}
					return false;
				}
			}

			// Allocate SLM ranges for the copy helper tasks
			// Copy input tasks
			for (task_idx = 0; task_idx < input_file_cnt; task_idx++) {
				copy_helper_slm_range[task_idx] = alloc_slm_range(freebie_params->input_buf_size[task_idx]);
				if (copy_helper_slm_range[task_idx] == -1) {
					NVMEV_FREEBIE_DEBUG("[ERROR on Relation ID %d] Input SLM Allocation failed (%d)\n",
										freebie_params->relation_id, freebie_params->input_buf_size[task_idx]);
					__enqueue_task(&task_table.free_list, task_id);
					__enqueue_task(&task_table.free_list, slm_buddy_task_id);
					for (int i = 0; i < task_idx; i++) {
						free_slm_range(copy_helper_slm_range[i]);
					}
					for (int i = 0; i < input_file_cnt + output_file_cnt; i++) {
						__enqueue_task(&task_table.free_list, copy_helper_task_id[i]);
					}

					if (nr_helper_task != 0) {
						__enqueue_task(&task_table.free_list, exec_helper_task_id);
					}
					return false;
				}
			}

			// Copy output tasks
			for (task_idx = 0 ; task_idx < output_file_cnt; task_idx++) {
				copy_helper_slm_range[input_file_cnt + task_idx] = alloc_slm_range(freebie_params->output_buf_alloc_size[task_idx]);
				if (copy_helper_slm_range[input_file_cnt + task_idx] == -1) {
					NVMEV_FREEBIE_DEBUG("[ERROR on Relation ID %d] Output SLM Allocation failed (%d)\n",
										freebie_params->relation_id, freebie_params->output_buf_alloc_size[task_idx]);

					__enqueue_task(&task_table.free_list, task_id);
					__enqueue_task(&task_table.free_list, slm_buddy_task_id);
					for (int i = 0; i < input_file_cnt + task_idx; i++) {
						free_slm_range(copy_helper_slm_range[i]);
					}
					for (int i = 0; i < input_file_cnt + output_file_cnt; i++) {
						__enqueue_task(&task_table.free_list, copy_helper_task_id[i]);
					}
					if (nr_helper_task != 0) {
						__enqueue_task(&task_table.free_list, exec_helper_task_id);
					}
					return false;
				}
			}

			// Exec tasks (intermediate buffer)
			if (nr_helper_task != 0) {
				for (int file_idx = 0; file_idx < output_file_cnt; file_idx++) {
					exec_helper_slm_range[file_idx]
								= alloc_slm_range(freebie_params->output_buf_alloc_size[file_idx]);
					if (exec_helper_slm_range[file_idx] == -1) {
						NVMEV_FREEBIE_DEBUG("[ERROR on Relation ID %d] Intermediate SLM Allocation failed (%d)\n",
										freebie_params->relation_id, freebie_params->output_buf_alloc_size[file_idx]);

						__enqueue_task(&task_table.free_list, task_id);
						__enqueue_task(&task_table.free_list, slm_buddy_task_id);
						for (int i = 0; i < file_idx ; i++) {
							free_slm_range(exec_helper_slm_range[i]);
						}
   	                	for (int i = 0; i < input_file_cnt + output_file_cnt; i++) {
   	                	    free_slm_range(copy_helper_slm_range[i]);
   	                	}
						for (int i = 0; i < input_file_cnt + output_file_cnt; i++) {
							__enqueue_task(&task_table.free_list, copy_helper_task_id[i]);
						}
						__enqueue_task(&task_table.free_list, exec_helper_task_id);
						return false;
					}
				}
			}

			// There is no failure after this point!!!

			// Fill in Buddy task (This updates metadata)
			slm_buddy_task->host_id = cmd->memory.cdw14;
			slm_buddy_task->task_step = CCSD_TASK_WAIT;
			slm_buddy_task->program_idx = COPY_TO_SLM_PROGRAM_INDEX;
			slm_buddy_task->proc_idx = proc_idx;
			slm_buddy_task->opcode = opcode;
			slm_buddy_task->slm_lba_info = slm_lba_info;
			strcpy(slm_buddy_task->task_name, "Meta update");

			slm_buddy_task->has_buddy = true;
			slm_buddy_task->buddy_task = task;

			// Fill in Repartition task
			task->task_step = CCSD_TASK_SCHEDULE;
			task->has_helper = true;
			task->copy_helper_count = output_file_cnt;
			bitmap_zero(task->helper_bitmap, MAX_TASK_COUNT);

			task->has_buddy = true;
			task->buddy_task = slm_buddy_task;

			// Fill in exec helper tasks
			if (nr_helper_task != 0) {
				// Create shared memory space for exec helper task and master task to communicate 
				// This is going to be freed when the helper task terminates
				FreeBIEShared *shared_data = (FreeBIEShared *)kmalloc_node(sizeof(FreeBIEShared), GFP_KERNEL, 1); 
				shared_data->child_idxs = NULL;
				shared_data->delta_list = NULL;
				shared_data->delta_ht = NULL;
				shared_data->delta_mgr = NULL;

 				// Use this for the helper taskto communicate with master task
				bitmap_zero(exec_helper_task->helper_bitmap, MAX_TASK_COUNT);

				exec_helper_task->shared = shared_data;
				task->shared = shared_data;

				exec_helper_task->has_master = true;
				exec_helper_task->master_task = task;

				exec_helper_task->host_id = cmd->memory.cdw14;
				exec_helper_task->task_step = CCSD_TASK_WAIT;
				exec_helper_task->program_idx = program_idx;
				exec_helper_task->proc_idx = proc_idx;
				exec_helper_task->opcode = opcode;

				task->exec_helper_task = exec_helper_task;
			}

			// Fill in the copy helper tasks
			// Input
			for (task_idx = 0; task_idx < input_file_cnt; task_idx++) {
				int input_sre_count = freebie_param.input_sre_count[task_idx];
				BUG_ON(input_sre_count == 0);
				sre = (struct source_range_entry *)vmalloc_node(sizeof(struct source_range_entry) * input_sre_count, 1);
				memcpy(sre, freebie_param.input_sre[task_idx], sizeof(struct source_range_entry) * input_sre_count);
				slm_lba_info = alloc_slm_lba_info(copy_helper_slm_range[task_idx], freebie_params->input_buf_size[task_idx],
												freebie_params->input_buf_size[task_idx],
												copy_helper_task_id[task_idx], sre, input_sre_count);

				copy_helper_task[task_idx]->input_buf_addr = copy_helper_slm_range[task_idx];
				copy_helper_task[task_idx]->slm_lba_info = slm_lba_info;

				copy_helper_task[task_idx]->host_id = cmd->memory.cdw14;
				copy_helper_task[task_idx]->task_step = CCSD_TASK_SCHEDULE;
				copy_helper_task[task_idx]->program_idx = COPY_TO_SLM_PROGRAM_INDEX;
				copy_helper_task[task_idx]->proc_idx = proc_idx;
				copy_helper_task[task_idx]->opcode = opcode;
				copy_helper_task[task_idx]->total_size = freebie_params->input_buf_size[task_idx];
				copy_helper_task[task_idx]->requested_io_offset = 0;
				strcpy(copy_helper_task[task_idx]->task_name, "INPUT_Copy_Helper");

				copy_helper_task[task_idx]->has_master = true;
				copy_helper_task[task_idx]->is_input_helper = true;
				copy_helper_task[task_idx]->can_terminate = true;
				copy_helper_task[task_idx]->master_task = task;

				// Set bit
				set_bit(copy_helper_task_id[task_idx], task->helper_bitmap);
			}

			// Output
			for (task_idx = input_file_cnt; task_idx < input_file_cnt + output_file_cnt; task_idx++) {
				int output_sre_count = freebie_param.output_sre_count[task_idx - input_file_cnt];
				sre = (struct source_range_entry *)vmalloc_node(sizeof(struct source_range_entry) * output_sre_count, 1);
				memcpy(sre, freebie_param.output_sre[task_idx - input_file_cnt], sizeof(struct source_range_entry) * output_sre_count);
				slm_lba_info = alloc_slm_lba_info(copy_helper_slm_range[task_idx], freebie_params->output_buf_alloc_size[task_idx - input_file_cnt],
												freebie_params->output_buf_alloc_size[task_idx - input_file_cnt],
												copy_helper_task_id[task_idx], sre, output_sre_count);
				copy_helper_task[task_idx]->input_buf_addr = copy_helper_slm_range[task_idx];
				copy_helper_task[task_idx]->slm_lba_info = slm_lba_info;

				copy_helper_task[task_idx]->host_id = cmd->memory.cdw14;
				copy_helper_task[task_idx]->task_step = CCSD_TASK_SCHEDULE;
				copy_helper_task[task_idx]->program_idx = COPY_TO_SLM_PROGRAM_INDEX;
				copy_helper_task[task_idx]->proc_idx = proc_idx;
				copy_helper_task[task_idx]->opcode = opcode;
				copy_helper_task[task_idx]->total_size = freebie_params->output_buf_size[task_idx - input_file_cnt];
				copy_helper_task[task_idx]->requested_io_offset = 0;
				strcpy(copy_helper_task[task_idx]->task_name, "OUTPUT_Copy_Helper");
				// The RUH is determined by the output file target level

				if (output_file_level < NR_MAX_LEVEL) {
					copy_helper_task[task_idx]->ruh = output_file_level;
				} else {
					copy_helper_task[task_idx]->ruh = NR_MAX_LEVEL - 1;
				}

				copy_helper_task[task_idx]->has_master = true;
				copy_helper_task[task_idx]->can_terminate = false;
				copy_helper_task[task_idx]->master_task = task;

				// Only the output file tasks are helper tasks!! (Going to reuse them after repartition)
				task->copy_helper_task[task_idx - input_file_cnt] = copy_helper_task[task_idx];

				if (copy_helper_task[task_idx]->total_size == 0) {
					// If output file is 0, Go to wait state directly
					copy_helper_task[task_idx]->task_step = CCSD_TASK_WAIT;
				}
				else {
					set_bit(copy_helper_task_id[task_idx], task->helper_bitmap);
				}
			}

			// Fill in SLM buffer address for repartition to access
			for (task_idx = 0; task_idx < input_file_cnt; task_idx++) {
				freebie_params->input_buf[task_idx] = copy_helper_slm_range[task_idx];
			}
			for (task_idx = 0; task_idx < output_file_cnt; task_idx++) {
				freebie_params->output_buf[task_idx] = copy_helper_slm_range[input_file_cnt + task_idx];
			}

			task->param_size = freebie_param.param_size;
			memcpy(task->params, freebie_param.param, freebie_param.param_size);

			// Fill in SLM buffer address for exec helper tasks to access
			if (nr_helper_task != 0) {
				for (int file_idx = 0; file_idx < output_file_cnt; file_idx++) {
					freebie_params->output_buf[file_idx] = exec_helper_slm_range[file_idx];
					freebie_params->output_buf_size[file_idx] = 0;
				}
				exec_helper_task->param_size = freebie_param.param_size;
				memcpy(exec_helper_task->params, freebie_param.param, freebie_param.param_size);
			}

			// Enqueue Buddy, copy helper tasks
			compute_core_id = task_table.copy_to_slm_list_id;
			__enqueue_compute_work(slm_buddy_task_id, compute_core_id);
			for(task_idx = 0; task_idx < input_file_cnt + output_file_cnt; task_idx++) {
				__enqueue_compute_work(copy_helper_task_id[task_idx], compute_core_id);
			}

            // BUG(0810) - The main task and the helper task should not target the
            // same Core. Since it does busy waiting.
            if (__find_idle_compute_core(&compute_core_id) == false) {
	        	compute_core_id = task_table.compute_turn;
	        	task_table.compute_turn = (task_table.compute_turn + 1) % task_table.num_cpu_resources;
	        }

			if (nr_helper_task != 0) {
            	int cores_per_task = task_table.num_cpu_resources / 2;
            	BUG_ON(cores_per_task == 0);

            	compute_core_id = compute_core_id % cores_per_task;
				__enqueue_compute_work(exec_helper_task_id, compute_core_id + cores_per_task);
				NVMEV_FREEBIE_DEBUG("Enqueue FreeBie Repartition Helper Task(%d) (Core ID: %d)\n",
                        0, compute_core_id + cores_per_task);
			}
	        __enqueue_compute_work(task_id, compute_core_id);
	        NVMEV_FREEBIE_DEBUG("Enqueue FreeBie Repartition Main Task (Core ID: %d) (main task id: %lu) (helper task id: %lu)\n", compute_core_id, task_id, exec_helper_task_id);
			currently_running_repartition++;

			atomic64_inc(&vdev->repartition_command_success_count);
			break;
		}

		case FREEBIE_SETUP_INDEX:
		{
			// Allocate an SLM range for the slm root (SIZE: 4 * FREEBIE_ROOT_CHUNK_SIZE)
			// 2 for ROOT BUFFER, 1 for Hidden Buffer, 1 for Garbage Buffer
			// Copy the SRE values to the task->params
			struct source_range_entry *sre;
			size_t nentry = cmd->memory_copy.format;
			size_t root_buffer = -1;
			uint32_t relation_id = cmd->common.cdw10[3];

			size_t gc_task_id = 0;
			size_t gc_buddy_task_id = 0;
			struct ccsd_task_info *gc_task = NULL;
			struct ccsd_task_info *gc_buddy_task = NULL;


			root_buffer = alloc_slm_range(FREEBIE_ROOT_CHUNK_SIZE * 4);
			if (root_buffer == -1) {
				NVMEV_FREEBIE_DEBUG("FreeBie Setup: SLM Allocation failed\n");
				__enqueue_task(&task_table.free_list, task_id);
				return false;
			}

			// Allocate a task for slm access
			slm_buddy_task_id = allocate_new_task(&slm_buddy_task);
			if (slm_buddy_task_id == -1) {
				NVMEV_FREEBIE_DEBUG("FreeBie Setup: SLM buddy allocation failed\n");
				__enqueue_task(&task_table.free_list, task_id);
				free_slm_range(root_buffer);
				return false;
			}
			// Allocate Two task for one for GC and one for GC Buddy
			gc_task_id = allocate_new_task(&gc_task);
			if (gc_task_id == -1) {
				NVMEV_FREEBIE_DEBUG("FreeBie Setup: GC allocation failed\n");
				__enqueue_task(&task_table.free_list, task_id);
				__enqueue_task(&task_table.free_list, slm_buddy_task_id);
				free_slm_range(root_buffer);
				return false;
			}

			gc_buddy_task_id = allocate_new_task(&gc_buddy_task);
			if (gc_buddy_task_id == -1) {
				NVMEV_FREEBIE_DEBUG("FreeBie Setup: GC buddy allocation failed\n");
				__enqueue_task(&task_table.free_list, task_id);
				__enqueue_task(&task_table.free_list, slm_buddy_task_id);
				__enqueue_task(&task_table.free_list, gc_task_id);
				free_slm_range(root_buffer);
				return false;
			}

			sre = (struct source_range_entry *)vmalloc_node(sizeof(struct source_range_entry) * nentry, 1);
			__copy_prp_data(sqid, sq_entry, sre, sizeof(struct source_range_entry) * nentry, true);

			slm_lba_info = alloc_slm_lba_info(root_buffer, FREEBIE_ROOT_CHUNK_SIZE * 4, FREEBIE_ROOT_CHUNK_SIZE * 4,
										task_id, sre, nentry);
			task->slm_lba_info = slm_lba_info;
			task->input_buf_addr = relation_id;

			slm_buddy_task->host_id = cmd->memory.cdw14;
			slm_buddy_task->program_idx = COPY_TO_SLM_PROGRAM_INDEX;
			slm_buddy_task->proc_idx = proc_idx;
			slm_buddy_task->opcode = opcode;
			slm_buddy_task->slm_lba_info = slm_lba_info;
			strcpy(slm_buddy_task->task_name, "Meta update");

			// Set two tasks as buddies
			slm_buddy_task->buddy_task = task;
			slm_buddy_task->has_buddy = true;

			task->buddy_task = slm_buddy_task;
			task->has_buddy = true;

			gc_task->host_id = cmd->memory.cdw14;
			gc_task->program_idx = FREEBIE_GARBAGE_COLLECT_INDEX;
			gc_task->proc_idx = proc_idx;
			gc_task->opcode = opcode;
			gc_task->slm_lba_info = slm_lba_info;
			strcpy(gc_task->task_name, "GC");

			gc_task->input_buf_addr = relation_id;

			gc_buddy_task->host_id = cmd->memory.cdw14;
			gc_buddy_task->program_idx = COPY_TO_SLM_PROGRAM_INDEX;
			gc_buddy_task->proc_idx = proc_idx;
			gc_buddy_task->opcode = opcode;
			gc_buddy_task->slm_lba_info = slm_lba_info;
			strcpy(gc_buddy_task->task_name, "GC BUDDY");

			// Set two tasks as buddies
			gc_buddy_task->buddy_task = gc_task;
			gc_buddy_task->has_buddy = true;

			gc_task->buddy_task = gc_buddy_task;
			gc_task->has_buddy = true;

			task->task_step = CCSD_TASK_SCHEDULE;
			slm_buddy_task->task_step = CCSD_TASK_WAIT;
			gc_task->task_step = CCSD_TASK_SCHEDULE;
			gc_buddy_task->task_step = CCSD_TASK_WAIT;

			compute_core_id = task_table.copy_to_slm_list_id;
			__enqueue_compute_work(slm_buddy_task_id, compute_core_id);
			__enqueue_compute_work(gc_buddy_task_id, compute_core_id);

			if (__find_idle_compute_core(&compute_core_id) == false) {
				compute_core_id = task_table.compute_turn;
				task_table.compute_turn = (task_table.compute_turn + 1) % task_table.num_cpu_resources;
			}
			__enqueue_compute_work(gc_task_id, compute_core_id);

		    NVMEV_FREEBIE_DEBUG("Enqueue FreeBie GC Task (Core ID: %d) (task_id %lu)\n", compute_core_id, gc_task_id);

            if (__find_idle_compute_core(&compute_core_id) == false) {
				compute_core_id = task_table.compute_turn;
				task_table.compute_turn = (task_table.compute_turn + 1) % task_table.num_cpu_resources;
			}
			__enqueue_compute_work(task_id, compute_core_id);

		    NVMEV_FREEBIE_DEBUG("Enqueue FreeBie Setup Task (Core ID: %d) (task_id %lu)\n", compute_core_id, task_id);

			break;
		}
		case FREEBIE_RELEASE_CAT_INDEX:
		{
			uint32_t relation_id = cmd->common.cdw10[2];
			uint32_t root_id = cmd->common.cdw10[3];
			task->task_step = CCSD_TASK_SCHEDULE;
			task->input_buf_addr = relation_id;
			task->output_buf_addr = root_id;

            if (__find_idle_compute_core(&compute_core_id) == false) {
				compute_core_id = task_table.compute_turn;
				task_table.compute_turn = (task_table.compute_turn + 1) % task_table.num_cpu_resources;
			}
			__enqueue_compute_work(task_id, compute_core_id);
		    // NVMEV_FREEBIE_DEBUG("Enqueue FreeBie Release Task (Core ID: %d)\n", compute_core_id);

			break;
		}
		
		case FREEBIE_TERMINATE_INDEX:
		{
			uint32_t relation_id = cmd->common.cdw10[2];
			task->task_step = CCSD_TASK_SCHEDULE;
			task->input_buf_addr = relation_id;
            
            if (__find_idle_compute_core(&compute_core_id) == false) {
				compute_core_id = task_table.compute_turn;
				task_table.compute_turn = (task_table.compute_turn + 1) % task_table.num_cpu_resources;
			}
			__enqueue_compute_work(task_id, compute_core_id);
		    NVMEV_FREEBIE_DEBUG("Enqueue FreeBie Terminate Task (Core ID: %d)\n", compute_core_id);

			break;
		}

        case FREEBIE_DEBUG_INDEX:
        {
			printk("START FREEBIE DEBUG\n");
            check_slm_allocated_status();
            __dump_all_task_status();
			check_freebie_map();
			check_g_array();
			check_g_object();
			check_delta_list_entry_pool();
			printk("END FREEBIE DEBUG\n");
			
            break;
        }

		default:
			NVMEV_FREEBIE_DEBUG("Unknown FreeBie command %d\n", program_idx);
			return false;
	}

	return true;
}

#if 0
static bool __do_perform_dispatch(int sqid, int sq_entry, unsigned int proc_idx, unsigned int *status, size_t *result)
{
	struct nvmev_submission_queue *sq = vdev->sqes[sqid];
	struct nvme_command_csd *cmd = (struct nvme_command_csd *)(&sq_entry(sq_entry));

	struct ccsd_parameter param;

	size_t input_size = 0;
	int nentry;

	int compute_core_id = 0;
	int opcode = cmd->common.opcode;
	int program_idx;
	int task_id = task_table.free_list.head;
	struct ccsd_task_info *task = NULL;

	size_t lba = 0;
	size_t requested_io_offset = 0;
	size_t input_buf_addr = 0;
	size_t output_buf_addr = 0;

	if (task_id == -1) {
		// No free task
		return false;
	}
	BUG_ON(task_id > MAX_TASK_COUNT);

	task = &(task_table.task[task_id]);
	if (task->task_step != CCSD_TASK_FREE) {
		NVMEV_ERROR("Free list is corrupted (Task id:%u, Task step:%u)", task_id, task->task_step);
		BUG();
	}

	if (opcode == nvme_cmd_execute_program) {
		__copy_prp_data(sqid, sq_entry, &param, sizeof(struct ccsd_parameter), true);

		lba = 0;
		input_size = param.nByte;
		program_idx = cmd->execute_program.pind;

		requested_io_offset = param.nByte;

		input_buf_addr = get_slm_addr(param.input_slm);
		output_buf_addr = get_slm_addr(param.output_slm);

		if (input_size >= 0xFFFFFFFF) {
			return true;
		}

	} else if (opcode == nvme_cmd_memory_copy) {
		unsigned int io_task_id = -1;
		struct source_range_entry *sre;
		bool is_copy_to_slm = cmd->memory_copy.cdw14;
		nentry = cmd->memory_copy.format;
		sre = (struct source_range_entry *)vmalloc_node(sizeof(struct source_range_entry) * nentry, 1);

		__copy_prp_data(sqid, sq_entry, sre, sizeof(struct source_range_entry) * nentry, true);

		input_size = cmd->memory_copy.length;
		param.param_size = 0;
		input_buf_addr = get_slm_addr(cmd->memory_copy.sdaddr);
		output_buf_addr = input_buf_addr;

		NVMEV_CSD_INFO("SCHD", "Issue SLMCPY (lba:%lu, size:%lu, input_addr:%lu, sre:%d)", lba, input_size, input_buf_addr, nentry);

		if (is_copy_to_slm == true) {
			program_idx = COPY_TO_SLM_PROGRAM_INDEX;
			task->io_quota = 0;
			task->slm_lba_info = alloc_slm_lba_info(input_buf_addr, input_size, input_size, task_id, sre, nentry);
		}
		else {
			// Copy data from slm to NAND
			program_idx = COPY_FROM_SLM_PROGRAM_INDEX;
			task->io_quota = 0;
			task->slm_lba_info = get_slm_memory_lba_info(input_buf_addr);
			if (task->slm_lba_info == NULL) {
				task->slm_lba_info = alloc_slm_lba_info(input_buf_addr, input_size, input_size, task_id, sre, nentry);
			}
		}
	} else {
		NVMEV_ERROR("Command Address %p", cmd);
		NVMEV_ERROR("opcode error : proc_index:%d, opcode:%d, sqid: %d, sq_entry: %d", proc_idx, opcode, sqid, sq_entry);
		__dump_all_task_status();
		BUG();
	}

	NVMEV_CSD_INFO(
		"SCHD",
		"Result - task_id:%u(program_idx:%d), input_size:%lu, nentry:%d, host_id:%d, input_offset:%lu, output_offset:%lu",
		task_id, program_idx, input_size / SLM_PAGE_SIZE, nentry, cmd->memory.cdw14,
		get_slm_offset(input_buf_addr) / SLM_PAGE_SIZE, get_slm_offset(output_buf_addr) / SLM_PAGE_SIZE);

	__dequeue_task(&task_table.free_list, task_id);

	task->host_id = cmd->memory.cdw14;
	task->task_step = CCSD_TASK_SCHEDULE;
	task->proc_idx = proc_idx;
	task->total_size = input_size;

	task->requested_io_offset = requested_io_offset;

	task->input_buf_addr = input_buf_addr;
	task->output_buf_addr = output_buf_addr;
	task->program_idx = program_idx;
	task->param_size = param.param_size;

	if (param.param_size != 0) {
		memcpy(task->params, param.param, param.param_size);
	}
	task->result = 0;
	task->status = (NVME_SCT_GENERIC_CMD_STATUS << 8) | NVME_SC_SUCCESS;

	task->opcode = opcode;
	task->internal_io_count = 0;

	task->prev = -1;
	task->next = -1;
	if (opcode == nvme_cmd_memory_copy) {
		compute_core_id = task_table.copy_to_slm_list_id;
	} else {
#if (USE_ONLY_ONE_COMPUTE_CORE == 1)
		compute_core_id = 0;
#elif (USE_IDLE_COMPUTE_CORE == 1)
		if (__find_idle_compute_core(&compute_core_id) == false) {
			// if these is no idle core, fallback to RR
			compute_core_id = task_table.compute_turn;
			task_table.compute_turn = (task_table.compute_turn + 1) % task_table.num_cpu_resources;
		}
#else
		compute_core_id = task_table.compute_turn;
		task_table.compute_turn = (task_table.compute_turn + 1) % task_table.num_cpu_resources;
#endif
	}
	__enqueue_compute_work(task_id, compute_core_id);

	return true;
}
#endif

static void __fill_cq_result(int sqid, int cqid, int sq_entry, unsigned int command_id, unsigned int status,
							 size_t result)
{
	struct nvmev_completion_queue *cq = vdev->cqes[cqid];
	int cq_head;

	NVMEV_CSD_INFO("SCHD", "cq_result:%lu", result);

	spin_lock(&cq->entry_lock);
	cq_head = cq->cq_head;
	cq_entry(cq_head).command_id = command_id;
	cq_entry(cq_head).sq_id = sqid;
	cq_entry(cq_head).sq_head = sq_entry;
	cq_entry(cq_head).status = cq->phase | (status << 1);
	cq_entry(cq_head).result0 = result;

	if (++cq_head == cq->queue_size) {
		cq_head = 0;
		cq->phase = !cq->phase;
	}

	cq->cq_head = cq_head;
	cq->interrupt_ready = true;
	spin_unlock(&cq->entry_lock);
}

static void __reclaim_io_req(void)
{
	unsigned int turn;
	unsigned int first_entry = -1;
	unsigned int last_entry = -1;
	unsigned int curr;
	unsigned int total_count = 0;
	struct csd_internal_io_req *io_req;

	unsigned long long curr_nsecs_wall = __get_wallclock();
	unsigned long long curr_nsecs_local = local_clock();
	long long delta = curr_nsecs_wall - curr_nsecs_local;
	unsigned long long curr_nsecs = local_clock() + delta;

	// SLM Queue
	for (turn = 0; turn < task_table.num_slm_resources; turn++) {
		int count = 0;
		struct ccsd_list *slm_list = &(io_req_table.slm_list[turn]);
		first_entry = slm_list->head;
		curr = first_entry;
		last_entry = -1;

		while (curr != -1) {
			io_req = &(io_req_table.io_req[curr]);

			if (io_req->io_req_status == CSD_INTERNAL_IO_REQ_WAITING_TIME) {
				if (curr_nsecs >= io_req->nsecs_target) {
					struct ccsd_task_info *task = &(task_table.task[io_req->task_id]);
					io_req->io_req_status = CSD_INTERNAL_IO_REQ_DONE;

					// If the difference was too large (> 10ms), print debug info
					if ((curr_nsecs - io_req->nsecs_target) > 10000000) {
						NVMEV_FREEBIE_DEBUG("Internal I/O request delayed more than 10ms! io_size: %llu, cur_time:%llu, target_time:%llu, diff:%llu\n",
							io_req->length, curr_nsecs, io_req->nsecs_target, curr_nsecs - io_req->nsecs_target);
					}
					NVMEV_DEBUG(
						"T_%d_ReadSource - TASK_%d_%d - io_size:%llu, io_lba:%llu, cur_time:%llu, target_time:%llu, diff:%llu",
						task->host_id, io_req->task_id, io_req_id, io_req->length, io_req->lba, curr_nsecs,
						io_req->nsecs_target, curr_nsecs - io_req->nsecs_target);
				}
			}

			if (io_req->io_req_status == CSD_INTERNAL_IO_REQ_DONE) {
				uint32_t task_id = io_req->task_id;
				struct ccsd_task_info *task = &(task_table.task[task_id]);

				task->done_size += io_req->length;

				// Increase internal io amount
				if (io_req->is_copy_to_slm == true) {
					atomic64_add(io_req->length, &vdev->repartition_read_bytes);
				}
				else {
					atomic64_add(io_req->length, &vdev->repartition_write_bytes[task->ruh]);
				}

				if (task->has_buddy == true) {
					if (io_req->length != task->total_size) {
						printk("The I/O request was split due to SRE (%lu %lu)\n",
														io_req->length, task->total_size);
						// Request another i/o request
						task->total_size -= io_req->length;
						task->input_buf_addr += io_req->length;
						task->requested_io_offset += io_req->length;
						task->task_step = CCSD_TASK_SCHEDULE;
					}
					else {
						// Wake up execution
						if (task->buddy_task->task_step == CCSD_TASK_WAIT) {
							task->task_step = CCSD_TASK_WAIT;
							task->buddy_task->task_step = CCSD_TASK_SCHEDULE;
						}
					}
				} else if (task->has_master == true) {
					if (task->total_size == task->done_size) {
						struct ccsd_task_info *master_task = task->master_task;
						clear_bit(task_id, master_task->helper_bitmap);

						if (task->can_terminate == true) {
							// NVMEV_FREEBIE_DEBUG("Helper task %d is done, terminating\n", task_id);
							task->task_step = CCSD_TASK_END;
						} else {
							// NVMEV_FREEBIE_DEBUG("Helper task %d is done, but not terminating\n", task_id);
							task->task_step = CCSD_TASK_WAIT;
						}
					} else if (task->total_size < task->done_size) {
						NVMEV_FREEBIE_DEBUG("Task ID %d - BUG::task->total_size < task->done_size (%lu < %lu)\n",
									task_id, task->total_size, task->done_size);
						BUG();
					}
				}
				
				task->internal_io_count--;
				last_entry = curr;
				curr = io_req->next;

				io_req->io_req_status = CSD_INTERNAL_IO_REQ_FREE;
				slm_list->running = slm_list->running - 1;
				NVMEV_DEBUG("[COMPLETION_ALERT] SLM_Q[%u] - TASK_%d_%d  - %d (%d)", turn, task_id, last_entry, count++,
							total_count++);

				notify_internal_cmd();
				io_req_table.enqueuing_io_size = io_req_table.enqueuing_io_size - io_req->length;
			} else {
				break;
			}
		}

		if (last_entry != -1) {
			io_req = &(io_req_table.io_req[last_entry]);
			slm_list->head = io_req->next;
			if (io_req->next != -1) {
				io_req_table.io_req[io_req->next].prev = -1;
			}
			io_req->next = -1;
			__enqueue_io_req_free_list(first_entry, last_entry);
		}
	}

	curr = task_table.comp_list[task_table.copy_to_slm_list_id].head;
	while (curr != -1) {
		struct ccsd_task_info *task = &(task_table.task[curr]);

		if (task->task_step == CCSD_TASK_SCHEDULE) {
			if ((task->done_size >= task->total_size) && (task->has_buddy == false)
									&& (task->has_master == false)) {
				task->task_step = CCSD_TASK_END;
			}
		}

		curr = task->next;
	}
}

static void __request_io(void)
{
	unsigned long long curr_time = __get_wallclock();
	uint32_t curr = task_table.comp_list[task_table.copy_to_slm_list_id].head;
	uint32_t token_expired = true;

	while (curr != -1) {
		struct ccsd_task_info *task = &(task_table.task[curr]);
		struct slm_lba_info *lba_info = task->slm_lba_info;

		if (lba_info == NULL) {
			NVMEV_DEBUG("A SLM load task has been forced to terminate!\n");
			task->task_step = CCSD_TASK_END;
			break;
		}

		// Used for internal load/store
		if ((task->has_buddy == true) && (task->task_step == CCSD_TASK_SCHEDULE)) {
			// NVMEV_FREEBIE_DEBUG("Buddy task Woken up\n");

			BUG_ON(task->buddy_task == NULL);
			if (task->buddy_task->task_step != CCSD_TASK_WAIT) {
				printk("[WARNING] When execution request to SLM worker, it should wait\n");
                printk("[DEBUG INFO] Task ID: %d, Buddy Task ID: %d, Current State: %d)\n",
                                                task->task_id, task->buddy_task->task_id,
                                                task->buddy_task->task_step);
				// Don't die, just break, don't execute (GC io)
				task->task_step = CCSD_TASK_END;
				break;
			}
			// EXECUTION is going to determine the following fields
			/*
			- task->program_idx					(Copy to slm? from slm?)
			- task->total_size					(How much data to copy?) 
			- task->requested_io_offset			(Which file offset to/from copy?)
			- task->input_buf_addr				(Where to/from copy?)
			*/

			bool is_copy_to_slm = (task->program_idx == COPY_TO_SLM_PROGRAM_INDEX);
			struct csd_internal_io_req *io_req;
			struct nvmev_result ret;
			uint32_t io_req_id = io_req_table.free_list.head;
			if (io_req_id == -1) { // No free task
				break;
			}

			size_t lba = 0;
			size_t io_offset = task->requested_io_offset;
			size_t io_size = task->total_size - task->done_size;

			lba = __calculate_lba(task->slm_lba_info, io_offset);
			io_size = __calculate_io_size(task->slm_lba_info, io_offset, io_size);
			ret = csd_get_target_latency(lba, io_size, curr_time, is_copy_to_slm, task->ruh);
			if (ret.nsecs_target == -1) {
				break;
			}

			io_req = &(io_req_table.io_req[io_req_id]);
			BUG_ON(io_req->io_req_status != CSD_INTERNAL_IO_REQ_FREE);
			io_req->io_req_status = CSD_INTERNAL_IO_REQ_ALLOC;
			io_req->is_copy_to_slm = is_copy_to_slm;
			io_req_table.free_list.head = io_req->next;
			if (io_req->next != -1) {
				io_req_table.io_req[io_req->next].prev = -1;
			}
			io_req->task_id = curr;
			io_req->buf_addr = task->input_buf_addr;
			io_req->local_offset = 0;
			io_req->lba = lba;
			io_req->length = io_size;
			io_req->prev = -1;
			io_req->next = -1;
			io_req->nsecs_nand_start = ret.nsecs_nand_start;
			io_req->local_offset = __calculate_local_offset(task->slm_lba_info, io_offset);
			io_req->nsecs_target = ret.nsecs_target;
			task->internal_io_count++;
			io_req_table.enqueuing_io_size = io_req_table.enqueuing_io_size + io_req->length;

			__enqueue_slm_work(io_req_id);
			io_req->io_req_status = CSD_INTERNAL_IO_REQ_READY;
			task->task_step = CCSD_TASK_WAIT;
		}
		else if (task->task_step == CCSD_TASK_SCHEDULE) {
			if (task->io_quota > 0) {
				bool is_copy_to_slm = (task->program_idx == COPY_TO_SLM_PROGRAM_INDEX);
				uint32_t i = 0;
				uint32_t quota = task->io_quota;
				for (i = 0; i < quota; i++) {
					uint32_t io_req_id = io_req_table.free_list.head;
					struct csd_internal_io_req *io_req;
					size_t io_size = IO_REQUEST_SIZE;
					size_t io_offset = task->requested_io_offset;
					size_t lba = 0;
					size_t local_offset = 0;
					struct nvmev_result ret;
					size_t remaining = task->total_size - task->requested_io_offset;

					token_expired = false;

					if (task->total_size < task->requested_io_offset) {
						remaining = 0;
					}

					if (remaining == 0) {
						task->io_quota = 0;
						break;
					}

					// No free task
					if (io_req_id == -1) {
						break;
					}

					if (io_size > remaining) {
						io_size = remaining;
					}

                    if (io_size == 0) {
                        printk("[ERROR] io_size is 0 (Before calculate lba, io_size)\n");
						BUG();
                    }

					lba = __calculate_lba(task->slm_lba_info, io_offset);
					io_size = __calculate_io_size(task->slm_lba_info, io_offset, io_size);
					if (io_size == 0) {
						printk("[ERROR] io_size is 0 (input buffer addr: %lu, task_id:%d, io_offset:%lu)\n",
								task->input_buf_addr, task->task_id, io_offset);
                        dump_sre_info(task->slm_lba_info);
						BUG();
					}

					local_offset = __calculate_local_offset(task->slm_lba_info, io_offset);
					NVMEV_DEBUG("calculated LBA: %llu, io_size: %llu\n", lba, io_size);

					ret = csd_get_target_latency(lba, io_size, curr_time, is_copy_to_slm, task->ruh);
					if (ret.nsecs_target == -1) {
						break;
					}

					io_req = &(io_req_table.io_req[io_req_id]);
					BUG_ON(io_req->io_req_status != CSD_INTERNAL_IO_REQ_FREE);
					io_req->io_req_status = CSD_INTERNAL_IO_REQ_ALLOC;
					io_req->is_copy_to_slm = is_copy_to_slm;

					io_req_table.free_list.head = io_req->next;
					if (io_req->next != -1) {
						io_req_table.io_req[io_req->next].prev = -1;
					}

					io_req->task_id = curr;
					io_req->buf_addr = task->input_buf_addr + io_offset;
					io_req->local_offset = local_offset;
					io_req->lba = lba;
					io_req->length = io_size;
					io_req->prev = -1;
					io_req->next = -1;
					io_req->nsecs_nand_start = ret.nsecs_nand_start;
					io_req->nsecs_target = ret.nsecs_target;

					task->internal_io_count++;
					io_req_table.enqueuing_io_size = io_req_table.enqueuing_io_size + io_req->length;

					task->requested_io_offset = task->requested_io_offset + io_size;

					__enqueue_slm_work(io_req_id);
					io_req->io_req_status = CSD_INTERNAL_IO_REQ_READY;

					task->io_quota = task->io_quota - 1;
				}
			}
		}
		curr = task->next;
	}

	if (token_expired == true) {
		curr = task_table.comp_list[task_table.copy_to_slm_list_id].head;
		while (curr != -1) {
			struct ccsd_task_info *task = &(task_table.task[curr]);

			if (task->task_step == CCSD_TASK_SCHEDULE) {
				size_t remaining = task->total_size - task->requested_io_offset;

				if (remaining > 0) {
					task->io_quota = IO_TOKEN_PER_TASK;
#if (CSD_IO_SCHEDULING_TYPE != CSD_SCHEDULING_TYPE_RR)
					break;
#endif
				}
			}

			curr = task->next;
		}
	}
}

static void __reclaim_slm_task(void)
{
	int io_count = 0;
	struct ccsd_list *comp_list = &(task_table.comp_list[task_table.copy_to_slm_list_id]);
	uint32_t first_entry = comp_list->head;
	uint32_t curr = first_entry;
	uint32_t last_entry = -1;
	uint32_t count = 0;

	curr = first_entry;
	while (curr != -1) {
		struct ccsd_task_info *task = &(task_table.task[curr]);

		if (task->task_step == CCSD_TASK_END) {
			if (last_entry == -1) {
				first_entry = curr;
			}
			last_entry = curr;
			count++;
			NVMEV_DEBUG("[COMPLETION_ALERT] found COM completed %d - %llu", last_entry, __get_wallclock());
		} else if (last_entry != -1) {
			break;
		}
		curr = task->next;
	}

	if (last_entry != -1) {
		__dequeue_task_multi(comp_list, first_entry, last_entry, count);
		__enqueue_task_multi(&task_table.done_list, first_entry, last_entry, count);
	}
}

static void __reclaim_task(void)
{
	uint32_t comp_list_count = task_table.num_cpu_resources; // For copy to slm list
	int io_count = 0;
	// Compute Queue
	// __request_io();

	for (uint32_t turn = 0; turn < comp_list_count; turn++) {
		struct ccsd_list *comp_list = &(task_table.comp_list[turn]);
		uint32_t first_entry = comp_list->head;
		uint32_t curr = first_entry;
		uint32_t last_entry = -1;
		uint32_t count = 0;

		curr = first_entry;
		while (curr != -1) {
			struct ccsd_task_info *task = &(task_table.task[curr]);

			if (task->task_step == CCSD_TASK_END) {
				if (last_entry == -1) {
					first_entry = curr;
				}
				last_entry = curr;
				count++;
				NVMEV_DEBUG("[COMPLETION_ALERT] found COM completed %d - %llu", last_entry, __get_wallclock());
			} else if (last_entry != -1) {
				break;
			}
			curr = task->next;
		}

		if (last_entry != -1) {
			__dequeue_task_multi(comp_list, first_entry, last_entry, count);
			__enqueue_task_multi(&task_table.done_list, first_entry, last_entry, count);
		}
	}
}

static void __insert_csd_req_into_cpl(unsigned int entry)
{
	struct nvmev_proc_info *pi = &vdev->csd_proc_info[0]; 
	BUG_ON(pi->proc_table[entry].prev != -1);
	BUG_ON(pi->proc_table[entry].next != -1);

	if (pi->cpl_seq == -1) {
		pi->cpl_seq = entry;
		pi->cpl_seq_end = entry;
	} else {
		pi->proc_table[pi->cpl_seq_end].next = entry;
		pi->proc_table[entry].prev = pi->cpl_seq_end;
		pi->cpl_seq_end = entry;
	}
}

static void __reclaim_completed_csd_reqs(void)
{
	struct nvmev_proc_table *pe;
	struct nvmev_proc_info *pi = &vdev->csd_proc_info[0]; 

	unsigned int first_entry = -1;
	unsigned int last_entry = -1;
	unsigned int curr;

	first_entry = pi->cpl_seq;
	curr = first_entry;

	while (curr != -1) {
		pe = &pi->proc_table[curr];
		if (pe->is_completed == true && pe->is_copied == true) {
			last_entry = curr;
			curr = pe->next;
		} else {
			break;
		}
	}

	if (last_entry != -1) {
		pe = &pi->proc_table[last_entry];
		pi->cpl_seq = pe->next;
		if (pe->next != -1) {
			pi->proc_table[pe->next].prev = -1;
		}
		pe->next = -1;
	}

	if (last_entry != -1) {
		spin_lock(&pi->free_lock);

		pe = &pi->proc_table[first_entry];
		pe->prev = pi->free_seq_end;

		pe = &pi->proc_table[pi->free_seq_end];
		pe->next = first_entry;

		pi->free_seq_end = last_entry;

		spin_unlock(&pi->free_lock);
	}
}

static int nvmev_kthread_ccsd_io(void *data)
{
	struct nvmev_proc_info *pi = (struct nvmev_proc_info *)data;
	unsigned long long prev_clock;
	prev_clock = __get_wallclock();

	NVMEV_INFO("%s started on cpu %d (node %d)", pi->thread_name, smp_processor_id(), cpu_to_node(smp_processor_id()));

	while (!kthread_should_stop()) {
		unsigned long long curr_nsecs_wall = __get_wallclock();
		unsigned long long curr_nsecs_local = local_clock();
		long long delta = curr_nsecs_wall - curr_nsecs_local;

		unsigned long long curr_nsecs;
		volatile unsigned int curr;
		struct nvmev_proc_table *pe;
		int qidx;

		while (true) {
			spin_lock(&pi->io_lock);
			curr = pi->io_seq;
			if (curr != -1) {
				pe = &pi->proc_table[curr];
				curr_nsecs = local_clock() + delta;
				pi->proc_io_nsecs = curr_nsecs;

				if (pe->next == -1) {
					pi->io_seq = pi->io_seq_end = -1;
				} else {
					pi->io_seq = pe->next;
					pi->proc_table[pe->next].prev = -1;
				}
				pe->prev = pe->next = -1;
			}
			spin_unlock(&pi->io_lock);

			if (curr == -1) {
				break;
			}

			// Compare the pe enqueue timestamp with current time
			// If the enqueue time and current time has large gap print debug info
			#define NVMEV_CSD_IO_SLOWPATH_THRESHOLD_NS (100ULL * 1000ULL * 1000ULL) // 100ms
			curr_nsecs_wall = __get_wallclock();
			if ((curr_nsecs_wall - pe->nsecs_enqueue) > NVMEV_CSD_IO_SLOWPATH_THRESHOLD_NS) {
				printk("[NVMEV_CSD_IO_SLOWPATH] %s: Detected slow path in CSD IO processing! (proc_idx: %u, diff: %llu)\n",
						pi->thread_name, curr, (curr_nsecs_wall - pe->nsecs_enqueue));
			}

			// Completed ones should not be here, it should be incompleted lists
			BUG_ON(pe->is_completed == true);

			if (pe->is_copied == false) {
				bool is_success = true;
				int sqid = pe->sqid;
				int sq_entry = pe->sq_entry;
				struct nvmev_submission_queue *sq = vdev->sqes[sqid];
				struct nvme_command *cmd = (struct nvme_command *)(&sq_entry(sq_entry));
				int opcode = cmd->common.opcode;
				bool send_cq = false;
				size_t result = 0;

				// Set Status Code to success (this could be omitted)
				pe->status = (NVME_SCT_GENERIC_CMD_STATUS << 8) | NVME_SC_SUCCESS;

				if (opcode == nvme_admin_load_program) {
					is_success = __do_perform_program_management(sqid, sq_entry, &(pe->status));
					send_cq = true;
				} else if (opcode == nvme_cmd_memory_management) {
					is_success = __do_perform_memory_management(sqid, sq_entry, &(pe->status));
					send_cq = true;
				} else if (opcode == nvme_cmd_freebie) {
					is_success = __do_perform_freebie_command(sqid, sq_entry, curr, &(pe->status), &result);
					if(((struct nvme_command_csd *)cmd)->execute_program.pind == FREEBIE_REPART_INDEX ||
					    ((struct nvme_command_csd *)cmd)->execute_program.pind == FREEBIE_DEBUG_INDEX) {
						send_cq = true;
					} else {
						send_cq = false;
					}

					if (is_success == false) {
						// Result 1 indicates that the command failed
						result = 1;
					}
				} else {
					NVMEV_ERROR("opcode error : proc_index:%d, opcode:%d, sqid: %d, sq_entry: %d", curr, opcode, sqid, sq_entry);
					// TODO: compare the command ID that is saved in proc table and the one in submission queue

					// __dump_all_task_status();
					BUG();
					// is_success = __do_perform_dispatch(sqid, sq_entry, curr, &(pe->status), &result);
				}

				pe->is_copied = true;
				NVMEV_DEBUG("%s: copied %u, %d %d %d", pi->thread_name, curr, pe->sqid, pe->cqid, pe->sq_entry);

				// If failed to process a command, just send completion right away
				if (is_success == false) {
					send_cq = true;
				}

				if (send_cq) {
					__fill_cq_result(pe->sqid, pe->cqid, pe->sq_entry, pe->command_id, pe->status, result);
					mb(); /* Reclaimer shall see after here */
					pe->is_completed = true;
				}
			}

			__insert_csd_req_into_cpl(curr);
			__reclaim_task();
		}
		__reclaim_task();
		// __reclaim_io_req();

		// Create CQ
		// Pop done_list head and insert to free list
		while ((curr = __pop_head_task(&task_table.done_list)) != -1) {
			int task_id = curr;
			struct ccsd_task_info *task = &(task_table.task[task_id]);
			struct nvmev_proc_table *pe = &(pi->proc_table[task->proc_idx]);
			bool send_cq = true;

			if (((FREEBIE_NO_CQ_ENTRY(task->program_idx) == true) && (task->has_buddy == true))
								|| task->has_master == true) {
				send_cq = false;
			}
			if (send_cq == true) {
				__fill_cq_result(pe->sqid, pe->cqid, pe->sq_entry, pe->command_id, task->status, task->result);

				NVMEV_DEBUG("%s: completed %u, %d %d %d", pi->thread_name, curr, pe->sqid, pe->cqid, pe->sq_entry);
				mb(); /* Reclaimer shall see after here */
				pe->is_completed = true;
			}

			task->task_step = CCSD_TASK_FREE;

			// Free SLM Ranges
			if (task->slm_lba_info == NULL) {
				// Do Nothing
				if (task->has_master == true) {
					// exec helper tasks -> free intermediate slm
					struct freebie_params *freebie_params = (struct freebie_params *)(task->params);
					for (int file_idx = 0; file_idx < freebie_params->spread_factor; file_idx++) {
						free_slm_range(freebie_params->output_buf[file_idx]);
					}
				}
				else if (task->has_helper == true) {
					// Main Repartitioning task -> free input buffers
					struct freebie_params *freebie_params = (struct freebie_params *)(task->params);
					for (int file_idx = 0; file_idx < freebie_params->input_file_cnt; file_idx++) {
						free_slm_range(freebie_params->input_buf[file_idx]);
					}
				}
			} else { // task->slm_lba_info != NULL
				if (task->has_buddy == true) {
					// Do nothing
				}
				else {
					// Input Helper tasks only free slm_lba_info
					if (task->is_input_helper == true) {
						// Do nothing
					} else {
						// Output Helper tasks free slm_lba_info and its range
						free_slm_range(task->input_buf_addr);
					}
				}
			}

			task->slm_lba_info = NULL;

			__enqueue_task(&task_table.free_list, task_id);
		}

		__reclaim_completed_csd_reqs();

		cond_resched();
	}

	return 0;
}

/*
	Legacy "compute_work" cannot handle the following cases (well)
	- Computation with multiple input, output
	- Computation which it's termination condition is not related to the amount of data (produced or processsed)
	- Computation which needs to temporarily stop (e.g. for NAND write)

	Therefore, we divide another handler which should ultimately replce the legacy function. 
	For now, the "compute_work" checks the operations to be handled and redirects it to here.
 */


#define SHOULD_REDIRECT(idx) 				\
	((idx) == FREEBIE_REPART_INDEX) || 		\
	((idx) == FREEBIE_SETUP_INDEX) ||		\
	((idx) == FREEBIE_RELEASE_CAT_INDEX) || \
	((idx) == FREEBIE_TERMINATE_INDEX)   || \
	((idx) == FREEBIE_GARBAGE_COLLECT_INDEX)

static int __compute_work_freebie(struct ccsd_task_info *task, int core_id)
{
	if (task->task_step == CCSD_TASK_SCHEDULE) {
		bool is_done = false;

		switch (task->program_idx) {
			case FREEBIE_REPART_INDEX:
				if (task->has_helper == true) {
					if (!bitmap_empty(task->helper_bitmap, MAX_TASK_COUNT)) {
						return 0;
					}
					vdev->core_in_use[core_id]++;
					if (task->is_delete) {
						is_done = freebie_repartition_with_delete(task);
					} else {
						is_done = freebie_repartition(task);
					}
					if (is_done == false) {
						// This function only returns false when something is wrong 
						NVMEV_FREEBIE_DEBUG("FreeBIE Repartitioning failed\n");
						BUG();
					}
					vdev->core_in_use[core_id]--;
					currently_running_repartition--;
				}
				else if (task->has_master == true) {
					vdev->core_in_use[core_id]++;
					is_done = freebie_helper_repartition(task);
					vdev->core_in_use[core_id]--;
				}
				break;

			case FREEBIE_SETUP_INDEX:
				is_done = freebie_setup(task, (uint32_t)task->input_buf_addr);
				break;

			case FREEBIE_RELEASE_CAT_INDEX:
				is_done = freebie_release_root((uint32_t)task->input_buf_addr,
												(uint32_t)task->output_buf_addr);
				break;
			
			case FREEBIE_TERMINATE_INDEX:
				is_done = freebie_terminate((uint32_t)task->input_buf_addr);
				break;
			
			case FREEBIE_GARBAGE_COLLECT_INDEX:
				is_done = freebie_garbage_collection(task, (uint32_t)task->input_buf_addr);
				break;

			default:
				NVMEV_FREEBIE_DEBUG("SOMETHING WRONG!!!!\n");
				BUG();
		}

		if (is_done) {
			if (task->has_buddy == true) {
				BUG_ON(task->buddy_task == NULL);
				// BUG_ON(task->buddy_task->task_step != CCSD_TASK_WAIT);
				task->buddy_task->task_step = CCSD_TASK_END;
			}
			// Fix BUG: Copy helpers will terminate by its own
			// if (task->has_helper == true) {
			// 	for (int i = 0; i < task->copy_helper_count; i++) {
			// 		struct ccsd_task_info *copy_helper_task = task->copy_helper_task[i];
			// 		copy_helper_task->task_step = CCSD_TASK_END;
			// 	}
			// }
			task->task_step = CCSD_TASK_END;
		}
	}
	return 0;
}

static int compute_work(void *data)
{
	struct ccsd_list *comp_list = (struct ccsd_list *)data;
	int pid = smp_processor_id();

	NVMEV_INFO("compute_work started on cpu %d (node %d)", pid, cpu_to_node(pid));
	while (!kthread_should_stop()) {
		spin_lock(&comp_list->lock);
		int task_id = comp_list->head;
		spin_unlock(&comp_list->lock);
		while (task_id != -1) {
			struct ccsd_task_info *task = &(task_table.task[task_id]);

			if (task->task_step == CCSD_TASK_END) {
				// Why did I add this?? Maybe some other task or the dispatcher might have set END 
				// NVMEV_FREEBIE_DEBUG("This task might have been already freed! (task_id:%d)", task_id);
				break;
			} else if (task->task_step == CCSD_TASK_WAIT) {
				task_id = task->next;
				continue;
			} else if (task->task_step == CCSD_TASK_FREE) {
				NVMEV_FREEBIE_DEBUG("Free task is in the compute queue! (task_id:%d)", task_id);
				break;
			}

			// Redirect FreeBIE Command
			if (SHOULD_REDIRECT(task->program_idx)) {
				__compute_work_freebie(task, comp_list->my_id);
				task_id = task->next;
				continue;
			} else if (task->program_idx == COPY_TO_SLM_PROGRAM_INDEX) {
				NVMEV_ERROR("WRONG PROGRAM IS COMING! (task_id:%d, program_idx:%d)", task_id, task->program_idx);
				BUG();	
			} else {
				NVMEV_FREEBIE_DEBUG("This CSD version does not support program rather than FreeBIE\n");
				task->task_step = CCSD_TASK_END;
			}
			task_id = task->next;
		}
		cond_resched();
	}
	return 0;
}

static int dispatcher_helper(void *data)
{
	struct nvmev_proc_info *pi = (struct nvmev_proc_info *)data;
	int pid = smp_processor_id();

	NVMEV_INFO("dispatcher_helper started on cpu %d (node %d)", pid, cpu_to_node(pid));
	while (!kthread_should_stop()) {
		__request_io();
		__reclaim_io_req();
		// This only reclaims task from SLM workers
		__reclaim_slm_task();

		cond_resched();
	}
	return 0;
}

static int slm_work(void *data)
{
	struct ccsd_list *slm_list = (struct ccsd_list *)data;
	int pid = smp_processor_id();
	int nsid = 0; // TODO
	unsigned long long curr_nsecs_wall = __get_wallclock();
	unsigned long long curr_nsecs_local = local_clock();
	long long delta = curr_nsecs_wall - curr_nsecs_local;

	NVMEV_INFO("slm_work started on cpu %d (node %d)", pid, cpu_to_node(smp_processor_id()));
	while (!kthread_should_stop()) {
		unsigned long long curr_nsecs = local_clock() + delta;
		unsigned long long min_target_time = -1;
		int io_req_id = slm_list->head;
		struct csd_internal_io_req *io_req;

		// Copy data
		io_req_id = slm_list->head;
		while (io_req_id != -1) {
			io_req = &(io_req_table.io_req[io_req_id]);

			if (io_req->io_req_status == CSD_INTERNAL_IO_REQ_READY) {
				if (io_req->is_copy_to_slm == true) { 
					copy_to_slm(io_req->buf_addr, vdev->ns[nsid].mapped + (io_req->lba << 9) + io_req->local_offset, io_req->length);
				}
				else {
					copy_from_slm(vdev->ns[nsid].mapped + (io_req->lba << 9) + io_req->local_offset, io_req->buf_addr, io_req->length);
				}
				io_req->io_req_status = CSD_INTERNAL_IO_REQ_WAITING_TIME;
			}
			io_req_id = io_req->next;
		}
		cond_resched();
	}
	return 0;
}

void NVMEV_CSD_PROC_INIT(struct nvmev_dev *vdev)
{
	char name[32];
	unsigned int i, proc_idx;
	int nr_compound_csd_cpu = 1; // only one CCSD scheduler exists

	vdev->csd_proc_info = kcalloc_node(sizeof(struct nvmev_proc_info), nr_compound_csd_cpu, GFP_KERNEL, 1);
	if (vdev->csd_proc_info == NULL) {
		NVMEV_ERROR("Memory alloc fail");
		BUG();
	}

	/* Initialize CCSD task management slots */
	task_table.free_list.head = 0;
	task_table.free_list.tail = MAX_TASK_COUNT - 1;
	task_table.free_list.running = MAX_TASK_COUNT;
	spin_lock_init(&task_table.free_list.lock);

	task_table.done_list.head = -1;
	task_table.done_list.tail = -1;
	task_table.done_list.running = 0;
	spin_lock_init(&task_table.done_list.lock);

	for (i = 0; i < MAX_TASK_COUNT; i++) {
		task_table.task[i].task_step = CCSD_TASK_FREE;
		task_table.task[i].next = i + 1;
		task_table.task[i].prev = i - 1;
		task_table.task[i].slm_lba_info = NULL;
	}
	task_table.task[MAX_TASK_COUNT - 1].next = -1;

	if (vdev->config.nr_csd_cpu < 2 || vdev->config.nr_slm_cpu < 1) {
		NVMEV_ERROR("CPU Count Error: %d %d", vdev->config.nr_csd_cpu, vdev->config.nr_slm_cpu);
	}

	task_table.num_cpu_resources = vdev->config.nr_csd_cpu;
	task_table.num_slm_resources = vdev->config.nr_slm_cpu;
	task_table.compute_turn = 0;
	task_table.copy_to_slm_list_id = task_table.num_cpu_resources;
	task_table.comp_list = kcalloc_node(sizeof(struct ccsd_list), task_table.num_cpu_resources + 1, GFP_KERNEL, 1);
	if (task_table.comp_list == NULL) {
		NVMEV_ERROR("Memory alloc fail");
		BUG();
	}

	io_req_table.slm_list = kcalloc_node(sizeof(struct ccsd_list), task_table.num_slm_resources, GFP_KERNEL, 1);
	io_req_table.slm_turn = 0;
	if (io_req_table.slm_list == NULL) {
		NVMEV_ERROR("Memory alloc fail");
		BUG();
	}

	for (i = 0; i < vdev->config.nr_csd_cpu; i++) {
		task_table.comp_list[i].head = -1;
		task_table.comp_list[i].tail = -1;
		task_table.comp_list[i].running = 0;
		task_table.comp_list[i].my_id = i;
		spin_lock_init(&task_table.comp_list[i].lock);

		snprintf(name, sizeof(name), "ccsd_compute_%d", i);
		csd_compute_workder[i] = kthread_create(compute_work, &(task_table.comp_list[i]), name);
		kthread_bind(csd_compute_workder[i], vdev->config.cpu_nr_csd[i]);
		wake_up_process(csd_compute_workder[i]);
	}
	task_table.comp_list[task_table.copy_to_slm_list_id].head = -1;
	task_table.comp_list[task_table.copy_to_slm_list_id].tail = -1;
	task_table.comp_list[task_table.copy_to_slm_list_id].running = 0;
	spin_lock_init(&task_table.comp_list[task_table.copy_to_slm_list_id].lock);

	for (i = 0; i < vdev->config.nr_slm_cpu; i++) {
		io_req_table.slm_list[i].head = -1;
		io_req_table.slm_list[i].tail = -1;
		io_req_table.slm_list[i].running = 0;

		snprintf(name, sizeof(name), "ccsd_slm_%d", i);
		csd_slm_workder[i] = kthread_create(slm_work, &(io_req_table.slm_list[i]), name);
		kthread_bind(csd_slm_workder[i], vdev->config.cpu_nr_slm[i]);
		wake_up_process(csd_slm_workder[i]);
	}

	io_req_table.enqueuing_io_size = 0;
	io_req_table.free_list.head = 0;
	io_req_table.free_list.tail = MAX_INTERNAL_IO_COUNT - 1;
	for (i = 0; i < MAX_INTERNAL_IO_COUNT; i++) {
		io_req_table.io_req[i].io_req_status = CSD_INTERNAL_IO_REQ_FREE;
		io_req_table.io_req[i].next = i + 1;
		io_req_table.io_req[i].prev = i - 1;
	}
	io_req_table.io_req[MAX_INTERNAL_IO_COUNT - 1].next = -1;

	for (proc_idx = 0; proc_idx < nr_compound_csd_cpu; proc_idx++) {
		struct nvmev_proc_info *pi = &vdev->csd_proc_info[proc_idx];

		pi->proc_table = vzalloc(sizeof(struct nvmev_proc_table) * NR_MAX_PARALLEL_IO);
		for (i = 0; i < NR_MAX_PARALLEL_IO; i++) {
			pi->proc_table[i].next = i + 1;
			pi->proc_table[i].prev = i - 1;
		}
		spin_lock_init(&pi->free_lock);
		spin_lock_init(&pi->io_lock);
		
		pi->proc_table[NR_MAX_PARALLEL_IO - 1].next = -1;

		pi->free_seq = 0;
		pi->free_seq_end = NR_MAX_PARALLEL_IO - 1;
		pi->io_seq = -1;
		pi->io_seq_end = -1;
		pi->cpl_seq = -1;
		pi->cpl_seq_end = -1;

		snprintf(pi->thread_name, sizeof(pi->thread_name), "csd_dispatcher");

		pi->nvmev_io_worker = kthread_create(nvmev_kthread_ccsd_io, pi, pi->thread_name);
		kthread_bind(pi->nvmev_io_worker, vdev->config.cpu_nr_csd_dispatcher[0]);
		wake_up_process(pi->nvmev_io_worker);

		csd_dispatcher_helper = kthread_create(dispatcher_helper, pi, "csd_dispatcher_helper");
		kthread_bind(csd_dispatcher_helper, vdev->config.cpu_nr_csd_dispatcher[1]);
		wake_up_process(csd_dispatcher_helper);
	}

	init_slm_memory(vdev->slm_mapped, vdev->config.slm_size);

	// Freebie Specific Initialization
	init_freebie_map();
	g_array_init();
    g_object_init();
	delta_list_entry_pool_init();

	vdev->tfm = crypto_alloc_comp("lz4", 0, 0);
	if (IS_ERR_OR_NULL(vdev->tfm)) {
		NVMEV_ERROR("Error allocating LZ4 compressor\n");
	}
}

void NVMEV_CSD_PROC_FINAL(struct nvmev_dev *vdev)
{
	unsigned int i;

	for (i = 0; i < 1; i++) {
		struct nvmev_proc_info *pi = &vdev->csd_proc_info[i];

		if (!IS_ERR_OR_NULL(pi->nvmev_io_worker)) {
			kthread_stop(pi->nvmev_io_worker);
		}

		if (!IS_ERR_OR_NULL(csd_dispatcher_helper)) {
			kthread_stop(csd_dispatcher_helper);
		}

		vfree(pi->proc_table);
	}

	for (i = 0; i < vdev->config.nr_csd_cpu; i++) {
		if (!IS_ERR_OR_NULL(csd_compute_workder[i])) {
			kthread_stop(csd_compute_workder[i]);
		}
	}

	for (i = 0; i < vdev->config.nr_slm_cpu; i++) {
		if (!IS_ERR_OR_NULL(csd_slm_workder[i])) {
			kthread_stop(csd_slm_workder[i]);
		}
	}

	kfree(vdev->csd_proc_info);
	kfree(task_table.comp_list);
	kfree(io_req_table.slm_list);

	final_slm_memory();

	crypto_free_comp(vdev->tfm);
}

bool IS_CSD_PROCESS(int opcode)
{
	bool result = false;
	switch (opcode) {
	case nvme_cmd_memory_management:
	case nvme_cmd_memory_copy:
	case nvme_cmd_execute_program:
	case nvme_cmd_freebie:
	case nvme_admin_load_program:
		result = true;
		break;
	default:
		break;
	}

	return result;
}
