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

#ifndef _NVMEVIRT_CSD_DISPATCHER_H
#define _NVMEVIRT_CSD_DISPATCHER_H

#include <linux/kthread.h>
#include <linux/ktime.h>
#include <linux/highmem.h>
#include <linux/sched/clock.h>
#include <linux/delay.h>

#include "nvmev.h"
#define ALIGNED_UP(value, align) (((value) + (align)-1) & ~((align)-1))

#define NVMEV_CSD_INFO(ns_name, string, args...)
// #define NVMEV_CSD_INFO(ns_name, string, args...) printk("[CSD_DEBUG][NS:%s] (%s) " string, ns_name, __func__, ##args)

// Async Command
#define SUPPORT_ASYNC_COMMAND (0)

// SLMCPY : memory copy (from TP4131), EXEC : execute program (from TP4091)
// TASK : Unit of processing NVMe CSD Command (SLMCPY or EXEC)
// INTERNAL_IO : SSD I/O for SLMCPY
#define MAX_TASK_COUNT (192)
#define MAX_INTERNAL_IO_COUNT (IO_TOKEN_PER_TASK * IO_REQUEST_SIZE / MIN_IO_REQUEST_SIZE)


#define IO_TOKEN_PER_TASK (16)
#define MIN_IO_REQUEST_SIZE (16 * 1024)
#define IO_REQUEST_SIZE (NAND_CHANNELS * LUNS_PER_NAND_CH * FLASH_PAGE_SIZE)

// Scheduling Parameters
// CPU core scheduling
#define USE_ONLY_ONE_COMPUTE_CORE (0)
#define USE_IDLE_COMPUTE_CORE (1 || SUPPORT_ASYNC_COMMAND) // support when async
// SLMCPY scheduling
#define CSD_SCHEDULING_TYPE_FIFO (1)
#define CSD_SCHEDULING_TYPE_RR (2)


#define CSD_IO_SCHEDULING_TYPE (CSD_SCHEDULING_TYPE_FIFO)

struct ccsd_list {
	unsigned int head; /* free io req head index */
	unsigned int tail; /* free io req tail index */

	int my_id;
	int running;
	spinlock_t lock;
};

enum CSD_INTERNAL_IO_REQ_STATUS {
	CSD_INTERNAL_IO_REQ_FREE = 0,
	CSD_INTERNAL_IO_REQ_ALLOC,
	CSD_INTERNAL_IO_REQ_READY,
	CSD_INTERNAL_IO_REQ_WAITING_TIME,
	CSD_INTERNAL_IO_REQ_DONE,
	CSD_INTERNAL_IO_REQ_END,
};

struct csd_internal_io_req {
	unsigned long long nsecs_nand_start;
	unsigned long long nsecs_target;

	int task_id;
	int io_req_status;

	size_t lba;
	size_t local_offset;
	size_t buf_addr;
	size_t length;

	bool is_copy_to_slm;
	unsigned int next, prev;
};

struct csd_io_req_table {
	struct ccsd_list free_list;
	struct ccsd_list *slm_list;
	int slm_turn;

	struct csd_internal_io_req io_req[MAX_INTERNAL_IO_COUNT];
	size_t enqueuing_io_size;
};

enum TASK_STEP { CCSD_TASK_FREE, CCSD_TASK_SCHEDULE, CCSD_TASK_WAIT, CCSD_TASK_END };

struct ccsd_task_info {
	// common info
	int task_id;
	int task_step;
	unsigned int proc_idx;
	size_t total_size;
	size_t done_size;
	unsigned int status;

	// I/O parameter
	void *slm_lba_info;
	size_t requested_io_offset;
	int internal_io_count;
	uint16_t ruh;

	// Compute parameter
	size_t input_buf_addr;
	size_t output_buf_addr;
	unsigned int program_idx;
	size_t param_size;
	unsigned int params[128];
	size_t result;
	char task_name[32];

	// Post process
	unsigned int opcode;

	unsigned int next, prev;
	unsigned long long nsecs_target;

	int host_id;
	int io_quota;

	/*
		For EXEC tasks, this contains the SLM task
		Fro SLM tasks, this contains the EXEC task

		This is only used for executions that needs to request data from nand
		during there execution. This is different from random access.
		By directly accessing each tasks state, we can strictly serialize and even
		request SLM->NAND during an execution (while the exection waits!)
	*/
	// If this task is a master
	bool has_buddy;
	bool has_helper;
	bool is_delete;
	int copy_helper_count; // SLM Load/Store helper tasks
	struct ccsd_task_info *buddy_task;
	DECLARE_BITMAP(helper_bitmap, MAX_TASK_COUNT);
	struct ccsd_task_info *copy_helper_task[FREEBIE_MAX_SPAN_OUT];
	struct ccsd_task_info *exec_helper_task;
	
	// If this task is a helper
	bool has_master;
	bool can_terminate;
	bool is_input_helper;
	struct ccsd_task_info *master_task;

	// Shared pointer (between master and helper tasks)
	void *shared;
};

struct ccsd_task_table {
	struct ccsd_task_info task[MAX_TASK_COUNT];

	struct ccsd_list free_list;
	struct ccsd_list done_list;
	struct ccsd_list *comp_list;

	int copy_to_slm_list_id;
	int num_slm_resources;
	int num_cpu_resources;
	int compute_turn;
};
#endif
