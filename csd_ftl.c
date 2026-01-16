/**********************************************************************
 * Copyright (c) 2020-2023
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
 **********************************************************************/

#include <linux/ktime.h>
#include <linux/highmem.h>
#include <linux/sched/clock.h>

#include "nvmev.h"
#include "nvme_csd.h"
#include "csd_ftl.h"
#include "csd_slm.h"
#include "csd_dispatcher.h"
#include "user_function/freebie/freebie_functions.h"

struct nvmev_ns internal_ns;
extern struct nvmev_dev *vdev;

size_t issue_count_for_internal;
size_t issue_count_for_external;
size_t complete_count_for_internal;
size_t complete_count_for_external;
size_t refill_token_for_internal;
size_t refill_token_for_external;
size_t token_for_internal;
size_t token_for_external;

size_t last_io_size;
size_t init_token = 2 * 1024 * 1024;

size_t lastest_token = -1;

static inline unsigned long long __get_wallclock(void)
{
	return cpu_clock(vdev->config.cpu_nr_dispatcher[0]);
}

// #define ALIGN(x, a)   (((x) + ((a) - 1)) & ~((a) - 1))

struct nvmev_result csd_get_target_latency(uint64_t slba, uint64_t len, unsigned long long nsecs_start,
												bool is_copy_to_slm, uint16_t ruh)
{
	struct nvme_command cmd;
	struct nvmev_request req = {
		.cmd = &cmd,
		.sq_id = 0xFFFFFFFF,
		.nsecs_start = nsecs_start,
	};

	struct nvmev_result ret = {
		.nsecs_target = nsecs_start,
		.status = NVME_SC_SUCCESS,
	};

	uint64_t nsecs_target = 0;
	uint64_t nsecs_nand_start = 0;
	// Round up to 512-byte blocks.
	size_t block_size = ALIGNED_UP(len, 512) / 512; //
	// if (len < 512) {
	// 	block_size = 1;
	// } else {
	// 	block_size = len / 512 - 1;
	// }

	uint64_t sent_size = 0;

	size_t offset = 0;
	size_t remain = block_size;
	while (remain > 0) {
		size_t temp_len = (remain > 0xFFFF) ? 0xFFFF : remain;
		memset(&cmd, 0, sizeof(struct nvme_command));
		cmd.rw.slba = slba + offset;
		cmd.rw.length = temp_len - 1;
		if (is_copy_to_slm == true) {
			cmd.rw.opcode = nvme_cmd_read;
		}
		else {
			if (remain - temp_len > 0) {
				NVMEV_ERROR("block size exceeding length %lu\n", block_size);
			}
			cmd.rw.opcode = nvme_cmd_write;
			cmd.rw.dsmgmt = ruh << 16;
		}

		if (temp_len > 0xFFFF) {
			NVMEV_ERROR("block size overflow:%lu", temp_len);
		}

		// Align to 4KiB for SLM access
		{
			size_t start_lpn = (slba + offset) / 8;
			size_t end_lpn = (slba + offset + temp_len) / 8;
			sent_size += (end_lpn - start_lpn + 1) * 4096;
		}

		if (csd_proc_nvme_io_cmd(&internal_ns, &req, &ret) == false) {
			ret.nsecs_target = -1;
			return ret;
		}

		if (nsecs_target < ret.nsecs_target) {
			nsecs_target = ret.nsecs_target;
		}

		if (nsecs_nand_start < ret.nsecs_nand_start) {
			nsecs_nand_start = ret.nsecs_nand_start;
		}

		offset = offset + (temp_len + 1);
		remain = remain - temp_len;
	}

	// Note. moved to __reclaim_io_req for more accurate accounting
	// if (is_copy_to_slm == true) {
	// 	vdev->repartition_read_bytes += sent_size;
	// } else {
	// 	vdev->repartition_write_bytes[ruh] += sent_size;
	// }

	ret.nsecs_target = nsecs_target;
	ret.nsecs_nand_start = nsecs_nand_start;

	return ret;
}

uint64_t get_seq_lpn(uint64_t start_lba, size_t max_size)
{
	struct nvme_command cmd;
	struct nvmev_request req = {
		.cmd = &cmd,
		.sq_id = 0xFFFFFFFF,
		.nsecs_start = 0,
	};

	struct nvmev_result ret = {
		.nsecs_nand_start = 0,
		.nsecs_target = 0,
		.status = NVME_SC_SUCCESS,
	};

	cmd.rw.slba = start_lba;
	cmd.rw.length = max_size / 512 - 1;
	cmd.rw.opcode = nvme_test;
	if (csd_proc_nvme_io_cmd(&internal_ns, &req, &ret) == false) {
		NVMEV_ERROR("csd_proc_nvme_io_cmd error");
	}

	return ret.nsecs_target;
}

bool get_extra_token(void)
{
	bool isEmpty = false;
	int dbs_idx;
	int new_db;
	int old_db;
	isEmpty = true;
	for (int qid = 1; qid <= vdev->nr_sq; qid++) {
		if (vdev->sqes[qid] == NULL)
			continue;
		dbs_idx = qid * 2;
		new_db = vdev->dbs[dbs_idx];
		old_db = vdev->old_dbs[dbs_idx];
		if (new_db != old_db) {
			isEmpty = false;
			break;
		}
	}

	if (isEmpty) {
		if (lastest_token == -1) {
			lastest_token = token_for_external;
		} else {
			if (lastest_token == token_for_external) {
				NVMEV_DEBUG("Extra Token Refill : %lu, %lu (%lu)", complete_count_for_external,
							issue_count_for_external, lastest_token);
				return true;
			}
			lastest_token = -1;
		}
	} else {
		lastest_token = -1;
	}

	return false;
}

void notify_internal_cmd(void)
{
	complete_count_for_internal += 128 * 1024;
}
void csd_notify_io_cmd(size_t len)
{
	complete_count_for_external += len;
	last_io_size = len;
}

void csd_init_namespace(struct nvmev_ns *ns)
{
	// Using conventional FTL
	memcpy(&internal_ns, ns, sizeof(struct nvmev_ns));

	ns->proc_io_cmd = csd_proc_nvme_io_cmd;
	ns->notify_io_cmd = csd_notify_io_cmd;

	token_for_external = init_token / 2;
	refill_token_for_external = token_for_external;
	token_for_internal = init_token / 2;
	refill_token_for_internal = token_for_external;
	issue_count_for_internal = 0;
	issue_count_for_external = 0;
	complete_count_for_internal = 0;
	complete_count_for_external = 0;

	spin_lock_init(&internal_ns.ns_lock);

	return;
}

void refill_token(void)
{
	if (token_for_internal == 0 && token_for_external == 0) {
		token_for_internal = refill_token_for_internal;
		token_for_external = refill_token_for_external;
	}
}

bool csd_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	bool result = true;
	struct nvme_command_csd *cmd = (struct nvme_command_csd *)req->cmd;

	switch (cmd->common.opcode) {
	case nvme_cmd_memory_management: {
		break;
	}
	case nvme_cmd_memory_copy: {
		break;
	}
	case nvme_cmd_memory_read:
	case nvme_cmd_memory_write: {
		spin_lock(&internal_ns.ns_lock);
		result = internal_ns.proc_io_cmd(ns, req, ret);
		spin_unlock(&internal_ns.ns_lock);
		break;
	}
	case nvme_cmd_freebie_get_root: {
		// Check if this root is allocated
		// If not, set the length to 0
		if (freebie_check_root_setup(cmd->memory.cdw13) == false) {
			cmd->memory.length = 0;
		}
		spin_lock(&internal_ns.ns_lock);
		result = internal_ns.proc_io_cmd(ns, req, ret);
		spin_unlock(&internal_ns.ns_lock);
		break;
	}
	case nvme_cmd_freebie: {
		spin_lock(&internal_ns.ns_lock);
		// Increaset the command count
		// Increase this here to make avoid race condition on these counters
		struct nvme_command_csd *cmd = (struct nvme_command_csd *)req->cmd;
		switch (cmd->execute_program.pind) {
			case FREEBIE_REPART_INDEX:
				atomic64_inc(&vdev->repartition_command_count);
				break;
			case FREEBIE_SETUP_INDEX:
				atomic64_inc(&vdev->setup_command_count);
				break;
			case FREEBIE_TERMINATE_INDEX:
				atomic64_inc(&vdev->terminate_command_count);
				break;
			default:
				break;
		}
		spin_unlock(&internal_ns.ns_lock);
		break;
	}
	case nvme_cmd_namespace_copy: {
#if (SUPPORT_ASYNC_COMMAND == 1)
		uint64_t slm_addr = get_slm_addr(cmd->memory.sb);
		size_t length = cmd->memory_copy.length;
		if (check_output_slm_data_ready(slm_addr, &length, false) == false) {
			result = false;
			break;
		}
		cmd->memory_copy.length = length;
#endif
		spin_lock(&internal_ns.ns_lock);
		result = internal_ns.proc_io_cmd(ns, req, ret);
		spin_unlock(&internal_ns.ns_lock);
		break;
	}
	case nvme_cmd_read: {
		if (result) {
			spin_lock(&internal_ns.ns_lock);
			result = internal_ns.proc_io_cmd(ns, req, ret);
			spin_unlock(&internal_ns.ns_lock);
		}
		break;
	}
	default: {
		if (result) {
			spin_lock(&internal_ns.ns_lock);
			result = internal_ns.proc_io_cmd(ns, req, ret);
			spin_unlock(&internal_ns.ns_lock);
		}
		break;
	}
	}
	return result;
}