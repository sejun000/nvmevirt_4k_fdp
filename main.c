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

#include <linux/kernel.h>
#include <linux/kthread.h>
#include <linux/types.h>
#include <linux/init.h>
#include <linux/module.h>
#include <linux/proc_fs.h>
#include <linux/seq_file.h>
#include <linux/delay.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include <asm/e820/types.h>
#include <asm/e820/api.h>

#include <linux/bpf.h>
#include <linux/filter.h>

#include "nvmev.h"
#include "conv_ftl.h"
#include "simple_ftl.h"
#include "csd_ftl.h"
#include "dma.h"

/****************************************************************
 * Memory Layout
 ****************************************************************
 * virtDev
 *  - PCI header
 *    -> BAR at 1MiB area
 *  - PCI capability descriptors
 *
 * +--- memmap_start
 * |
 * v
 * +------------+-------------------+--------------------------+
 * | <--1MiB--> | <--- SLM Area---> | <---- Storage Area ----> |
 * +------------+-------------------+--------------------------+
 *
 * 1MiB area for metadata
 *  - BAR : 1 page
 *	- DBS : 1 page
 *	- MSI-x table: 16 bytes/entry * 32
 *
 * SLM area
 * 
 * Storage area
 *
 ****************************************************************/

/****************************************************************
 * Argument
 ****************************************************************
 * 1. Memmap start (size in GiB)
 * 2. Memmap size (size in MiB)
 ****************************************************************/

struct nvmev_dev *vdev = NULL;

unsigned long memmap_start = 0;
unsigned long memmap_size = 0;

unsigned long slm_size = 0;
unsigned int read_time = 1;
unsigned int read_delay = 1;
unsigned int read_trailing = 0;

unsigned int write_time = 1;
unsigned int write_delay = 1;
unsigned int write_trailing = 0;

unsigned int nr_io_units = 8;
unsigned int io_unit_shift = 12;

char *dispatcher_cpus;
char *worker_cpus;
char *slm_cpus;
char *csd_cpus;
unsigned int debug = 0;

int io_using_dma = true;

module_param(memmap_start, ulong, 0444);
MODULE_PARM_DESC(memmap_start, "Memmap start in GiB");
module_param(memmap_size, ulong, 0444);
MODULE_PARM_DESC(memmap_size, "Memmap size in MiB");
module_param(slm_size, ulong, 0444);
MODULE_PARM_DESC(slm_size, "SLM size in MiB");
module_param(read_time, uint, 0644);
MODULE_PARM_DESC(read_time, "Read time in nanoseconds");
module_param(read_delay, uint, 0644);
MODULE_PARM_DESC(read_delay, "Read delay in nanoseconds");
module_param(read_trailing, uint, 0644);
MODULE_PARM_DESC(read_trailing, "Read trailing in nanoseconds");
module_param(write_time, uint, 0644);
MODULE_PARM_DESC(write_time, "Write time in nanoseconds");
module_param(write_delay, uint, 0644);
MODULE_PARM_DESC(write_delay, "Write delay in nanoseconds");
module_param(write_trailing, uint, 0644);
MODULE_PARM_DESC(write_trailing, "Write trailing in nanoseconds");
module_param(nr_io_units, uint, 0444);
MODULE_PARM_DESC(nr_io_units, "Number of I/O units that operate in parallel");
module_param(io_unit_shift, uint, 0444);
MODULE_PARM_DESC(io_unit_shift, "Size of each I/O unit (2^)");
module_param(dispatcher_cpus, charp, 0444);
MODULE_PARM_DESC(dispatcher_cpus, "CPU list for dispatcher threads, Seperated by Comma(,)");
module_param(worker_cpus, charp, 0444);
MODULE_PARM_DESC(worker_cpus, "CPU list for worker threads, Seperated by Comma(,)");
module_param(csd_cpus, charp, 0444);
MODULE_PARM_DESC(csd_cpus, "CSD's CPU list for process, completion(int.) threads, Seperated by Comma(,)");
module_param(slm_cpus, charp, 0444);
MODULE_PARM_DESC(slm_cpus, "CSD's CPU list for SLM process, completion(int.) threads, Seperated by Comma(,)");
module_param(debug, uint, 0644);

static void nvmev_proc_dbs(unsigned int id)
{
	int qid;
	int dbs_idx;
	int new_db;
	int old_db;
	int start_qid = 1 + id;
	int num_dispatchers = vdev->config.nr_dispatchers;

	// Admin queue
	if (id == 0) {
		new_db = vdev->dbs[0];
		if (new_db != vdev->old_dbs[0]) {
			nvmev_proc_admin_sq(new_db, vdev->old_dbs[0]);
			vdev->old_dbs[0] = new_db;
		}
		new_db = vdev->dbs[1];
		if (new_db != vdev->old_dbs[1]) {
			nvmev_proc_admin_cq(new_db, vdev->old_dbs[1]);
			vdev->old_dbs[1] = new_db;
		}
	}

	// Submission queues
	for (qid = start_qid; qid <= vdev->nr_sq; qid+=num_dispatchers) {
		if (vdev->sqes[qid] == NULL)
			continue;
		dbs_idx = qid * 2;
		new_db = vdev->dbs[dbs_idx];
		old_db = vdev->old_dbs[dbs_idx];
		if (new_db != old_db) {
			vdev->old_dbs[dbs_idx] = nvmev_proc_io_sq(qid, new_db, old_db);
		}
	}

	// Completion queues
	for (qid = start_qid; qid <= vdev->nr_cq; qid+=num_dispatchers) {
		if (vdev->cqes[qid] == NULL)
			continue;
		dbs_idx = qid * 2 + 1;
		new_db = vdev->dbs[dbs_idx];
		old_db = vdev->old_dbs[dbs_idx];
		if (new_db != old_db) {
			nvmev_proc_io_cq(qid, new_db, old_db);
			vdev->old_dbs[dbs_idx] = new_db;
		}
	}
}

static int nvmev_dispatcher(void *data)
{
	struct nvmev_dispatcher *dispatcher = (struct nvmev_dispatcher *)data;
	NVMEV_INFO("%s started on cpu %d (node %d)\n", dispatcher->thread_name, 
		smp_processor_id(), cpu_to_node(smp_processor_id()));

	while (!kthread_should_stop()) {
		if (dispatcher->id == 0) {
			nvmev_proc_bars();
		}
		nvmev_proc_dbs(dispatcher->id);

		cond_resched();
	}

	return 0;
}

static void NVMEV_DISPATCHER_INIT(struct nvmev_dev *vdev)
{
	unsigned int i;

	vdev->dispatchers = 
		kcalloc_node(sizeof(struct nvmev_dispatcher), vdev->config.nr_dispatchers, GFP_KERNEL, 1);

	for (i = 0; i < vdev->config.nr_dispatchers; i++) {
		struct nvmev_dispatcher *dispatcher = &vdev->dispatchers[i];

		dispatcher->id = i;
		snprintf(dispatcher->thread_name, sizeof(dispatcher->thread_name), "nvmev_dispatcher_%d", i);
		dispatcher->task_struct = kthread_create(nvmev_dispatcher, dispatcher, "nvmev_dispatcher_%d", i);
		
		kthread_bind(dispatcher->task_struct, vdev->config.cpu_nr_dispatcher[i]);
		wake_up_process(dispatcher->task_struct);
	}
}

static void NVMEV_REG_PROC_FINAL(struct nvmev_dev *vdev)
{
	unsigned int i;

	for (i = 0; i < vdev->config.nr_dispatchers; i++) {
		struct nvmev_dispatcher *dispatcher = &vdev->dispatchers[i];

		if (!IS_ERR_OR_NULL(dispatcher->task_struct)) {
			kthread_stop(dispatcher->task_struct);
		}
	}

	kfree(vdev->dispatchers);
}

static int __validate_configs(void)
{
	unsigned long resv_start_bytes;
	unsigned long resv_end_bytes;

	if (!memmap_start) {
		NVMEV_ERROR("[memmap_start] should be specified\n");
		return -EINVAL;
	}

	if (!memmap_size) {
		NVMEV_ERROR("[memmap_size] should be specified\n");
		return -EINVAL;
	} else if (memmap_size == 1) {
		NVMEV_ERROR("[memmap_size] should be bigger than 1MiB\n");
		return -EINVAL;
	}

#if (CSD_ENABLE == 1)
	if (!slm_size) {
		NVMEV_ERROR("[slm_size] should be specified\n");
		return -EINVAL;
	}
#endif

	resv_start_bytes = memmap_start << 30;
	resv_end_bytes = resv_start_bytes + (memmap_size << 20) - 1;

	if (e820__mapped_any(resv_start_bytes, resv_end_bytes, E820_TYPE_RAM)) {
		NVMEV_ERROR("[mem %#010lx-%#010lx] is usable, not reseved region\n", (unsigned long)resv_start_bytes,
					(unsigned long)resv_end_bytes);
		return -EPERM;
	}

	if (!e820__mapped_any(resv_start_bytes, resv_end_bytes, E820_TYPE_RESERVED)) {
		NVMEV_ERROR("[mem %#010lx-%#010lx] is not reseved region\n", (unsigned long)resv_start_bytes,
					(unsigned long)resv_end_bytes);
		return -EPERM;
	}

	if (nr_io_units == 0 || io_unit_shift == 0) {
		NVMEV_ERROR("Need non-zero IO unit size and at least one IO unit\n");
		return -EINVAL;
	}
	if (read_time == 0) {
		NVMEV_ERROR("Need non-zero read time\n");
		return -EINVAL;
	}
	if (write_time == 0) {
		NVMEV_ERROR("Need non-zero write time\n");
		return -EINVAL;
	}

	return 0;
}

static void __print_perf_configs(void)
{
#if 0
	unsigned long unit_perf_kb =
			vdev->config.nr_io_units << (vdev->config.io_unit_shift - 10);
	struct nvmev_config *cfg = &vdev->config;

	NVMEV_INFO("=============== Configurations ===============\n");
	NVMEV_INFO("* IO units : %d x %d\n",
			cfg->nr_io_units, 1 << cfg->io_unit_shift);
	NVMEV_INFO("* I/O times\n");
	NVMEV_INFO("  Read     : %u + %u x + %u ns\n",
				cfg->read_delay, cfg->read_time, cfg->read_trailing);
	NVMEV_INFO("  Write    : %u + %u x + %u ns\n",
				cfg->write_delay, cfg->write_time, cfg->write_trailing);
	NVMEV_INFO("* Bandwidth\n");
	NVMEV_INFO("  Read     : %lu MiB/s\n",
			(1000000000UL / (cfg->read_time + cfg->read_delay + cfg->read_trailing)) * unit_perf_kb >> 10);
	NVMEV_INFO("  Write    : %lu MiB/s\n",
			(1000000000UL / (cfg->write_time + cfg->write_delay + cfg->write_trailing)) * unit_perf_kb >> 10);
#endif
}

static int __get_nr_entries(int dbs_idx, int queue_size)
{
	int diff = vdev->dbs[dbs_idx] - vdev->old_dbs[dbs_idx];
	if (diff < 0) {
		diff += queue_size;
	}
	return diff;
}

static int __proc_file_read(struct seq_file *m, void *data)
{
	const char *filename = m->private;
	struct nvmev_config *cfg = &vdev->config;

	if (strcmp(filename, "read_times") == 0) {
		seq_printf(m, "%u + %u x + %u", cfg->read_delay, cfg->read_time, cfg->read_trailing);
	} else if (strcmp(filename, "write_times") == 0) {
		seq_printf(m, "%u + %u x + %u", cfg->write_delay, cfg->write_time, cfg->write_trailing);
	} else if (strcmp(filename, "io_units") == 0) {
		seq_printf(m, "%u x %u", cfg->nr_io_units, cfg->io_unit_shift);
	} else if (strcmp(filename, "stat") == 0) {
		int i;
		unsigned int nr_in_flight = 0;
		unsigned int nr_dispatch = 0;
		unsigned int nr_dispatched = 0;
		unsigned long long total_io = 0;
		for (i = 1; i <= vdev->nr_sq; i++) {
			struct nvmev_submission_queue *sq = vdev->sqes[i];
			if (!sq)
				continue;

			seq_printf(m, "%2d: %2u %4u %4u %4u %4u %llu\n", i, __get_nr_entries(i * 2, sq->queue_size),
					   sq->stat.nr_in_flight, sq->stat.max_nr_in_flight, sq->stat.nr_dispatch, sq->stat.nr_dispatched,
					   sq->stat.total_io);

			nr_in_flight += sq->stat.nr_in_flight;
			nr_dispatch += sq->stat.nr_dispatch;
			nr_dispatched += sq->stat.nr_dispatched;
			total_io += sq->stat.total_io;

			barrier();
			sq->stat.max_nr_in_flight = 0;
		}
		seq_printf(m, "total: %u %u %u %llu\n", nr_in_flight, nr_dispatch, nr_dispatched, total_io);
	} else if (strcmp(filename, "debug") == 0) {
		/* Left for later use */
	} else if (strcmp(filename, "freebie") == 0) {
		seq_printf(m, "%llu %llu %llu %llu\n",
			atomic64_read(&vdev->repartition_command_count), atomic64_read(&vdev->repartition_command_success_count),
			atomic64_read(&vdev->setup_command_count), atomic64_read(&vdev->terminate_command_count));

		// Repartition Read  Bytes
		seq_printf(m, "%llu ", atomic64_read(&vdev->repartition_read_bytes));

		// Repartition Write Bytes
		for (int i = 0; i < NR_MAX_LEVEL; i++) {
			seq_printf(m, "%llu ", atomic64_read(&vdev->repartition_write_bytes[i]));
		}
		seq_printf(m, "\n");

		// Host read / write io amount
		seq_printf(m, "%llu %llu\n", atomic64_read(&vdev->host_read), atomic64_read(&vdev->host_write));

		// GC Read / Write io amount
		seq_printf(m, "%llu %llu\n", atomic64_read(&vdev->gc_read), atomic64_read(&vdev->gc_write));

		// partition map read io amount
		seq_printf(m, "%llu\n", atomic64_read(&vdev->repartition_map_read));

		// for (int i = 0; i < vdev->config.nr_csd_cpu; i++) {
		// 	seq_printf(m, "%d ", vdev->core_in_use[i]);
		// }
	}

	return 0;
}

static ssize_t __proc_file_write(struct file *file, const char __user *buf, size_t len, loff_t *offp)
{
	ssize_t count = len;
	const char *filename = file->f_path.dentry->d_name.name;
	char input[128];
	unsigned int ret;
	unsigned long long *old_stat;
	struct nvmev_config *cfg = &vdev->config;
	size_t nr_copied;

	nr_copied = copy_from_user(input, buf, min(len, sizeof(input)));

	if (!strcmp(filename, "read_times")) {
		ret = sscanf(input, "%u %u %u", &cfg->read_delay, &cfg->read_time, &cfg->read_trailing);
		//adjust_ftl_latency(0, cfg->read_time);
	} else if (!strcmp(filename, "write_times")) {
		ret = sscanf(input, "%u %u %u", &cfg->write_delay, &cfg->write_time, &cfg->write_trailing);
		//adjust_ftl_latency(1, cfg->write_time);
	} else if (!strcmp(filename, "io_units")) {
		ret = sscanf(input, "%d %d", &cfg->nr_io_units, &cfg->io_unit_shift);
		if (ret < 1)
			goto out;

		old_stat = vdev->io_unit_stat;
		vdev->io_unit_stat = kzalloc_node(sizeof(*vdev->io_unit_stat) * cfg->nr_io_units, GFP_KERNEL, 1);

		mdelay(100); /* XXX: Delay the free of old stat so that outstanding
						 * requests accessing the unit_stat are all returned
						 */
		kfree(old_stat);
	} else if (!strcmp(filename, "stat")) {
		int i;
		for (i = 1; i <= vdev->nr_sq; i++) {
			struct nvmev_submission_queue *sq = vdev->sqes[i];
			if (!sq)
				continue;

			memset(&sq->stat, 0x00, sizeof(sq->stat));
		}
	} else if (!strcmp(filename, "debug")) {
		/* Left for later use */
	} else if (!strcmp(filename, "ebpf")) {
#ifdef CSD_eBPF_ENABLE
#if (CSD_eBPF_ENABLE == 1)
		int ebpf_fd;
		memcpy(&ebpf_fd, input, sizeof(int));
		printk("[1] ebpf_fd:%u, BPF_PROG_TYPE_CSLCSD:%u, bpf_prog:%p", ebpf_fd, BPF_PROG_TYPE_CSLCSD, vdev->bpf_prog);

		vdev->bpf_prog = bpf_prog_get_type(ebpf_fd, BPF_PROG_TYPE_CSLCSD);
		printk("[2]ebpf_fd:%u, BPF_PROG_TYPE_CSLCSD:%u, bpf_prog:%p", ebpf_fd, BPF_PROG_TYPE_CSLCSD, vdev->bpf_prog);
		if (IS_ERR(vdev->bpf_prog)) {
			pr_err("err: bpf_prog_get: %ld\n", PTR_ERR(vdev->bpf_prog));
			return PTR_ERR(vdev->bpf_prog);
		}
#endif
#endif
	}

out:
	__print_perf_configs();

	return count;
}

static int __proc_file_open(struct inode *inode, struct file *file)
{
	return single_open(file, __proc_file_read, (char *)file->f_path.dentry->d_name.name);
}

#if LINUX_VERSION_CODE > KERNEL_VERSION(5, 0, 0)
static const struct proc_ops proc_file_fops = {
	.proc_open = __proc_file_open,
	.proc_write = __proc_file_write,
	.proc_read = seq_read,
	.proc_lseek = seq_lseek,
	.proc_release = single_release,
};
#else
static const struct file_operations proc_file_fops = {
	.open = __proc_file_open,
	.write = __proc_file_write,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
};
#endif

void NVMEV_STORAGE_INIT(struct nvmev_dev *vdev)
{
	NVMEV_INFO("SLM : %lx + %lx\n", vdev->config.slm_start, vdev->config.slm_size);
	NVMEV_INFO("Storage : %lx + %lx\n", vdev->config.storage_start, vdev->config.storage_size);

	vdev->io_unit_stat = kzalloc_node(sizeof(*vdev->io_unit_stat) * vdev->config.nr_io_units, GFP_KERNEL, 1);

#if (CSD_ENABLE == 1)
	vdev->slm_mapped = memremap(vdev->config.slm_start, vdev->config.slm_size, MEMREMAP_WB);
	if (vdev->slm_mapped == NULL)
		NVMEV_ERROR("Failed to map slm.\n");
	memset(vdev->slm_mapped, 0, vdev->config.slm_size);
#endif

	vdev->storage_mapped = memremap(vdev->config.storage_start, vdev->config.storage_size, MEMREMAP_WB);

	if (vdev->storage_mapped == NULL)
		NVMEV_ERROR("Failed to map storage memory.\n");

	vdev->proc_root = proc_mkdir("nvmev", NULL);
	vdev->proc_read_times = proc_create("read_times", 0664, vdev->proc_root, &proc_file_fops);
	vdev->proc_write_times = proc_create("write_times", 0664, vdev->proc_root, &proc_file_fops);
	vdev->proc_io_units = proc_create("io_units", 0664, vdev->proc_root, &proc_file_fops);
	vdev->proc_stat = proc_create("stat", 0444, vdev->proc_root, &proc_file_fops);
	vdev->proc_debug = proc_create("debug", 0444, vdev->proc_root, &proc_file_fops);
	vdev->proc_ebpf = proc_create("ebpf", 0664, vdev->proc_root, &proc_file_fops);
	vdev->proc_ebpf = proc_create("freebie", 0664, vdev->proc_root, &proc_file_fops);
}

void NVMEV_STORAGE_FINAL(struct nvmev_dev *vdev)
{
	remove_proc_entry("read_times", vdev->proc_root);
	remove_proc_entry("write_times", vdev->proc_root);
	remove_proc_entry("io_units", vdev->proc_root);
	remove_proc_entry("stat", vdev->proc_root);
	remove_proc_entry("debug", vdev->proc_root);
	remove_proc_entry("ebpf", vdev->proc_root);
	remove_proc_entry("freebie", vdev->proc_root);

	remove_proc_entry("nvmev", NULL);

#if (CSD_ENABLE == 1)
	if (vdev->slm_mapped)
		memunmap(vdev->slm_mapped);
#endif

	if (vdev->storage_mapped)
		memunmap(vdev->storage_mapped);

	if (vdev->io_unit_stat)
		kfree(vdev->io_unit_stat);
}

static bool __load_configs(struct nvmev_config *config)
{
	unsigned int cpu_nr;
	char *cpu;

	if (__validate_configs() < 0) {
		return false;
	}

#if (BASE_SSD == KV_PROTOTYPE)
	memmap_size -= KV_MAPPING_TABLE_SIZE; // Reserve space for KV mapping table
#endif

	config->memmap_start = memmap_start << 30;
	config->memmap_size = memmap_size << 20;
#if (CSD_ENABLE == 1)
	config->slm_start = config->memmap_start + (1UL << 20);
	config->slm_size = slm_size << 20;
	config->storage_start = config->slm_start + (slm_size << 20);
	config->storage_size = (memmap_size - slm_size - 1) << 20;
#else
	config->storage_start = config->memmap_start + (1UL << 20);
	config->storage_size = (memmap_size - 1) << 20;
#endif
	config->read_time = read_time;
	config->read_delay = read_delay;
	config->read_trailing = read_trailing;
	config->write_time = write_time;
	config->write_delay = write_delay;
	config->write_trailing = write_trailing;
	config->nr_io_units = nr_io_units;
	config->io_unit_shift = io_unit_shift;

	config->nr_io_cpu = 0;
	config->nr_dispatchers = 0;

	while ((cpu = strsep(&dispatcher_cpus, ",")) != NULL) {
		cpu_nr = (unsigned int)simple_strtol(cpu, NULL, 10);
		config->cpu_nr_dispatcher[config->nr_dispatchers] = cpu_nr;
		config->nr_dispatchers++;
	}

	while ((cpu = strsep(&worker_cpus, ",")) != NULL) {
		cpu_nr = (unsigned int)simple_strtol(cpu, NULL, 10);
		config->cpu_nr_proc_io[config->nr_io_cpu] = cpu_nr;
		config->nr_io_cpu++;
	}

#if (CSD_ENABLE == 1)
	config->nr_csd_cpu = 0;
	int count = 0;
	while ((cpu = strsep(&csd_cpus, ",")) != NULL) {
		cpu_nr = (unsigned int)simple_strtol(cpu, NULL, 10);
		if (count < 2) {
			config->cpu_nr_csd_dispatcher[count] = cpu_nr;
		} else {
			config->cpu_nr_csd[config->nr_csd_cpu] = cpu_nr;
			config->nr_csd_cpu++;
		}
		count++;
	}
	
	config->nr_slm_cpu = 0;
	while ((cpu = strsep(&slm_cpus, ",")) != NULL) {
		cpu_nr = (unsigned int)simple_strtol(cpu, NULL, 10);
		config->cpu_nr_slm[config->nr_slm_cpu] = cpu_nr;
		config->nr_slm_cpu++;
	}
	printk("csd: %d, slm: %d\n", config->nr_csd_cpu, config->nr_slm_cpu);
#endif
	return true;
}

void NVMEV_NAMESPACE_INIT(struct nvmev_dev *vdev)
{
	unsigned long long remaining_capacity = vdev->config.storage_size; // byte
	void *ns_addr = vdev->storage_mapped;
	const int nr_ns = NR_NAMESPACES;
	const unsigned int disp_no = vdev->config.cpu_nr_dispatcher[0];
	int i;
	unsigned long long size;

	struct nvmev_ns *ns = kmalloc_node(sizeof(struct nvmev_ns) * nr_ns, GFP_KERNEL, 1);

	for (i = 0; i < nr_ns; i++) {
		if (NS_CAPACITY(i) == 0)
			size = remaining_capacity;
		else
			size = min(NS_CAPACITY(i), remaining_capacity);

		if (NS_SSD_TYPE(i) == SSD_TYPE_NVM)
			simple_init_namespace(&ns[i], i, size, ns_addr, disp_no);
		else if (NS_SSD_TYPE(i) == SSD_TYPE_CONV)
			conv_init_namespace(&ns[i], i, size, ns_addr, disp_no);
		else
			NVMEV_ASSERT(0);

		remaining_capacity -= size;
		ns_addr += size;
		NVMEV_INFO("[%s] ns=%d ns_addr=%p ns_size=%lld(MiB) \n", __FUNCTION__, i, ns[i].mapped, BYTE_TO_MB(ns[i].size));

		ns->notify_io_cmd = NULL;
#if (CSD_ENABLE == 1)
		csd_init_namespace(&ns[i]);
#endif
	}

	vdev->ns = ns;
	vdev->nr_ns = nr_ns;
	vdev->mdts = MDTS;
}

void NVMEV_NAMESPACE_FINAL(struct nvmev_dev *nvmev_vdev)
{
	struct nvmev_ns *ns = nvmev_vdev->ns;
	const int nr_ns = NR_NAMESPACES; // XXX: allow for dynamic nvmev_vdev->nr_ns
	int i;

	for (i = 0; i < nr_ns; i++) {
		if (NS_SSD_TYPE(i) == SSD_TYPE_NVM)
			; // simple_remove_namespace(&ns[i]);
		else if (NS_SSD_TYPE(i) == SSD_TYPE_CONV)
			conv_remove_namespace(&ns[i]);
		else
			NVMEV_ASSERT(0);
	}

	kfree(ns);
	nvmev_vdev->ns = NULL;
}

static int NVMeV_init(void)
{
	vdev = VDEV_INIT();
	if (!vdev)
		return -EINVAL;

	if (!__load_configs(&vdev->config)) {
		goto ret_err;
	}

	NVMEV_STORAGE_INIT(vdev);

	NVMEV_NAMESPACE_INIT(vdev);

	if (io_using_dma) {
		if (ioat_dma_chan_set("dma4chan0") != 0) {
			io_using_dma = false;
			NVMEV_ERROR("Cannot use DMA engine, Fall back to memcpy\n");
		}
	}

	if (!NVMEV_PCI_INIT(vdev)) {
		goto ret_err;
	}

	__print_perf_configs();

	NVMEV_IO_PROC_INIT(vdev);
	NVMEV_DISPATCHER_INIT(vdev);
#if (CSD_ENABLE == 1)
	NVMEV_CSD_PROC_INIT(vdev);
#endif

	pci_bus_add_devices(vdev->virt_bus);

	// Freebie io stat
	atomic64_set(&vdev->repartition_command_count, 0);
	atomic64_set(&vdev->repartition_command_success_count, 0);
	atomic64_set(&vdev->setup_command_count, 0);
	atomic64_set(&vdev->terminate_command_count, 0);

	atomic64_set(&vdev->repartition_read_bytes, 0);
	for (int i = 0; i < NR_MAX_LEVEL; i++) {
		atomic64_set(&vdev->repartition_write_bytes[i], 0);
	}

	atomic64_set(&vdev->host_read, 0);
	atomic64_set(&vdev->host_write, 0);
	atomic64_set(&vdev->gc_read, 0);
	atomic64_set(&vdev->gc_write, 0);
	atomic64_set(&vdev->repartition_map_read, 0);

	for (int i = 0; i < 16; i++) {
		vdev->core_in_use[i] = 0;
	}

	NVMEV_INFO("Successfully created Virtual NVMe deivce\n");

	return 0;

ret_err:
	VDEV_FINALIZE(vdev);
	return -EIO;
}

static void NVMeV_exit(void)
{
	int i;

	if (vdev->virt_bus != NULL) {
		pci_stop_root_bus(vdev->virt_bus);
		pci_remove_root_bus(vdev->virt_bus);
	}

	NVMEV_REG_PROC_FINAL(vdev);
	NVMEV_IO_PROC_FINAL(vdev);
#if (CSD_ENABLE == 1)
	NVMEV_CSD_PROC_FINAL(vdev);
#endif
	NVMEV_NAMESPACE_FINAL(vdev);
	NVMEV_STORAGE_FINAL(vdev);

	if (io_using_dma) {
		ioat_dma_cleanup();
	}

	for (i = 0; i < vdev->nr_sq; i++) {
		kfree(vdev->sqes[i]);
	}

	for (i = 0; i < vdev->nr_cq; i++) {
		kfree(vdev->cqes[i]);
	}

	VDEV_FINALIZE(vdev);

	NVMEV_INFO("Virtual NVMe device closed\n");
}

MODULE_LICENSE("Dual BSD/GPL");
module_init(NVMeV_init);
module_exit(NVMeV_exit);
