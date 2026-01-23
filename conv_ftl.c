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
#include <linux/sched/clock.h>
#include <linux/highmem.h>
#include <linux/vmalloc.h>

#include "nvmev.h"
#include "nvme_csd.h"
#include "conv_ftl.h"

// Include this for modeling Partitoin Map
#include "user_function/freebie/freebie_functions.h"

extern struct nvmev_dev *vdev;

void enqueue_writeback_io_req(int sqid, unsigned long long nsecs_target, struct buffer *write_buffer,
							  unsigned int buffs_to_release);
void enqueue_gc_io_req(int sqid, unsigned long long nsecs_target, bool is_write, unsigned int io_length);

static inline bool last_pg_in_wordline(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	return (ppa->g.pg % spp->pgs_per_oneshotpg) == (spp->pgs_per_oneshotpg - 1);
}

static inline bool should_gc_high(struct conv_ftl *conv_ftl)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	return conv_ftl->lm.free_line_cnt <= conv_ftl->cp.gc_thres_lines_high;
}

static inline struct ppa get_maptbl_ent(struct conv_ftl *conv_ftl, uint64_t lpn)
{
	return conv_ftl->maptbl[lpn];
}

static inline void set_maptbl_ent(struct conv_ftl *conv_ftl, uint64_t lpn, struct ppa *ppa)
{
	NVMEV_ASSERT(lpn < conv_ftl->ssd->sp.tt_pgs);
	conv_ftl->maptbl[lpn] = *ppa;
}

static uint64_t ppa2pgidx(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	uint64_t pgidx;

	NVMEV_DEBUG("ppa2pgidx: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", ppa->g.ch, ppa->g.lun, ppa->g.pl, ppa->g.blk,
				ppa->g.pg);

	pgidx = ppa->g.ch * spp->pgs_per_ch + ppa->g.lun * spp->pgs_per_lun + ppa->g.pl * spp->pgs_per_pl +
			ppa->g.blk * spp->pgs_per_blk + ppa->g.pg;

	NVMEV_ASSERT(pgidx < spp->tt_pgs);

	return pgidx;
}

static inline uint64_t get_rmap_ent(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	uint64_t pgidx = ppa2pgidx(conv_ftl, ppa);

	return conv_ftl->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct conv_ftl *conv_ftl, uint64_t lpn, struct ppa *ppa)
{
	uint64_t pgidx = ppa2pgidx(conv_ftl, ppa);

	conv_ftl->rmap[pgidx] = lpn;
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
	return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a)
{
	return ((struct line *)a)->vpc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
	((struct line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
	return ((struct line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
	((struct line *)a)->pos = pos;
}

static inline void consume_write_credit(struct conv_ftl *conv_ftl)
{
	conv_ftl->wfc.write_credits--;
}

static void forground_gc(struct conv_ftl *conv_ftl);

static inline void check_and_refill_write_credit(struct conv_ftl *conv_ftl)
{
	struct write_flow_control *wfc = &(conv_ftl->wfc);
	if (wfc->write_credits <= 0) {
		forground_gc(conv_ftl);

		wfc->write_credits += wfc->credits_to_refill;
	}
}

static void init_lines(struct conv_ftl *conv_ftl)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct line *line;
	int i;

	lm->tt_lines = spp->blks_per_pl;
	NVMEV_ASSERT(lm->tt_lines == spp->tt_lines);
	lm->lines = vmalloc_node(sizeof(struct line) * lm->tt_lines, 1);

	INIT_LIST_HEAD(&lm->free_line_list);
	lm->victim_line_pq = pqueue_init(spp->tt_lines, victim_line_cmp_pri, victim_line_get_pri, victim_line_set_pri,
									 victim_line_get_pos, victim_line_set_pos);
	INIT_LIST_HEAD(&lm->full_line_list);

	lm->free_line_cnt = 0;
	for (i = 0; i < lm->tt_lines; i++) {
		line = &lm->lines[i];
		line->id = i;
		line->ipc = 0;
		line->vpc = 0;
		line->pos = 0;
		/* initialize all the lines as free lines */
		list_add_tail(&line->entry, &lm->free_line_list);
		lm->free_line_cnt++;
	}

	NVMEV_ASSERT(lm->free_line_cnt == lm->tt_lines);
	lm->victim_line_cnt = 0;
	lm->full_line_cnt = 0;
}

static void remove_lines(struct conv_ftl *conv_ftl)
{
	pqueue_free(conv_ftl->lm.victim_line_pq);
	vfree(conv_ftl->lm.lines);
}

static struct line *get_next_free_line(struct conv_ftl *conv_ftl)
{
	struct line_mgmt *lm = &conv_ftl->lm;
	struct line *curline = NULL;

	curline = list_first_entry(&lm->free_line_list, struct line, entry);
	if (!curline) {
		NVMEV_ERROR("No free lines left in VIRT !!!!\n");
		return NULL;
	}

	list_del_init(&curline->entry);
	lm->free_line_cnt--;
	NVMEV_DEBUG("[%s] free_line_cnt %d\n", __FUNCTION__, lm->free_line_cnt);
	return curline;
}

// Returns current WP for RUH in FTL
static struct write_pointer *__get_wp(struct conv_ftl *ftl, uint16_t ruh, uint32_t io_type)
{
	if (io_type == USER_IO) {
		return &ftl->wps[ruh];
	} else if (io_type == GC_IO) {
		return &ftl->gc_wp;
	} else {
		NVMEV_ASSERT(0);
	}
	return NULL;
}

static void prepare_an_write_pointer(struct conv_ftl *conv_ftl, uint16_t ruh, uint32_t io_type) {
	struct write_pointer *wp = __get_wp(conv_ftl, ruh, io_type);
	struct line *curline = get_next_free_line(conv_ftl);

	NVMEV_ASSERT(wp);
	NVMEV_ASSERT(curline);

	/* wp->curline is always our next-to-write super-block */
	*wp = (struct write_pointer){
		.curline = curline,
		.ch = 0,
		.lun = 0,
		.pg = 0,
		.blk = curline->id,
		.pl = 0,
	};
}

static void init_write_pointer(struct conv_ftl *conv_ftl, uint32_t io_type)
{
	struct write_pointer *wpp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct line *curline = NULL;

	if (io_type == USER_IO) {
		for (int i = 0; i < NR_MAX_RUH; i++) {
			prepare_an_write_pointer(conv_ftl, i, io_type);
		}
	} else if (io_type == GC_IO) {
		prepare_an_write_pointer(conv_ftl, 0, io_type);
	}
}

static void init_write_flow_control(struct conv_ftl *conv_ftl)
{
	struct write_flow_control *wfc = &(conv_ftl->wfc);
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	wfc->write_credits = spp->pgs_per_line;
	wfc->credits_to_refill = spp->pgs_per_line;
}

static inline void check_addr(int a, int max)
{
	NVMEV_ASSERT(a >= 0 && a < max);
}

static void advance_write_pointer(struct conv_ftl *conv_ftl, uint16_t ruh, uint32_t io_type)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct write_pointer *wpp = __get_wp(conv_ftl, ruh, io_type);

	NVMEV_DEBUG("current wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", wpp->ch, wpp->lun, wpp->pl, wpp->blk, wpp->pg);

	check_addr(wpp->pg, spp->pgs_per_blk);
	wpp->pg++;
	if ((wpp->pg % spp->pgs_per_flashpg) != 0)
		goto out;
	wpp->pg -= spp->pgs_per_flashpg;

	check_addr(wpp->ch, spp->nchs);
	wpp->ch++;
	if (wpp->ch != spp->nchs)
		goto out;
	wpp->ch = 0;

	check_addr(wpp->lun, spp->luns_per_ch);
	wpp->lun++;
	if (wpp->lun != spp->luns_per_ch)
		goto out;
	wpp->lun = 0;

	wpp->pg += spp->pgs_per_flashpg;
	if (wpp->pg != spp->pgs_per_blk)
		goto out;
	wpp->pg = 0;

	/* move current line to {victim,full} line list */
	if (wpp->curline->vpc == spp->pgs_per_line) {
		/* all pgs are still valid, move to full line list */
		NVMEV_ASSERT(wpp->curline->ipc == 0);
		list_add_tail(&wpp->curline->entry, &lm->full_line_list);
		lm->full_line_cnt++;
		NVMEV_DEBUG("wpp: move line to full_line_list\n");
	} else {
		NVMEV_DEBUG("wpp: line is moved to victim list\n");
		NVMEV_ASSERT(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
		/* there must be some invalid pages in this line */
		NVMEV_ASSERT(wpp->curline->ipc > 0);
		pqueue_insert(lm->victim_line_pq, wpp->curline);
		lm->victim_line_cnt++;
	}
	/* current line is used up, pick another empty line */
	check_addr(wpp->blk, spp->blks_per_pl);
	wpp->curline = NULL;
	wpp->curline = get_next_free_line(conv_ftl);
	BUG_ON(!wpp->curline);
	NVMEV_DEBUG("wpp: got new clean line %d\n", wpp->curline->id);

	wpp->blk = wpp->curline->id;
	check_addr(wpp->blk, spp->blks_per_pl);

	/* make sure we are starting from page 0 in the super block */
	NVMEV_ASSERT(wpp->pg == 0);
	NVMEV_ASSERT(wpp->lun == 0);
	NVMEV_ASSERT(wpp->ch == 0);
	/* TODO: assume # of pl_per_lun is 1, fix later */
	NVMEV_ASSERT(wpp->pl == 0);
out:
	NVMEV_DEBUG("advanced wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d (curline %d)\n", wpp->ch, wpp->lun, wpp->pl,
				wpp->blk, wpp->pg, wpp->curline->id);
}

static struct ppa get_new_page(struct conv_ftl *conv_ftl, uint16_t ruh, uint32_t io_type)
{
	struct write_pointer *wpp = __get_wp(conv_ftl, ruh, io_type);
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct ppa ppa;

	ppa.ppa = 0;
	ppa.g.ch = wpp->ch;
	ppa.g.lun = wpp->lun;
	ppa.g.pg = wpp->pg;
	ppa.g.blk = wpp->blk;
	ppa.g.pl = wpp->pl;

	if (ppa.g.ch >= spp->nchs || ppa.g.lun >= spp->luns_per_ch ||
		ppa.g.blk >= spp->blks_per_pl || ppa.g.pg >= spp->pgs_per_blk) {
		NVMEV_ERROR("get_new_page OOB: ch=%u/%d lun=%u/%d blk=%u/%d pg=%u/%d ruh=%u io_type=%u\n",
					ppa.g.ch, spp->nchs, ppa.g.lun, spp->luns_per_ch,
					ppa.g.blk, spp->blks_per_pl, ppa.g.pg, spp->pgs_per_blk, ruh, io_type);
	}

	NVMEV_ASSERT(ppa.g.pl == 0);
	return ppa;
}

static void init_maptbl(struct conv_ftl *conv_ftl)
{
	int i;
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	conv_ftl->maptbl = vmalloc_node(sizeof(struct ppa) * spp->tt_pgs, 1);
	for (i = 0; i < spp->tt_pgs; i++) {
		conv_ftl->maptbl[i].ppa = UNMAPPED_PPA;
	}
}

static void remove_maptbl(struct conv_ftl *conv_ftl)
{
	vfree(conv_ftl->maptbl);
}

static void init_rmap(struct conv_ftl *conv_ftl)
{
	int i;
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	conv_ftl->rmap = vmalloc_node(sizeof(uint64_t) * spp->tt_pgs, 1);
	for (i = 0; i < spp->tt_pgs; i++) {
		conv_ftl->rmap[i] = INVALID_LPN;
	}
}

static void remove_rmap(struct conv_ftl *conv_ftl)
{
	vfree(conv_ftl->rmap);
}

static void conv_init_ftl(struct conv_ftl *conv_ftl, struct convparams *cpp, struct ssd *ssd)
{
	/*copy convparams*/
	conv_ftl->cp = *cpp;

	conv_ftl->ssd = ssd;

	
	/* initialize maptbl */
	NVMEV_INFO("initialize maptbl\n");
	init_maptbl(conv_ftl); // mapping table

	/* initialize rmap */
	NVMEV_INFO("initialize rmap\n");
	init_rmap(conv_ftl); // reverse mapping table (?)

	/* initialize all the lines */
	NVMEV_INFO("initialize lines\n");
	init_lines(conv_ftl);

	/* initialize write pointer, this is how we allocate new pages for writes */
	NVMEV_INFO("initialize write pointer\n");
	init_write_pointer(conv_ftl, USER_IO);
	init_write_pointer(conv_ftl, GC_IO);

	init_write_flow_control(conv_ftl);

	NVMEV_INFO("Init FTL Instance with %d channels(%ld pages)\n", conv_ftl->ssd->sp.nchs, conv_ftl->ssd->sp.tt_pgs);

	return;
}

static void conv_remove_ftl(struct conv_ftl *conv_ftl)
{
	remove_lines(conv_ftl);
	remove_rmap(conv_ftl);
	remove_maptbl(conv_ftl);
}

static void conv_init_params(struct convparams *cpp)
{
	cpp->op_area_pcent = OP_AREA_PERCENT;
	cpp->gc_thres_lines = NR_MAX_RUH + 1; /* Need only two lines.(host write, gc)*/
	cpp->gc_thres_lines_high = NR_MAX_RUH + 1; /* Need only two lines.(host write, gc)*/
	cpp->enable_gc_delay = 1;
	cpp->pba_pcent = (int)((1 + cpp->op_area_pcent) * 100);
}

void conv_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr, uint32_t cpu_nr_dispatcher)
{
	struct ssdparams spp;
	struct convparams cpp;
	struct conv_ftl *conv_ftls;
	struct ssd *ssd;
	uint32_t i;
	const uint32_t nr_parts = SSD_PARTITIONS;

	ssd_init_params(&spp, size, nr_parts);
	conv_init_params(&cpp);

	conv_ftls = kmalloc_node(sizeof(struct conv_ftl) * nr_parts, GFP_KERNEL, 1);

	for (i = 0; i < nr_parts; i++) {
		ssd = kmalloc_node(sizeof(struct ssd), GFP_KERNEL, 1);
		ssd_init(ssd, &spp, cpu_nr_dispatcher);
		conv_init_ftl(&conv_ftls[i], &cpp, ssd);
	}

	/* PCIe, Write buffer are shared by all instances*/
	for (i = 1; i < nr_parts; i++) {
		vfree(conv_ftls[i].ssd->pcie->perf_model);
		kfree(conv_ftls[i].ssd->pcie);
		kfree(conv_ftls[i].ssd->write_buffer);

		conv_ftls[i].ssd->pcie = conv_ftls[0].ssd->pcie;
		conv_ftls[i].ssd->write_buffer = conv_ftls[0].ssd->write_buffer;
	}

	ns->id = id;
	ns->csi = NVME_CSI_NVM;
	ns->nr_parts = nr_parts;
	ns->ftls = (void *)conv_ftls;
	ns->size = (uint64_t)((size * 100) / cpp.pba_pcent);
	ns->mapped = mapped_addr;
	/*register io command handler*/
	ns->proc_io_cmd = conv_proc_nvme_io_cmd;

	NVMEV_INFO("FTL physical space: %lld, logical space: %lld (physical/logical * 100 = %d)\n", size, ns->size,
			   cpp.pba_pcent);

	/* Print allocation summary */
	{
		struct ssdparams *spp = &conv_ftls[0].ssd->sp;
		uint64_t total_lines = spp->tt_lines;
		uint64_t reserved_lines = cpp.gc_thres_lines_high;
		uint64_t usable_lines = total_lines - reserved_lines;
		uint64_t line_size_bytes = (uint64_t)spp->pgs_per_line * spp->pgsz;
		uint64_t physical_capacity = (uint64_t)total_lines * line_size_bytes * nr_parts;
		uint64_t op_capacity = physical_capacity - ns->size;
		uint64_t op_percent_x100 = (op_capacity * 10000) / physical_capacity;

		NVMEV_INFO("========== FTL Allocation Summary ==========\n");
		NVMEV_INFO("Total lines (per partition): %llu\n", total_lines);
		NVMEV_INFO("Reserved lines (gc_thres_high): %llu (NR_MAX_RUH+1=%d+1)\n",
				   reserved_lines, NR_MAX_RUH);
		NVMEV_INFO("Usable lines: %llu\n", usable_lines);
		NVMEV_INFO("Line size: %llu MiB (%lu pages/line * %d bytes/page)\n",
				   line_size_bytes / (1024*1024), spp->pgs_per_line, spp->pgsz);
		NVMEV_INFO("Physical capacity (all partitions): %llu GiB\n", physical_capacity / (1024ULL*1024*1024));
		NVMEV_INFO("Host-visible capacity (ns->size): %llu GiB\n", ns->size / (1024ULL*1024*1024));
		NVMEV_INFO("OP size: %llu GiB (%llu.%02llu%% of physical, config=%d%%)\n",
				   op_capacity / (1024ULL*1024*1024), op_percent_x100 / 100, op_percent_x100 % 100,
				   (int)(cpp.op_area_pcent * 100));
		NVMEV_INFO("=============================================\n");
	}

	return;
}

void conv_remove_namespace(struct nvmev_ns *ns)
{
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	const uint32_t nr_parts = SSD_PARTITIONS;
	uint32_t i;

	/* PCIe, Write buffer are shared by all instances*/
	for (i = 1; i < nr_parts; i++) {
		/*
		 * These were freed from conv_init_namespace() already.
		 * Mark these NULL so that ssd_remove() skips it.
		 */
		conv_ftls[i].ssd->pcie = NULL;
		conv_ftls[i].ssd->write_buffer = NULL;
	}

	for (i = 0; i < nr_parts; i++) {
		conv_remove_ftl(&conv_ftls[i]);
		ssd_remove(conv_ftls[i].ssd);
		kfree(conv_ftls[i].ssd);
	}

	kfree(conv_ftls);
	ns->ftls = NULL;
}

static inline bool valid_ppa(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	int ch = ppa->g.ch;
	int lun = ppa->g.lun;
	int pl = ppa->g.pl;
	int blk = ppa->g.blk;
	int pg = ppa->g.pg;
	//int sec = ppa->g.sec;

	if (ch < 0 || ch >= spp->nchs)
		return false;
	if (lun < 0 || lun >= spp->luns_per_ch)
		return false;
	if (pl < 0 || pl >= spp->pls_per_lun)
		return false;
	if (blk < 0 || blk >= spp->blks_per_pl)
		return false;
	if (pg < 0 || pg >= spp->pgs_per_blk)
		return false;

	return true;
}

static inline bool valid_lpn(struct conv_ftl *conv_ftl, uint64_t lpn)
{
	return (lpn < conv_ftl->ssd->sp.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
	return !(ppa->ppa == UNMAPPED_PPA);
}

static inline struct line *get_line(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	return &(conv_ftl->lm.lines[ppa->g.blk]);
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	bool was_full_line = false;
	struct line *line;

	/* update corresponding page status */
	pg = get_pg(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_VALID);
	pg->status = PG_INVALID;

	/* update corresponding block status */
	blk = get_blk(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
	blk->ipc++;
	NVMEV_ASSERT(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
	blk->vpc--;

	/* update corresponding line status */
	line = get_line(conv_ftl, ppa);
	NVMEV_ASSERT(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
	if (line->vpc == spp->pgs_per_line) {
		NVMEV_ASSERT(line->ipc == 0);
		was_full_line = true;
	}
	line->ipc++;
	NVMEV_ASSERT(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
	/* Adjust the position of the victime line in the pq under over-writes */
	if (line->pos) {
		/* Note that line->vpc will be updated by this call */
		pqueue_change_priority(lm->victim_line_pq, line->vpc - 1, line);
	} else {
		line->vpc--;
	}

	if (was_full_line) {
		/* move line: "full" -> "victim" */
		list_del_init(&line->entry);
		lm->full_line_cnt--;
		pqueue_insert(lm->victim_line_pq, line);
		lm->victim_line_cnt++;
	}
}

static void mark_page_valid(struct conv_ftl *conv_ftl, struct ppa *ppa, uint16_t ruh)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct line *line;

	/* update page status */
	pg = get_pg(conv_ftl->ssd, ppa);
	if (pg->status != PG_FREE) {
		NVMEV_ERROR("BUG mark_page_valid: ppa(ch=%u lun=%u pl=%u blk=%u pg=%u) status=%d\n",
					ppa->g.ch, ppa->g.lun, ppa->g.pl, ppa->g.blk, ppa->g.pg, pg->status);
		NVMEV_ERROR("  limits: nchs=%d luns=%d pls=%d blks=%d pgs=%d\n",
					spp->nchs, spp->luns_per_ch, spp->pls_per_lun,
					spp->blks_per_pl, spp->pgs_per_blk);
	}
	NVMEV_ASSERT(pg->status == PG_FREE);
	pg->status = PG_VALID;
	pg->ruh = ruh;

	/* update corresponding block status */
	blk = get_blk(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(blk->vpc >= 0 && blk->vpc < spp->pgs_per_blk);
	blk->vpc++;

	/* update corresponding line status */
	line = get_line(conv_ftl, ppa);
	NVMEV_ASSERT(line->vpc >= 0 && line->vpc < spp->pgs_per_line);
	line->vpc++;
}

static void mark_block_free(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct nand_block *blk = get_blk(conv_ftl->ssd, ppa);
	struct nand_page *pg = NULL;
	int i;

	for (i = 0; i < spp->pgs_per_blk; i++) {
		/* reset page status */
		pg = &blk->pg[i];
		NVMEV_ASSERT(pg->nsecs == spp->secs_per_pg);
		pg->status = PG_FREE;
		// Reset the RUH info
		pg->ruh = -1;
	}

	/* reset block status */
	NVMEV_ASSERT(blk->npgs == spp->pgs_per_blk);
	blk->ipc = 0;
	blk->vpc = 0;
	blk->erase_cnt++;
}

/* move valid page data (already in DRAM) from victim line to a new page */
static uint64_t gc_write_page(struct conv_ftl *conv_ftl, struct ppa *old_ppa, uint16_t ruh)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct convparams *cpp = &conv_ftl->cp;
	struct ppa new_ppa;
	uint64_t nsecs_completed = 0;
	uint64_t completed_time = 0;
	uint64_t lpn = get_rmap_ent(conv_ftl, old_ppa);

	NVMEV_ASSERT(valid_lpn(conv_ftl, lpn));
	new_ppa = get_new_page(conv_ftl, 0, GC_IO);
	/* update maptbl */
	set_maptbl_ent(conv_ftl, lpn, &new_ppa);
	/* update rmap */
	set_rmap_ent(conv_ftl, lpn, &new_ppa);

	// increase_fdp_counter(0, GC_IO);

	mark_page_valid(conv_ftl, &new_ppa, ruh);

	/* need to advance the write pointer here */
	advance_write_pointer(conv_ftl, 0, GC_IO);

	if (cpp->enable_gc_delay) {
		struct nand_cmd gcw;
		gcw.type = GC_IO;
		gcw.cmd = NAND_NOP;
		gcw.stime = 0;
		gcw.interleave_pci_dma = false;
		gcw.ppa = &new_ppa;
		if (last_pg_in_wordline(conv_ftl, &new_ppa)) {
			gcw.cmd = NAND_WRITE;
			gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;
		}

		nsecs_completed = ssd_advance_nand(conv_ftl->ssd, &gcw);
		// enqueue_gc_io_req(0, completed_time, true, spp->pgsz);
	}

	/* advance per-ch gc_endtime as well */
#if 0
	new_ch = get_ch(conv_ftl, &new_ppa);
	new_ch->gc_endtime = new_ch->next_ch_avail_time;

	new_lun = get_lun(conv_ftl, &new_ppa);
	new_lun->gc_endtime = new_lun->next_lun_avail_time;
#endif

	return nsecs_completed;
}

static struct line *select_victim_line(struct conv_ftl *conv_ftl, bool force)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct line *victim_line = NULL;

	victim_line = pqueue_peek(lm->victim_line_pq);
	if (!victim_line) {
		return NULL;
	}

	if (!force && (victim_line->vpc > (spp->pgs_per_line / 8))) {
		return NULL;
	}

	pqueue_pop(lm->victim_line_pq);
	victim_line->pos = 0;
	lm->victim_line_cnt--;

	/* victim_line is a danggling node now */
	return victim_line;
}

/* here ppa identifies the block we want to clean */
static uint64_t clean_one_flashpg(struct conv_ftl *conv_ftl, struct ppa *ppa, int *ret_cnt, uint32_t *valid_ruh,
								  uint32_t *invalid_ruh)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct convparams *cpp = &conv_ftl->cp;
	struct nand_page *pg_iter = NULL;
	int cnt = 0, i = 0;
	uint64_t nsecs_completed, nsecs_latest = 0;
	struct ppa ppa_copy = *ppa;

	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(conv_ftl->ssd, &ppa_copy);
		/* there shouldn't be any free page in victim blocks */
		NVMEV_ASSERT(pg_iter->status != PG_FREE);
		if (pg_iter->ruh >= NR_MAX_LEVEL) {
			NVMEV_ERROR("Invalid RUH %d in GC clean\n", pg_iter->ruh);
			NVMEV_ASSERT(0);
		}
		if (pg_iter->status == PG_VALID) {
			valid_ruh[pg_iter->ruh]++;
			cnt++;
		} else {
			invalid_ruh[pg_iter->ruh]++;
		}

		ppa_copy.g.pg++;
	}

	ppa_copy = *ppa;

	if (cnt <= 0)
		return 0;

	if (cpp->enable_gc_delay) {
		struct nand_cmd gcr;
		gcr.type = GC_IO;
		gcr.cmd = NAND_READ;
		gcr.stime = 0;
		gcr.xfer_size = spp->pgsz * cnt;
		gcr.interleave_pci_dma = false;
		gcr.ppa = &ppa_copy;
		nsecs_completed = ssd_advance_nand(conv_ftl->ssd, &gcr);
		nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;

		// enqueue_gc_io_req(0, completed_time, false, spp->pgsz * cnt);
	}

	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(conv_ftl->ssd, &ppa_copy);

		/* there shouldn't be any free page in victim blocks */
		if (pg_iter->status == PG_VALID) {
			/* delay the maptbl update until "write" happens */
			nsecs_completed = gc_write_page(conv_ftl, &ppa_copy, pg_iter->ruh);
			nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;
		}

		ppa_copy.g.pg++;
	}

	*ret_cnt += cnt;
	return nsecs_latest;
}

static void mark_line_free(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct line_mgmt *lm = &conv_ftl->lm;
	struct line *line = get_line(conv_ftl, ppa);
	line->ipc = 0;
	line->vpc = 0;
	/* move this line to free line list */
	list_add_tail(&line->entry, &lm->free_line_list);
	lm->free_line_cnt++;
}

static int do_gc(struct conv_ftl *conv_ftl, bool force)
{
	struct line *victim_line = NULL;
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct convparams *cpp = &conv_ftl->cp;
	struct nand_lun *lunp;
	struct ppa ppa;
	int ch, lun, flashpg;

	victim_line = select_victim_line(conv_ftl, force);
	if (!victim_line) {
		return -1;
	}

	ppa.g.blk = victim_line->id;
	NVMEV_DEBUG("GC-ing line:%d,ipc=%d(%d),victim=%d,full=%d,free=%d\n", ppa.g.blk, victim_line->ipc, victim_line->vpc,
				conv_ftl->lm.victim_line_cnt, conv_ftl->lm.full_line_cnt, conv_ftl->lm.free_line_cnt);

	conv_ftl->wfc.credits_to_refill = victim_line->ipc;

	uint32_t valid_ruh[NR_MAX_LEVEL] = {0};
	uint32_t invalid_ruh[NR_MAX_LEVEL] = {0};

	/* copy back valid data */
	for (flashpg = 0; flashpg < spp->flashpgs_per_blk; flashpg++) {
		ppa.g.pg = flashpg * spp->pgs_per_flashpg;
		uint64_t nsecs_completed, nsecs_latest = 0;
		int cnt = 0;

		for (ch = 0; ch < spp->nchs; ch++) {
			for (lun = 0; lun < spp->luns_per_ch; lun++) {
				ppa.g.ch = ch;
				ppa.g.lun = lun;
				ppa.g.pl = 0;
				lunp = get_lun(conv_ftl->ssd, &ppa);
				nsecs_completed = clean_one_flashpg(conv_ftl, &ppa, &cnt, valid_ruh, invalid_ruh);
				nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;

				if (flashpg == (spp->flashpgs_per_blk - 1)) {
					mark_block_free(conv_ftl, &ppa);

					if (cpp->enable_gc_delay) {
						struct nand_cmd gce;
						gce.type = GC_IO;
						gce.cmd = NAND_ERASE;
						gce.stime = 0;
						gce.interleave_pci_dma = false;
						gce.ppa = &ppa;
						ssd_advance_nand(conv_ftl->ssd, &gce);
					}

					lunp->gc_endtime = lunp->next_lun_avail_time;
				}
			}
		}

		enqueue_gc_io_req(0, nsecs_latest, true, spp->pgsz * cnt);
	}

	/* update line status */
	mark_line_free(conv_ftl, &ppa);

	NVMEV_FREEBIE_DEBUG("GC - valid[%u %u %u %u %u %u %u %u] invalid[%u %u %u %u %u %u %u %u]\n",
		   valid_ruh[0], valid_ruh[1], valid_ruh[2], valid_ruh[3], valid_ruh[4], valid_ruh[5], valid_ruh[6], valid_ruh[7],
		   invalid_ruh[0], invalid_ruh[1], invalid_ruh[2], invalid_ruh[3], invalid_ruh[4], invalid_ruh[5], invalid_ruh[6], invalid_ruh[7]);

	return 0;
}

static void forground_gc(struct conv_ftl *conv_ftl)
{
	if (should_gc_high(conv_ftl)) {
		NVMEV_DEBUG("should_gc_high passed");
		/* perform GC here until !should_gc(conv_ftl) */
		do_gc(conv_ftl, true);
	}
}

static bool is_same_flash_page(struct conv_ftl *conv_ftl, struct ppa ppa1, struct ppa ppa2)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	uint32_t ppa1_page = ppa1.g.pg / spp->pgs_per_flashpg;
	uint32_t ppa2_page = ppa2.g.pg / spp->pgs_per_flashpg;

	return (ppa1.h.blk_in_ssd == ppa2.h.blk_in_ssd) && (ppa1_page == ppa2_page);
}

bool pcie_rw(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command_csd *cmd = (struct nvme_command_csd *)(req->cmd);
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	struct conv_ftl *conv_ftl = &conv_ftls[0];
	uint64_t nsecs_latest = ssd_advance_pcie(conv_ftl->ssd, req->nsecs_start, cmd->memory.length);

	ret->nsecs_target = nsecs_latest;
	ret->status = NVME_SC_SUCCESS;
	return true;
}

uint64_t __get_seq_lpn(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;
	uint64_t lba = cmd->rw.slba;
	uint64_t max_size = (cmd->rw.length + 1);

	struct ppa cur_ppa, prev_ppa;
	uint32_t i;
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	struct conv_ftl *conv_ftl = &conv_ftls[0];
	uint32_t nr_parts = ns->nr_parts;

	struct ssdparams *spp = &conv_ftl->ssd->sp;
	uint64_t start_lpn = lba / spp->secs_per_pg;
	uint64_t start_lpn2 = lba / spp->secs_per_pg;
	uint64_t end_lpn = (lba + max_size - 1) / spp->secs_per_pg;
	uint64_t seq_lpn = start_lpn;
	for (i = 0; (i < nr_parts) && (start_lpn <= end_lpn); i++, start_lpn++) {
		uint64_t lpn, local_lpn, temp_lpn;
		temp_lpn = -1;
		conv_ftl = &conv_ftls[start_lpn % nr_parts];
		prev_ppa = get_maptbl_ent(conv_ftl, start_lpn / nr_parts);

		for (lpn = start_lpn; lpn <= end_lpn; lpn += nr_parts) {
			local_lpn = lpn / nr_parts;
			// printk("[%s] conv_ftl=%p, ftl_ins=%lld, local_lpn=%lld",__FUNCTION__, conv_ftl, lpn%nr_parts, lpn/nr_parts);
			// printk("[%s] start_lpn=%lld, lpn=%lld, end_lpn=%lld",__FUNCTION__, start_lpn, lpn, end_lpn);
			cur_ppa = get_maptbl_ent(conv_ftl, local_lpn);
			if (!mapped_ppa(&cur_ppa) || !valid_ppa(conv_ftl, &cur_ppa)) {
				break;
			}

			// aggregate read io in same flash page
			if (mapped_ppa(&prev_ppa) && is_same_flash_page(conv_ftl, cur_ppa, prev_ppa)) {
				temp_lpn = lpn;
				//printk("[%s] start_lpn=%lld, temp_lpn=%lld, end_lpn=%lld",__FUNCTION__, start_lpn2, temp_lpn, end_lpn);
				continue;
			}
			prev_ppa = cur_ppa;
		}

		if (temp_lpn != -1) {
			seq_lpn = (seq_lpn < temp_lpn) ? temp_lpn : seq_lpn;
		}
	}
	//printk("[%s] start_lpn=%lld, temp_lpn=%lld, end_lpn=%lld [final]",__FUNCTION__, start_lpn2, seq_lpn, end_lpn);
	ret->nsecs_target = (seq_lpn + 1 - start_lpn2) * spp->secs_per_pg;
	return (seq_lpn * spp->secs_per_pg);
}

#if (CSD_ENABLE == 1)
bool get_partition_map (struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	struct conv_ftl *conv_ftl = &conv_ftls[0];
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	struct nvme_command *cmd = req->cmd;
	unsigned int relation_id = cmd->common.cdw10[3];
	unsigned int start_offset = cmd->common.cdw10[4]; // Where to start reading in root map
	unsigned int chunk_count = cmd->common.cdw10[2] / FREEBIE_DATA_CHUNK_SIZE;
	unsigned int end_offset = start_offset + chunk_count;

	uint64_t lba = -1;
	uint64_t lpn = -1;
	uint64_t local_lpn = -1;
	uint32_t nr_parts = ns->nr_parts;
	struct ppa ppa;

	uint64_t nsecs_start = req->nsecs_start;
	uint64_t nsecs_completed, nsecs_latest = nsecs_start;

	struct nand_cmd srd;
	srd.type = USER_IO;
	srd.cmd = NAND_READ;
	srd.stime = nsecs_start;
    srd.true_size = spp->pgsz;
	srd.nand_stime = 0;
	srd.interleave_pci_dma = false;

	NVMEV_ASSERT(conv_ftls);

	uint32_t valid_root_buffer;
	if (start_offset == 0) {
		valid_root_buffer = freebie_start_read_partition_map(relation_id, start_offset);
		if (valid_root_buffer == -1) {
			NVMEV_ERROR("Get partition map on relation id %d failed \n", relation_id);
			ret->nsecs_target = nsecs_latest;
			ret->status = NVME_SC_INTERNAL;
			return true;
		}
		cmd->common.cdw10[4] |= valid_root_buffer << 16; // Save this for future use (io, end_read_partition_map)
	}
	valid_root_buffer = (cmd->common.cdw10[4] >> 16) & 0xFFFF;

	for (int i = start_offset; i < end_offset; i++) {
		lba = freebie_get_lba_of_map_block(relation_id, valid_root_buffer, i);
		lpn = lba / spp->secs_per_pg;
		if ((lpn / nr_parts) >= spp->tt_pgs) {
			NVMEV_ERROR("Get partition map: lpn passed FTL range(lpn=%lld,tt_pgs=%ld)\n", lpn, spp->tt_pgs);
			ret->nsecs_target = nsecs_latest;
			ret->status = NVME_SC_INTERNAL;
			return true;
		}

		// Always send request in 4KiB chunks
		srd.stime += spp->fw_4kb_rd_lat;
		conv_ftl = &conv_ftls[lpn % nr_parts];
		local_lpn = lpn / nr_parts;
		ppa = get_maptbl_ent(conv_ftl, local_lpn);
		if (!mapped_ppa(&ppa) || !valid_ppa(conv_ftl, &ppa)) {
			NVMEV_DEBUG("lpn 0x%llx not mapped to valid ppa\n", local_lpn);
			NVMEV_DEBUG("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d\n", ppa.g.ch, ppa.g.lun, ppa.g.blk,
						ppa.g.pl, ppa.g.pg);
			continue;
		}
		srd.xfer_size = spp->pgsz;
		srd.ppa = &ppa;
		nsecs_completed = ssd_advance_nand(conv_ftl->ssd, &srd);

		nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;
	}
	nsecs_latest = ssd_advance_pcie(conv_ftl->ssd, nsecs_latest, chunk_count * FREEBIE_DATA_CHUNK_SIZE);

	ret->nsecs_nand_start = srd.nand_stime;
	ret->nsecs_target = nsecs_latest;
	ret->status = NVME_SC_SUCCESS;
	return true;
}
#endif

bool conv_read (struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	struct conv_ftl *conv_ftl = &conv_ftls[0];
	/* spp are shared by all instances*/
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	struct nvme_command *cmd = req->cmd;
	uint64_t lba = cmd->rw.slba;
	uint64_t nr_lba = (cmd->rw.length + 1);
	uint64_t start_lpn = lba / spp->secs_per_pg;
	uint64_t end_lpn = (lba + nr_lba - 1) / spp->secs_per_pg;
	uint64_t lpn, local_lpn;
	uint64_t nsecs_start = req->nsecs_start;
	uint64_t nsecs_completed, nsecs_latest = nsecs_start;
	uint32_t xfer_size, i;
	uint32_t nr_parts = ns->nr_parts;

	struct ppa cur_ppa, prev_ppa;
	struct nand_cmd srd;
	srd.type = USER_IO;
	srd.cmd = NAND_READ;
	srd.stime = nsecs_start;
	srd.true_size = LBA_TO_BYTE(nr_lba);
	srd.nand_stime = 0;
	srd.interleave_pci_dma = false;

	NVMEV_ASSERT(conv_ftls);
	NVMEV_DEBUG("conv_read: start_lpn=%lld, len=%d, end_lpn=%ld", start_lpn, nr_lba, end_lpn);
	if ((end_lpn / nr_parts) >= spp->tt_pgs) {
		NVMEV_ERROR("conv_read: lpn passed FTL range(start_lpn=%lld,tt_pgs=%ld)\n", start_lpn, spp->tt_pgs);
		return false;
	}

	if (LBA_TO_BYTE(nr_lba) <= (KB(4))) {
		srd.stime += spp->fw_4kb_rd_lat;
	} else {
		srd.stime += spp->fw_rd_lat;
	}

	for (i = 0; (i < nr_parts) && (start_lpn <= end_lpn); i++, start_lpn++) {
		conv_ftl = &conv_ftls[start_lpn % nr_parts];
		xfer_size = 0;
		prev_ppa = get_maptbl_ent(conv_ftl, start_lpn / nr_parts);

		NVMEV_DEBUG("[%s] conv_ftl=%p, ftl_ins=%lld, local_lpn=%lld", __FUNCTION__, conv_ftl, lpn % nr_parts,
					lpn / nr_parts);

		/* normal IO read path */
		for (lpn = start_lpn; lpn <= end_lpn; lpn += nr_parts) {
			local_lpn = lpn / nr_parts;
			cur_ppa = get_maptbl_ent(conv_ftl, local_lpn);
			if (!mapped_ppa(&cur_ppa) || !valid_ppa(conv_ftl, &cur_ppa)) {
				NVMEV_DEBUG("lpn 0x%llx not mapped to valid ppa\n", local_lpn);
				NVMEV_DEBUG("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d\n", cur_ppa.g.ch, cur_ppa.g.lun, cur_ppa.g.blk,
							cur_ppa.g.pl, cur_ppa.g.pg);
				continue;
			}

			// aggregate read io in same flash page
			if (mapped_ppa(&prev_ppa) && is_same_flash_page(conv_ftl, cur_ppa, prev_ppa)) {
				xfer_size += spp->pgsz;
				continue;
			}

			if (xfer_size > 0) {
				srd.xfer_size = xfer_size;
				srd.ppa = &prev_ppa;
				nsecs_completed = ssd_advance_nand(conv_ftl->ssd, &srd);
				nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;
			}

			xfer_size = spp->pgsz;
			prev_ppa = cur_ppa;
		}

		// issue remaining io
		if (xfer_size > 0) {
			srd.xfer_size = xfer_size;
			srd.ppa = &prev_ppa;
			nsecs_completed = ssd_advance_nand(conv_ftl->ssd, &srd);
			nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;
		}
	}
	if (srd.interleave_pci_dma == false) {
		nsecs_latest = ssd_advance_pcie(conv_ftl->ssd, nsecs_latest, LBA_TO_BYTE(nr_lba));
	}

	ret->nsecs_nand_start = srd.nand_stime;
	ret->nsecs_target = nsecs_latest;
	ret->status = NVME_SC_SUCCESS;
	return true;
}

/* RUH write statistics */
static atomic64_t ruh_write_cnt[NR_MAX_RUH];
static atomic64_t ruh_total_writes;

bool conv_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	struct conv_ftl *conv_ftl = &conv_ftls[0];

	/* wbuf and spp are shared by all instances */
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct buffer *wbuf = conv_ftl->ssd->write_buffer;

	struct nvme_command *cmd = req->cmd;
	uint64_t lba = cmd->rw.slba;
	uint64_t nr_lba = (cmd->rw.length + 1);
	uint64_t start_lpn = lba / spp->secs_per_pg;
	uint64_t end_lpn = (lba + nr_lba - 1) / spp->secs_per_pg;

	uint64_t lpn, local_lpn;
	uint32_t nr_parts = ns->nr_parts;

	uint64_t nsecs_start = req->nsecs_start;
	uint64_t nsecs_completed = 0, nsecs_latest;
	uint64_t nsecs_xfer_completed;
	uint32_t allocated_buf_size;

	uint8_t dtype = (cmd->rw.control >> 4) & 0xF;
	uint16_t ruh = (cmd->rw.dsmgmt) >> 16 & 0xFFFF;
	uint16_t ruh_copy = ruh;
	if (ruh >= NR_MAX_RUH) {
		ruh = 0;
		// NVMEV_ERROR("conv_write: Invalid Replacement Unit Handle (ruh=%d)\n", ruh);
		// return false;
	}

	struct ppa ppa;
	struct nand_cmd swr;

	NVMEV_ASSERT(conv_ftls);

	/* RUH statistics (in LBA units, 4K each) */
	atomic64_add(nr_lba, &ruh_write_cnt[ruh]);
	if (atomic64_add_return(nr_lba, &ruh_total_writes) % 1000000 == 0) {
		NVMEV_INFO("RUH stats: [%lld %lld %lld %lld %lld %lld %lld %lld] total=%lld\n",
			atomic64_read(&ruh_write_cnt[0]), atomic64_read(&ruh_write_cnt[1]),
			atomic64_read(&ruh_write_cnt[2]), atomic64_read(&ruh_write_cnt[3]),
			atomic64_read(&ruh_write_cnt[4]), atomic64_read(&ruh_write_cnt[5]),
			atomic64_read(&ruh_write_cnt[6]), atomic64_read(&ruh_write_cnt[7]),
			atomic64_read(&ruh_total_writes));
	}
	NVMEV_DEBUG("conv_write: start_lpn=%lld, len=%d, end_lpn=%lld", start_lpn, nr_lba, end_lpn);
	if ((end_lpn / nr_parts) >= spp->tt_pgs) {
		NVMEV_ERROR("conv_write: lpn passed FTL range(start_lpn=%lld,tt_pgs=%ld)\n", start_lpn, spp->tt_pgs);
		return false;
	}

	allocated_buf_size = buffer_allocate(wbuf, LBA_TO_BYTE(nr_lba));

	if (allocated_buf_size < LBA_TO_BYTE(nr_lba))
		return false;

	nsecs_latest = nsecs_start;
	if (req->sq_id != 0xFFFFFFFF) {
		nsecs_latest = ssd_advance_write_buffer(conv_ftl->ssd, nsecs_latest, LBA_TO_BYTE(nr_lba), true);
	} else {
		nsecs_latest = ssd_advance_write_buffer(conv_ftl->ssd, nsecs_latest, LBA_TO_BYTE(nr_lba), false);
	}
	nsecs_xfer_completed = nsecs_latest;

	swr.type = USER_IO;
	swr.cmd = NAND_WRITE;
	swr.stime = nsecs_latest;
	swr.interleave_pci_dma = false;

	for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
		conv_ftl = &conv_ftls[lpn % nr_parts];
		local_lpn = lpn / nr_parts;
		ppa = get_maptbl_ent(conv_ftl, local_lpn); // 현재 LPN에 대해 전에 이미 쓰인 PPA가 있는지 확인
		if (mapped_ppa(&ppa)) {
			/* update old page information first */
			mark_page_invalid(conv_ftl, &ppa);
			set_rmap_ent(conv_ftl, INVALID_LPN, &ppa);
			NVMEV_DEBUG("conv_write: %lld is invalid, ", ppa2pgidx(conv_ftl, &ppa));
		}

		/* new write */
		ppa = get_new_page(conv_ftl, ruh, USER_IO);
		/* update maptbl */
		set_maptbl_ent(conv_ftl, local_lpn, &ppa);
		NVMEV_DEBUG("conv_write: got new ppa %lld, ", ppa2pgidx(conv_ftl, &ppa));
		/* update rmap */
		set_rmap_ent(conv_ftl, local_lpn, &ppa);

		// increase_fdp_counter(ruh, USER_IO);

		mark_page_valid(conv_ftl, &ppa, ruh_copy);

		/* need to advance the write pointer here */
		advance_write_pointer(conv_ftl, ruh, USER_IO);

		/* Aggregate write io in flash page */
		if (last_pg_in_wordline(conv_ftl, &ppa)) {
			swr.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;
			swr.ppa = &ppa;
			nsecs_completed = ssd_advance_nand(conv_ftl->ssd, &swr);
			nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;

			enqueue_writeback_io_req(req->sq_id, nsecs_completed, wbuf, spp->pgs_per_oneshotpg * spp->pgsz);
		}

		consume_write_credit(conv_ftl);
		check_and_refill_write_credit(conv_ftl);
	}

	if ((cmd->rw.control & NVME_RW_FUA) || (spp->write_early_completion == 0)) {
		/* Wait all flash operations */
		ret->nsecs_target = nsecs_latest;
	} else {
		/* Early completion */
		ret->nsecs_target = nsecs_xfer_completed;
	}
	ret->nsecs_nand_start = swr.stime;
	ret->status = NVME_SC_SUCCESS;

	return true;
}

void conv_flush(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	uint64_t start, latest;
	uint32_t i;
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;

	start = local_clock();
	latest = start;
	for (i = 0; i < ns->nr_parts; i++) {
		latest = max(latest, ssd_next_idle_time(conv_ftls[i].ssd));
	}

	NVMEV_DEBUG("%s latency=%llu\n", __FUNCTION__, latest - start);

	ret->status = NVME_SC_SUCCESS;
	ret->nsecs_target = latest;
	return;
}

void conv_dsm(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	// Currently only the Trim command is supported
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	struct conv_ftl *conv_ftl = &conv_ftls[0];
	struct nvme_command *cmd = req->cmd;
	uint32_t num_range = cmd->dsm.nr + 1; 
	uint32_t attr = cmd->dsm.attributes;

	if (!(attr & NVME_DSMGMT_AD)) {
		NVMEV_ERROR("Only deallocate attribute is supported in DSM\n");
		ret->status = NVME_SC_INVALID_FIELD;
		return;
	}

	size_t size = num_range * sizeof(struct nvme_dsm_range);
	struct nvme_dsm_range *ranges = kmalloc_node(sizeof(struct nvme_dsm_range) * num_range, GFP_KERNEL, 1);
	get_prp_data(req->cmd, ranges, size, true);

	for (int i = 0; i < num_range; i++) {
		struct nvme_dsm_range *r = (ranges + i);
		uint64_t slba = r->slba;
		uint32_t nlb = r->nlb; // 1's based value
		uint32_t nr_parts = ns->nr_parts;
		struct ssdparams *spp = &conv_ftls[0].ssd->sp;
		uint64_t start_lpn = slba / spp->secs_per_pg;
		uint64_t end_lpn = (slba + nlb - 1) / spp->secs_per_pg;
		uint64_t lpn, local_lpn;
		struct ppa ppa;

		NVMEV_INFO("DSM TRIM: slba=%llu, nlb=%u, start_lpn=%llu, end_lpn=%llu\n",
			slba, nlb, start_lpn, end_lpn);

		for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
			conv_ftl = &conv_ftls[lpn % nr_parts];
			local_lpn = lpn / nr_parts;
			ppa = get_maptbl_ent(conv_ftl, local_lpn);
			if (!mapped_ppa(&ppa) || !valid_ppa(conv_ftl, &ppa)) {
				continue;
			} else {
				/* update old page information first */
				mark_page_invalid(conv_ftl, &ppa);
				set_rmap_ent(conv_ftl, INVALID_LPN, &ppa);
				ppa.ppa = UNMAPPED_PPA;
				set_maptbl_ent(conv_ftl, local_lpn, &ppa);
			}
		}
	}
	kfree(ranges);
}

bool conv_namespace_copy(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command_csd *cmd = (struct nvme_command_csd *)req->cmd;
	struct nvme_command dummy_cmd;
	struct source_range_entry sre;
	int nentry;

	struct nvmev_request dummy_req = {
		.cmd = &dummy_cmd,
		.sq_id = 0xFFFFFFFF,
		.nsecs_start = req->nsecs_start,
	};

	struct nvmev_result dummy_ret = {
		.nsecs_target = req->nsecs_start,
		.status = NVME_SC_SUCCESS,
	};

	nentry = cmd->namespace_copy.format;
	if (nentry != 1) {
		NVMEV_ERROR("parameter error (nentry:%d)", nentry);
		BUG();
	}

	get_prp_data(req->cmd, &sre, sizeof(struct source_range_entry), true);

	uint64_t nsecs_nand_start = 0;
	size_t block_size = 0;
	if (sre.nByte < 4096) {
		block_size = 1;
	} else {
		block_size = sre.nByte / 4096 - 1;
	}

	// printk("%lld %lld %ld %d\n", sre.saddr, cmd->namespace_copy.sdaddr, block_size, cmd->namespace_copy.control_flag);

	size_t offset = 0;
	size_t remain = block_size;
	while (remain > 0) {
		size_t temp_len = (remain > 0xFFFF) ? 0xFFFF : remain;
		if (temp_len > 0xFFFF) {
			NVMEV_ERROR("block size overflow:%lu", temp_len);
		}
		memset(&dummy_cmd, 0, sizeof(struct nvme_command));
		dummy_cmd.rw.slba = sre.saddr + offset;
		dummy_cmd.rw.length = temp_len;
		dummy_cmd.rw.opcode = cmd->namespace_copy.control_flag;

		if (dummy_cmd.rw.opcode == nvme_cmd_write) {
			if (remain - temp_len > 0) {
				NVMEV_ERROR("block size exceeding length %lu\n", block_size);
			}
			if (conv_write(ns, &dummy_req, &dummy_ret) == false) {
				ret->nsecs_target = -1;
				return false;
			}
		} else if (dummy_cmd.rw.opcode == nvme_cmd_read) {
			if (conv_read(ns, &dummy_req, &dummy_ret) == false) {
				ret->nsecs_target = -1;
				return false;
			}
		}

		ret->nsecs_target = dummy_ret.nsecs_target;
		offset = offset + (temp_len + 1);
		remain = remain - temp_len;
	}

	return true;
}
bool conv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;

	NVMEV_ASSERT(ns->csi == NVME_CSI_NVM);

	switch (cmd->common.opcode) {
	case nvme_cmd_write:
		if (!conv_write(ns, req, ret))
			return false;
		break;
	case nvme_cmd_read:
		if (!conv_read(ns, req, ret))
			return false;
		break;
	case nvme_cmd_flush:
		conv_flush(ns, req, ret);
		break;
#if (CSD_ENABLE == 1)
	case nvme_cmd_freebie_get_partition_map:
		get_partition_map(ns, req, ret);
		break;
#endif
	case nvme_cmd_write_uncor:
	case nvme_cmd_compare:
	case nvme_cmd_write_zeroes:
	case nvme_cmd_dsm:
		conv_dsm(ns, req, ret);
		break;
	case nvme_cmd_resv_register:
	case nvme_cmd_resv_report:
	case nvme_cmd_resv_acquire:
	case nvme_cmd_resv_release:
		break;
#if (CSD_ENABLE == 1)
	case nvme_cmd_memory_write:
	case nvme_cmd_memory_read:
	case nvme_cmd_freebie_get_root:
		pcie_rw(ns, req, ret);
		break;
#endif

	case nvme_cmd_fdp_get_written:
		pcie_rw(ns, req, ret);
		break;

	case nvme_test:
		__get_seq_lpn(ns, req, ret);
		break;
	case nvme_cmd_namespace_copy:
		if (!conv_namespace_copy(ns, req, ret))
			return false;
		break;
	default:
		break;
	}

	return true;
}
