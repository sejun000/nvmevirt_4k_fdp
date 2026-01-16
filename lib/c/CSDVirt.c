#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <linux/nvme_ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <dirent.h>
#include <ftw.h>
#include <time.h>
#include <malloc.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>

#include <linux/fs.h>
#include <linux/fiemap.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>

#include "CSDVirt.h"
#include "../../user_function/freebie/freebie_functions.h"

int csdvirt_init_dev(struct CSDVirt* csdvirt, const char* path)
{
    csdvirt->m_fd = open(path, O_RDWR | O_DIRECT);
    if (csdvirt->m_fd <= 0) {
        printf("[Failed]\tOpen device file %d %d\n", csdvirt->m_fd, errno);
        return -1;
    }

    sync();
    sync();
    sync();

    csdvirt->m_nsid = 1;

    return 0;
}

void csdvirt_release_dev(struct CSDVirt* csdvirt)
{
    if (csdvirt->m_fd > 0) {
        close(csdvirt->m_fd);
    }
}

void _initNvmeCommand(struct CSDVirt* csdvirt, struct nvme_passthru_cmd* nvme_cmd, int opcode)
{
    memset(nvme_cmd, 0, sizeof(struct nvme_passthru_cmd));
    nvme_cmd->nsid = csdvirt->m_nsid;
    nvme_cmd->opcode = opcode;
}

void* _getLBA(int fd)
{
    struct fiemap* fiemap = malloc(sizeof(struct fiemap));
    
    memset(fiemap, 0, sizeof(struct fiemap));
    fiemap->fm_start = 0;
    fiemap->fm_length = ~0;  // All extents
    fiemap->fm_flags = FIEMAP_FLAG_SYNC;

    if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
        perror("Failed to execute FS_IOC_FIEMAP ioctl");
        return NULL;
    }

    int num_extents = fiemap->fm_mapped_extents;
    size_t extents_size = sizeof(struct fiemap_extent) * num_extents;
    
    /* Resize fiemap to allow us to read in the extents */
	if ((fiemap = (struct fiemap*)realloc(fiemap, sizeof(struct fiemap) + extents_size)) == NULL) {
		fprintf(stderr, "Out of memory allocating fiemap\n");	
		return NULL;
	}
	memset(fiemap->fm_extents, 0, extents_size);
	fiemap->fm_extent_count = fiemap->fm_mapped_extents;

    if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
        perror("Failed to execute FS_IOC_FIEMAP second ioctl");
        return NULL;
    }

    return (void *)fiemap;
}

// FREEBIE Functions
int csdvirt_freebie_repartition_command(struct CSDVirt *csdvirt, const char **input_file_list, const char **output_file_list,
                                        int input_file_count, int output_file_count,
                                        struct freebie_params *user_params) 
{
    int m_fd = csdvirt->m_fd;
    struct nvme_passthru_cmd nvmeCmd;
    struct nvme_passthru_cmd *m_nvmeCmd = &nvmeCmd;
    struct source_range_entry *sources;
    struct fiemap *fiemap;
    size_t len;
    int ret = 0;

    struct ccsd_freebie_parameter *freebie_param =
        (struct ccsd_freebie_parameter *)malloc(sizeof(struct ccsd_freebie_parameter));

    if (!freebie_param) {
        perror("malloc failed");
        return -1;
    }

    for (int i = 0; i < input_file_count; i++) {
        int fd = open(input_file_list[i], O_RDWR | O_DIRECT | O_SYNC, 0666);
        if (fd == -1) {
            printf("[FAIL] Unable to open file: %s\n", input_file_list[i]);
            free(freebie_param);
            return -1;
        }

        fiemap = (struct fiemap *)_getLBA(fd);  // extern or define this
        int nentry = 0;
        int num_extents = fiemap->fm_mapped_extents;
        int leftover_size = user_params->input_buf_size[i];

        if (num_extents >= FREEBIE_PARAM_MAX_SRE_COUNT) {
          printf("[FAIL] Too many Extents on file %s. (extent #: %d)\n", output_file_list[i], num_extents);
          printf("Report this error to HJ\n");
          free(freebie_param);
          close(fd);
          return -1;
        }

        for (int j = 0; j < num_extents; j++) {
            sources = &freebie_param->input_sre[i][j];
            len = fiemap->fm_extents[j].fe_length;

            if (len > leftover_size)
                len = leftover_size;

            sources->nsid = 1;
            sources->saddr = fiemap->fm_extents[j].fe_physical / 4096;
            sources->nByte = len;

            nentry++;
            leftover_size -= len;

            if (leftover_size == 0)
                break;
        }

        freebie_param->input_sre_count[i] = nentry;

        free(fiemap);
        close(fd);
    }

    for (int i = 0; i < output_file_count; i++) {
        int fd = open(output_file_list[i], O_RDWR | O_DIRECT | O_SYNC, 0666);
        if (fd == -1) {
            printf("[FAIL] Unable to open file: %s\n", output_file_list[i]);
            free(freebie_param);
            return -1;
        }

        fiemap = (struct fiemap *)_getLBA(fd);
        int nentry = 0;
        int num_extents = fiemap->fm_mapped_extents;
        int leftover_size = user_params->output_buf_alloc_size[i];

        if (num_extents >= FREEBIE_PARAM_MAX_SRE_COUNT) {
          printf("[FAIL] Too many Extents on file %s. (extent #: %d)\n", output_file_list[i], num_extents);
          printf("Report this error to HJ\n");
          free(freebie_param);
          close(fd);
          return -1;
        }

        for (int j = 0; j < num_extents; j++) {
            sources = &freebie_param->output_sre[i][j];
            len = fiemap->fm_extents[j].fe_length;

            if (len > leftover_size)
                len = leftover_size;

            sources->nsid = 1;
            sources->saddr = fiemap->fm_extents[j].fe_physical / 4096;
            sources->nByte = len;

            nentry++;
            leftover_size -= len;

            if (leftover_size == 0)
                break;
        }

        freebie_param->output_sre_count[i] = nentry;

        free(fiemap);
        close(fd);
    }

    memcpy(&freebie_param->param, user_params, sizeof(struct freebie_params));
    freebie_param->param_size = sizeof(struct freebie_params);

    _initNvmeCommand(csdvirt, m_nvmeCmd, nvme_cmd_freebie);  // extern or define this
    m_nvmeCmd->cdw2 = FREEBIE_REPART_INDEX;
    m_nvmeCmd->addr = (__u64)freebie_param;
    m_nvmeCmd->data_len = sizeof(struct ccsd_freebie_parameter);
    m_nvmeCmd->cdw15 = sizeof(struct ccsd_freebie_parameter);

    ret = ioctl(m_fd, NVME_IOCTL_IO_CMD, m_nvmeCmd);
    if (ret < 0) {
        printf("[FAIL] freebie command : %d\n", errno);
        free(freebie_param);
        return -1;
    }

	ret = m_nvmeCmd->result;
    if (ret != 0) {
        // printf("[FAIL] FreeBie Repartition failed with resource allocation fail\n");
        free(freebie_param);
        return -1;
    }

    free(freebie_param);
    return ret;
}

int csdvirt_freebie_setup_root_command(struct CSDVirt *csdvirt, const char * file_path, uint32_t relation_id, size_t size)
{
	// Open Metadata File
    int m_fd = csdvirt->m_fd;
	int fd = open(file_path, O_RDWR | O_SYNC | O_DIRECT, 0666);
	if (fd == -1) {
		printf("Unable to open file: %s\n", file_path);
		return 0;
	}

	struct fiemap *fiemap = (struct fiemap *)_getLBA(fd);
	size_t len = 0;
	size_t nentry = 0;
	size_t num_extents = fiemap->fm_mapped_extents;
	size_t leftover_size = size;
	struct source_range_entry *sources = (struct source_range_entry *)malloc(sizeof(struct source_range_entry) * 200);

	// Create SRE
	for (int i = 0; i < num_extents; i++) {
		len = fiemap->fm_extents[i].fe_length;
		/* Handle 4K unalignment */
		if ((len > leftover_size)) {
			len = leftover_size;
		}

		sources[nentry].nsid = 1;
		sources[nentry].saddr = fiemap->fm_extents[i].fe_physical / 4096;
		sources[nentry].nByte = len;

		nentry++;
		leftover_size -= len;

		if (leftover_size == 0) {
			break;
		}
	}
	free(fiemap);
	close(fd);

	struct nvme_passthru_cmd nvmeCmd;
	struct nvme_passthru_cmd *m_nvmeCmd = &nvmeCmd;
	int ret = 0;

	_initNvmeCommand(csdvirt, m_nvmeCmd, nvme_cmd_freebie);
	m_nvmeCmd->cdw2 = FREEBIE_SETUP_INDEX;
	m_nvmeCmd->addr = (__u64)sources;
	m_nvmeCmd->data_len = sizeof(struct source_range_entry) * nentry;
	m_nvmeCmd->cdw12 = nentry;
	m_nvmeCmd->cdw13 = relation_id;

	ret = ioctl(m_fd, NVME_IOCTL_IO_CMD, m_nvmeCmd);
	if (ret < 0) {
		printf("FREEBIE setup failed : %d\n", ret);
		return -1;
	}
	free(sources);

	return 0;
}

int csdvirt_freebie_get_partition_map_command(struct CSDVirt *csdvirt, void *host_buf, unsigned int relation_id,
                                                    size_t total_count)
{
    int m_fd = csdvirt->m_fd;
	struct nvme_passthru_cmd nvmeCmd;
	struct nvme_passthru_cmd *m_nvmeCmd = &nvmeCmd;
    size_t offset = 0;
    size_t count = 0;
    size_t remaining = total_count;
    int valid_root_buffer = 0;
    int ret = 0;

    if (total_count > FREEBIE_MAX_VALID_ROOT_INDEX) {
        printf("Get Partition Map root index count %ld exceeds the limit %d\n", count, FREEBIE_MAX_VALID_ROOT_INDEX);
        return -1;
    }
	_initNvmeCommand(csdvirt, m_nvmeCmd, nvme_cmd_freebie_get_partition_map);
    m_nvmeCmd->cdw13 = relation_id;

    while (remaining) {
        count = FREEBIE_ROOT_ENTRY_COUNT_PER_MDTS;
        count = (remaining < count) ? remaining : count;
        if (count == remaining) {
	        m_nvmeCmd->cdw11 = 1;
        }

	    m_nvmeCmd->addr = (__u64)host_buf + (offset * FREEBIE_DATA_CHUNK_SIZE);
	    m_nvmeCmd->data_len = count * FREEBIE_DATA_CHUNK_SIZE;
	    m_nvmeCmd->cdw12 = count * FREEBIE_DATA_CHUNK_SIZE;
        m_nvmeCmd->cdw14 = offset | (valid_root_buffer << 16);
	    if(ioctl(m_fd, NVME_IOCTL_IO_CMD, m_nvmeCmd)) {
	        printf("Get Partition Map Failed (count : %lu)\n", count);
            return -1;
	    }

        if (m_nvmeCmd->result < 0) {
            printf("Get Partition Map Failed (result : %d)\n", m_nvmeCmd->result);
            return -1;
        }

        if (offset == 0) {
            valid_root_buffer = m_nvmeCmd->result;
        }

        if (count == remaining) {
            ret = m_nvmeCmd->result;
        }
        offset += count;
        remaining -= count;
    }

	return ret;
}

int csdvirt_freebie_debug_command(struct CSDVirt *csdvirt)
{
    int m_fd = csdvirt->m_fd;
	struct nvme_passthru_cmd nvmeCmd;
	struct nvme_passthru_cmd *m_nvmeCmd = &nvmeCmd;
	int ret = 0;

	_initNvmeCommand(csdvirt, m_nvmeCmd, nvme_cmd_freebie);
	m_nvmeCmd->cdw2 = FREEBIE_DEBUG_INDEX;

	ret = ioctl(m_fd, NVME_IOCTL_IO_CMD, m_nvmeCmd);
	if (ret < 0) {
		printf("FREEBIE debug failed : %d\n", ret);
		return -1;
	}
	return ret;
}

#define NR_MAX_RUH 4

int csdvirt_fdp_get_written(struct CSDVirt *csdvirt, void* host_buf)
{
    int m_fd = csdvirt->m_fd;
	struct nvme_passthru_cmd nvmeCmd;
	struct nvme_passthru_cmd *m_nvmeCmd = &nvmeCmd;
	int ret = 0;

	_initNvmeCommand(csdvirt, m_nvmeCmd, nvme_cmd_fdp_get_written);
    m_nvmeCmd->addr = (__u64)host_buf;
	m_nvmeCmd->data_len = sizeof(uint64_t) * (NR_MAX_RUH + 1); // 1 for MBMW
	m_nvmeCmd->cdw12 = sizeof(uint64_t) * (NR_MAX_RUH + 1);

	ret = ioctl(m_fd, NVME_IOCTL_IO_CMD, m_nvmeCmd);
	if (ret < 0) {
		printf("FREEBIE failed to get fdp written: %d\n", ret);
		return -1;
	}

    return m_nvmeCmd->result;
}

int csdvirt_freebie_terminate_root_command(struct CSDVirt *csdvirt, unsigned int relation_id)
{
    int m_fd = csdvirt->m_fd;
	struct nvme_passthru_cmd nvmeCmd;
	struct nvme_passthru_cmd *m_nvmeCmd = &nvmeCmd;
	int ret = 0;

	_initNvmeCommand(csdvirt, m_nvmeCmd, nvme_cmd_freebie);
	m_nvmeCmd->cdw2 = FREEBIE_TERMINATE_INDEX;
	m_nvmeCmd->cdw12 = relation_id;

	ret = ioctl(m_fd, NVME_IOCTL_IO_CMD, m_nvmeCmd);
	if (ret < 0) {
		printf("FREEBIE terminate relation id %u failed : %d\n", relation_id, ret);
		return -1;
	}
	return ret;
}

/// Depricated Functions

unsigned int csdvirt_freebie_get_root_command(struct CSDVirt *csdvirt, void *host_buf, unsigned int relation_id)
{
    int m_fd = csdvirt->m_fd;
	struct nvme_passthru_cmd nvmeCmd;
	struct nvme_passthru_cmd *m_nvmeCmd = &nvmeCmd;

	int ret = 0;
	_initNvmeCommand(csdvirt, m_nvmeCmd, nvme_cmd_freebie_get_root);
	m_nvmeCmd->addr = (__u64)host_buf;
	m_nvmeCmd->data_len = FREEBIE_ROOT_CHUNK_SIZE;
	m_nvmeCmd->cdw13 = relation_id;
	m_nvmeCmd->cdw12 = FREEBIE_ROOT_CHUNK_SIZE;

	ret = ioctl(m_fd, NVME_IOCTL_IO_CMD, m_nvmeCmd);

	if (ret < 0) {
		printf("Get ROOT failed: %d\n", ret);

		return -1;
	} else if (ret == 0) {
		ret = m_nvmeCmd->result;
	}

	return m_nvmeCmd->result;
}

int csdvirt_freebie_release_root_command(struct CSDVirt *csdvirt, unsigned int relation_id, unsigned int root_id)
{
    int m_fd = csdvirt->m_fd;
	struct nvme_passthru_cmd nvmeCmd;
	struct nvme_passthru_cmd *m_nvmeCmd = &nvmeCmd;
	int ret = 0;

	_initNvmeCommand(csdvirt, m_nvmeCmd, nvme_cmd_freebie);
	m_nvmeCmd->cdw2 = FREEBIE_RELEASE_CAT_INDEX;
	m_nvmeCmd->cdw12 = relation_id;
	m_nvmeCmd->cdw13 = root_id;

	ret = ioctl(m_fd, NVME_IOCTL_IO_CMD, m_nvmeCmd);
	if (ret < 0) {
		printf("FREEBIE release relation id %u failed : %d\n", relation_id, ret);
		return -1;
	}
	return ret;
}
