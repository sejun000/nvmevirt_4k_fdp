#ifndef __CSD_VIRT_H__
#define __CSD_VIRT_H__
#include <linux/nvme_ioctl.h>
#include <time.h>
#include <pthread.h>

#define u32 __u32
#define u64 __u64
#define uint64_t __u64
#define uint32_t __u32
#define uint8_t __u8

#include "../../nvme_csd.h"

struct CSDVirt {
    int m_fd;
    int m_nsid;
};
void _initNvmeCommand(struct CSDVirt* csdvirt, struct nvme_passthru_cmd* nvme_cmd, int opcode);
void* _getLBA(int fd);

// CSD Initialize
int csdvirt_init_dev(struct CSDVirt* csdvirt, const char* path);
void csdvirt_release_dev(struct CSDVirt* csdvirt);

// FREEBIE Commands
int csdvirt_freebie_repartition_command(struct CSDVirt *csdvirt, const char **input_file_list, const char **output_file_list,
                                        int input_file_count, int output_file_count,
                                        struct freebie_params *user_params);
int csdvirt_freebie_setup_root_command(struct CSDVirt *csdvirt, const char * file_path, uint32_t relation_id, size_t size);
int csdvirt_freebie_terminate_root_command(struct CSDVirt *csdvirt, unsigned int relation_id);

int csdvirt_freebie_get_partition_map_command(struct CSDVirt *csdvirt, void *host_buf, unsigned int relation_id,
                                                    size_t total_count);

unsigned int csdvirt_freebie_get_root_command(struct CSDVirt *csdvirt, void *host_buf, unsigned int relation_id);
int csdvirt_freebie_release_root_command(struct CSDVirt *csdvirt, unsigned int relation_id, unsigned int root_id);

int csdvirt_freebie_debug_command(struct CSDVirt *csdvirt);
int csdvirt_fdp_get_written(struct CSDVirt *csdvirt, void* host_buf);

#endif
