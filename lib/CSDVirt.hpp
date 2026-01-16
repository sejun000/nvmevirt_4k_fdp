#ifndef __CSD_VIRT_H__
#define __CSD_VIRT_H__
#include <linux/nvme_ioctl.h>
#include <time.h>
#include <pthread.h>
#include <string>

#include "../nvme_csd.h"

#define __DEBUG__ (0)

#define u32 __u32
#define u64 __u64
#define uint64 __u64

static const int INVALID_VALUE = -1;
static const size_t MAX_IO_SPLIT_SIZE = 256 * 1024;

#define COPY_TO_SLM 	(true)
#define COPY_FROM_SLM 	(false)

class CSDVirt {
public:
	CSDVirt();
	~CSDVirt();

	int loadMultiInputData(char *sourceBuf, __u64 destBuf, __u8 sourceNr);

	int loadBPF(int fd);

	int write(char *hostBuf, __u64 lba, size_t len);
	int read(char *hostBuf, __u64 lba, size_t len);

	int csdvirt_init_dev(const char *path);
	void csdvirt_release_dev(void);

	// Memory API
	size_t csdvirt_alloc_memory(size_t len);
	int csdvirt_release_memory(size_t addr);

	// with filesystem api
	int csdvirt_open(const char *file_path);
	int csdvirt_close(int fd);
	size_t csdvirt_load(int fd, __u64 device_buf, size_t count, off_t offset);
	size_t csdvirt_load_files(std::string *file_list, int nfiles, __u64 device_buf, size_t *actual_sizes, bool is_copy_to_slm);
	void *csdvirt_make_and_get_file_extent(std::string file_list, size_t size);
	size_t csdvirt_write_file(void *fiemap, __u64 device_buf, size_t offset, size_t *size);
	void csdvirt_truncate_file(std::string file_list, size_t size);

	// Raw Device api
	size_t csdvirt_load_raw(size_t lba, __u64 device_buf, size_t size);

	size_t csdvirt_read_slm(void *host_buf, __u64 device_buf, size_t size);
	size_t csdvirt_write_slm(void *host_buf, __u64 device_buf, size_t size);
	int csdvirt_execute(int program_index, size_t input_buf, size_t output_buf, size_t len, void *user_params,
						size_t param_size, void *result);

	size_t csdvirt_namespace_copy(__u64 offset, __u64 slm_offset, size_t size, int control_flag);

	// FreeBie Commands
	int csdvirt_freebie_repartition_command(std::string *input_file_list, std::string *output_file_list,
												int input_file_count, int output_file_count, 
												struct freebie_params* user_params);
	int csdvirt_freebie_metadata_setup_command(std::string file_path, uint32_t relation_id, size_t size);
	int csdvirt_freebie_get_root_command(void *host_buf, unsigned int relation_id);
	unsigned int csdvirt_freebie_release_root_command(unsigned int relation_id, unsigned int root_id);
	unsigned int csdvirt_freebie_terminate_root_command(unsigned int relation_id);

	// Debug
	class TimeLog {
	public:
		unsigned long long totalTime;
		unsigned long long minTime;
		unsigned long long maxTime;
		size_t size;

		void init()
		{
			totalTime = 0L;
			minTime = 0xFFFFFFFFFFFFFFFF;
			maxTime = 0L;
			size = 0;
		}

		TimeLog insert_log(unsigned long long time, size_t size);
	};
	void csdvirt_init_stat();
	void csdvirt_print_stat();
	void csdvirt_set_debugging_info(int value);

private:
	void _initNvmeCommand(struct nvme_passthru_cmd *m_nvmeCmd, int opcode);
	void *_getLBA(int fd);
	size_t _calculate_lba(struct fiemap *fiemap, size_t offset);
	size_t _calculate_io_size(struct fiemap *fiemap, size_t offset, size_t size);

	unsigned long long _getNanoSecondTime(struct timespec *tt1, struct timespec *tt2);

	int m_fd;
	int m_nsid;
	int m_debug_info;

	// with file system
	static const int MAX_OPEN_FILE_COUNT = 8;
	int m_cur_fd_index;
	int m_fd_map[8];

	// Debug
	enum TIME_TYPE {
		TIME_TYPE_START = 0,
		LOAD_TIME = TIME_TYPE_START,
		EXECUTE_TIME,
		COMPOUND_EXECUTE_TIME,
		READ_SLM_TIME,
		WRITE_SLM_TIME,
		READ_NVM_TIME,
		WRITE_NVM_TIME,
		NS_COPY_TIME,
		MEMORY_ALLOC,
		MEMORY_RELEASE,
		CSD_VIRT_OPEN,
		CSD_VIRT_CLOSE,
		CSD_VIRT_LOAD_WITH_FS,
		TIME_TYPE_COUNT,
		TIME_TYPE_END = TIME_TYPE_COUNT
	};

	TimeLog m_time_log[TIME_TYPE_COUNT];
};
#endif