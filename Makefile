KERNELVER := $(shell uname -r)
KERNELDIR := /lib/modules/$(KERNELVER)/build
PWD     := $(shell pwd)

# Select one of the targets to build
#CONFIG_NVMEVIRT_NVM := y
#CONFIG_NVMEVIRT_SSD := y
CONFIG_NVMEVIRT_FDP := y
#CONFIG_NVMEVIRT_CSD := y

obj-m   := nvmev.o
nvmev-objs := main.o pci.o admin.o io.o dma.o

ccflags-y += -Wno-unused-variable -Wno-error -Wno-declaration-after-statement -Wno-missing-prototypes -Wno-missing-declarations -Wno-frame-larger-than
ccflags-y += -mindirect-branch=keep -mfunction-return=keep
ccflags-y += -falign-functions=32 -falign-loops=32 -falign-jumps=16
ccflags-y += -frename-registers

ccflags-y += -O3
ccflags-y += -ftree-loop-im
ccflags-y += -foptimize-sibling-calls
# ccflags-y += -fomit-frame-pointer
ccflags-y += -fdelete-null-pointer-checks

ccflags-y += -fno-stack-protector
ccflags-y += -fno-stack-clash-protection
ccflags-y += -fno-strict-aliasing
ccflags-y += -fno-stack-check
# ccflags-y += -ffunction-sections -fdata-sections
ccflags-y += -ftree-vectorize
ccflags-y += -fcf-protection=none

OBJECT_FILES_NON_STANDARD := y

ccflags-$(CONFIG_NVMEVIRT_NVM) += -DBASE_SSD=INTEL_OPTANE
nvmev-$(CONFIG_NVMEVIRT_NVM) += simple_ftl.o

ccflags-$(CONFIG_NVMEVIRT_SSD) += -DBASE_SSD=SAMSUNG_970PRO
nvmev-$(CONFIG_NVMEVIRT_SSD) += ssd.o conv_ftl.o pqueue.o channel_model.o

ccflags-$(CONFIG_NVMEVIRT_FDP) += -DBASE_SSD=SAMSUNG_PM9D3A
nvmev-$(CONFIG_NVMEVIRT_FDP) += ssd.o conv_ftl.o pqueue.o channel_model.o

ccflags-$(CONFIG_NVMEVIRT_CSD) += -DCSD_ENABLE=1
nvmev-$(CONFIG_NVMEVIRT_CSD) += csd_dispatcher.o csd_ftl.o csd_slm.o buddy.o

nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/freebie_repartition.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/freebie_functions.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/garray.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/gobject.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_column_reader.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_column_writer.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_plain_encoder.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_reader.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_rle_bp_encoder.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_types.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_writer.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/thrift_compact_protocol.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/thrift_file_transport.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/thrift_transport.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/thrift_protocol.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/thrift_struct.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/gerror.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/custom-memcpy/custom-memcpy.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_delta_column_reader.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/parquet_delta_column_writer.o
nvmev-$(CONFIG_NVMEVIRT_CSD) += user_function/freebie/freebie_delta_mgr.o

ccflags-$(CONFIG_NVMEVIRT_CSD_eBPF) += -DCSD_eBPF_ENABLE=1
default:
		$(MAKE) -C $(KERNELDIR) M=$(PWD) EXTRA_CFLAGS="-O2" modules

install:
	   $(MAKE) -C $(KERNELDIR) M=$(PWD) modules_install

.PHONY: clean
clean:
	   $(MAKE) -C $(KERNELDIR) M=$(PWD) clean
	   rm -f cscope.out tags

.PHONY: cscope
cscope:
		cscope -b -R
		ctags *.[ch]
