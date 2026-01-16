#!/bin/bash

sudo make clean; sudo make -j

# Unmount previously mounted virt
sudo umount /mnt/nvme
sudo rmmod nvmev
# sudo rm  -rf /mnt/nvme
# Check unmount
sudo nvme list

# memmap_start: GiB
# memmap_size: MiB
# slm_size: MiB
# cpus: NVM namespace
# slm_cpus: SLM worker
# csd_cpus: single dispatcher, CSD worker
echo "Load CSD NVMeVirt kernel module"
sudo insmod nvmev.ko \
	memmap_start=280 memmap_size=229376 slm_size=8192 \
    dispatcher_cpus=96,99,102,105,108 \
	worker_cpus=111,114,117,120,123,126,129,132 \
	slm_cpus=135,136,137,138,139,140 \
	csd_cpus=150,151,160,161,162,163,164,165,166,167

# sudo insmod nvmev.ko \
# 	memmap_start=280 memmap_size=196608 slm_size=8192 \
#     dispatcher_cpus=96,99,102,105 \
# 	worker_cpus=111,114,117,120 \
# 	slm_cpus=123,126,129\
# 	csd_cpus=150,160,161


# Check mount
sudo nvme list

# Build user library
# echo "Build CSD NVMeVirt library(C++)..."
# pushd lib
# ./build.sh
# popd

# Build user library
echo "Build CSD NVMeVirt library(C)..."
pushd lib/c
./build.sh
popd

../mount.sh || exit

sudo ../perf.sh || exit

echo "Done"
