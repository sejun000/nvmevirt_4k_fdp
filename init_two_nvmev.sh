#!/bin/bash
# ============================================================================
# Load TWO virtual NVMe devices at once, with DIFFERENT capacities.
#
#   nvmev.ko   -> instance 1 : 128 GiB  (/dev/nvmeXn1, domain 0001)
#   nvmev2.ko  -> instance 2 : 256 GiB  (/dev/nvmeYn1, domain 0002)
#
# Capacity is driven purely by `memmap_size` (MiB), because NS_CAPACITY_0 == 0
# makes the namespace take the whole storage area. So "different capacity" just
# means "different memmap_size".
#
# REQUIREMENT: both memmap regions must sit inside a reserved-memory window and
# must NOT overlap. Reserve it on the kernel command line, e.g.:
#     memmap=384G\$1024G          # reserve [1024G, 1408G) as type-12 (reserved)
# (escape the '$' as '\$' in the bootloader config). Instance 1 uses
# [1024G, 1152G), instance 2 uses [1152G, 1408G).
#
# memmap_start is in GiB, memmap_size is in MiB.
# ============================================================================
set -e

# ---- Instance 1 : 128 GiB -------------------------------------------------
sudo insmod nvmev.ko \
    memmap_start=1024 \
    memmap_size=131072 \
    proc_name=nvmev \
    pci_domain=1 \
    inst_id=1 \
    dma_chan=dma4chan0 \
    dispatcher_cpus=24 \
    worker_cpus=26,27,28,29

# ---- Instance 2 : 256 GiB (different capacity) ----------------------------
# Starts right after instance 1: 1024 + 128 = 1152 GiB.
# Uses a separate procfs dir, PCI domain, instance id and CPUs. DMA is left to
# memcpy here (io_using_dma=0) so the two instances don't fight over the same
# IOAT channel; set dma_chan=dma4chanN instead if you have a spare channel.
sudo insmod nvmev2.ko \
    memmap_start=1152 \
    memmap_size=262144 \
    proc_name=nvmev2 \
    pci_domain=2 \
    inst_id=2 \
    io_using_dma=0 \
    dispatcher_cpus=25 \
    worker_cpus=30,31,32,33

echo "Loaded nvmev (128GiB) + nvmev2 (256GiB)."
echo "Check: lsblk; ls -l /proc/nvmev /proc/nvmev2; lspci -d 0c51:"
