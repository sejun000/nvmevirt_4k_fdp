# FreeBIE CSD NVMeVirt

## 1. Installation

### Linux kernel requirement

The recommended Linux kernel version is v5.15.x and higher (tested on Linux vanilla kernel v5.15.37 and Ubuntu kernel v5.15.0-58-generic).

### Reserving physical memory

A part of the main memory should be reserved for the storage of the emulated NVMe device.
To reserve a chunk of physical memory, add the following option to `GRUB_CMDLINE_LINUX` in `/etc/default/grub` as follows:

```bash
GRUB_CMDLINE_LINUX="memmap=64G\\\$128G"
```

This example will reserve 64GiB of physical memory chunk (out of the total 192GiB physical memory) starting from the 128GiB memory offset.
You may need to adjust those values depending on the available physical memory size and the desired storage capacity.
Also you should consider the NUMA configuration in your environment.

After changing the `/etc/default/grub` file, you are required to run the following commands to update `grub` and reboot your system.

```bash
$ sudo update-grub
$ sudo reboot
```

### Compiling `nvmevirt`

`nvmevirt` is implemented as a Linux kernel module. Thus, the kernel headers should be installed in the `/lib/modules/$(shell uname -r)` directory to compile `nvmevirt`.

Build the kernel module by running the `make` command in the `nvmevirt` source directory.
```bash
$ make
make -C /lib/modules/5.15.37/build M=/path/to/nvmev modules
make[1]: Entering directory '/path/to/linux-5.15.37'
  CC [M]  /path/to/nvmev/main.o
  CC [M]  /path/to/nvmev/pci.o
  CC [M]  /path/to/nvmev/admin.o
  CC [M]  /path/to/nvmev/io.o
  CC [M]  /path/to/nvmev/dma.o
  CC [M]  /path/to/nvmev/simple_ftl.o
  LD [M]  /path/to/nvmev/nvmev.o
  MODPOST /path/to/nvmev/Module.symvers
  CC [M]  /path/to/nvmev/nvmev.mod.o
  LD [M]  /path/to/nvmev/nvmev.ko
  BTF [M] /path/to/nvmev/nvmev.ko
make[1]: Leaving directory '/path/to/linux-5.15.37'
$
```

### Using `nvmevirt`

`nvmevirt` is configured to emulate the NVM SSD by default.
You can attach an emulated NVM SSD in your system by loading the `nvmevirt` kernel module as follows:

```bash
$ sudo insmod nvmev.ko \
    memmap_start=4 \           # in GiB
    memmap_size=16384 \        # in MiB
    slm_size=4096 \            # in MiB
    cpus=6,7,8,9,10\
    slm_cpus=11,12,13,14 \
    csd_cpus=15,16,17,18,19
```

In the above example, `memmap_start` and `memmap_size` indicate the relative offset and the size of the reserved memory, respectively. Those values should match the configurations specified in the `/etc/default/grub` file shown earlier. Please note that `memmap_size` should be given in the unit of MiB (for instance, 65536 denotes 64GiB).

When you are successfully load the `nvmevirt` module, you can see something like these from the system message.

```log
$ sudo dmesg
[  144.812975] NVMeVirt: Successfully created Virtual NVMe deivce
```

Now the emulated `nvmevirt` device is ready to be used as shown below. The actual device number (`/dev/nvme0`) can vary depending on the number of real NVMe devices in your system.

```bash
$ ls -l /dev/nvme*
crw------- 1 root root 242, 0 Feb 22 14:13 /dev/nvme0
brw-rw---- 1 root disk 259, 5 Feb 22 14:13 /dev/nvme0n1
```

### Troubleshooting

- `insmod` is failed with warnings in `include/linux/msi.h` (`pci_msi_setup_msi_irqs` and `free_msi_irqs`): We've got reports that VT-d and interrupt remapping are influencing on the MSI-X setup in NVMeVirt. Try to add `intremap=off` in the kernel boot option. If the failure persists please contact us at [nvmevirt@gmail.com](mailto:nvmevirt@gmail.com) with dmesg.

## 2. User API (Library)

The user APIs are supported in `lib/CSDVirt.cpp`.
The standar NVMe CSD commands are all supported using NVMe passthru commands (using IOCTL).
This will be further extended to use IOuring to support asynchronous commands.
Here are some FreeBIE specific user APIs.
1. `csdvirt_freebie_repartition_command`
2. `csdvirt_freebie_metadata_setup_command` 
3. `csdvirt_freebie_get_catalog_command`
4. `csdvirt_freebie_release_catalog_command`
5. `csdvirt_freebie_terminate_catalog_command`

The descriptions of each commands can be found in the user guide[TBD].

## 3. FreeBIE repartition code

The code offloaded repartitioning code is located in `user_function/freebie`.
The following parts of the code is modified to run under the CSD environment.
Everything else is identical to the user level repartition code.

1. `freebie_repartition.c` - How it handles input parameters
2. `freebie_functions.c` - Handling metadata file (catalog, GC, etc)
3. `parquet.c` - Use memory(SLM) buffers instead of file
4. `thrift_file_transport.c` - Use memcpy instead of file I/Os
5. `custom-memcpy` - Custom memcpy to use AVX or MMX registers inside the kernel
