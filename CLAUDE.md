# CSD-Virt Project Context

## 프로젝트 개요
NVMeVirt 기반 가상 NVMe 디바이스 드라이버 (커널 모듈)

## 현재 설정 (2024-01)

### Makefile 설정
- `CONFIG_NVMEVIRT_FDP := y` (활성화)
- `CONFIG_NVMEVIRT_SSD` (비활성화)
- `CONFIG_NVMEVIRT_CSD` (비활성화)

### 빌드 수정사항
1. **main.c:257** - `E820_TYPE_RESERVED_KERN` 제거 (최신 커널 6.15에서 없어짐)
   - `E820_TYPE_RAM`만 체크하도록 수정

2. **io.c** - CSD 관련 코드를 `#if (CSD_ENABLE == 1)` 조건부 컴파일로 감쌈
   - 76-125줄: CSD 명령어 처리 블록
   - 169-185줄: CSD memcpy 블록
   - 190-211줄: freebie partition map 블록
   - 901-920줄: CSD 결과 처리 블록

3. **conv_ftl.c**
   - `#include <linux/vmalloc.h>` 추가 (vmalloc_node, vfree용)
   - `get_partition_map` 함수를 `#if (CSD_ENABLE == 1)` 조건으로 감쌈
   - CSD 관련 case문들 조건부 컴파일 처리

## 듀얼 모듈 (인스턴스 2개 동시 로드)

같은 소스에서 이름이 다른 두 모듈을 빌드한다: `nvmev.ko`(인스턴스1) + `nvmev2.ko`(인스턴스2).
`nvmev2`의 오브젝트는 `*_2.c` 래퍼(`#include "원본.c"`)로, kbuild가 동일 소스를
다른 오브젝트 이름으로 빌드해 별도 모듈로 링크하게 한다. `make` 한 번에 둘 다 생성됨.

두 인스턴스가 충돌하지 않도록 아래 전역 자원을 **모듈 파라미터**로 분리했다(기본값은
기존 동작 유지 → `insmod nvmev.ko ...` 단독 로드는 종전과 동일):

| 파라미터 | 기본값 | 설명 |
|---------|--------|------|
| `proc_name`    | `nvmev`     | `/proc/<proc_name>` 디렉터리 (인스턴스마다 달라야 함) |
| `pci_domain`   | `1`         | 가상 PCI 도메인 (인스턴스마다 달라야 함) |
| `pci_bus`      | `0x10`      | 가상 PCI 버스 번호 |
| `inst_id`      | `1`         | 컨트롤러 serial/model 문자열(`CSL_Virt_SN_%02d`)에 사용 |
| `dma_chan`     | `dma4chan0` | 잡을 IOAT DMA 채널명 (인스턴스마다 달라야 함) |
| `io_using_dma` | `1`         | 0이면 DMA 대신 memcpy (2번째 인스턴스는 0 권장) |

- **용량 차이**: `NS_CAPACITY_0 == 0`이라 네임스페이스가 storage 전체를 차지 →
  용량은 전적으로 `memmap_size`(MiB)가 결정. 두 인스턴스에 다른 `memmap_size`만 주면 됨.
- **메모리 영역**: 두 `memmap` 구간은 겹치면 안 되고, 예약 영역(`memmap=` 커널 cmdline) 안에 있어야 함.
- 예제 스크립트: `init_two_nvmev.sh` (128GiB + 256GiB).

```bash
# 인스턴스1: 128GiB
sudo insmod nvmev.ko  memmap_start=1024 memmap_size=131072 \
    proc_name=nvmev  pci_domain=1 inst_id=1 dma_chan=dma4chan0 \
    dispatcher_cpus=24 worker_cpus=26,27,28,29
# 인스턴스2: 256GiB (용량 다름)
sudo insmod nvmev2.ko memmap_start=1152 memmap_size=262144 \
    proc_name=nvmev2 pci_domain=2 inst_id=2 io_using_dma=0 \
    dispatcher_cpus=25 worker_cpus=30,31,32,33
# 해제: sudo rmmod nvmev2 nvmev
```

## 모듈 로드 명령어

### CSD 비활성화 상태 (현재)
```bash
sudo insmod nvmev.ko \
    memmap_start=512 \
    memmap_size=524288 \
    dispatcher_cpus=24,25 \
    worker_cpus=26,27,28,29,30
```

### 파라미터 설명
| 파라미터 | 값 | 설명 |
|---------|-----|------|
| memmap_start | 512 | 시작 위치 (GiB) |
| memmap_size | 524288 | 크기 (MiB) = 512GB |
| dispatcher_cpus | 24,25 | Dispatcher용 CPU |
| worker_cpus | 26,27,28,29,30 | IO worker용 CPU |

## NUMA 정보
- Node 0: CPU 0-23, ~515GB
- Node 1: CPU 24-47, ~1TB
- CPU 24번부터 사용하면 Node 1

## 생성된 디바이스
- PCI: `0001:10:00.0`
- Vendor/Device ID: `0c51:0110`
- Serial: `CSL_Virt_SN_01`

## TODO
- [ ] memmap_start를 Node 1 메모리 영역으로 조정 필요 (현재 512GB → 1TB 이후로 변경)
