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
