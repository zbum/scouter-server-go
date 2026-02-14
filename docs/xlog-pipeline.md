# XLog Pipeline

XLog는 Scouter APM의 핵심 데이터로, 하나의 트랜잭션(HTTP 요청, 배치 작업 등)이 완료될 때 에이전트가 생성하는 성능 로그다. 이 문서는 XLog 데이터의 수신, 처리, 저장, 조회에 이르는 전체 파이프라인을 설명한다.

## 전체 흐름

```
에이전트 ──UDP──→ Dispatcher ──→ XLogCore ──┬──→ XLogCache    (링 버퍼, 실시간 스트리밍)
                                           ├──→ XLogGroupPerf (서비스 그룹 집계)
                                           ├──→ VisitorCore   (유니크 사용자 카운팅)
                                           ├──→ TagCountCore  (태그 통계)
                                           └──→ XLogWR        (디스크 비동기 기록)

클라이언트 ──TCP──→ handler_xlog.go      ──→ XLogCache     (실시간 조회)
                   handler_xlog_read.go  ──→ XLogWR/XLogRD (과거 데이터 조회)
```

## 데이터 모델 — XLogPack

XLogPack은 40개 이상의 필드를 가진 가장 복잡한 Pack 타입이다.

| 분류 | 필드 | 설명 |
|------|------|------|
| 식별 | `Txid` | 트랜잭션 고유 ID (int64) |
| | `Gxid` | 글로벌 트랜잭션 ID (분산 추적용) |
| | `ObjHash` | 에이전트(오브젝트) 해시 |
| | `Caller` | 호출자 트랜잭션 ID |
| 시간 | `EndTime` | 트랜잭션 종료 시각 (ms) |
| | `Elapsed` | 응답시간 (ms) |
| 성능 | `Cpu` | CPU 시간 (ms) |
| | `SqlCount`, `SqlTime` | SQL 실행 횟수/시간 |
| | `ApicallCount`, `ApicallTime` | 외부 API 호출 횟수/시간 |
| | `Kbytes` | 전송 바이트 |
| 서비스 | `Service` | 서비스 URL 해시 |
| | `Group` | 서비스 그룹 해시 |
| | `XType` | 타입 (0=WebService, 1=AppService) |
| 에러 | `Error` | 에러 메시지 해시 (0이면 정상) |
| 네트워크 | `IPAddr` | 클라이언트 IP |
| | `Status` | HTTP 상태 코드 |
| | `UserAgent`, `Referer` | HTTP 헤더 해시 |
| 지역 | `CountryCode`, `City` | GeoIP 파생 |
| 사용자 | `Userid` | 사용자 ID |
| | `Login` | 로그인 해시 |
| 프로파일 | `ProfileCount`, `ProfileSize` | 프로파일 스텝 수/크기 |
| 확장 | `Text1`~`Text5` | 사용자 정의 텍스트 |

직렬화는 inner blob 방식을 사용한다. 모든 필드를 내부 버퍼에 쓴 뒤 blob으로 감싸서 전송하며, `d.Available() > 0` 체크로 하위 호환성을 유지한다.

**소스**: `internal/protocol/pack/xlog_pack.go`

## 수신 및 처리 — XLogCore

XLogCore는 에이전트로부터 수신한 XLogPack을 처리하는 중앙 프로세서다. 4,096 용량의 채널 큐를 통해 비동기 처리한다.

```
Handler() 수신
  │  EndTime == 0이면 현재 시각 설정
  │  채널 큐에 enqueue
  ▼
run() — 단일 백그라운드 goroutine
  │
  ├─ [1] GeoIP 조회
  │      IPAddr → CountryCode, City 설정
  │
  ├─ [2] 그룹 해시 파생
  │      XLogGroupPerf.Process(xp)
  │      서비스 URL에서 그룹 해시를 파생하여 xp.Group에 설정
  │
  ├─ [3] 직렬화 + 인메모리 캐시
  │      WritePack(xp) → []byte
  │      XLogCache.Put(objHash, elapsed, isError, data)
  │
  ├─ [4] 서비스 그룹 집계
  │      XLogGroupPerf.Add(xp)
  │      (objHash, group) 키별 TPS/응답시간/에러 집계
  │
  ├─ [5] 방문자 카운팅
  │      Userid != 0이면 VisitorCore.Add(xp)
  │
  ├─ [6] 태그 카운팅
  │      설정 활성화 시 TagCountCore.ProcessXLog()
  │
  └─ [7] 디스크 기록
         XLogWR.Add(XLogEntry{Time, Txid, Gxid, Elapsed, Data})
```

XLogCore.run()은 **단일 goroutine**에서 순차 처리하므로 내부 상태에 대한 별도 동기화가 불필요하다. WebService(0)와 AppService(1) 타입만 그룹 집계에 참여한다.

**소스**: `internal/core/xlog_core.go`

## 인메모리 캐시 — XLogCache (링 버퍼)

실시간 모니터링(Scouter Client의 XLog 차트)을 위한 고정 크기 링 버퍼다. Java의 `XLogLoopCache`를 포팅한 구조이다.

#### 구조

```
entries: [ E0 | E1 | E2 | ... | E(size-1) ]
                          ↑
                         pos (다음 쓰기 위치)

pos가 끝에 도달하면 → loop 증가 → pos = 0 (덮어쓰기)
```

각 엔트리에는 XLogPack 전체가 아닌 필터링에 필요한 메타데이터와 직렬화된 바이트만 저장한다.

```go
type XLogEntry struct {
    ObjHash int32   // 필터링용
    Elapsed int32   // 필터링용
    IsError bool    // 필터링용
    Data    []byte  // 직렬화된 XLogPack (클라이언트에 그대로 전송)
}
```

#### Put

```go
c.entries[c.pos] = entry
c.pos++
if c.pos >= c.size {
    c.loop++
    c.pos = 0
}
```

#### Get — Loop/Index 페이지네이션

클라이언트는 이전 응답에서 받은 `(lastLoop, lastIndex)`를 보내고, 서버는 그 이후에 추가된 엔트리만 반환한다.

| `endLoop - lastLoop` | 처리 |
|-----------------------|------|
| `0` | `lastIndex..endIndex` 구간 반환 |
| `1` | 랩 어라운드 처리, 두 구간 결합 |
| `> 1` | 클라이언트가 뒤처짐, 현재 버퍼 전체 반환 |

필터링 조건:
- `elapsed >= minElapsed` 또는 `isError == true`인 엔트리만 포함
- `objHashSet`이 주어지면 해당 오브젝트만 포함

**소스**: `internal/core/cache/xlog_cache.go`

## 디스크 저장

### 디렉터리 구조

```
{data_dir}/{YYYYMMDD}/xlog/
├── xlog.data              ← 직렬화된 XLogPack 데이터
├── xlog_tim.hfile         ← 시간 인덱스 (MemHashBlock)
├── xlog_tim.kfile         ← 시간 인덱스 (레코드)
├── xlog_tid.hfile         ← Txid 인덱스
├── xlog_tid.kfile
├── xlog_gid.hfile         ← Gxid 인덱스 (멀티밸류)
└── xlog_gid.kfile
```

일별 디렉터리로 자동 분리되며, 과거 데이터는 `PurgeOldDays()`로 정리한다.

### XLogWR — 비동기 배치 Writer

비동기 큐(10,000 용량)를 통해 데이터를 수신하고, **배치 드레인** 방식으로 디스크 I/O를 최적화한다.

```
큐에서 첫 엔트리 blocking 대기
    │
    ├── 큐에 남은 엔트리를 non-blocking으로 최대 512개 drain
    │
    ▼
배치 내 각 엔트리에 대해:
    │
    ├── XLogData.Write(data) → 데이터 파일에 기록, 오프셋 반환
    │
    └── 3중 인덱스 기록
         ├── SetByTime(endTime, offset)  ← 시간 범위 조회용
         ├── SetByTxid(txid, offset)     ← 단건 트랜잭션 조회용
         └── SetByGxid(gxid, offset)     ← 분산 트랜잭션 조회용 (gxid != 0)
    │
    ▼
배치 완료 후 flushData() 호출
```

**소스**: `internal/db/xlog/xlog_wr.go`

### XLogData — 데이터 파일

데이터 파일(`xlog.data`)은 레코드를 순차적으로 append하는 단순한 구조다.

```
┌──────────┬──────────────────────┐
│ 2B 길이   │ body                 │  ← 레코드 1 (offset = 0)
├──────────┼──────────────────────┤
│ 2B 길이   │ body                 │  ← 레코드 2 (offset = 2 + len(body1))
├──────────┼──────────────────────┤
│ ...      │ ...                  │
└──────────┴──────────────────────┘
```

- **압축**: `compress_xlog_enabled` 설정 시 body에 zstd 압축 적용
- **읽기**: `ReadAt` (pread) 사용으로 **lock-free 동시 읽기** 지원. 별도의 읽기 전용 파일 핸들을 lazy 초기화하여 여러 goroutine이 동시에 읽을 수 있다

**소스**: `internal/db/xlog/xlog_data.go`

### 3중 인덱스 — XLogIndex

세 가지 접근 패턴을 지원하기 위해 별도의 인덱스 파일을 유지한다.

| 인덱스 | 파일 | 타입 | 키 | 값 | 용도 |
|--------|------|------|-----|-----|------|
| 시간 | `xlog_tim` | IndexTimeFile | 시간(ms) | 5B offset | 시간 범위 조회 |
| 트랜잭션 | `xlog_tid` | IndexKeyFile | txid(8B) | 5B offset | 단건 조회 |
| 글로벌 TX | `xlog_gid` | IndexKeyFile | gxid(8B) | 5B offset[] | 분산 TX 체인 조회 |

- **시간 인덱스**: 정렬된 시간 범위 스캔을 지원. `Read(stime, etime)`과 `ReadFromEnd(stime, etime)` 양방향 조회 가능
- **Txid 인덱스**: 해시 기반 단건 조회. `Get(txid)` → 하나의 offset 반환
- **Gxid 인덱스**: 해시 기반 멀티밸류 조회. `GetAll(gxid)` → 여러 offset 반환. 하나의 분산 트랜잭션에 속한 모든 XLog를 찾을 수 있다

**소스**: `internal/db/xlog/xlog_index.go`

## 읽기 우선순위 — Dual-Path Read

모든 조회 핸들러는 동일한 패턴을 따른다.

```
xlogWR (인메모리 인덱스, 최신 데이터)
    │
    └── 해당 날짜 컨테이너 없음 (found=false)
           │
           ▼
        xlogRD (디스크 인덱스, 과거 데이터)
```

XLogWR은 서버 기동 후 쓰여진 데이터의 최신 MemHashBlock을 보유하므로 항상 최신 상태다. XLogRD는 파일 오픈 시점의 스냅샷이므로 이후 추가된 데이터를 볼 수 없다. 따라서 XLogWR을 먼저 시도하고, 해당 날짜의 컨테이너가 없을 때만 XLogRD로 폴백한다.

## 조회 프로토콜

### 실시간 스트리밍

| 명령어 | 설명 |
|--------|------|
| `TRANX_REAL_TIME_GROUP` | 링 버퍼에서 (loop, index) 페이지네이션으로 조회. `elapsed >= limit OR isError` 필터 적용 |
| `TRANX_REAL_TIME_GROUP_LATEST` | 동일하되 elapsed 필터 없음 (minElapsed=0) |

클라이언트 요청/응답 흐름:

```
클라이언트 → { lastLoop, lastIndex, limit, objHash[] }
    │
    ▼
서버: XLogCache.Get(lastLoop, lastIndex, limit, objHashSet)
    │
    ▼
응답: { loop, index } + XLogPack[] (직렬화 바이트 그대로 전송)
    │
    ▼
클라이언트: 다음 요청에 응답받은 (loop, index) 사용
```

**소스**: `internal/netio/service/handler_xlog.go`

### 과거 데이터 조회

| 명령어 | 입력 | 동작 |
|--------|------|------|
| `XLOG_READ_BY_TXID` | date, txid | Txid 인덱스로 단건 조회 |
| `XLOG_READ_BY_GXID` | date, gxid | Gxid 인덱스로 분산 TX 체인 조회 |
| `XLOG_LOAD_BY_TXIDS` | date, txid[] | 다건 Txid 배치 조회 |
| `XLOG_LOAD_BY_GXID` | stime, etime, gxid | Gxid 조회 + 날짜 경계 처리 |
| `TRANX_LOAD_TIME_GROUP` | date, stime, etime, limit, objHash[] | 시간 범위 + elapsed/objHash 필터 |
| `SEARCH_XLOG_LIST` | stime, etime, objHash | 시간 범위 검색 (최대 건수 제한) |
| `QUICKSEARCH_XLOG_LIST` | date, txid, gxid | txid 또는 gxid 빠른 검색 |

#### 시간 범위 조회 필터링

`TRANX_LOAD_TIME_GROUP` 핸들러는 디스크에서 읽은 각 XLogPack을 역직렬화하여 필터링한다.

```
시간 인덱스 범위 스캔 → 각 offset에서 데이터 읽기 → XLogPack 역직렬화
    │
    ├── objHashFilter에 포함? → 아니면 건너뜀
    ├── elapsed > limit?      → 아니면 건너뜀
    └── max 건수 초과?        → 반복 종료
```

날짜 경계를 넘는 시간 범위는 `date`와 `date2`를 비교하여 두 날짜에 걸쳐 조회한다.

**소스**: `internal/netio/service/handler_xlog_read.go`

### 프로파일 조회

| 명령어 | 동작 |
|--------|------|
| `TRANX_PROFILE` | ProfileWR에서 txid로 프로파일 블록 조회 → 전체 결합 → XLogProfilePack으로 응답 |
| `TRANX_PROFILE_FULL` | 동일 (연관 트랜잭션 포함) |

프로파일은 트랜잭션 실행 중 기록된 상세 스텝(SQL 실행, API 호출, 메서드 진입 등)의 바이너리 데이터다. ProfileWR이 txid 기반 인덱스를 보유하며, 여러 블록으로 분할 저장된 프로파일을 결합하여 하나의 바이트 배열로 반환한다.

## 서비스 그룹 집계 — XLogGroupPerf

실시간 대시보드의 서비스 그룹별 TPS, 평균 응답시간, 에러율을 계산한다.

#### 데이터 구조

```
XLogGroupPerf
  └── meters: map[groupKey]*meterService  (최대 2,000개)
         │
         └── groupKey = (objHash, group)
                │
                └── meterService
                      └── buckets: [600]meterBucket  (10분, 초 단위 링 버퍼)
                             │
                             └── meterBucket = { timeSec, count, error, elapsed }
```

#### Add — 메트릭 기록

```go
sec := time.Now().Unix()
idx := sec % 600
bucket := &m.buckets[idx]
if bucket.timeSec != sec {
    // 새 초 → 버킷 리셋
    bucket = { timeSec: sec, count: 0, error: 0, elapsed: 0 }
}
bucket.count++
bucket.elapsed += elapsed
if isError { bucket.error++ }
```

Group == 0인 XLog는 무시한다 (Java 동작과 동일). 최대 2,000개 (objHash, group) 조합을 추적하며, 초과 시 랜덤 항목을 제거한다.

#### GetGroupPerfStat — 통계 조회

최근 30초간의 버킷을 합산하여 `PerfStat{Count, Error, Elapsed}`를 반환한다. 결과는 1초간 캐시하여 반복 호출 시 재계산을 방지한다.

```go
type PerfStat struct {
    Count   int64
    Error   int64
    Elapsed int64
}

func (s *PerfStat) AvgElapsed() float32 {
    return float32(s.Elapsed) / float32(s.Count)
}
```

**소스**: `internal/core/xlog_group_perf.go`

#### 그룹 해시 파생 — XLogGroupUtil

서비스 URL에서 그룹 해시를 자동 파생한다.

| URL 패턴 | 그룹 |
|----------|------|
| `*.jsp` | `"*.jsp"` |
| `*.gif`, `*.jpg`, `*.png`, ... | `"images"` |
| `*.html`, `*.css`, `*.js`, ... | `"statics"` |
| `/admin/users/list` | `"/admin"` (첫 번째 경로) |
| `/` | `"/**"` |

파생 결과는 최대 50,000개 `service → group` 매핑으로 캐시한다. 서비스 URL 문자열은 TextCache → TextRD 순서로 조회한다.

**소스**: `internal/core/xlog_group_util.go`

## 설정

| 설정 키 | 설명 |
|---------|------|
| `xlog_queue_size` | XLogCache 링 버퍼 크기 |
| `compress_xlog_enabled` | XLog 데이터 zstd 압축 활성화 |
| `xlog_realtime_lower_bound_ms` | 실시간 스트리밍 최소 elapsed 필터 |
| `xlog_pasttime_lower_bound_ms` | 과거 조회 최소 elapsed 필터 |
| `req_search_xlog_max_count` | SEARCH_XLOG_LIST 최대 반환 건수 |
| `tagcnt_enabled` | 태그 카운팅 활성화 |

## 핵심 설계 포인트

| 포인트 | 설명 |
|--------|------|
| 링 버퍼 페이지네이션 | loop/index 상태로 실시간 스트리밍 시 중복/누락 방지 |
| 배치 드레인 | 최대 512개씩 묶어서 디스크 I/O 횟수 절감 |
| 3중 인덱스 | 시간 범위, 단건 TX, 분산 TX 세 가지 접근 패턴 모두 지원 |
| Dual-Path Read | XLogWR(최신) → XLogRD(과거) 순으로 조회하여 데이터 정합성 보장 |
| Lock-free pread | XLogData.Read()에 ReadAt 사용, 다수 goroutine 동시 읽기 가능 |
| Zstd 압축 | 설정 시 XLog 데이터를 압축 저장하여 디스크 사용량 절감 |
| 초 단위 버킷 | XLogGroupPerf가 초 단위로 메트릭을 쌓아 실시간 통계 제공 |
| 단일 goroutine 처리 | XLogCore.run()이 모든 처리를 순차 수행하여 동기화 비용 제거 |