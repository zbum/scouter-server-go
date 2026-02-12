# Text Cache Database

Scouter 서버의 텍스트 캐시 데이터베이스는 에이전트가 전송하는 문자열 데이터를 **해시 → 원본 문자열** 매핑으로 저장하고 조회하는 시스템이다. 서비스명, SQL, API URI, 에러 메시지 등 반복적으로 참조되는 문자열을 해시 값으로 압축하여 네트워크 트래픽과 저장 공간을 절약한다.

## 개요

에이전트는 모니터링 데이터를 전송할 때 문자열을 직접 보내지 않고 해시 값만 전송한다. 서버는 해시 → 문자열 매핑을 별도로 저장해두고, 클라이언트(뷰어)가 데이터를 조회할 때 해시를 원본 문자열로 변환하여 응답한다.

```
[에이전트]                        [서버]                         [클라이언트]
    │                              │                               │
    │── TextPack(hash, text) ─────>│  저장: hash → text             │
    │── XLogPack(serviceHash) ────>│  저장: XLog 데이터             │
    │                              │                               │
    │                              │<── GET_TEXT(serviceHash) ──────│
    │                              │── "UserService.login" ────────>│
```

## 텍스트 타입

| 타입 | 설명 | 예시 | 저장 방식 |
|------|------|------|-----------|
| `service` | 서비스/트랜잭션 이름 | `"/api/users/login"` | 영구 + 일별(설정) |
| `sql` | SQL 문장 | `"SELECT * FROM users WHERE id = ?"` | 영구 |
| `method` | 메서드 이름 | `"login"`, `"createOrder"` | 영구 |
| `error` | 에러 메시지 | `"NullPointerException"` | 영구 |
| `apicall` | 외부 API 호출 URI | `"/api/v1/orders/{id}"` | 영구 + 일별(설정) |
| `ua` | HTTP User-Agent | `"Chrome/120.0"` | 영구 + 일별(설정) |
| `object` | 오브젝트(에이전트) 이름 | `"app-server-01"` | 영구 |

## 아키텍처

### 계층 구조

```
┌─────────────────────────────────────────────────────┐
│                  TextCache (LRU)                    │ ← Layer 1: 인메모리 LRU 캐시
│              최대 100,000 엔트리                      │    O(1) 조회
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────┐
│                    TextWR                           │ ← Layer 2: 비동기 Writer
│         인메모리 중복 검사 + 비동기 큐(10,000)          │    최신 인덱스 보유
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────┐
│                    TextRD                           │ ← Layer 3: Reader
│            인메모리 캐시(map) + 디스크 조회             │    서버 기동 이전 데이터
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────┐
│                  TextTable                          │ ← Layer 4: 디스크 스토리지
│         타입별 IndexKeyFile (div별 파일 분리)          │
├─────────────────────────────────────────────────────┤
│  IndexKeyFile = MemHashBlock (.hfile)               │
│               + RealKeyFile  (.kfile)               │
└─────────────────────────────────────────────────────┘
```

### 주요 컴포넌트

| 컴포넌트 | 파일 | 역할 |
|----------|------|------|
| `TextCache` | `internal/core/cache/text_cache.go` | LRU 기반 인메모리 캐시 (100,000 엔트리) |
| `TextCore` | `internal/core/text_core.go` | TextPack 수신 및 저장 라우팅 |
| `TextWR` | `internal/db/text/text_wr.go` | 비동기 텍스트 Writer, 중복 제거 |
| `TextRD` | `internal/db/text/text_rd.go` | 텍스트 Reader, 읽기 캐시 |
| `TextTable` | `internal/db/text/text_table.go` | 타입(div)별 IndexKeyFile 관리 |
| `IndexKeyFile` | `internal/db/io/index_key_file.go` | MemHashBlock + RealKeyFile 결합 인덱스 |
| `MemHashBlock` | `internal/db/io/mem_hash_block.go` | 인메모리 해시 버킷 (.hfile 디스크 백업) |
| `RealKeyFile` | `internal/db/io/real_key_file.go` | 해시 체인 레코드 (.kfile) |
| `TextCacheReset` | `internal/core/text_cache_reset.go` | 일자 변경 시 에이전트 캐시 리셋 |

## 디스크 저장 구조

### 디렉터리 레이아웃

```
{data_dir}/
├── 00000000/                        ← 영구 저장소
│   ├── text_service.hfile           ← service 타입 해시 인덱스
│   ├── text_service.kfile           ← service 타입 레코드 파일
│   ├── text_sql.hfile
│   ├── text_sql.kfile
│   ├── text_method.hfile
│   ├── text_method.kfile
│   ├── text_error.hfile
│   ├── text_error.kfile
│   ├── text_apicall.hfile
│   ├── text_apicall.kfile
│   └── ...
│
└── 20260212/                        ← 일별 저장소 (설정 활성화 시)
    └── text/
        ├── text_service.hfile
        ├── text_service.kfile
        ├── text_apicall.hfile
        ├── text_apicall.kfile
        └── ...
```

### 파일 포맷

#### .hfile (MemHashBlock)

인메모리에 로드되는 해시 버킷 배열이다. 키의 해시 값으로 버킷 위치를 O(1)로 결정하고, 해당 버킷에 `.kfile`의 파일 오프셋을 저장한다.

```
┌──────────────────────────────────────┐
│ Header (1024 bytes)                  │
│  [0-1]  Magic: 0xCA 0xFE            │
│  [4-7]  Entry count (int32)          │
│  [8-1023] Reserved                   │
├──────────────────────────────────────┤
│ Hash Bucket Array                    │
│  각 버킷: 5 bytes (long5 포맷)        │
│  값: .kfile 내 레코드의 파일 오프셋     │
│  버킷 수: (파일 크기 - 1024) / 5       │
└──────────────────────────────────────┘
```

- 기본 크기: 1MB (약 204,000 버킷)
- 4초 간격으로 디스크에 flush

#### .kfile (RealKeyFile)

실제 키-값 레코드를 순차적으로 저장하는 파일이다. 해시 충돌은 체인(linked list)으로 해결한다.

```
┌──────────────────────────────────────┐
│ Header (2 bytes)                     │
│  Magic: 0xCA 0xFE                    │
├──────────────────────────────────────┤
│ Record 1                             │
│  [1B] deleted flag (0=active, 1=del) │
│  [5B] prevPos (이전 레코드 오프셋)     │
│  [2B] keyLen                         │
│  [nB] key (4바이트: hash big-endian)  │
│  [?B] blob (가변 길이 텍스트 데이터)    │
├──────────────────────────────────────┤
│ Record 2                             │
│  ...                                 │
├──────────────────────────────────────┤
│ Record N                             │
│  ...                                 │
└──────────────────────────────────────┘
```

**Blob 인코딩 규칙:**

| prefix 값 | 의미 |
|-----------|------|
| `0` | 빈 데이터 |
| `1-253` | prefix 자체가 데이터 길이 (바이트) |
| `254` | 이후 4바이트가 데이터 길이 (int32) |
| `255` | 이후 2바이트가 데이터 길이 (uint16) |

**해시 충돌 해결:**

```
.hfile 버킷 ──→ Record C (최신)
                  │ prevPos
                  ▼
                Record B
                  │ prevPos
                  ▼
                Record A (최초)
                  │ prevPos = 0
```

같은 해시 버킷에 매핑되는 레코드들은 `prevPos`로 연결된 체인을 형성한다. 조회 시 체인을 순회하며 키가 일치하는 레코드를 찾는다.

### 버퍼링과 Flush

쓰기 성능을 위해 RealKeyFile은 append 데이터를 인메모리 버퍼에 모은 뒤, 16KB 임계치를 초과하거나 읽기 요청이 발생할 때 디스크에 flush한다.

```
Append 호출 ──→ appendBuf에 추가
                  │
                  ├── 16KB 초과? → 디스크 flush
                  ├── 읽기 요청? → 디스크 flush 후 읽기
                  └── 4초 타이머  → 디스크 flush (FlushController)
```

## 데이터 흐름

### 쓰기 (Write)

```
에이전트 → TextPack(type, hash, text)
              │
              ▼
         TextCore.Handler()
              │
              ├── TextCache.Put(type, hash, text)     [LRU 캐시 즉시 갱신]
              │
              ▼
         TextCore.run()  (비동기 goroutine)
              │
              ├── shouldUseDailyText(type)?
              │    └── Yes → TextWR.AddDaily(date, type, hash, text)
              │
              └── TextWR.Add(type, hash, text)        [항상 영구 저장소에 기록]
                     │
                     ▼
              TextWR.process()  (비동기 goroutine)
                     │
                     ├── dupCheck에 존재? → 건너뜀
                     │
                     └── TextTable.Set(type, hash, text)
                            │
                            ├── IndexKeyFile.HasKey()? → 건너뜀
                            │
                            └── IndexKeyFile.Put(key, textBytes)
                                   │
                                   ├── MemHashBlock.Put(keyHash, pos)
                                   └── RealKeyFile.Append(record)
```

### 읽기 (Read)

```
클라이언트 → GET_TEXT_100(type, hashList)
                │
                ▼
          resolveText() (handler_text.go)
                │
                ├── [1] TextCache.Get(type, hash)       → Hit? 반환
                │
                ├── [2] TextWR.GetString(type, hash)    → Found? 캐시 후 반환
                │        (최신 인덱스, 서버 기동 후 쓰인 데이터)
                │
                └── [3] TextRD.GetString(type, hash)    → Found? 캐시 후 반환
                         (스냅샷 인덱스, 서버 기동 이전 데이터)
```

TextWR이 TextRD보다 먼저 조회되는 이유: TextWR의 MemHashBlock은 서버 기동 후 쓰인 최신 데이터를 포함하지만, TextRD의 MemHashBlock은 파일을 열 시점의 스냅샷이므로 이후 추가된 데이터를 볼 수 없다.

## 중복 제거

텍스트 데이터는 두 단계에서 중복을 제거한다.

| 단계 | 위치 | 방식 |
|------|------|------|
| 1차 | `TextWR.dupCheck` (인메모리) | `map[dupKey]struct{}`로 `(div, hash)` 쌍 추적 |
| 2차 | `TextTable.Set()` → `IndexKeyFile.HasKey()` | 디스크 인덱스에서 키 존재 여부 확인 |

1차 검사로 대부분의 중복을 빠르게 걸러내고, 2차 검사로 서버 재시작 후에도 디스크 수준의 정합성을 보장한다.

## 일별 로테이션

특정 텍스트 타입(`service`, `apicall`, `ua`)은 설정에 따라 일별 디렉터리에도 저장된다.

### 설정

| 설정 키 | 기본값 | 설명 |
|---------|--------|------|
| `mgr_text_db_daily_service_enabled` | `false` | service 텍스트 일별 저장 |
| `mgr_text_db_daily_api_enabled` | `false` | apicall 텍스트 일별 저장 |
| `mgr_text_db_daily_ua_enabled` | `false` | ua 텍스트 일별 저장 |

### 일자 변경 처리

`TextCacheReset`은 2초 간격으로 날짜 변경을 감시한다. 날짜가 바뀌면 모든 활성 에이전트에 `OBJECT_RESET_CACHE` 명령을 전송하여 에이전트의 텍스트 캐시를 초기화한다. 에이전트는 이후 모든 텍스트 매핑을 다시 전송하며, 서버는 새 날짜의 디렉터리에 기록한다.

```
2초 간격 체크 ──→ 날짜 변경 감지
                     │
                     ▼
              모든 활성 에이전트에 OBJECT_RESET_CACHE 전송
                     │
                     ▼
              에이전트: 텍스트 캐시 초기화 → 텍스트 재전송
                     │
                     ▼
              서버: 새 날짜 디렉터리(YYYYMMDD/text/)에 저장
```

## 프로토콜 명령어

| 명령어 | 설명 | 응답 |
|--------|------|------|
| `GET_TEXT` | 단일 MapPack 응답 | hash → text 매핑 |
| `GET_TEXT_100` | 100개 단위 배치 응답 | 100개 초과 시 다중 MapPack |
| `GET_TEXT_PACK` | TextPack 스트림 응답 | 개별 TextPack |
| `GET_TEXT_ANY_TYPE` | 혼합 타입 조회 | type/hash 배열 병렬 처리 |

## Java 서버와의 차이점

| 항목 | Java (Scala) | Go |
|------|-------------|-----|
| 인덱스 키 | 8바이트 (div hash + text hash) | 4바이트 (text hash만) |
| 비동기 큐 크기 | `DBCtr.LARGE_MAX_QUE_SIZE` | 10,000 고정 |
| 중복 제거 | `TextDupCheck` 싱글톤 | TextWR 인스턴스 내 map |
| 유휴 정리 | 5분 타임아웃으로 DB 닫기 | 날짜 변경 시 에이전트 리셋 |
| 영구/일별 분리 | `TextPermWR`/`TextPermRD` 별도 클래스 | TextWR/TextRD에서 경로 분기 |
| 캐시 계층 | TextCache (LRU) | TextCache (LRU) + TextRD map + MemHashBlock |