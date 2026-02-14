# Scouter Server Go 통신 프로토콜 분석

## 1. 전체 아키텍처

```
┌──────────┐   UDP (6100)    ┌──────────────────┐   TCP (6100)    ┌──────────┐
│  Agent   │ ──────────────→ │  Scouter Server  │ ←────────────→ │  Client  │
│ (Java등) │   단방향 데이터   │    (Go 서버)      │   양방향 통신    │ (Eclipse │
└──────────┘                 └──────────────────┘                │  /WebApp)│
     ↑                              ↑                            └──────────┘
     └── TCP_AGENT (6100) ──────────┘
         서버→에이전트 명령 채널
```

- **UDP 6100**: 에이전트가 서버로 메트릭/트랜잭션 데이터를 전송 (단방향)
- **TCP 6100**: 클라이언트 연결 + 에이전트 명령 채널 (양방향)

---

## 2. 바이너리 직렬화 기본 형식

모든 통신은 **Big-Endian** 바이너리 프로토콜을 사용합니다.

### 기본 데이터 타입

| 타입 | 크기 | 설명 |
|------|------|------|
| Byte | 1 | 단일 바이트 |
| Boolean | 1 | 0x00/0x01 |
| Int16 | 2 | Big-endian 16비트 |
| Int32 | 4 | Big-endian 32비트 |
| Int64 | 8 | Big-endian 64비트 |
| Int3 | 3 | Big-endian 24비트 (-8388608 ~ 8388607) |
| Long5 | 5 | Big-endian 40비트 (-549755813888 ~ 549755813887) |
| Float32 | 4 | IEEE 754 (Int32 비트 변환) |
| Float64 | 8 | IEEE 754 (Int64 비트 변환) |
| Text | 가변 | 길이 접두사 + UTF-8 문자열 |
| Blob | 가변 | 길이 접두사 + 바이트 배열 |

### Decimal (가변길이 정수 압축)

정수 크기에 따라 자동으로 바이트 수를 최적화합니다:

```
0x00                        → 값 = 0        (1바이트)
0x01 + int8                 → [-128, 127]   (2바이트)
0x02 + int16                → [-32768, 32767] (3바이트)
0x03 + int24 (3바이트)      → 확장 범위      (4바이트)
0x04 + int32                → 32비트 전체     (5바이트)
0x05 + int40 (5바이트)      → 확장 범위      (6바이트)
0x08 + int64                → 64비트 전체     (9바이트)
```

(`internal/protocol/dataoutx.go:155-178`, `datainx.go:214-251`)

### Text 길이 인코딩

```
0              → 빈 문자열
1~253          → 1바이트 길이 + 데이터
255 + uint16   → 최대 65535바이트
254 + int32    → 그 이상
```

---

## 3. 매직 바이트 (연결 식별)

### UDP 매직 바이트

| 매직 | 16진수 | 용도 |
|------|--------|------|
| `CAFE` | `0x43414645` | 단일 팩 전송 |
| `CAFN` | `0x4341464E` | 복수 팩 전송 (N개) |
| `CAFM` | `0x4341464D` | MTU 분할 패킷 |
| `JAVA` | `0x4A415641` | Java 에이전트 호환 |
| `JAVN` | `0x4A41564E` | Java 복수 팩 |
| `JMTU` | `0x4A4D5455` | Java MTU 분할 |

### TCP 매직 바이트

| 매직 | 값 | 용도 |
|------|---|------|
| TCP_AGENT | `0xCAFE1001` | 에이전트 TCP 연결 |
| TCP_AGENT_V2 | `0xCAFE1002` | 에이전트 v2 (길이 접두사) |
| TCP_AGENT_REQ | `0xCAFE1011` | 에이전트 명령 요청 |
| TCP_CLIENT | `0xCAFE2001` | 클라이언트 연결 |
| TCP_SHUTDOWN | `0xCAFE1999` | 서버 종료 신호 |
| TCP_SEND_STACK | `0xEDED0001` | 스택 트레이스 전송 |

(`internal/protocol/netcafe.go`)

---

## 4. UDP 패킷 구조

### 단일 팩 (CAFE)

```
┌────────────┬───────────┬──────────────┐
│ Magic (4B) │ Type (1B) │ Pack Data    │
│ 0x43414645 │           │ (가변)       │
└────────────┴───────────┴──────────────┘
```

### 복수 팩 (CAFN)

```
┌────────────┬──────────┬──────────┬───────┬──────────┬───────┐
│ Magic (4B) │Count(2B) │Type1(1B) │Data1  │Type2(1B) │Data2  │...
│ 0x4341464E │          │          │       │          │       │
└────────────┴──────────┴──────────┴───────┴──────────┴───────┘
```

### MTU 분할 (CAFM) — 대용량 데이터 분할 전송

```
┌────────────┬───────────┬──────────┬──────────┬─────────┬──────────┐
│ Magic (4B) │ObjHash(4B)│ PkID(8B) │Total(2B) │Num(2B)  │Blob Data │
│ 0x4341464D │ 객체 해시  │패킷 고유ID│총 분할수  │현재 번호 │데이터     │
└────────────┴───────────┴──────────┴──────────┴─────────┴──────────┘
```

- 동일 `PkID`의 모든 분할이 도착하면 재조립
- **10초 타임아웃** 후 미완성 분할 폐기
- 최대 1000개 미완성 패킷 보관

(`internal/netio/udp/processor.go`, `multipacket.go`)

---

## 5. TCP 통신 플로우

### 5.1 TCP 응답 플래그

| 플래그 | 값 | 의미 |
|--------|---|------|
| FLAG_OK | `0x01` | 성공 |
| FLAG_NOT_OK | `0x02` | 실패 |
| FLAG_HAS_NEXT | `0x03` | 추가 데이터 있음 |
| FLAG_NO_NEXT | `0x04` | 응답 종료 |
| FLAG_FAIL | `0x05` | 오류 |
| FLAG_INVALID_SESSION | `0x44` | 세션 무효 |

(`internal/protocol/tcpflag.go`)

### 5.2 클라이언트 연결 (TCP_CLIENT)

```
CLIENT                           SERVER
  │                                │
  │──── 0xCAFE2001 (4B) ─────────→│  연결 식별
  │                                │
  │──── cmd="LOGIN" (text) ──────→│
  │──── session=0 (int64) ──────→│  첫 요청은 세션 없음
  │──── MapPack {id, pass} ─────→│
  │                                │  인증 처리
  │←── FLAG_HAS_NEXT (0x03) ─────│
  │←── MapPack {session, ...} ───│  세션 토큰 발급
  │←── FLAG_NO_NEXT (0x04) ──────│  응답 종료
  │                                │
  │──── cmd="OBJECT_LIST" ──────→│
  │──── session=<token> (int64) ─→│  세션 토큰으로 인증
  │──── MapPack (query) ────────→│
  │                                │
  │←── FLAG_HAS_NEXT (0x03) ─────│
  │←── Pack data ────────────────│
  │←── FLAG_NO_NEXT (0x04) ──────│  응답 종료
  │                                │
  │         ... 반복 ...           │
```

#### 세션 인증

- `LOGIN`, `SERVER_VERSION`, `SERVER_TIME` 명령은 인증 없이 호출 가능 (FreeCmds)
- 그 외 모든 명령은 유효한 세션 토큰(int64)이 필요
- 세션이 무효하면 `FLAG_INVALID_SESSION (0x44)` 반환 후 연결 종료

(`internal/protocol/requestcmd.go:324-329`)

#### 핸들러 패턴

```go
func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
    // 요청 데이터 읽기
    pk, _ := pack.ReadPack(din)
    m := pk.(*pack.MapPack)

    // 처리
    result := m.GetText("key")

    // 응답 (FLAG_HAS_NEXT + 팩 데이터)
    dout.WriteByte(protocol.FLAG_HAS_NEXT)
    pack.WritePack(dout, responsePack)
    // FLAG_NO_NEXT는 handleClient()가 자동 추가
}
```

#### 주요 클라이언트 명령

| 명령 | 용도 |
|------|------|
| `LOGIN` | 인증, 세션 토큰 획득 |
| `GET_XML_COUNTER` | 카운터 정의 XML 다운로드 |
| `OBJECT_LIST_REAL_TIME` | 실시간 에이전트 목록 |
| `COUNTER_REAL_TIME` | 실시간 메트릭 조회 |
| `GET_TEXT_100` | 해시→텍스트 역변환 |
| `TRANX_REAL_TIME` | 실시간 트랜잭션 데이터 |
| `CLOSE` | 연결 종료 |

(`internal/netio/tcp/server.go:156-222`)

### 5.3 에이전트 TCP 연결 (TCP_AGENT)

에이전트 TCP 연결에 대한 상세 내용은 [tcp-agent-protocol.md](tcp-agent-protocol.md)를 참조하세요.

---

## 6. Pack 타입 체계

### 주요 Pack 타입 코드 (1바이트)

| 코드 | 이름 | 용도 | 파일 |
|------|------|------|------|
| 10 | MapPack | 범용 Key-Value 맵 | `map_pack.go` |
| 21 | XLogPack | 트랜잭션 로그 (40+ 필드) | `xlog_pack.go` |
| 22 | DroppedXLogPack | 드롭된 트랜잭션 | `dropped_xlog_pack.go` |
| 26 | XLogProfilePack | 프로파일링 데이터 | `xlog_profile_pack.go` |
| 27 | XLogProfilePack2 | 확장 프로파일링 데이터 | `xlog_profile_pack2.go` |
| 31 | SpanPack | 분산 트레이싱 스팬 | `span_pack.go` |
| 32 | SpanContainerPack | 스팬 컨테이너 | `span_container_pack.go` |
| 50 | TextPack | 해시→텍스트 매핑 | `text_pack.go` |
| 60 | PerfCounterPack | 성능 메트릭 | `perf_counter_pack.go` |
| 61 | StatusPack | 상태 정보 | `status_pack.go` |
| 62 | StackPack | 스택 트레이스 | `stack_pack.go` |
| 63 | SummaryPack | 요약 통계 | `summary_pack.go` |
| 64 | BatchPack | 배치 작업 데이터 | `batch_pack.go` |
| 65 | InteractionPerfCounterPack | 상호작용 메트릭 | `interaction_perf_counter_pack.go` |
| 70 | AlertPack | 알림 데이터 | `alert_pack.go` |
| 80 | ObjectPack | 에이전트 하트비트/메타데이터 | `object_pack.go` |

(`internal/protocol/pack/pack.go`)

### Pack 인터페이스

```go
type Pack interface {
    PackType() byte
    Write(o *DataOutputX)
    Read(d *DataInputX) error
}
```

### Pack 직렬화 형식

```
[pack_type: 1바이트][pack 고유 필드들: 가변]
```

`pack.WritePack()`은 타입 코드 1바이트를 먼저 쓰고, `pack.Write()`로 필드를 직렬화합니다.
`pack.ReadPack()`은 타입 코드를 읽어 `CreatePack()`으로 인스턴스를 생성한 후 `pack.Read()`를 호출합니다.

(`internal/protocol/pack/pack.go:76-99`)

---

## 7. Value 타입 체계

각 Value는 `[type_code:1바이트][데이터]` 형식으로 직렬화됩니다.

### Value 타입 코드

| 코드 | 타입 | 구현체 | 설명 |
|------|------|--------|------|
| 0 | NULL | NullValue | null 값 |
| 10 | BOOLEAN | BooleanValue | 불리언 |
| 20 | DECIMAL | DecimalValue | 가변길이 정수 |
| 30 | FLOAT | FloatValue | 32비트 부동소수점 |
| 40 | DOUBLE | DoubleValue | 64비트 부동소수점 |
| 45 | DOUBLE_SUMMARY | DoubleSummary | 통계 (min/max/avg/count) |
| 46 | LONG_SUMMARY | LongSummary | 정수 통계 |
| 50 | TEXT | TextValue | 문자열 |
| 51 | TEXT_HASH | TextHashValue | 해시된 문자열 (Hexa32) |
| 60 | BLOB | BlobValue | 바이트 배열 |
| 61 | IP4ADDR | IP4Value | IPv4 주소 |
| 70 | LIST | ListValue | Value 리스트 |
| 71 | ARRAY_INT | IntArray | int 배열 |
| 72 | ARRAY_FLOAT | FloatArray | float 배열 |
| 73 | ARRAY_TEXT | TextArray | 문자열 배열 |
| 74 | ARRAY_LONG | LongArray | long 배열 |
| 80 | MAP | MapValue | String→Value 맵 |

(`internal/protocol/value/value.go`)

### MapValue/MapPack 직렬화 형식

```
[count: Decimal(가변)][key1: Text][value1: type_byte + value_data][key2: Text][value2: type_byte + value_data]...
```

---

## 8. 주요 Pack 구조 상세

### ObjectPack (에이전트 하트비트, 코드 80)

```
[ObjType: Text][ObjHash: Decimal][ObjName: Text][Address: Text]
[Version: Text][Alive: Boolean][Wakeup: Decimal][Tags: Value]
```

(`internal/protocol/pack/object_pack.go:26-35`)

### PerfCounterPack (성능 메트릭, 코드 60)

```
[Time: Int64][ObjName: Text][TimeType: Byte][Data: Value(MapValue)]
```

(`internal/protocol/pack/perf_counter_pack.go:22-27`)

### TextPack (문자열 해시, 코드 50)

```
[XType: Text][Hash: Int32][Text: Text]
```

(`internal/protocol/pack/text_pack.go:20-24`)

### XLogPack (트랜잭션 로그, 코드 21)

```
[EndTime: Decimal][ObjHash: Decimal][Service: Decimal][Txid: Int64]
[ThreadNameHash: Int64][Caller: Int64][Gxid: Int64][Elapsed: Decimal]
[Error: Decimal][Cpu: Decimal][SqlCount: Decimal][SqlTime: Decimal]
[IPAddr: Blob][Kbytes: Decimal][Status: Decimal][Userid: Decimal]
[UserAgent: Decimal][Referer: Decimal][Group: Decimal]
[ApicallCount: Decimal][ApicallTime: Decimal][CountryCode: Text]
[City: Decimal][XType: Byte][Login: Decimal][Desc: Decimal]
[WebHash: Decimal][WebTime: Decimal][HasDump: Byte]
[Text1: Text][Text2: Text][QueuingHostHash: Decimal]
[QueuingTime: Decimal][Queuing2ndHostHash: Decimal]
[Queuing2ndTime: Decimal][Text3: Text][Text4: Text][Text5: Text]
[ProfileCount: Decimal][B3Mode: Boolean][ProfileSize: Decimal]
[DiscardType: Byte][IgnoreGlobalConsequentSampling: Boolean]
```

(`internal/protocol/pack/xlog_pack.go:60-140+`)

---

## 9. UDP 데이터 흐름

### 수신 흐름

```
[에이전트 UDP 패킷]
     │
     ▼
UDP Server (0.0.0.0:6100)
     │ 데이터그램 수신 (최대 65535 바이트)
     │ 버퍼 복사 (UDP 경계 보존)
     ▼
Magic 바이트 파싱 (4바이트)
     │
     ├─ CAFE (0x43414645) → processCafe()
     │   └─ [type:1B][data] → Pack 생성 → 디스패처
     │
     ├─ CAFN (0x4341464E) → processCafeN()
     │   └─ [count:2B][type1:1B][data1][type2:1B][data2]...
     │      → 각 Pack 생성 → 디스패처
     │
     ├─ CAFM (0x4341464D) → processCafeMTU()
     │   └─ [objHash:4B][pkid:8B][total:2B][num:2B][data]
     │      → MultiPacketProcessor 에 저장
     │      → 모든 분할 도착 시 재조립 → 디스패처
     │
     ├─ JAVA → processCafe() (CAFE와 동일 처리)
     ├─ JAVN → processCafeN() (CAFN와 동일 처리)
     └─ JMTU → processCafeMTU() (CAFM와 동일 처리)
```

(`internal/netio/udp/server.go:43-88`, `processor.go:55-138`)

### 디스패처 라우팅

| Pack 타입 | 핸들러 | 동작 |
|-----------|--------|------|
| ObjectPack (80) | AgentManager | 에이전트 등록/갱신 → ObjectCache |
| PerfCounterPack (60) | PerfCountCore | 메트릭 저장 |
| XLogPack (21) | XLogCore | 트랜잭션 로그 저장 |
| TextPack (50) | TextCore | 해시→텍스트 캐시 + 디스크 저장 |
| AlertPack (70) | AlertCore | 알림 처리 |

### MTU 분할 재조립

```
MultiPacketProcessor
├── pending: map[int64]*multiPacket   ← pkid → 분할 저장소
│
├── 분할 도착 시:
│   ├─ pending[pkid] 없으면 → 새 multiPacket 생성
│   ├─ fragments[num] = data 저장
│   ├─ receivedCount++
│   └─ receivedCount == total → 재조립 완료
│       └─ 모든 분할 순서대로 합침 → 완성된 바이트 배열
│
└── 만료 처리:
    └─ 10초 경과한 미완성 패킷 → 폐기 (최대 1000개 보관)
```

(`internal/netio/udp/multipacket.go:34-105`)

---

## 10. TCP 클라이언트 완전한 초기화 시퀀스

```
CLIENT                                    SERVER
  │                                         │
  │ ──── 0xCAFE2001 ───────────────────────→│  TCP_CLIENT 매직
  │                                         │
  │ ── (1) LOGIN ──────────────────────────→│
  │    cmd="LOGIN", session=0               │
  │    MapPack {id:"admin", pass:"...",     │
  │             ip:"...", hostname:"...",   │
  │             version:"..."}              │
  │                                         │  인증 처리
  │ ←── FLAG_HAS_NEXT (0x03) ──────────────│
  │ ←── MapPack {session:token,            │
  │              server_id:...,            │
  │              timezone:...}             │
  │ ←── FLAG_NO_NEXT (0x04) ───────────────│
  │                                         │
  │ ── (2) GET_XML_COUNTER ────────────────→│  카운터 정의 요청
  │    cmd="GET_XML_COUNTER", session=token │
  │                                         │
  │ ←── FLAG_HAS_NEXT (0x03) ──────────────│
  │ ←── MapPack {xml: counters.xml 내용}   │
  │ ←── FLAG_NO_NEXT (0x04) ───────────────│
  │                                         │
  │ ── (3) OBJECT_LIST_REAL_TIME ──────────→│  에이전트 목록 폴링
  │    cmd="OBJECT_LIST_REAL_TIME"          │
  │    session=token                        │
  │                                         │
  │ ←── FLAG_HAS_NEXT (0x03) ──────────────│
  │ ←── ObjectPack (에이전트1) ────────────│
  │ ←── FLAG_HAS_NEXT (0x03) ──────────────│
  │ ←── ObjectPack (에이전트2) ────────────│
  │ ←── FLAG_NO_NEXT (0x04) ───────────────│
  │                                         │
  │         ... 주기적 폴링 반복 ...         │
  │                                         │
  │ ── (N) CLOSE ──────────────────────────→│  연결 종료
```

---

## 11. 설정 파라미터

| 설정 | 기본값 | 설명 |
|------|-------|------|
| `net.udp.listen.ip` | 0.0.0.0 | UDP 바인드 주소 |
| `net.udp.listen.port` | 6100 | UDP 포트 |
| `net.udp.packet.buffer.size` | 65535 | UDP 버퍼 크기 |
| `net.udp.so.rcvbuf.size` | 8MB | UDP 소켓 수신 버퍼 |
| `net.tcp.listen.ip` | 0.0.0.0 | TCP 바인드 주소 |
| `net.tcp.listen.port` | 6100 | TCP 포트 |
| `net.tcp.client.so.timeout.ms` | 8000 | 클라이언트 읽기 타임아웃 |
| `net.tcp.agent.keepalive.interval.ms` | 60000 | 에이전트 Keepalive 간격 |
| `net.tcp.get.agent.connection.wait.ms` | 5000 | 에이전트 연결 대기 시간 |

---

## 12. 전체 데이터 흐름 요약

```
[Java Agent]
    │
    ├─ UDP ObjectPack (하트비트, 30초 주기)
    │   → dispatcher → AgentManager → ObjectCache
    │
    ├─ UDP PerfCounterPack (메트릭)
    │   → dispatcher → PerfCountCore → 저장
    │
    ├─ UDP XLogPack (트랜잭션)
    │   → dispatcher → XLogCore → 저장
    │
    ├─ UDP TextPack (문자열 해시)
    │   → dispatcher → TextCore → textCache + 디스크
    │
    └─ TCP_AGENT (명령 채널)
        ← 서버가 명령 전송 (KEEP_ALIVE, THREAD_DUMP 등)

[Eclipse Client]
    │
    └─ TCP_CLIENT
        ├─ LOGIN → 세션 토큰 획득
        ├─ GET_XML_COUNTER → 카운터 정의 XML
        ├─ OBJECT_LIST_REAL_TIME → 에이전트 목록 폴링
        ├─ COUNTER_REAL_TIME → 실시간 메트릭
        ├─ GET_TEXT_100 → 해시→텍스트 역변환
        └─ ... (200+ 커맨드)
```

---

## 13. 주요 특징 요약

| 항목 | 내용 |
|------|------|
| **프로토콜 형식** | 바이너리 (텍스트 기반 아님) |
| **직렬화** | Big-endian, 가변길이 정수 (Decimal) |
| **UDP 포트** | 6100 (에이전트 데이터 수신) |
| **TCP 포트** | 6100 (클라이언트 + 에이전트) |
| **최대 UDP 패킷** | 65535바이트 (초과 시 MTU 분할) |
| **매직 바이트** | 4바이트 서명 (CAFE 변형, JAVA 변형) |
| **Pack 타입** | 18종 (ObjectPack, XLogPack, PerfCounterPack 등) |
| **Value 타입** | 16종 (Decimal, Text, TextHash, MapValue, ListValue 등) |
| **압축** | 가변길이 정수 인코딩만 사용 (gzip/deflate 없음) |
| **암호화** | 없음 (네트워크 분리에 의존) |
| **인증** | ID/Password → int64 세션 토큰 |
| **Keepalive** | TCP_AGENT 연결에 KEEP_ALIVE 명령 (60초 간격) |
| **MTU 분할 만료** | 10초 |
| **바이너리 호환** | Java 원본 Scouter와 완전 호환 |
