# Scouter TCP 에이전트 연결 프로토콜 상세 분석

## 1. 개요

TCP 에이전트 연결은 **서버가 에이전트에게 명령을 보내기 위한 역방향 채널**입니다. 에이전트가 먼저 서버에 TCP 연결을 맺고, 서버는 그 연결을 **커넥션 풀**에 보관했다가, 필요할 때 에이전트에게 명령(Thread Dump, 설정 변경 등)을 전송합니다.

```
┌──────────┐                          ┌──────────────┐
│  Agent   │ ─── (1) TCP 연결 ──────→ │   Server     │
│          │                          │              │
│          │ ←── (2) 풀에 보관 ────── │ AgentManager │
│          │                          │ (커넥션 풀)   │
│          │                          │              │
│          │ ←── (3) 명령 전송 ────── │ AgentCall    │
│          │ ─── (4) 응답 반환 ─────→ │              │
│          │                          │              │
│          │ ←── (5) 풀로 반환 ────── │              │
└──────────┘                          └──────────────┘
```

> UDP는 에이전트→서버 방향(메트릭/트랜잭션 전송)이고, TCP Agent는 서버→에이전트 방향(명령 전달)입니다.

---

## 2. 연결 수립 (Handshake)

### 2.1 프로토콜 버전

| 매직 바이트 | 값 | 프로토콜 | 차이점 |
|------------|---|---------|--------|
| `TCP_AGENT` | `0xCAFE1001` | v1 (스트리밍) | 명령/팩을 순차적으로 바로 직렬화 |
| `TCP_AGENT_V2` | `0xCAFE1002` | v2 (길이 접두사) | 데이터 앞에 `int32` 길이를 붙여 프레이밍 |

### 2.2 연결 시퀀스

```
AGENT                                    SERVER
  │                                        │
  │ ──── Magic (4B) ─────────────────────→ │  0xCAFE1001 또는 0xCAFE1002
  │ ──── ObjHash (4B, int32) ────────────→ │  에이전트 고유 식별자
  │                                        │
  │                               handleConnection() (server.go:114)
  │                               ├─ magic 읽기 (4바이트)
  │                               ├─ switch case: TCP_AGENT / TCP_AGENT_V2
  │                               ├─ objHash 읽기 (4바이트)
  │                               ├─ NewAgentWorker() 생성
  │                               └─ AgentManager.Add(objHash, worker)
  │                                        │
  │       ← 연결 유지 (풀에 보관) ──────── │
```

**핵심**: 연결 수립 후 `conn.Close()`를 호출하지 않습니다. 연결은 풀에 보관되어 재사용됩니다.

(`server.go:136-148`)

---

## 3. AgentWorker — 개별 연결 관리

`AgentWorker`(`agent_worker.go`)는 에이전트와의 단일 TCP 연결을 관리하는 구조체입니다.

### 3.1 구조체

```go
type AgentWorker struct {
    mu            sync.Mutex        // 동시성 보호
    conn          net.Conn          // TCP 소켓
    din           *protocol.DataInputX   // 읽기 스트림
    dout          *protocol.DataOutputX  // 쓰기 스트림
    writer        *bufio.Writer
    protocolType  uint32            // TCP_AGENT 또는 TCP_AGENT_V2
    objHash       int32             // 에이전트 식별자
    lastWriteTime time.Time         // 마지막 쓰기 시각 (Keepalive 판단용)
    closed        bool              // 닫힘 여부
}
```

### 3.2 명령 전송 (Write) — 서버 → 에이전트

두 프로토콜 버전에 따라 직렬화 방식이 다릅니다.

#### V1 (TCP_AGENT: `0xCAFE1001`) — 스트리밍 방식

```
┌───────────────┬────────────────────────────┐
│ cmd (Text)    │ Pack (type + data)         │
│ 가변 길이      │ 가변 길이                   │
└───────────────┴────────────────────────────┘
```

```go
// agent_worker.go:51-53
w.dout.WriteText(cmd)
pack.WritePack(w.dout, p)
```

명령 이름(Text)과 팩 데이터를 순차적으로 스트림에 씁니다.

#### V2 (TCP_AGENT_V2: `0xCAFE1002`) — 길이 접두사 방식

```
┌──────────────────┬───────────────┬────────────────────────────┐
│ Length (4B int32) │ cmd (Text)    │ Pack (type + data)         │
│ 전체 프레임 크기  │ 가변 길이      │ 가변 길이                   │
└──────────────────┴───────────────┴────────────────────────────┘
```

```go
// agent_worker.go:54-58
buf := protocol.NewDataOutputX()      // 임시 버퍼에 직렬화
buf.WriteText(cmd)
pack.WritePack(buf, p)
w.dout.WriteIntBytes(buf.ToByteArray()) // int32 길이 + 바이트 배열로 전송
```

V2는 전체 프레임을 먼저 메모리에 직렬화한 뒤, `[length:4바이트][data]` 형식으로 보냅니다. 이 방식은 수신측에서 프레임 경계를 명확히 알 수 있어 안정적입니다.

### 3.3 응답 수신 (Read) — 에이전트 → 서버

에이전트의 응답도 프로토콜 버전에 따라 다릅니다.

#### V1 응답

```go
// agent_worker.go:80-81
return pack.ReadPack(w.din)  // 스트림에서 직접 팩을 읽음
```

#### V2 응답

```go
// agent_worker.go:82-88
buf, err := w.din.ReadIntBytes()            // [length:4B][data] 읽기
return pack.ReadPack(protocol.NewDataInputX(buf))  // 버퍼에서 팩 파싱
```

### 3.4 응답 프로토콜 (플래그 기반 스트리밍)

에이전트의 응답은 **플래그 바이트**로 스트리밍됩니다:

```
AGENT 응답 스트림:

┌─────────────────┬──────────────────┐
│ FLAG_HAS_NEXT   │ Pack 데이터       │    ← 첫 번째 응답
│ (0x03, 1바이트)  │ (가변)           │
├─────────────────┼──────────────────┤
│ FLAG_HAS_NEXT   │ Pack 데이터       │    ← 두 번째 응답 (선택적)
│ (0x03)          │ (가변)           │
├─────────────────┼──────────────────┤
│ FLAG_NO_NEXT    │                  │    ← 응답 종료 신호
│ (0x04, 1바이트)  │                  │
└─────────────────┴──────────────────┘
```

서버는 이 루프로 응답을 읽습니다:

```go
for {
    flag := worker.ReadByte()    // 1바이트 플래그 읽기
    if flag != FLAG_HAS_NEXT {   // 0x03이 아니면 종료
        break
    }
    p := worker.ReadPack()       // 팩 읽기
    // p 처리
}
```

---

## 4. AgentManager — 커넥션 풀

`AgentManager`(`agent_manager.go`)는 `objHash` 별로 에이전트 연결을 관리하는 **커넥션 풀**입니다.

### 4.1 풀 구조

```
AgentManager
├── agents: map[int32]*agentQueue    ← objHash → 큐 매핑
│
├── objHash=100 → agentQueue
│                   ├── AgentWorker (conn1)
│                   ├── AgentWorker (conn2)
│                   └── AgentWorker (conn3)
│
├── objHash=200 → agentQueue
│                   └── AgentWorker (conn1)
│
└── objHash=300 → agentQueue
                    ├── AgentWorker (conn1)
                    └── AgentWorker (conn2)
```

### 4.2 풀 설정값

| 설정 | 기본값 | 설명 |
|------|-------|------|
| `maxConnsPerAgent` | 50 | 에이전트당 최대 연결 수 |
| `keepaliveInterval` | 60초 | Keepalive 전송 간격 |
| `keepaliveTimeout` | 3초 | Keepalive 응답 대기 시간 |
| `getConnWait` | 5초 | 연결 대기 최대 시간 |
| `defaultMaxAgents` | 5000 | 전체 최대 에이전트 수 |

(`agent_manager.go:10-16`)

### 4.3 풀 동작

```
[에이전트 연결 수립]
     │
     ▼
 AgentManager.Add(objHash, worker)     ← 큐에 추가
     │
     │  큐 크기 < maxConnsPerAgent?
     ├─ YES → 큐에 저장, cond.Signal()
     └─ NO  → worker.Close() (초과분 폐기)


[서버가 에이전트에 명령 전송 시]
     │
     ▼
 AgentManager.Get(objHash)             ← 큐에서 꺼냄
     │
     │  큐에 사용 가능한 워커 있음?
     ├─ YES → 즉시 반환
     └─ NO  → getConnWait(5초) 동안 cond.Wait()
              ├─ 워커 도착 → 반환
              └─ 타임아웃 → nil 반환


[명령 처리 완료 후]
     │
     ▼
 AgentManager.Add(objHash, worker)     ← 풀로 반환 (재사용)
```

---

## 5. AgentCall — RPC 패턴

`AgentCall`(`agent_call.go`)은 풀에서 워커를 꺼내 명령을 보내고 응답을 받는 **RPC 스타일 호출**을 제공합니다.

### 5.1 단일 응답 호출 (Call)

```
Server (AgentCall)                              Agent
     │                                            │
     │  (1) AgentManager.Get(objHash)              │
     │      → 풀에서 worker 꺼냄                    │
     │                                            │
     │  (2) worker.Write("THREAD_DUMP", param) ──→│  명령 전송
     │                                            │
     │  (3) ReadByte()                            │
     │  ←── FLAG_HAS_NEXT (0x03) ─────────────────│  응답 시작
     │                                            │
     │  (4) ReadPack()                            │
     │  ←── MapPack {result: "..."} ──────────────│  응답 데이터
     │                                            │
     │  (5) ReadByte()                            │
     │  ←── FLAG_NO_NEXT (0x04) ──────────────────│  응답 종료
     │                                            │
     │  (6) AgentManager.Add(objHash, worker)      │
     │      → 풀로 워커 반환                        │
     │                                            │
     └─ return MapPack                            │
```

(`agent_call.go:21-67`)

### 5.2 스트리밍 응답 호출 (CallStream)

여러 개의 팩을 연속으로 받는 경우 (예: Thread Dump의 여러 스레드 정보):

```
Server (AgentCall)                              Agent
     │                                            │
     │  Write("ACTIVE_THREAD_LIST", param) ──────→│
     │                                            │
     │  ←── FLAG_HAS_NEXT (0x03) ─────────────────│
     │  ←── MapPack {thread1 info} ───────────────│  → handler(pack) 호출
     │                                            │
     │  ←── FLAG_HAS_NEXT (0x03) ─────────────────│
     │  ←── MapPack {thread2 info} ───────────────│  → handler(pack) 호출
     │                                            │
     │  ←── FLAG_HAS_NEXT (0x03) ─────────────────│
     │  ←── MapPack {thread3 info} ───────────────│  → handler(pack) 호출
     │                                            │
     │  ←── FLAG_NO_NEXT (0x04) ──────────────────│  응답 종료
     │                                            │
     │  → 풀로 워커 반환                            │
```

(`agent_call.go:70-105`)

---

## 6. Keepalive 메커니즘

### 6.1 동작 원리

서버는 **5초 주기**로 모든 풀의 워커를 점검합니다:

```
StartKeepalive() (agent_manager.go:201)
     │
     │  매 5초마다 ticker 발생
     ▼
 runKeepalive()
     │
     ├─ (1) 모든 agentQueue 순회
     │
     ├─ (2) 각 큐에서 drainAll()로 모든 워커 꺼냄
     │
     ├─ (3) 각 워커 점검:
     │       ├─ IsClosed() → 폐기
     │       ├─ IsExpired(60초) → SendKeepAlive() 실행
     │       └─ 살아있으면 → 다시 풀에 put()
     │
     └─ (4) 빈 큐 정리: delete(m.agents, objHash)
```

### 6.2 SendKeepAlive 상세

```go
// agent_worker.go:118-148
func (w *AgentWorker) SendKeepAlive(readTimeout time.Duration) {
    // (1) 읽기 타임아웃 설정 (3초)
    w.conn.SetReadDeadline(time.Now().Add(readTimeout))
    defer w.conn.SetReadDeadline(time.Time{})  // 리셋

    // (2) KEEP_ALIVE 명령 전송
    w.Write("KEEP_ALIVE", &pack.MapPack{})

    // (3) 응답 drain (에이전트가 보내는 데이터 버림)
    for {
        b := w.ReadByte()
        if b != FLAG_HAS_NEXT {
            break
        }
        // 응답 데이터 읽고 폐기
        pack.ReadPack(w.din)  // v1
        // 또는
        w.din.ReadIntBytes()  // v2
    }
}
```

```
Server                                Agent
  │                                     │
  │ ── "KEEP_ALIVE" + MapPack{} ──────→│
  │                                     │  (에이전트가 응답 반환)
  │ ←── FLAG_HAS_NEXT ─────────────────│  (선택적)
  │ ←── Pack (데이터) ─────────────────│  (선택적)
  │ ←── FLAG_NO_NEXT ──────────────────│
  │                                     │
  │    또는 3초 타임아웃 → 연결 닫힘      │
```

---

## 7. 바이트 레벨 패킷 예시

### 7.1 V1 연결 수립

```
바이트 스트림 (에이전트 → 서버):

CA FE 10 01              ← Magic: TCP_AGENT (0xCAFE1001)
00 00 00 64              ← ObjHash: 100 (int32, big-endian)
```

### 7.2 V1 명령 전송 (서버 → 에이전트)

```
바이트 스트림:

0B                       ← Text 길이: 11
54 48 52 45 41 44 5F     ← "THREAD_"
44 55 4D 50              ← "DUMP"
0A                       ← Pack type: MapPack (10)
01                       ← MapPack entry 수: 1 (Decimal)
06                       ← Key 길이: 6
61 63 74 69 6F 6E        ← "action"
32                       ← Value type: TEXT (50)
04                       ← Text 길이: 4
64 75 6D 70              ← "dump"
```

### 7.3 V2 명령 전송 (서버 → 에이전트)

```
바이트 스트림:

00 00 00 1A              ← Frame 길이: 26바이트 (int32)
┌─ 프레임 시작 ──────────────────────────────┐
│ 0B                     ← Text 길이: 11     │
│ 54 48 52 45 41 44 5F   ← "THREAD_"        │
│ 44 55 4D 50            ← "DUMP"           │
│ 0A                     ← Pack type: 10     │
│ 01                     ← entry 수: 1       │
│ 06                     ← Key 길이: 6       │
│ 61 63 74 69 6F 6E      ← "action"         │
│ 32                     ← Value type: 50    │
│ 04                     ← Text 길이: 4      │
│ 64 75 6D 70            ← "dump"           │
└─ 프레임 끝 ────────────────────────────────┘
```

### 7.4 에이전트 응답

```
바이트 스트림 (에이전트 → 서버):

03                       ← FLAG_HAS_NEXT
0A                       ← Pack type: MapPack
01                       ← entry 수: 1
06                       ← Key 길이: 6
72 65 73 75 6C 74        ← "result"
32                       ← Value type: TEXT (50)
02                       ← Text 길이: 2
6F 6B                    ← "ok"
04                       ← FLAG_NO_NEXT (응답 종료)
```

---

## 8. V1 vs V2 비교 요약

| 항목 | V1 (`0xCAFE1001`) | V2 (`0xCAFE1002`) |
|------|-------------------|-------------------|
| **프레이밍** | 없음 (스트리밍) | `[int32 길이][데이터]` |
| **명령 전송** | `WriteText(cmd)` + `WritePack(p)` 직접 전송 | 임시 버퍼에 직렬화 → `WriteIntBytes()` |
| **응답 읽기** | `pack.ReadPack(din)` 스트림 직접 파싱 | `ReadIntBytes()` → 버퍼에서 파싱 |
| **장점** | 단순, 오버헤드 적음 | 프레임 경계 명확, 에러 복구 용이 |
| **응답 플래그** | 동일: `0x03`(HAS_NEXT) / `0x04`(NO_NEXT) | 동일 |

---

## 9. 전체 생명주기 다이어그램

```
[에이전트 시작]
     │
     ▼
TCP 연결 → 0xCAFE1001 + objHash 전송
     │
     ▼
서버: AgentManager.Add(objHash, worker)
     │                              ┌────────────────────┐
     ▼                              │  Keepalive 데몬     │
풀에서 대기 ←───────────────────── │  5초마다 점검        │
     │                              │  60초 유휴 시        │
     │                              │  → KEEP_ALIVE 전송  │
     │                              │  3초 내 무응답 시    │
     │                              │  → 연결 종료         │
     │                              └────────────────────┘
     │
[클라이언트가 Thread Dump 요청]
     │
     ▼
서버: AgentCall.Call(objHash, "THREAD_DUMP", param)
     │
     ├─ Get(objHash)  → 풀에서 워커 꺼냄
     ├─ Write()       → 에이전트에 명령 전송
     ├─ ReadByte()    → 플래그 읽기 (0x03/0x04)
     ├─ ReadPack()    → 응답 팩 파싱
     ├─ Add()         → 풀로 반환
     └─ return result
     │
     ▼
클라이언트에게 결과 전달
```
