# Scouter Server (Go)

Scouter APM의 **경량 백엔드 서버**입니다. 기존 Java 서버를 Go로 재작성하여 **메모리 사용량을 약 70% 절감**(Java 대비 30% 수준)하면서 동일한 APM 데이터 수집 기능을 제공합니다. 단일 바이너리로 배포되며, JVM 없이 즉시 실행할 수 있습니다.

## Features

- **경량 실행 환경**: JVM 불필요, 단일 바이너리 배포, 낮은 메모리 사용량 (Java 서버 대비 ~30%)
- TCP/UDP 기반 에이전트 데이터 수집 (기본 포트: 6100)
- XLog, Counter, Profile, Alert, Summary 등 APM 데이터 처리
- 인메모리 캐시 및 디스크 기반 스토리지
- REST API 서버 (기본 포트: 6180, 설정으로 활성화)
- 설정 파일 핫 리로드 지원
- 일 단위 데이터 보관 및 자동 삭제

## Requirements

- Go 1.22+

## Build

```bash
# 빌드
make build

# 크로스 컴파일 (linux, darwin, windows)
make build-all

# 테스트
make test

# 전체 명령어 확인
make help
```

## Configuration

설정 파일 경로: `./scouter.conf` (환경변수 `SCOUTER_CONF`로 변경 가능)

| 설정 | 기본값 | 설명 |
|------|--------|------|
| UDP 포트 | 6100 | 에이전트 데이터 수신 |
| TCP 포트 | 6100 | 에이전트/클라이언트 연결 |
| HTTP 포트 | 6180 | REST API |
| 데이터 디렉토리 | `./data` | 저장소 경로 (`SCOUTER_DATA_DIR`) |

## Run

```bash
make run
```

## Documentation

- [통신 프로토콜 개요](docs/protocol-overview.md) — 바이너리 직렬화, UDP/TCP 패킷 구조, Pack/Value 타입 체계
- [TCP 에이전트 프로토콜 상세](docs/tcp-agent-protocol.md) — 에이전트 연결 수립, 커넥션 풀, Keepalive, RPC 호출 패턴
- [Text Cache Database](docs/text-cache-database.md) — 해시 기반 텍스트 저장소, 3계층 캐시 구조, 디스크 파일 포맷, 일별 로테이션
- [XLog Pipeline](docs/xlog-pipeline.md) — XLog 수신/처리/저장/조회 파이프라인, 링 버퍼 실시간 스트리밍, 3중 인덱스, 서비스 그룹 집계

## License

Apache-2.0
