# Scouter Server (Go)

Scouter APM의 백엔드 서버를 Go로 구현한 프로젝트입니다. 모니터링 대상 애플리케이션에서 실행되는 에이전트로부터 성능 메트릭, 트랜잭션 로그, 알림 등의 데이터를 수집하고 저장합니다.

## Features

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

## License

Apache-2.0
