#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="$SCRIPT_DIR/scouter-server"
CONF="$SCRIPT_DIR/conf/scouter.conf"
LOG_DIR="$SCRIPT_DIR/logs"
PID_FILE="$SCRIPT_DIR/scouter-server.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Scouter Server is already running (PID: $PID)"
        exit 1
    fi
    rm -f "$PID_FILE"
fi

mkdir -p "$LOG_DIR"

export SCOUTER_CONF="$CONF"

nohup "$BINARY" > "$LOG_DIR/scouter-server.out" 2>&1 &
echo $! > "$PID_FILE"
echo "Scouter Server started (PID: $!)"
