#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$SCRIPT_DIR/scouter-server.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found. Scouter Server may not be running."
    exit 1
fi

PID=$(cat "$PID_FILE")
if kill -0 "$PID" 2>/dev/null; then
    echo "Stopping Scouter Server (PID: $PID)..."
    kill "$PID"
    for i in $(seq 1 30); do
        if ! kill -0 "$PID" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
        echo "Force killing Scouter Server (PID: $PID)..."
        kill -9 "$PID"
    fi
    rm -f "$PID_FILE"
    echo "Scouter Server stopped."
else
    echo "Process $PID is not running. Removing stale PID file."
    rm -f "$PID_FILE"
fi
