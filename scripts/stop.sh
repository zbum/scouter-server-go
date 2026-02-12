#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Find *.scouter PID files (the server creates {PID}.scouter)
PID_FILES=("$SCRIPT_DIR"/*.scouter)

if [ ! -e "${PID_FILES[0]}" ]; then
    echo "No .scouter PID file found. Scouter Server may not be running."
    exit 1
fi

for PID_FILE in "${PID_FILES[@]}"; do
    PID=$(basename "$PID_FILE" .scouter)
    echo "Stopping Scouter Server (PID: $PID) by removing $PID_FILE ..."
    rm -f "$PID_FILE"
done

# Wait for the process to exit gracefully (up to 30 seconds)
for PID_FILE in "${PID_FILES[@]}"; do
    PID=$(basename "$PID_FILE" .scouter)
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
done

echo "Scouter Server stopped."
