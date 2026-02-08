@echo off
setlocal

set SCRIPT_DIR=%~dp0
set BINARY=%SCRIPT_DIR%scouter-server.exe
set SCOUTER_CONF=%SCRIPT_DIR%conf\scouter.conf

if not exist "%SCRIPT_DIR%logs" mkdir "%SCRIPT_DIR%logs"

echo Starting Scouter Server...
start /b "" "%BINARY%" > "%SCRIPT_DIR%logs\scouter-server.out" 2>&1
echo Scouter Server started.
