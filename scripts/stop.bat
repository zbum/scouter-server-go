@echo off
setlocal

echo Stopping Scouter Server...
taskkill /im scouter-server.exe /f
echo Scouter Server stopped.
