@echo off
setlocal

set "ROOT=%~dp0"
set "BACKEND=%ROOT%backend"
set "FRONTEND=%ROOT%frontend"

if "%TIMETABLE_MAX_GENERATION_MS%"=="" set "TIMETABLE_MAX_GENERATION_MS=180000"

if not exist "%BACKEND%\mvnw.cmd" (
	echo [ERROR] backend\mvnw.cmd not found.
	exit /b 1
)

if not exist "%FRONTEND%\package.json" (
	echo [ERROR] frontend\package.json not found.
	exit /b 1
)

where npm >nul 2>nul
if errorlevel 1 (
	echo [ERROR] npm is not installed or not in PATH.
	exit /b 1
)

echo Starting backend in a new terminal with TIMETABLE_MAX_GENERATION_MS=%TIMETABLE_MAX_GENERATION_MS%...
start "Backend - Spring Boot" cmd /k "set TIMETABLE_MAX_GENERATION_MS=%TIMETABLE_MAX_GENERATION_MS% && cd /d ""%BACKEND%"" && mvnw.cmd spring-boot:run"

echo Starting frontend in a new terminal...
start "Frontend - Vite" cmd /k "cd /d ""%FRONTEND%"" && npm install && npm run dev"

echo.
echo Services are starting in separate terminals.
echo Backend: http://localhost:8081
echo Frontend: http://localhost:5173

endlocal

