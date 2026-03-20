@echo off
setlocal

:: ─── Настройки ──────────────────────────────────────────────────────────────
set REMOTE_HOST1=192.168.1.100
set REMOTE_USER1=root
set REMOTE_PASSWORD1=changeme

set REMOTE_HOST2=192.168.1.101
set REMOTE_USER2=root
set REMOTE_PASSWORD2=changeme

set REMOTE_DIR=/tmp
set IMAGE_FILE=migration-images.tar

:: Имена образов (берутся из docker-compose: <папка>-<сервис>)
set COORDINATOR_IMAGE=front-coordinator
set WORKER_IMAGE=front-worker

:: ─── 1. Сборка контейнеров ─────────────────────────────────────────────────
echo [1/4] Building Docker images...
docker compose build
if %ERRORLEVEL% neq 0 (
    echo ERROR: Docker build failed!
    exit /b 1
)
echo Build OK.

:: ─── 2. Сохранение образов в tar ───────────────────────────────────────────
echo [2/4] Saving images to %IMAGE_FILE%...
docker save -o %IMAGE_FILE% %COORDINATOR_IMAGE% %WORKER_IMAGE%
if %ERRORLEVEL% neq 0 (
    echo ERROR: docker save failed!
    exit /b 1
)
echo Save OK.

:: ─── 3. Деплой на сервер 1 ─────────────────────────────────────────────────
echo [3/4] Deploying to %REMOTE_HOST1%...
pscp -pw %REMOTE_PASSWORD1% %IMAGE_FILE% %REMOTE_USER1%@%REMOTE_HOST1%:%REMOTE_DIR%/%IMAGE_FILE%
if %ERRORLEVEL% neq 0 (
    echo ERROR: pscp to %REMOTE_HOST1% failed!
    exit /b 1
)
plink -pw %REMOTE_PASSWORD1% %REMOTE_USER1%@%REMOTE_HOST1% "docker load -i %REMOTE_DIR%/%IMAGE_FILE% && rm %REMOTE_DIR%/%IMAGE_FILE%"
if %ERRORLEVEL% neq 0 (
    echo ERROR: docker load on %REMOTE_HOST1% failed!
    exit /b 1
)
echo Server 1 OK.

:: ─── 4. Деплой на сервер 2 ─────────────────────────────────────────────────
echo [4/4] Deploying to %REMOTE_HOST2%...
pscp -pw %REMOTE_PASSWORD2% %IMAGE_FILE% %REMOTE_USER2%@%REMOTE_HOST2%:%REMOTE_DIR%/%IMAGE_FILE%
if %ERRORLEVEL% neq 0 (
    echo ERROR: pscp to %REMOTE_HOST2% failed!
    exit /b 1
)
plink -pw %REMOTE_PASSWORD2% %REMOTE_USER2%@%REMOTE_HOST2% "docker load -i %REMOTE_DIR%/%IMAGE_FILE% && rm %REMOTE_DIR%/%IMAGE_FILE%"
if %ERRORLEVEL% neq 0 (
    echo ERROR: docker load on %REMOTE_HOST2% failed!
    exit /b 1
)
echo Server 2 OK.

:: ─── Очистка локального tar ────────────────────────────────────────────────
del %IMAGE_FILE%

echo.
echo === Deploy complete (both servers) ===
endlocal
