@echo off
setlocal

set "REMOTE_HOST1=10.200.103.100"
set "REMOTE_HOST2=10.200.103.101"
set "REMOTE_USER=a.gavrilov_yun"
set "REMOTE_DIR=/tmp"

set "SSH_KEY=D:\ssh_servers\keys\a.gavrilov_yun-ed25519"

if not "%~1"=="" set "SSH_KEY=%~1"

echo [check] Validating deploy prerequisites...

where docker >nul 2>nul
if errorlevel 1 (
    echo ERROR: docker is not available in PATH.
    exit /b 1
)

where scp >nul 2>nul
if errorlevel 1 (
    echo ERROR: scp is not available in PATH.
    exit /b 1
)

if not exist "%SSH_KEY%" (
    echo ERROR: SSH key was not found at "%SSH_KEY%".
    echo Usage: deploy-images.bat C:\path\to\openssh-private-key
    exit /b 1
)

if not exist ".\Dockerfile.worker" (
    echo ERROR: Dockerfile.worker was not found.
    exit /b 1
)

if not exist ".\Dockerfile.coordinator" (
    echo ERROR: Dockerfile.coordinator was not found.
    exit /b 1
)

echo Checks OK.

echo [1/8] Building Worker image...
docker build -f .\Dockerfile.worker -t m-worker:latest .
if errorlevel 1 (
    echo ERROR: Worker Docker build failed.
    exit /b 1
)
echo Build OK.

echo [2/8] Building Coordinator image...
docker build -f .\Dockerfile.coordinator -t m-coordinator:latest .
if errorlevel 1 (
    echo ERROR: Coordinator Docker build failed.
    exit /b 1
)
echo Build OK.

echo [3/8] Saving image to m-coordinator...
docker save m-coordinator:latest -o m-coordinator
if errorlevel 1 (
    echo ERROR: docker save for m-coordinator failed.
    exit /b 1
)
echo Save OK.

echo [4/8] Saving image to m-worker...
docker save m-worker:latest -o m-worker
if errorlevel 1 (
    echo ERROR: docker save for m-worker failed.
    exit /b 1
)
echo Save OK.

echo [5/8] Deploying m-coordinator to %REMOTE_HOST1%...
scp -i "%SSH_KEY%" -o BatchMode=yes -o IdentitiesOnly=yes m-coordinator %REMOTE_USER%@%REMOTE_HOST1%:%REMOTE_DIR%/m-coordinator
if errorlevel 1 (
    echo ERROR: scp m-coordinator to %REMOTE_HOST1% failed.
    exit /b 1
)

echo [6/8] Deploying m-worker to %REMOTE_HOST1%...
scp -i "%SSH_KEY%" -o BatchMode=yes -o IdentitiesOnly=yes m-worker %REMOTE_USER%@%REMOTE_HOST1%:%REMOTE_DIR%/m-worker
if errorlevel 1 (
    echo ERROR: scp m-worker to %REMOTE_HOST1% failed.
    exit /b 1
)

echo [7/8] Deploying m-coordinator to %REMOTE_HOST2%...
scp -i "%SSH_KEY%" -o BatchMode=yes -o IdentitiesOnly=yes m-coordinator %REMOTE_USER%@%REMOTE_HOST2%:%REMOTE_DIR%/m-coordinator
if errorlevel 1 (
    echo ERROR: scp m-coordinator to %REMOTE_HOST2% failed.
    exit /b 1
)

echo [8/8] Deploying m-worker to %REMOTE_HOST2%...
scp -i "%SSH_KEY%" -o BatchMode=yes -o IdentitiesOnly=yes m-worker %REMOTE_USER%@%REMOTE_HOST2%:%REMOTE_DIR%/m-worker
if errorlevel 1 (
    echo ERROR: scp m-worker to %REMOTE_HOST2% failed.
    exit /b 1
)

echo Deploy OK.
