@echo off
setlocal

REM Check if the script receives a parameter
IF "%1"=="" (
    echo Please provide a parameter: "stream-analysis" or "stream-profiling".
    exit /b 1
)

REM Set the container name using the first argument
set container_name=%1-graphs

REM Build the Docker container with the specified Dockerfile
docker build -t %container_name% -f DockerfileGraphs .

REM Create necessary directories if they don't exist
IF NOT EXIST "%cd%\%1\graphs" (
    mkdir "%cd%\%1\graphs"
)

REM Run the Docker container with the appropriate script based on the parameter
IF "%1"=="stream-analysis" (
    docker run -v /"%cd%\%1:/app/%1" %container_name% algorithm/src/main/bash/it/unibo/big/streamanalysis/graphs-script.sh
) ELSE (
    docker run -v /"%cd%\%1:/app/%1" %container_name% algorithm/src/main/bash/it/unibo/big/streamprofiling/graphs-script.sh
)

endlocal