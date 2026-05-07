#!/bin/bash


# This script is designed to stop the OPA pipeline.
# It stops and removes all containers defined in the docker-compose.live.yml file to ensure a clean shutdown of the pipeline services.
# It shall run daily at 17:00 UTC to stop the pipeline after the peak trading hours have ended, ensuring that resources are not consumed
# unnecessarily outside of active trading periods.

set -e

exec >> /tmp/opa_pipeline.log 2>&1

echo "========== $(date) =========="
echo "Stopping OPA pipeline"

DOCKER="/usr/local/bin/docker"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

$DOCKER compose -f docker-compose.live.yml down -v --timeout 30 --remove-orphans

# Remove stopped containers
$DOCKER container prune -f

echo "OPA pipeline stopped successfully"