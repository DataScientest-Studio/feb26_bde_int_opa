#!/bin/bash

# Best Time to Trade: Peak activity and liquidity typically occur during the US-Europe overlap, between 13:00 and 17:00 UTC.
# This script is designed to start the OPA pipeline, which includes multiple services defined in the docker-compose.live.yml file.
# This scripts starts daily at 13:00 UTC to ensure that the pipeline is active during the most volatile and liquid trading hours

set -e

exec >> /tmp/opa_pipeline.log 2>&1

echo "========== $(date) =========="
echo "Starting OPA pipeline"

DOCKER="/usr/local/bin/docker"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Current directory: $(pwd)"

$DOCKER compose -f docker-compose.live.yml down -v
$DOCKER compose -f docker-compose.live.yml up -d

echo "OPA pipeline started successfully"

