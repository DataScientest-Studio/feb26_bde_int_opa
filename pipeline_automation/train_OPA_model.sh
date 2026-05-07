#!/bin/bash

# =========================================================
# OPA ML Training Pipeline
#
# This script trains the Random Forest ML model using
# historical Binance market data.
#
# The pipeline:
# 1. Starts PostgreSQL
# 2. Loads historical data
# 3. Performs feature engineering
# 4. Trains the ML model
# 5. Saves trained artifacts to /models
#
# =========================================================

set -euo pipefail

exec >> /tmp/opa_training.log 2>&1

echo "================================================="
echo "OPA ML Training started: $(date)"
echo "================================================="

DOCKER="/usr/local/bin/docker"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Current directory: $(pwd)"

# ---------------------------------------------------------
# Cleanup old compose stack
# ---------------------------------------------------------

echo "Cleaning previous training containers..."

$DOCKER compose -f docker-compose.historical.yml down --remove-orphans

# Remove stopped containers
$DOCKER container prune -f

# ---------------------------------------------------------
# Start training pipeline
# ---------------------------------------------------------

echo "Starting ML training pipeline..."

set +e

$DOCKER compose \
    -f docker-compose.historical.yml \
    up \
    --build \
    --abort-on-container-exit \
    --exit-code-from ML-training \
    ML-training

TRAIN_EXIT_CODE=$?

set -e

# ---------------------------------------------------------
# Shutdown compose stack cleanly
# ---------------------------------------------------------

echo "Shutting down training stack..."

$DOCKER compose -f docker-compose.historical.yml down --remove-orphans

# ---------------------------------------------------------
# Final status
# ---------------------------------------------------------

if [ $TRAIN_EXIT_CODE -eq 0 ]; then
    echo "================================================="
    echo "OPA ML model trained successfully"
    echo "Training finished at: $(date)"
    echo "================================================="
else
    echo "================================================="
    echo "OPA ML training FAILED"
    echo "Exit code: $TRAIN_EXIT_CODE"
    echo "================================================="
    exit $TRAIN_EXIT_CODE
fi