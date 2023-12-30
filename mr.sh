#!/usr/bin/bash
set -euo pipefail

PLUGIN="${1:-wc}"
WORKERS="${2:-5}"

cd src/main
echo "Building ${PLUGIN}.go plugin..."
go build -buildmode=plugin ../mrapps/${PLUGIN}.go
echo "Running coordinator with $WORKERS workers..."
go run mrcoordinator.go pg-* & parallel -N0 --lb go run mrworker.go ${PLUGIN}.so ::: $(seq $WORKERS)

wait