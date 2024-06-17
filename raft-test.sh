#!/usr/bin/bash
set -euo pipefail

# export RAFT_LOGS="true"
PATTERN="2A|2B|2C"
TIMEOUT=300
ITERS=50

cd src/raft
mkdir -p logs
rm -rf logs/*.log
parallel --timeout $TIMEOUT --lb "go test -run '$PATTERN' > logs/{}.log; echo {}" ::: $(seq $ITERS)

# Grepping failed files: `grep "FAIL" src/raft/logs/*.log`
