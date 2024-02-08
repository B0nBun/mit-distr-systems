#!/usr/bin/bash
set -euo pipefail

PATTERN="TestFailAgree2B"
TIMEOUT=20
ITERS=400

# For some reason certain tests sometimes fail or timeout,
# but only when $PATTERN is set to "2B" and not to something specific like "FailNoAgree".
# So I guess there is some sort of bug in the tests themselves

mkdir -p logs
rm -rf logs/*.log
parallel --timeout $TIMEOUT --lb "go test -run '$PATTERN' > logs/{}.log; echo {}" ::: $(seq $ITERS)

# Grepping failed files: `grep -- "--- FAIL" logs/*.log`
# Last Timeouts: 10 51 56 74 98
