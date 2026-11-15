#!/usr/bin/env bash
# Starts what the tests need and waits until it answers: CI runs this before the tests
# (the shared workflow looks for it), and so can you. Oracle Free takes a minute or two to
# start the first time.
set -euo pipefail
cd "$(dirname "$0")/.."
testing/gen-keys.sh
docker compose -f testing/compose.yaml up -d --wait --wait-timeout 900
