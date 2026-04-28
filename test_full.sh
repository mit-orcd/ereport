#!/usr/bin/env bash
#
# One-shot: build ./test with test_setup.sh, then run test.sh on it (ecrawl + ereport correlation).
#
# Usage:
#   ./test_full.sh
#   DEPTH=8 ./test_full.sh
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

"$SCRIPT_DIR/test_setup.sh"
exec "$SCRIPT_DIR/test.sh" "$SCRIPT_DIR/test"
