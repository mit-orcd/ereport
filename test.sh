#!/usr/bin/env bash
# Moved to scripts/test/test.sh; this shim keeps ./test.sh working.
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/scripts/test/test.sh" "$@"
