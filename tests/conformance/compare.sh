#!/bin/bash
# Run a conformance probe against a real Confluent registry and this one, then diff.
# Both registries should start empty. Example:
#   tests/conformance/compare.sh http://localhost:8081 http://localhost:18081 tests/conformance/import_mode.sh
set -euo pipefail
diff <("$3" "$1") <("$3" "$2") && echo "identical responses"
