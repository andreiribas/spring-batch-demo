#!/usr/bin/env bash
# Demonstrates Spring Batch restart-via-job-parameters using the /jobs/import endpoint.
# Prereq: the app must already be running (mvn spring-boot:run) on localhost:8080.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"

call() {
    local description="$1"
    local url="$2"
    local method="${3:-GET}"
    echo "=== ${description} ==="
    curl -s -X "${method}" "${url}" -w '\nHTTP %{http_code}\n'
    echo
}

call "1) Executions before we start" "${BASE_URL}/jobs/debug"

call "2) Trigger the job with forceFailure=true -> job FAILS in helloStep" \
    "${BASE_URL}/jobs/import?forceFailure=true" POST

call "3) Executions after the failure" "${BASE_URL}/jobs/debug"

call "4) Restart with forceFailure=false (same identifying params) -> job COMPLETES" \
    "${BASE_URL}/jobs/import?forceFailure=false" POST

call "5) Executions after the restart" "${BASE_URL}/jobs/debug"

call "6) Rerun the same (now completed) JobInstance -> expected to FAIL (HTTP 409)" \
    "${BASE_URL}/jobs/import?forceFailure=false" POST

call "7) Executions after the rerun attempt (should be unchanged from step 5)" "${BASE_URL}/jobs/debug"
