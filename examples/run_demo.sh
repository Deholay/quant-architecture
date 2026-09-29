#!/usr/bin/env bash
set -euo pipefail

# Run from the project root, or pass an absolute path to ipc_demo.
demo=${1:-./build/ipc_demo}
name="/quant.demo.$$"
consumer_pid=""
created=0

cleanup() {
    if [[ -n "$consumer_pid" ]]; then
        kill "$consumer_pid" 2>/dev/null || true
        wait "$consumer_pid" 2>/dev/null || true
    fi
    if [[ "$created" == 1 ]]; then
        "$demo" remove "$name"
    fi
}
trap cleanup EXIT

"$demo" create "$name" 1
created=1
"$demo" receive "$name" 1 &
consumer_pid=$!
"$demo" send "$name" 1
wait "$consumer_pid"
consumer_pid=""
