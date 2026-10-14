#!/usr/bin/env bash
set -euo pipefail

if (( $# < 1 || $# > 4 )); then
    echo "Usage: bash examples/run_tick_replay.sh ticks.bin [binary] [stall_timeout_ms] [consumer_delay_us]" >&2
    exit 2
fi
input=$1
binary=${2:-./build-wsl/tick_replay}
timeout_ms=${3:-10000}
delay_us=${4:-0}
name="/quant.replay.$$.$RANDOM"
created=0
pids=()
cleanup() {
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    if (( created )); then "$binary" remove "$name"; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$binary" create "$name" 1
created=1
"$binary" receive "$name" 1 "$timeout_ms" "$delay_us" &
pids+=("$!")
"$binary" send "$name" 1 "$input" "$timeout_ms" &
pids+=("$!")
# Bash 5.1+: react to either endpoint failing, then stop and reap the peer.
while (( ${#pids[@]} )); do
    completed=""
    status=0
    if (( ${#pids[@]} == 1 )); then
        completed=${pids[0]}
        wait "$completed" || status=$?
    else
        wait -n -p completed "${pids[@]}" || status=$?
    fi
    remaining=()
    for pid in "${pids[@]}"; do
        if [[ "$pid" != "$completed" ]]; then remaining+=("$pid"); fi
    done
    pids=("${remaining[@]}")
    if (( status )); then exit "$status"; fi
done
