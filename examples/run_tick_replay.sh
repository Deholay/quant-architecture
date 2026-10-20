#!/usr/bin/env bash
set -euo pipefail

if (( $# < 1 || $# > 5 )); then
    echo "Usage: bash examples/run_tick_replay.sh ticks.bin [binary] [stall_timeout_ms] [consumer_delay_us] [consumer_binary]" >&2
    exit 2
fi
input=$1
binary=${2:-./build-wsl/tick_replay}
timeout_ms=${3:-10000}
delay_us=${4:-0}
consumer_binary=${5:-$binary}
log_dir=${REPLAY_LOG_DIR:-"$(dirname -- "$binary")/logs"}
mkdir -p -- "$log_dir"
log_file=$(mktemp "$log_dir/replay-$(date +%Y%m%d-%H%M%S)-XXXXXX.log")

# Keep signal handling in the runner; wait for tee after closing its input.
exec 3>&1 4>&2
exec > >(tee -i -- "$log_file") 2>&1
log_pid=$!
name="/quant.replay.$$.$RANDOM"
created=0
pids=()
cleanup() {
    status=$?
    trap - EXIT
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    if (( created )); then
        if ! "$binary" remove "$name"; then
            if (( status == 0 )); then status=1; fi
        fi
    fi
    printf 'finished=%s exit_status=%s\n' "$(date --iso-8601=seconds)" "$status"
    exec 1>&3 2>&4 3>&- 4>&-
    log_status=0
    wait "$log_pid" || log_status=$?
    if (( log_status != 0 )); then
        printf 'Failed to save replay log: %s\n' "$log_file" >&2
        if (( status == 0 )); then status=$log_status; fi
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf 'log_file=%s\n' "$log_file"
printf 'started=%s\ninput=%s\nbinary=%s\nstall_timeout_ms=%s consumer_delay_us=%s\n' \
    "$(date --iso-8601=seconds)" "$input" "$binary" "$timeout_ms" "$delay_us"
printf 'consumer_binary=%s\n' "$consumer_binary"

"$binary" create "$name" 1
created=1
"$consumer_binary" receive "$name" 1 "$timeout_ms" "$delay_us" &
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
