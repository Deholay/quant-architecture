#!/usr/bin/env bash
# Portable to Linux bash 4+/5 and the macOS system bash 3.2 (BSD date/mktemp).
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
# BSD mktemp only replaces trailing X's: reserve a unique stem, then add .log.
log_stem=$(mktemp "$log_dir/replay-$(date +%Y%m%d-%H%M%S)-XXXXXX")
log_file="$log_stem.log"
mv -- "$log_stem" "$log_file"

timestamp() { date +%Y-%m-%dT%H:%M:%S%z; }

# Runs in a pipeline subshell; its combined output is copied to the log by tee.
# State is global (not local) so the EXIT trap still sees it after return.
replay() {
    name="/quant.replay.$$.$RANDOM"
    created=0 pids=() status=0
    cleanup() {
        status=$?
        trap - EXIT
        # ${pids[@]+...}: bash 3.2 with set -u rejects expanding an empty array.
        for pid in ${pids[@]+"${pids[@]}"}; do kill "$pid" 2>/dev/null || true; done
        for pid in ${pids[@]+"${pids[@]}"}; do wait "$pid" 2>/dev/null || true; done
        if (( created )); then
            if ! "$binary" remove "$name"; then
                if (( status == 0 )); then status=1; fi
            fi
        fi
        printf 'finished=%s exit_status=%s\n' "$(timestamp)" "$status"
        exit "$status"
    }
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    printf 'log_file=%s\n' "$log_file"
    printf 'started=%s\ninput=%s\nbinary=%s\nstall_timeout_ms=%s consumer_delay_us=%s\n' \
        "$(timestamp)" "$input" "$binary" "$timeout_ms" "$delay_us"
    printf 'consumer_binary=%s\n' "$consumer_binary"

    "$binary" create "$name" 1
    created=1
    "$consumer_binary" receive "$name" 1 "$timeout_ms" "$delay_us" &
    pids+=("$!")
    "$binary" send "$name" 1 "$input" "$timeout_ms" &
    pids+=("$!")
    # React to either endpoint failing, then stop and reap the peer. Bash reaps
    # exited children itself, so kill -0 fails once an endpoint has finished.
    while (( ${#pids[@]} )); do
        remaining=()
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                remaining+=("$pid")
                continue
            fi
            status=0
            wait "$pid" || status=$?
            if (( status )); then exit "$status"; fi # cleanup stops the peer
        done
        pids=(${remaining[@]+"${remaining[@]}"})
        if (( ${#pids[@]} )); then sleep 0.01; fi
    done
    exit 0 # explicit: bash 3.2 skips the EXIT trap when a pipeline subshell just returns
}

# tee -i: on Ctrl-C keep logging while the replay subshell cleans up.
set +e
replay 2>&1 | tee -i -- "$log_file"
statuses=("${PIPESTATUS[@]}")
set -e
status=${statuses[0]}
if (( statuses[1] != 0 )); then
    printf 'Failed to save replay log: %s\n' "$log_file" >&2
    if (( status == 0 )); then status=${statuses[1]}; fi
fi
exit "$status"
