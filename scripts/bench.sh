#!/bin/bash
# Compare the performance of two git refs with the masa-bench suite.
#
#   scripts/bench.sh <baseline-ref> [<candidate-ref>=HEAD] [options]
#
# Each ref is checked out in its own detached worktree, built in release mode
# with its own target directory, and benchmarked in interleaved rounds (round
# 1: baseline, candidate; round 2: candidate, baseline; ...) so that slow drift
# of the machine hits both sides alike. A ref that predates the benchmark crate
# gets a copy of this checkout's libs/masa-bench, so the same benchmark source
# runs on both sides; a ref that predates the rpcstack refactor also gets the
# `legacy-main` compat feature (see libs/masa-bench/README.md).
#
# Options:
#   --features SET   a comma-separated Masa feature set for the hook benchmarks;
#                    repeatable. Default: the five sets listed in README.md.
#   --queues LIST    comma-separated run queues for spawn_poll
#                    (fifo,prio,tailclipper,custom). Default: all four.
#   --rounds N       interleaved rounds (default 7)
#   --core C         CPU to pin to with taskset (default: the idlest core)
#   --workdir DIR    scratch directory (default /tmp/claude-$UID/masa-bench-$$
#                    if /tmp/claude-$UID exists, else /tmp/masa-bench-$$)
#   --out FILE       also write the comparison table to FILE
#   --keep           keep worktrees, target directories and raw results
#   -h, --help       this text

set -euo pipefail

MIN_FREE_GB=150
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON:-python3}"

DEFAULT_FEATURE_SETS=(
    "sched_slo"
    "sched_slo,abort_slo"
    "sched_slo,ac_rajomon"
    "sched_pred,abort_slack,ac_pred,est_mean_var"
    "sched_slo,trace_queue_latency"
)
FEATURE_SETS=()
QUEUES="fifo,prio,tailclipper,custom"
ROUNDS=7
CORE=""
WORKDIR=""
OUT_FILE=""
KEEP=false
POSITIONAL=()

usage() {
    sed -n '2,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

die() {
    echo "bench.sh: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --features) FEATURE_SETS+=("$2"); shift 2 ;;
        --queues) QUEUES="$2"; shift 2 ;;
        --rounds) ROUNDS="$2"; shift 2 ;;
        --core) CORE="$2"; shift 2 ;;
        --workdir) WORKDIR="$2"; shift 2 ;;
        --out) OUT_FILE="$2"; shift 2 ;;
        --keep) KEEP=true; shift ;;
        -h | --help) usage; exit 0 ;;
        -*) die "unknown option $1" ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
[[ ${#POSITIONAL[@]} -ge 1 && ${#POSITIONAL[@]} -le 2 ]] || { usage >&2; exit 1; }
BASE_REF="${POSITIONAL[0]}"
CAND_REF="${POSITIONAL[1]:-HEAD}"
[[ ${#FEATURE_SETS[@]} -gt 0 ]] || FEATURE_SETS=("${DEFAULT_FEATURE_SETS[@]}")
[[ "$ROUNDS" =~ ^[0-9]+$ && "$ROUNDS" -ge 1 ]] || die "--rounds must be a positive integer"

if [[ -z "$WORKDIR" ]]; then
    if [[ -d "/tmp/claude-$(id -u)" ]]; then
        WORKDIR="/tmp/claude-$(id -u)/masa-bench-$$"
    else
        WORKDIR="/tmp/masa-bench-$$"
    fi
fi

# The suite builds two full workspaces; refuse to start on a nearly full disk.
mkdir -p "$WORKDIR"
free_gb="$(df -BG --output=avail "$WORKDIR" | tail -1 | tr -dc '0-9')"
if [[ "$free_gb" -lt "$MIN_FREE_GB" ]]; then
    rmdir "$WORKDIR" 2>/dev/null || true
    die "only ${free_gb} GB free under $WORKDIR; need at least ${MIN_FREE_GB} GB"
fi

cat >"$WORKDIR/collect_executables.py" <<'PYEOF'
"""Copy the bench executables named in cargo's JSON messages (stdin) to DEST."""
import json
import shutil
import sys

dest, variant = sys.argv[1], sys.argv[2]
for line in sys.stdin:
    try:
        message = json.loads(line)
    except ValueError:
        continue
    if message.get("reason") != "compiler-artifact" or not message.get("executable"):
        continue
    if "bench" in message["target"]["kind"]:
        name = message["target"]["name"]
        shutil.copy(message["executable"], f"{dest}/{name}.{variant}")
PYEOF

RESULTS="$WORKDIR/results"
BINS="$WORKDIR/bins"
mkdir -p "$RESULTS" "$BINS/base" "$BINS/cand"

cleanup() {
    if [[ "$KEEP" = true ]]; then
        echo "kept $WORKDIR (worktrees, targets, raw results in $RESULTS)" >&2
        return
    fi
    for side in base cand; do
        if [[ -d "$WORKDIR/$side" ]]; then
            git -C "$REPO_ROOT" worktree remove --force "$WORKDIR/$side" 2>/dev/null || true
        fi
    done
    git -C "$REPO_ROOT" worktree prune 2>/dev/null || true
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

# Rewrite a worktree's workspace so it contains the benchmark crate and, for a
# ref that predates parts of the stack, only the parts it can build. Prints the
# extra Cargo features the compat layer needs, comma-separated.
prepare_worktree() {
    local dir="$1"
    if [[ ! -d "$dir/libs/masa-bench" ]]; then
        cp -r "$REPO_ROOT/libs/masa-bench" "$dir/libs/masa-bench"
    fi
    if ! grep -q '"libs/masa-bench"' "$dir/Cargo.toml"; then
        sed -i 's#^members = \[#members = [\n  "libs/masa-bench",#' "$dir/Cargo.toml"
    fi
    if [[ ! -d "$dir/libs/rpcstack-sched" ]]; then
        sed -i '/^# BEGIN rpcstack-only$/,/^# END rpcstack-only$/d' "$dir/libs/masa-bench/Cargo.toml"
    fi
    if grep -q 'pub struct RootContext' "$dir/libs/masa/src/lib.rs"; then
        echo ""
    else
        echo "legacy-main"
    fi
}

# Build every benchmark binary of one side and copy it to $BINS/<side>/.
# Binaries are named <bench>.<variant>, where the variant is the feature set
# (commas replaced by +) or the queue.
build_side() {
    local side="$1" compat="$2"
    local dir="$WORKDIR/$side"
    local target="$WORKDIR/target-$side"
    local log="$WORKDIR/build-$side.log"
    local has_sched=true
    [[ -d "$dir/libs/rpcstack-sched" ]] || has_sched=false

    build() { # build <variant> <features> <bench>...
        local variant="$1" features="$2"
        shift 2
        local benches=()
        local bench
        for bench in "$@"; do benches+=(--bench "$bench"); done
        local json
        json="$(cd "$dir" && CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$target" \
            cargo build --release -j 12 -p masa-bench "${benches[@]}" \
            --features "$features" --message-format=json-render-diagnostics 2>>"$log")" \
            || { tail -30 "$log" >&2; die "build failed on $side ($variant); log: $log"; }
        "$PYTHON_BIN" "$WORKDIR/collect_executables.py" "$BINS/$side" "$variant" <<<"$json"
    }

    local set
    for set in "${FEATURE_SETS[@]}"; do
        build "${set//,/+}" "${compat:+$compat,}$set" hook_cost priority_read wire_codec
    done
    local queue first=true
    for queue in ${QUEUES//,/ }; do
        local qfeatures=""
        case "$queue" in
            fifo) qfeatures="" ;;
            prio) qfeatures="queue_prio" ;;
            tailclipper) qfeatures="queue_tailclipper" ;;
            custom)
                [[ "$has_sched" = true ]] || continue
                qfeatures="queue_custom" ;;
            *) die "unknown queue $queue" ;;
        esac
        local benches=(spawn_poll)
        if [[ "$first" = true && "$has_sched" = true ]]; then
            benches+=(run_queue)
            first=false
        fi
        local joined="$compat"
        [[ -n "$qfeatures" ]] && joined="${joined:+$joined,}$qfeatures"
        build "$queue" "$joined" "${benches[@]}"
    done
}

echo "baseline $BASE_REF, candidate $CAND_REF, workdir $WORKDIR" >&2
git -C "$REPO_ROOT" rev-parse --verify --quiet "$BASE_REF^{commit}" >/dev/null || die "unknown ref $BASE_REF"
git -C "$REPO_ROOT" rev-parse --verify --quiet "$CAND_REF^{commit}" >/dev/null || die "unknown ref $CAND_REF"
BASE_SHA="$(git -C "$REPO_ROOT" rev-parse --short=9 "$BASE_REF")"
CAND_SHA="$(git -C "$REPO_ROOT" rev-parse --short=9 "$CAND_REF")"

COMPAT_BASE="" COMPAT_CAND=""
git -C "$REPO_ROOT" worktree add --detach --quiet "$WORKDIR/base" "$BASE_REF"
git -C "$REPO_ROOT" worktree add --detach --quiet "$WORKDIR/cand" "$CAND_REF"
COMPAT_BASE="$(prepare_worktree "$WORKDIR/base")"
COMPAT_CAND="$(prepare_worktree "$WORKDIR/cand")"
echo "compat: baseline '${COMPAT_BASE:-current}', candidate '${COMPAT_CAND:-current}'" >&2

echo "building both sides (logs in $WORKDIR/build-*.log)..." >&2
build_side base "$COMPAT_BASE" &
base_pid=$!
build_side cand "$COMPAT_CAND" &
cand_pid=$!
wait "$base_pid" || die "baseline build failed"
wait "$cand_pid" || die "candidate build failed"

# Pin to one CPU so the scheduler does not move the benchmark between cores.
PIN=()
if command -v taskset >/dev/null 2>&1; then
    if [[ -z "$CORE" ]]; then
        CORE="$("$PYTHON_BIN" - <<'EOF'
import time

def busy():
    cores = {}
    with open("/proc/stat") as stat:
        for line in stat:
            fields = line.split()
            if fields[0].startswith("cpu") and fields[0] != "cpu":
                values = list(map(int, fields[1:]))
                cores[int(fields[0][3:])] = (sum(values), values[3] + values[4])
    return cores

before = busy()
time.sleep(1.0)
after = busy()
load = {
    core: 1 - (after[core][1] - before[core][1]) / max(1, after[core][0] - before[core][0])
    for core in before
}
print(min(load, key=load.get))
EOF
)"
    fi
    PIN=(taskset -c "$CORE")
    echo "pinned to core $CORE" >&2
else
    echo "taskset not found; running unpinned" >&2
fi

PERF=()
if command -v perf >/dev/null 2>&1 && perf stat -e cycles,instructions true >/dev/null 2>&1; then
    PERF=(perf stat -x, -e cycles,instructions)
else
    echo "perf unavailable; skipping cycle and instruction counts" >&2
fi

# Arguments that shorten the benchmarks that would otherwise dominate the run.
args_for() {
    case "$1" in
        wire_codec.*) echo "100000" ;;
        run_queue.*) echo "1000000" ;;
        *) echo "" ;;
    esac
}

JOBS=()
for file in "$BINS/base"/*; do
    JOBS+=("$(basename "$file")")
done
for file in "$BINS/cand"/*; do
    name="$(basename "$file")"
    [[ " ${JOBS[*]} " == *" $name "* ]] || JOBS+=("$name")
done

run_job() { # run_job <side> <round> <job>
    local side="$1" round="$2" job="$3"
    local bin="$BINS/$side/$job"
    [[ -x "$bin" ]] || return 0
    local stem="$RESULTS/$side.$round.${job//\//_}"
    local perf_file="$stem.perf"
    local args
    args="$(args_for "$job")"
    if [[ ${#PERF[@]} -gt 0 ]]; then
        # shellcheck disable=SC2086
        "${PIN[@]}" "${PERF[@]}" -o "$perf_file" "$bin" $args >"$stem.txt" 2>"$stem.log" || true
        awk -F, -v job="$job" '
            $3 ~ /^cycles/ { printf "perf.%s.cycles_G %.4f\n", job, $1 / 1e9 }
            $3 ~ /^instructions/ { printf "perf.%s.instructions_G %.4f\n", job, $1 / 1e9 }
        ' "$perf_file" >>"$stem.txt" || true
        rm -f "$perf_file"
    else
        # shellcheck disable=SC2086
        "${PIN[@]}" "$bin" $args >"$stem.txt" 2>"$stem.log" || true
    fi
}

for ((round = 1; round <= ROUNDS; round++)); do
    if ((round % 2 == 1)); then order=(base cand); else order=(cand base); fi
    for job in "${JOBS[@]}"; do
        for side in "${order[@]}"; do
            run_job "$side" "$round" "$job"
        done
    done
    echo "round $round/$ROUNDS done" >&2
done

echo
echo "baseline  $BASE_REF ($BASE_SHA)${COMPAT_BASE:+ [compat $COMPAT_BASE]}"
echo "candidate $CAND_REF ($CAND_SHA)${COMPAT_CAND:+ [compat $COMPAT_CAND]}"
echo "rounds $ROUNDS, core ${CORE:-unpinned}, minimum over rounds; lower is better"
echo
if [[ -n "$OUT_FILE" ]]; then
    "$PYTHON_BIN" "$SCRIPT_DIR/bench_compare.py" "$RESULTS" | tee "$OUT_FILE"
else
    "$PYTHON_BIN" "$SCRIPT_DIR/bench_compare.py" "$RESULTS"
fi
