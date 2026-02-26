#!/usr/bin/env bash
set -eo pipefail

# ─── stream_select A/B Test ───
# Interleaved: sequential pipe drain vs stream_select concurrent drain

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
STATE_FILE="$PROJECT_DIR/.benchmark-server"
ENV_FILE="$PROJECT_DIR/.env"

RUNS="${RUNS:-7}"
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/id_rsa}"
SSH_USER="${SSH_USER:-m1}"
ZONE="${ZONE:-fr-par-3}"

set -a
source "$ENV_FILE"
set +a

export SCW_ACCESS_KEY="${SCALEWAY_ACCESS_KEY_ID:?}"
export SCW_SECRET_KEY="${SCALEWAY_SECRET_KEY:?}"

SERVER_ID=$(cat "$STATE_FILE")

PROJECT_ID=$(curl -sf -H "X-Auth-Token: $SCW_SECRET_KEY" \
    "https://api.scaleway.com/iam/v1alpha1/api-keys/$SCW_ACCESS_KEY" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['default_project_id'])")
ORG_ID=$(curl -sf -H "X-Auth-Token: $SCW_SECRET_KEY" \
    "https://api.scaleway.com/account/v3/projects/$PROJECT_ID" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['organization_id'])")
export SCW_DEFAULT_PROJECT_ID="$PROJECT_ID"
export SCW_DEFAULT_ORGANIZATION_ID="$ORG_ID"

IP=$(scw apple-silicon server get "$SERVER_ID" zone="$ZONE" -o json \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['ip'])")

log() { echo "$(date +%H:%M:%S) │ $*" >&2; }

RSYNC_EXCLUDES=(
    --exclude='.git'
    --exclude='data/data.csv'
    --exclude='data/real-data*'
    --exclude='vendor/'
    --exclude='.env'
    --exclude='.idea'
    --exclude='.tempest'
    --exclude='poc/'
    --exclude='.multiagent-chat/'
    --exclude='.benchmark-server'
)

remote_ssh() {
    local ip="$1"; shift
    ssh -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -i "$SSH_KEY" "$SSH_USER@$ip" \
        "eval \"\$(/opt/homebrew/bin/brew shellenv 2>/dev/null)\"; $*"
}

BENCH_CMD="php -d opcache.enable_cli=1 -d opcache.jit=0 tempest data:parse data/data.csv /tmp/bench-out.json"

run_single() {
    local parser_file="$1"

    rsync -az --delete "${RSYNC_EXCLUDES[@]}" \
        -e "ssh -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -i $SSH_KEY" \
        "$PROJECT_DIR/" "$SSH_USER@$IP:~/benchmark/" >/dev/null 2>&1

    scp -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -i "$SSH_KEY" \
        "$parser_file" "$SSH_USER@$IP:~/benchmark/app/Parser.php" >/dev/null 2>&1

    remote_ssh "$IP" "cd ~/benchmark && hyperfine --warmup 0 --runs 1 --export-json /tmp/bench-result.json '$BENCH_CMD'" >/dev/null 2>&1
    remote_ssh "$IP" "cat /tmp/bench-result.json" | python3 -c "import json,sys; print(f\"{json.load(sys.stdin)['results'][0]['times'][0]:.4f}\")"
}

# Deploy and install deps
log "Initial deploy to $IP..."
rsync -az --delete "${RSYNC_EXCLUDES[@]}" \
    -e "ssh -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -i $SSH_KEY" \
    "$PROJECT_DIR/" "$SSH_USER@$IP:~/benchmark/"
remote_ssh "$IP" "cd ~/benchmark && composer install --no-dev --quiet"

# Warmup
log "Warmup run..."
remote_ssh "$IP" "cd ~/benchmark && $BENCH_CMD" >/dev/null 2>&1

SEQUENTIAL="$PROJECT_DIR/app/Parser.php"
STREAM_SELECT="$PROJECT_DIR/poc/parser-stream-select.php"

TMPDIR_RESULTS=$(mktemp -d)
> "$TMPDIR_RESULTS/sequential.txt"
> "$TMPDIR_RESULTS/stream-select.txt"

log "Starting interleaved stream_select test ($RUNS rounds × 2 variants)..."
log ""

for ((round=1; round<=RUNS; round++)); do
    log "─── Round $round/$RUNS ───"

    t=$(run_single "$SEQUENTIAL")
    echo "$t" >> "$TMPDIR_RESULTS/sequential.txt"
    log "  sequential:     ${t}s"

    t=$(run_single "$STREAM_SELECT")
    echo "$t" >> "$TMPDIR_RESULTS/stream-select.txt"
    log "  stream_select:  ${t}s"
done

log ""
log "════════════════════════════════════════════════════════════════"
log "  STREAM_SELECT RESULTS (interleaved, $RUNS rounds)"
log "════════════════════════════════════════════════════════════════"

python3 -c "
import statistics

variants = [
    ('sequential (current)', '$TMPDIR_RESULTS/sequential.txt'),
    ('stream_select',        '$TMPDIR_RESULTS/stream-select.txt'),
]

fmt = '  {:<22s} {:>8s} {:>8s} {:>8s} {:>8s} {:>8s} {:>8s}'
print(fmt.format('Variant', 'Median', 'Mean', 'StdDev', 'Min', 'Max', 'Δmed%'))
print(fmt.format('-------', '------', '----', '------', '---', '---', '-----'))

baseline_median = None
for name, path in variants:
    with open(path) as f:
        times = [float(line.strip()) for line in f if line.strip()]
    median = statistics.median(times)
    mean = statistics.mean(times)
    stddev = statistics.stdev(times) if len(times) > 1 else 0
    mn, mx = min(times), max(times)

    if baseline_median is None:
        baseline_median = median
        delta = '---'
    else:
        delta = f'{((median - baseline_median) / baseline_median) * 100:+.1f}%'

    print(fmt.format(name, f'{median:.4f}', f'{mean:.4f}', f'{stddev:.4f}', f'{mn:.4f}', f'{mx:.4f}', delta))
    print(f'  [{name} times: {\" \".join(f\"{t:.4f}\" for t in times)}]')
" >&2

log "════════════════════════════════════════════════════════════════"

rm -rf "$TMPDIR_RESULTS"
