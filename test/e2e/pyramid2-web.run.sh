#!/bin/bash
# Runs test/e2e/pyramid2-web.spec.ts on a pyramid2 box under an nc-build lease, with bench/pyramid2-swarm.py driving
# SWARMS (default 16) swarm slots in the background. Usage, from a copy of this repo in your own mktemp dir that also holds
# node_modules (playwright, ws) and browsers/ (PLAYWRIGHT_BROWSERS_PATH):
#   test/e2e/pyramid2-web.run.sh MINUTES PHASES TAG
# Env: CORES (default 4), SWARMS (default 16), LIVE (token, query, cookie or relay; default: auth picks), PYRAMID2_TOKEN_FILE (read by path only), WEB, NODES (comma-separated), PROBES, SOAK_MIN, BOX_CLOSE (epoch s:
# no take may run past it). Leaves out-TAG/result.json, screenshots, spec-TAG.log and swarm-TAG/.
set -u
here=$(cd "$(dirname "$0")/../.." && pwd)
work=$PWD
min=$1 phases=$2 tag=$3
nodes=${NODES:-http://127.0.0.1:18480,http://127.0.0.1:18500,http://127.0.0.1:18520}
if [ -n "${BOX_CLOSE:-}" ] && [ $(( $(date +%s) + min*60 )) -gt "$BOX_CLOSE" ]; then echo "take would run past the box close"; exit 1; fi
lease=$(nc-build slots take --cores "${CORES:-4}" --label "p2live-swarms-web-$tag" --kind build --machine "$(hostname)" --minutes "$min" --env) || { echo "take failed"; exit 1; }
eval "$lease"
echo "lease $NC_BUILD_LEASE ($NC_BUILD_PREFIX)"
trap 'kill $(cat "$work/swarm-$tag.pid") 2>/dev/null; nc-build slots give "$NC_BUILD_LEASE"; echo "gave $NC_BUILD_LEASE"' EXIT
end=$(( $(date +%s) + min*60 - 45 ))
mkdir -p "$work/swarm-$tag" "$work/out-$tag"
$NC_BUILD_PREFIX python3 "$here/bench/pyramid2-swarm.py" --nodes "$nodes" --swarms "${SWARMS:-16}" --children 4 --duration $(( min*60 - 90 )) \
  --tag "web$tag" --dump "$work/swarm-$tag" > "$work/swarm-$tag.log" 2>&1 &
echo $! > "$work/swarm-$tag.pid"
cp "$here/test/e2e/pyramid2-web.spec.ts" "$work/pyramid2-web.spec.mts"
export PLAYWRIGHT_BROWSERS_PATH="$work/browsers" DEADLINE_MS=$(( end*1000 )) PHASES="$phases" OUT="$work/out-$tag" NODES="$nodes"
$NC_BUILD_PREFIX node --experimental-strip-types --no-warnings "$work/pyramid2-web.spec.mts" > "$work/spec-$tag.log" 2>&1
echo "spec exit $?"
