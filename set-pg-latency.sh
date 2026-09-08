#!/usr/bin/env bash
# Puts a toxiproxy shim in front of Postgres so we can add realistic datastore
# round-trip time.
#
# Why this matters: kine repairs a hole in the revision sequence with roughly
# three poll queries plus a Fill insert, and that advances the cursor by exactly
# one revision. So the repair rate is bound by round trips, not by throughput.
# On a local Docker Postgres (~0.2ms RTT) that runs at ~116 fills/sec, which
# drains a burst-sized backlog well inside one compaction interval and the poll
# loop recovers. Production talks to RDS across a VPC hop and repairs at ~27/sec,
# so the backlog outlives the compaction window, compaction deletes rows the
# poll loop has not read, and the repair path collapses further -- permanently.
#
# Usage:
#   ./set-pg-latency.sh 3     # 3ms added per round trip
#   ./set-pg-latency.sh 0     # remove added latency
#
# Then start k3s pointed at the proxy:
#   DSN_HOST=kine-proxy ./start-k3s.sh
set -euo pipefail
MS="${1:?usage: $0 <milliseconds>}"

if ! docker ps --filter name=kine-proxy --filter status=running -q | grep -q .; then
  docker rm -f kine-proxy >/dev/null 2>&1 || true
  docker run -d --name kine-proxy --network kinenet -p 8474:8474 \
    ghcr.io/shopify/toxiproxy:2.12.0 >/dev/null
  sleep 2
fi

# Recreate the proxy so the script is idempotent.
docker exec kine-proxy /toxiproxy-cli delete pg >/dev/null 2>&1 || true
docker exec kine-proxy /toxiproxy-cli create --listen 0.0.0.0:5432 --upstream kine-pg:5432 pg >/dev/null

if [ "$MS" != "0" ]; then
  # Applied in both directions, so a query round trip pays roughly 2 x MS.
  docker exec kine-proxy /toxiproxy-cli toxic add -t latency -a "latency=$MS" -a jitter=0 -d pg >/dev/null
  docker exec kine-proxy /toxiproxy-cli toxic add -t latency -a "latency=$MS" -a jitter=0 -u pg >/dev/null
fi

echo "kine-proxy: postgres latency set to ${MS}ms each way"
docker exec kine-proxy /toxiproxy-cli inspect pg
