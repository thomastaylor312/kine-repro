#!/usr/bin/env bash
# Starts two k3s v1.33.13+k3s1 servers sharing one Postgres datastore, mirroring my production
# setup. Compaction interval is left at the kube-apiserver default of 5m, matching production --
# the stall does not need an abnormally short interval.
set -euo pipefail
# Point at kine-proxy instead of kine-pg to run through the toxiproxy latency
# shim (see set-pg-latency.sh). Production talks to RDS across a VPC hop, and
# that round-trip time is what throttles the poll loop's gap-repair path.
DSN_HOST="${DSN_HOST:-kine-pg}"
DSN="postgres://kine:kine@${DSN_HOST}:5432/k3sdb?sslmode=disable"
for n in a b; do
  port=$([ "$n" = a ] && echo 6444 || echo 6445)
  docker rm -f "k3s-$n" >/dev/null 2>&1 || true
  docker run -d --name "k3s-$n" --privileged --network kinenet \
    -e K3S_TOKEN=reprotoken -p "$port:6443" \
    -e KUBE_WATCHCACHE_CONSISTENCY_CHECKER=true \
    rancher/k3s:v1.33.13-k3s1 server \
    --datastore-endpoint="$DSN" \
    --disable=traefik,servicelb,metrics-server,local-storage \
    --disable-helm-controller \
    --kube-apiserver-arg=profiling=true \
    --write-kubeconfig-mode=644 >/dev/null
  echo "k3s-$n started (api on localhost:$port)"
  sleep 8
done
