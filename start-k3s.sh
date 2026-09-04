#!/usr/bin/env bash
# Starts two k3s v1.33.13+k3s1 servers sharing one Postgres datastore, mirroring my production
# setup. Compaction interval is set to 1 minute to reproduce the issue
set -euo pipefail
DSN="postgres://kine:kine@kine-pg:5432/k3sdb?sslmode=disable"
for n in a b; do
  port=$([ "$n" = a ] && echo 6444 || echo 6445)
  docker rm -f "k3s-$n" >/dev/null 2>&1 || true
  docker run -d --name "k3s-$n" --privileged --network kinenet \
    -e K3S_TOKEN=reprotoken -p "$port:6443" \
    rancher/k3s:v1.33.13-k3s1 server \
    --datastore-endpoint="$DSN" \
    --disable=traefik,servicelb,metrics-server,local-storage \
    --disable-helm-controller \
    --kube-apiserver-arg=etcd-compaction-interval=1m \
    --kube-apiserver-arg=profiling=true \
    --write-kubeconfig-mode=644 >/dev/null
  echo "k3s-$n started (api on localhost:$port)"
  sleep 8
done
