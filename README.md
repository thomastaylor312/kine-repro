# kine watch stall: compaction outruns the poll loop

This is a reproduction of a bug in [kine](https://github.com/k3s-io/kine) where every watch on a k3s
server goes permanently silent while writes keep committing normally. No error is logged, nothing
retries, and the only way out is restarting the process.

I hit this on a busy production tenant running k3s with an external Postgres datastore, spent a while
chasing the wrong theories, and eventually reproduced it locally. Everything here runs against real
k3s v1.33.13+k3s1 and real kine v0.16.1 — no patched builds, no mocks. It reproduces in about four
minutes on a laptop.

## What it looks like

This was hard to diagnose because almost everything still works. But this seemed to be the pattern:

- `apiserver_watch_cache_events_received_total` goes flat for **every** resource, on **every**
  replica, at roughly the same time.
- Writes keep committing at full rate.
- `kubectl get` still returns current data, because a quorum read goes straight to the datastore and
  never touches the watch cache.
- Controllers and operators quietly stop reacting to anything. Their informers are alive, connected,
  and receiving nothing.
- It never recovers on its own. Restarting the process fixes it instantly and completely.

## What I think is happening

TL;DR: kine's compactor deletes rows that its own poll loop has not read yet, and the repair path
for the resulting hole is much slower than normal operation, so it can never catch back up.

The longer version, in order:

1. `poll()` in `pkg/logstructured/sqllog/sql.go` is a single goroutine that queries all keys and
   is the only source of watch events for every prefix on the process. There is one cursor, and
   everything downstream is fed from it.
2. Under sustained write load that cursor falls behind the write revision. Under normal operation it
   catches up in batches of `pollBatchSize` (500–1000).
3. The compactor is driven entirely by the write revision. `safeCompactRev` compares against
   `currentRev`, and `polledRev` is referenced in exactly three places in the whole codebase: where
   it's declared, where `poll()` writes it, and where `WaitForSyncTo` reads it. Nothing in the
   compaction path ever looks at it. So compaction happily deletes rows the poll loop has not
   consumed.
4. Now `poll()` queries for `pollRevision+1` and gets back a row with a higher `ModRevision`. It sees
   a hole. That hole is permanent, because the row it's waiting for was deleted.
5. The gap branch calls `Fill()`, which inserts a `gap-<rev>` marker row and advances the cursor by
   _exactly one revision_ before starting on the next loop
6. So the poll loop is now repairing at one revision per query-plus-insert round trip instead of
   draining hundreds per query. I measured roughly 27 revisions/second in my production workload.

Once the repair rate drops below the write rate (which was around 60-70/s in my production
workload), the gap diverges and the poll loop never emits another real event. For all intents and
purposes, every watch on that process is dead.

There is a `canSkipRevision` that will give up on a hole after one second and skip forward, logging a
`GAP` error. But it only fires when `Fill` keeps *failing*. On the success path, filling advances the
cursor by one, which changes the revision being waited on, which resets the tracking — so the
one-second timer never expires. You end up with zero `GAP` lines. 

## Running it

IMPORTANT NOTE: I ran this on a MacBook Pro (M4) with 24 GB RAM and 10 cores. Because the bug is a race
on how fast the poll loop can fill the backlog on reads vs writes, a faster machine may not
reproduce.

You need Docker and Go to run this. Nothing else.

Start postgres:

```bash
docker run -d --name kine-pg -e POSTGRES_PASSWORD=kine -e POSTGRES_USER=kine \
  -e POSTGRES_DB=kine -p 55432:5432 postgres:16 -c max_connections=200
docker network create kinenet && docker network connect kinenet kine-pg
```

Once it is up, create the database:

```bash
docker exec kine-pg psql -U kine -d kine -c "CREATE DATABASE k3sdb;"
```

Then two k3s servers sharing that one datastore, which is the topology this shows up on:

```bash
./start-k3s.sh

for n in a b; do
  port=$([ "$n" = a ] && echo 6444 || echo 6445)
  docker exec "k3s-$n" cat /etc/rancher/k3s/k3s.yaml > "kubeconfig-$n.yaml"
  python3 -c "p='kubeconfig-$n.yaml';s=open(p).read().replace('https://127.0.0.1:6443','https://127.0.0.1:$port');open(p,'w').write(s)"
done
```

Then drive it:

```bash
GOWORK=off go build -o k3sload-bin ./k3sload
./k3sload-bin -duration 12m -objects 300 -writers 16 -payload-kb 8 -stall-after 45s
```

Watch the `lag-between-read-and-cache` column climb and `flat-for` start counting. When it trips, it
pulls a goroutine dump from both apiservers automatically.

## What you should see

Here's a run of mine. `cache-count` is the watch cache counter, `flat-for` is how long it's been
frozen, `lag-between-read-and-cache` is direct read revision minus cache revision:

```
[  169s] | a write=1127/s conflicts=5268 cache-count=67772 flat-for=0s lag-between-read-and-cache=82983 | b write=996/s conflicts=5620 cache-count=69051 flat-for=0s lag-between-read-and-cache=85702
[  175s] | a write=1091/s conflicts=5531 cache-count=70740 flat-for=0s lag-between-read-and-cache=86984 | b write=1145/s conflicts=5777 cache-count=71951 flat-for=0s lag-between-read-and-cache=87301
[  182s] | a write=735/s conflicts=5693 cache-count=72458 flat-for=0s lag-between-read-and-cache=89809 | b write=762/s conflicts=5950 cache-count=72455 flat-for=0s lag-between-read-and-cache=91863
[  188s] | a write=745/s conflicts=5832 cache-count=72458 flat-for=3s lag-between-read-and-cache=93883 | b write=656/s conflicts=6111 cache-count=72455 flat-for=3s lag-between-read-and-cache=96503
[  195s] | a write=722/s conflicts=6073 cache-count=72458 flat-for=6s lag-between-read-and-cache=100339 | b write=1034/s conflicts=6354 cache-count=72455 flat-for=6s lag-between-read-and-cache=103630
[  202s] | a write=1136/s conflicts=6297 cache-count=72458 flat-for=9s lag-between-read-and-cache=106591 | b write=999/s conflicts=6579 cache-count=72455 flat-for=9s lag-between-read-and-cache=110018
[  208s] | a write=1017/s conflicts=6527 cache-count=72458 flat-for=12s lag-between-read-and-cache=112934 | b write=1023/s conflicts=6816 cache-count=72455 flat-for=12s lag-between-read-and-cache=116717
[  215s] | a write=1055/s conflicts=6716 cache-count=72458 flat-for=15s lag-between-read-and-cache=117923 | b write=784/s conflicts=6905 cache-count=72455 flat-for=15s lag-between-read-and-cache=119599
```

Both replicas frozen at the same counter value, both still committing 600–900 writes/s, and the lag
growing without bound.

You can also check the table after load has stopped to see the lag in the DB as it is trying to
catch up. Try running this multiple times to see how far behind the poll loop is and how long it is
taking to catch up:

```bash
# The substring is strip the "gap-" prefix and cast to bigint so we can get the max revision number.
docker exec kine-pg psql -U kine -d k3sdb -t -A -F'|' -c \
      "select (select max(cast(substring(name from 5) as bigint)) from kine where name like 'gap-%') as poll_at,
              (select max(id) from kine) as head,
              (select count(*) from kine where name like 'gap-%') as fills;"
74974|256021|162
```

## Cleanup

```bash
docker rm -f k3s-a k3s-b kine-pg
docker network rm kinenet
```
