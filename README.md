# Kine Watch Stall Reproduction

This is a reproduction of a bug in [kine](https://github.com/k3s-io/kine) where every watch on a k3s
server goes permanently silent while writes keep committing normally. No error is logged, nothing
retries, and the only way out is restarting the process.

I hit this on a busy production tenant running k3s with an external Postgres datastore, spent a
while chasing the wrong theories, and eventually reproduced it locally. Everything here runs against
real k3s v1.37.1-rc2+k3s1 and real kine v0.17.1 (which includes the watch pipeline rewrite from
[k3s-io/kine#746](https://github.com/k3s-io/kine/pull/746)) with no patched builds or mocking
(outside of the latency injection). It reproduces the same way on k3s v1.33.13+k3s1 with kine
v0.16.1; set `K3S_IMAGE=rancher/k3s:v1.33.13-k3s1` when running `start-k3s.sh` to use that.

An earlier version of this repo blamed raw write volume and needed about 1,700 writes/s to trip.
That turned out to be the wrong knob entirely. I have since gone back through the metrics and logs
from the actual production incident that sent me down this path, and the write rate never moved (it
sat at a totally ordinary 50-66 writes/s for the entire incident). What spiked was the rate of
_conflicting_ inserts. This repo reproduces the real thing at my production write rates, with the
compaction interval left at its default.

## What it looks like

This was hard to diagnose because almost everything still works. But this seemed to be the pattern:

- `apiserver_watch_cache_events_received_total` goes flat for **every** resource, on **every**
  replica, at roughly the same time.
- Writes keep committing at full rate.
- `kubectl get` still returns current data, because a quorum read goes straight to the datastore and
  never touches the watch cache.
- Any LIST asking for a newer resourceVersion comes back as a 504 `Too large resource version`.
- Controllers and operators quietly stop reacting to anything. Their informers are alive, connected,
  and receiving nothing.
- It never recovers on its own as long as writes continue. Restarting the process fixes it immediately.

## What is actually happening

TL;DR: a burst of failed inserts punches permanent holes in the revision sequence (when using the
postgres backend), the poll loop can only repair those holes one revision at a time, and if the
datastore is far enough away that the repair is slow, compaction eventually deletes the backlog out
from under it, making it worse.

There are 2 (and a half) things that need to happen, and you need both to occur at the same time.
That is why this took so long to pin down, since a single one of these things happening isn't a
problem in and of itself.

### Step 1: Failed inserts punch permanent holes

Postgres `SERIAL` types are not transactional and the schema for a row in postgres uses a
`BIGSERIAL`. When an `INSERT` rolls back, the `id` it pulled off the sequence is gone forever, and
the `kine` table is left with a hole where that revision should have been.

The usual source of those rollbacks is contention on `kine_name_prev_revision_uindex`. kine's create
path does a `Get` to see whether the key exists, then an `Append` with `prev_revision` set to the
revision of the last (deleted) row for that key. If several creates for the same name land at once,
they all pass the existence check, they all read the same `prev_revision`, and then they all insert.
One wins. Every other one violates the unique index and burns a sequence id on the way out.

In our production incident this happened to be a burst of `Event` creates. But it could be any
resource with a high enough write rate and a high enough conflict rate.

### Step 2: The poll loop repairs holes one revision at a time

`poll()` in `pkg/logstructured/sqllog/sql.go` is a single goroutine that queries all keys and is the
only source of watch events for every prefix on the process. There is one cursor, and everything
downstream is fed from it. Under normal operation it drains in batches of `pollBatchSize` (500-1000),
which is quick.

When it hits a hole, all of that goes out the window. The gap branch calls `Fill()`, which inserts a
`gap-<rev>` marker row and advances the cursor by _exactly one revision_. Each hole costs roughly a
couple of batch fetches plus an insert. When you combine this with any sort of latency (such as
using a hosted postgres DB), that means it is only incrementing after 2-4 queries (the batch fetches
+ the insert) * the amount of latency. In this repro I inject 3ms of latency (each way), which means
kine fills one row at a time at the cost of 12-24ms. With a gap of several thousand revisions, the
repair rate is fairly slow. Basically this means the repair rate is a property of how far away your
database is, and has almost nothing to do with how much work the database is doing.

As a concrete example: on a Postgres container sitting on the same machine, I measured about 116
fills/second. Against the production RDS instance across a VPC hop, it was closer to 27/second. With
a write rate of 55/s it can't catch up fast enough before compaction happens.

There is a `canSkipRevision` that will give up on a hole after one second and skip forward, logging
a `GAP` error. But it only fires when `Fill` keeps _failing_, not when there is a large gap that is
taking a while to fill. On the success path, filling advances the cursor by one, which changes the
revision being waited on, which resets the tracking, so the one-second timer never expires.
Production logged exactly one `GAP` line during the whole incident, and this repro logs zero.

### Step 2.5 Compaction deletes the backlog and makes the gap bigger

Compaction is driven entirely by the write revision. `safeCompactRev` compares against `currentRev`,
and nothing in the compaction path ever looks at the poll loop's cursor (`pollRevision` is a local
variable inside `poll()`).

Even the `compactMinRetain` doesn't seem to help in this situation either. It is a ceiling that
protects the most recent 1,000 revisions from getting compacted away. For example, in the production
incident, mid-stall, kine was retaining about 14,250 revisions, which at ~56 revisions/second is a
little over four minutes of history:

```
COMPACT compactRev=1015216802 targetCompactRev=1015217464 currentRev=1015231711
```

So the question is simply whether the poll loop can drain its backlog inside that window. If it can,
everything recovers and you never notice. If it can't, compaction starts deleting rows the poll loop
has not read yet, and nothing anywhere tells the poll loop that happened.

So it carries on doing exactly what it always does. `poll()` queries for everything after
`pollRevision`, gets back a row whose `ModRevision` is far higher than the one it was expecting, and
treats the difference as a hole, which by this point it genuinely is. It calls `Fill()`, inserts a
`gap-<rev>` marker row, advances the cursor by exactly one revision, and goes around again. The
difference is that before compaction ran there were real rows in that range and the loop could drain
up to `pollBatchSize` of them per query; afterwards there is nothing left in the range at all, so
every revision has to be filled individually. The loop is working at the same speed it always was
before, but now the gap was made artificially larger by compaction, which slows down any chance of
recovery (especially if the write rate is fairly consistent, like in our production workloads).

## About issue #731

[k3s-io/kine#731](https://github.com/k3s-io/kine/issues/731) describes a different failure, where a
watch progress notification carries a revision before the events for that revision have been sent,
and the apiserver's cache permanently desyncs as a result. That was a very reasonable thing to
suspect here, so I went looking for it specifically.

k3s in this repo runs with `KUBE_WATCHCACHE_CONSISTENCY_CHECKER=true`, which makes the apiserver
periodically compare a LIST from its watch cache against a LIST from the datastore at the same
revision, and panic if the digests differ. Across every run I did it has never reported a single
failure. If compaction happens during the stall it can't even complete a comparison, and instead
reports:

```
Cache consistency check error: failed calculating etcd digest:
  The resourceVersion for the provided list is too old.
```

As long as I am interpreting this correctly, this isn't cache corruption, it is a cache that is
entirely stale. So stale that the resourceVersion it is trying to compare against has been compacted
away.

## Running it

IMPORTANT NOTE: I ran this on a MacBook Pro (M4) with 24 GB RAM and 10 cores. The whole thing is a
race between how fast the poll loop can fill holes and how fast compaction advances, so a
substantially faster or slower machine may need the knobs adjusted.

You need Docker and Go to run this. Nothing else.

Start Postgres:

```bash
docker run -d --name kine-pg -e POSTGRES_PASSWORD=kine -e POSTGRES_USER=kine \
  -e POSTGRES_DB=kine -p 55432:5432 postgres:16 -c max_connections=200
docker network create kinenet && docker network connect kinenet kine-pg
```

Once it is up, create the database:

```bash
docker exec kine-pg psql -U kine -d kine -c "CREATE DATABASE k3sdb;"
```

Now put a latency shim in front of it. This is the part that makes this closer to reality. It stands
in for the VPC hop between the k3s pods and RDS, and it is what drops the gap-repair rate into the
range where the poll loop can't dig itself out:

```bash
./set-pg-latency.sh 3   # 3ms each way, so ~6ms per round trip. Tweak as needed for your machine.
```

Then two k3s servers sharing that one datastore, which is the topology this shows up on. Note that
compaction is left at the kube-apiserver default of 5 minutes, and the only non-default thing here
is the consistency checker:

```bash
DSN_HOST=kine-proxy ./start-k3s.sh

for n in a b; do
  port=$([ "$n" = a ] && echo 6444 || echo 6445)
  docker exec "k3s-$n" cat /etc/rancher/k3s/k3s.yaml > "kubeconfig-$n.yaml"
  python3 -c "p='kubeconfig-$n.yaml';s=open(p).read().replace('https://127.0.0.1:6443','https://127.0.0.1:$port');open(p,'w').write(s)"
done
```

Then drive it:

```bash
GOWORK=off go build -o conflictload-bin ./conflictload
./conflictload-bin -hot-keys 8 -hot-delete-every 500ms -burst-writers 64 -duration 30m
```

The load generator runs in three phases. It holds a steady 60 writes/s across both servers the whole
time, which is the production baseline. Ninety seconds in it starts a three-minute conflict storm,
tuned to produce roughly 60 unique-constraint violations per second per replica, which is the same
rate the production incident hit. Then the storm stops and the baseline keeps running to demonstrate
the recovery phase.

### What you should see

Here is the transition from a run of mine. `cache-count` is the watch cache counter and `lag` is
direct read revision minus cache revision:

```
[   88s baseline] | a base-write=30/s cache-count=5695 flat=0s lag=2  | b base-write=29/s cache-count=5700 flat=0s lag=18

*** BURST START: 64 creators/server racing 8 hot keys, deleted every 500ms ***

[  109s BURST   ] | a base-write=29/s cache-count=5915 flat=0s lag=3899  | b base-write=29/s cache-count=5935 flat=0s lag=4533
[  130s BURST   ] | a base-write=30/s cache-count=6104 flat=0s lag=7704  | b base-write=29/s cache-count=6141 flat=0s lag=8355

*** BURST END -- conflicts stop here. Everything past this point is recovery. ***

[  449s recovery] | a base-write=29/s cache-count=10824 flat=0s   lag=35517 | b base-write=30/s cache-count=10776 flat=0s   lag=35449
[ 1718s recovery] | a base-write=30/s cache-count=13312 flat=260s lag=90372 | b base-write=30/s cache-count=13310 flat=265s lag=90566
```

Both replicas frozen at the same counter value, both still committing 60 writes/s between them,
and the lag growing without bound for 20+ minutes after the last conflict.

The other side of it is visible in the database. This shows how far the poll loop has crawled, where
the head is, and where compaction has gotten to. Run it a few times and watch which gap line is
filling compared to the current revision and compaction revision:

```bash
# The substring strips the "gap-" prefix and casts to bigint so we can get the max revision number.
docker exec kine-pg psql -U kine -d k3sdb -t -A -F'|' -c \
      "select (select max(cast(substring(name from 5) as bigint)) from kine where name like 'gap-%') as poller,
              (select max(id) from kine) as head,
              (select max(prev_revision) from kine where name = 'compact_rev_key') as compact_rev;"
50474|132777|112402
```

The poller there is 61,928 revisions *below* the compact revision, which means every single revision
it still has to get through has already been deleted. It was advancing at about 25 revisions/second
against a write rate of 62, so it was never going to close that gap.

You can also see the number of conflict errors by grepping the logs of either k3s-a or k3s-b:

```bash
docker logs k3s-a 2>&1 | rg kine_name_prev_revision_uindex | wc -l
11425
```

### Counterexample: Try it without the latency

This is optional, but it does help demonstrate that the latency between kine and the DB itself is
what makes the problem worse. Run `./set-pg-latency.sh 0`, restart k3s, and run the exact same load.
You will get the exact same ~60 conflicts/second, and the poll loop will still fall about 20-30k
revisions behind. Then it drains the whole backlog in roughly 110 seconds and carries on as if
nothing happened, because at 116 fills/second it clears the hole well before compaction gets
anywhere near it.

## Cleanup

```bash
docker rm -f k3s-a k3s-b kine-pg kine-proxy
docker network rm kinenet
```

## A couple of notes

- I completely understand that this is fairly stable code and is considered mostly correct. If the
  right answer is that the datastore should be closer, or that nothing should ever be generating
  that many conflicting creates, that is a fair response. However, this is a possible failure mode
  that I feel would be at least worth documenting for operators, and may be worth fixing as well
  since the failure mode is mostly silent and requires a restart to recover
- [k3s-io/kine#746](https://github.com/k3s-io/kine/pull/746) rewrites the watch pipeline, but the
  gap-repair path in `poll()` (one `Fill()` per loop, with `canSkipRevision` resetting on every
  successful fill) is unchanged by it, and this repro still stalls on kine v0.17.1. I have no
  attachment to any particular fix; the reproduction is the useful part.
- AI helped me write the repro and dig through the production telemetry, but I have read through
  every line and the conclusions here are mine.
