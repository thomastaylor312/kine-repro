// Command conflictload reproduces the kine poll-loop stall the way it actually happens in
// production: not with brute-force write volume, but with a short burst of *conflicting inserts*.
//
// Why conflicts and not volume: the poll loop repairs a hole in the revision sequence at roughly
// 11-27 revisions/second (one Fill plus a few poll queries per hole). Holes are created by
// rolled-back INSERTs, because Postgres sequences are non-transactional -- every failed insert
// burns an id permanently. So the thing that decides whether the poll loop survives is the hole
// creation rate, not the write rate.
//
// The production issue this is based on had a flat 50-66 writes/s throughout, and a burst of 3,541
// unique-constraint violations per minute (~59/s) on Kubernetes Event creates. 59 holes/s against a
// ~11-27/s repair rate stalled the poll loop in about three minutes, and it stayed dead for 25
// minutes after the conflicts had already stopped.
//
// How this generates the same conflicts: kine's Create path does a Get to check whether the key
// exists, then an Append with prev_revision set to the revision of the last (deleted) row for that
// key. Concurrent creates of the *same* name all pass the existence check and then all insert with
// the *same* prev_revision, so every one but the winner violates kine_name_prev_revision_uindex --
// and burns a sequence id. That is exactly the shape of the production log lines:
//
//	Insert error for key /registry/events/storage/...:
//	  duplicate key value violates unique constraint "kine_name_prev_revision_uindex"
//	  createRevision=0 previousRevision=1014795600
//
// Phases: baseline (production-rate writes) -> burst (conflict storm) -> recovery (baseline only).
// The point of the recovery phase is the actual finding: the stall must OUTLIVE the burst.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NOTE(thomastaylor312): When I had AI help me write this, it made almost everything configurable.
// I left all of these in place in case we want to tweak params for each run

var (
	kubeconfigs    = flag.String("kubeconfigs", "kubeconfig-a.yaml,kubeconfig-b.yaml", "comma-separated kubeconfigs, one per k3s server")
	namespace      = flag.String("namespace", "loadtest", "namespace for the churn objects")
	objects        = flag.Int("objects", 300, "number of ConfigMaps in the baseline churn set")
	baselineRate   = flag.Int("baseline-rate", 30, "baseline writes/sec PER SERVER (30 => ~60/s total, matching prod)")
	baselineWork   = flag.Int("baseline-workers", 8, "goroutines per server servicing the baseline rate")
	hotKeys        = flag.Int("hot-keys", 4, "number of contended key names in the conflict storm")
	burstWriters   = flag.Int("burst-writers", 64, "conflict-storm creator goroutines PER SERVER")
	hotDeleteEvery = flag.Duration("hot-delete-every", time.Second, "how often each hot key is deleted, reopening the create race")
	burstAfter     = flag.Duration("burst-after", 90*time.Second, "how long to run baseline before the burst")
	burstDuration  = flag.Duration("burst-duration", 3*time.Minute, "how long the conflict storm lasts")
	duration       = flag.Duration("duration", 25*time.Minute, "total run duration")
	payloadKB      = flag.Int("payload-kb", 1, "approximate size of each baseline object")
	outDir         = flag.String("out", "dumps-conflict", "directory for goroutine dumps")
	resource       = flag.String("resource", "configmaps", "resource label in apiserver_watch_cache_events_received_total")
)

type server struct {
	name         string
	client       *kubernetes.Clientset
	roundTripper http.RoundTripper
	host         string

	writes    atomic.Int64 // successful baseline updates
	conflicts atomic.Int64 // 409s from the apiserver on baseline updates
	creates   atomic.Int64 // successful hot-key creates
	exists    atomic.Int64 // AlreadyExists on hot-key creates
	errs      atomic.Int64

	lastWatchCacheCounter int64
	flatFor               time.Duration
}

func main() {
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal("mkdir: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var servers []*server
	for kc := range strings.SplitSeq(*kubeconfigs, ",") {
		kc = strings.TrimSpace(kc)
		cfg, err := clientcmd.BuildConfigFromFlags("", kc)
		if err != nil {
			fatal("kubeconfig %s: %v", kc, err)
		}
		cfg.QPS, cfg.Burst = 5000, 10000
		cs, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			fatal("client %s: %v", kc, err)
		}
		rt, err := rest.TransportFor(cfg)
		if err != nil {
			fatal("transport %s: %v", kc, err)
		}
		servers = append(servers, &server{
			name:   strings.TrimSuffix(strings.TrimPrefix(filepath.Base(kc), "kubeconfig-"), ".yaml"),
			client: cs, roundTripper: rt, host: strings.TrimSuffix(cfg.Host, "/"),
		})
	}

	fmt.Printf("conflictload: %d servers | baseline %d/s per server (~%d/s total) | burst %d writers/server on %d hot keys\n",
		len(servers), *baselineRate, *baselineRate*len(servers), *burstWriters, *hotKeys)
	fmt.Printf("  timeline: baseline 0-%v | BURST %v-%v | recovery %v-%v\n",
		*burstAfter, *burstAfter, *burstAfter+*burstDuration, *burstAfter+*burstDuration, *duration)
	fmt.Printf("  watching apiserver_watch_cache_events_received_total{resource=%q}\n\n", *resource)

	seed(ctx, servers[0])
	for _, s := range servers {
		startBaseline(ctx, s)
	}

	// The conflict storm runs on its own schedule inside the overall run.
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(*burstAfter):
		}
		bctx, bcancel := context.WithTimeout(ctx, *burstDuration)
		defer bcancel()
		fmt.Printf("\n*** BURST START: %d creators/server racing %d hot keys, deleted every %v ***\n\n",
			*burstWriters, *hotKeys, *hotDeleteEvery)
		for i, s := range servers {
			startConflictStorm(bctx, s, i == 0)
		}
		<-bctx.Done()
		fmt.Printf("\n*** BURST END -- conflicts stop here. Everything past this point is recovery. ***\n\n")
	}()

	monitor(ctx, servers)
}

func seed(ctx context.Context, s *server) {
	_, err := s.client.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: *namespace}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		fatal("create ns: %v", err)
	}
	fmt.Printf("seeding %d objects... ", *objects)
	rnd := rand.New(rand.NewSource(1))
	for i := 0; i < *objects; i++ {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("obj-%04d", i), Namespace: *namespace},
			Data:       map[string]string{"payload": payload(rnd)},
		}
		if _, err := s.client.CoreV1().ConfigMaps(*namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			fatal("seed %d: %v", i, err)
		}
	}
	fmt.Println("done")
}

func payload(rnd *rand.Rand) string {
	b := make([]byte, *payloadKB*1024)
	for i := range b {
		b[i] = byte('a' + rnd.Intn(26))
	}
	return string(b)
}

// startBaseline holds a steady, production-like write rate: read-modify-write
// updates spread over the churn set, ticker-limited so the poll loop is NOT
// under volume pressure. This is deliberately well within what the poll loop
// can drain in batches of pollBatchSize.
func startBaseline(ctx context.Context, s *server) {
	tick := make(chan struct{}, *baselineWork)
	go func() {
		t := time.NewTicker(time.Second / time.Duration(*baselineRate))
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select { // drop the tick if every worker is busy, capping the rate
				case tick <- struct{}{}:
				default:
				}
			}
		}
	}()

	for w := 0; w < *baselineWork; w++ {
		go func(id int) {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			cms := s.client.CoreV1().ConfigMaps(*namespace)
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick:
				}
				name := fmt.Sprintf("obj-%04d", rnd.Intn(*objects))
				cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
				cm, err := cms.Get(cctx, name, metav1.GetOptions{})
				if err != nil {
					cancel()
					s.errs.Add(1)
					continue
				}
				cm.Data["payload"] = payload(rnd)
				_, err = cms.Update(cctx, cm, metav1.UpdateOptions{})
				cancel()
				switch {
				case err == nil:
					s.writes.Add(1)
				case apierrors.IsConflict(err):
					s.conflicts.Add(1)
				default:
					s.errs.Add(1)
				}
			}
		}(w)
	}
}

// startConflictStorm is the actual repro mechanism. Every goroutine on every
// server races to Create the same small set of names. kine checks existence and
// then inserts with prev_revision taken from the last deleted row for that name,
// so concurrent creates collide on kine_name_prev_revision_uindex and each
// loser burns a sequence id -- a permanent hole the poll loop must Fill one
// revision at a time.
//
// Conflict yield and write volume are deliberately decoupled, because the whole
// point is to show the stall at a production-rate write load:
//
//   - Creators loop tightly on Create. Once a key exists, Create costs kine a
//     Get and nothing else -- no insert, no revision, no row. So creator
//     concurrency is nearly free in write volume.
//   - A separate deleter removes each hot key once per hot-delete-every. Each
//     delete is one revision, and it reopens the race: every creator then sees
//     the key absent, reads the same prev_revision off the deleted row, and
//     inserts. One wins; the rest violate the unique index and burn a sequence id.
//
// So conflicts/sec is roughly (deletes/sec) x (racing creators per key), while
// added write volume is only about 2 x deletes/sec. That lets the burst hit
// production's ~59 conflicts/sec while total writes stay near 60-80/sec.
//
// Note kine maps a unique violation to ErrKeyExists, so the client just sees
// AlreadyExists. The real conflict count has to be read from the k3s logs.
func startConflictStorm(ctx context.Context, s *server, deleter bool) {
	for w := 0; w < *burstWriters; w++ {
		go func(id int) {
			cms := s.client.CoreV1().ConfigMaps(*namespace)
			// Small payload: we want round trips and collisions, not bytes.
			data := map[string]string{"c": strings.Repeat("x", 256)}
			for i := 0; ctx.Err() == nil; i++ {
				name := fmt.Sprintf("hot-%02d", (id+i)%*hotKeys)
				cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, err := cms.Create(cctx, &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *namespace},
					Data:       data,
				}, metav1.CreateOptions{})
				cancel()
				switch {
				case err == nil:
					s.creates.Add(1)
				case apierrors.IsAlreadyExists(err):
					s.exists.Add(1)
				default:
					s.errs.Add(1)
				}
			}
		}(w)
	}

	// Only one server runs the deleters, so the delete rate -- and therefore the
	// added write volume -- stays deterministic regardless of replica count.
	if !deleter {
		return
	}
	for k := 0; k < *hotKeys; k++ {
		go func(k int) {
			name := fmt.Sprintf("hot-%02d", k)
			cms := s.client.CoreV1().ConfigMaps(*namespace)
			// Stagger the deleters so the races do not all land at once.
			time.Sleep(time.Duration(k) * *hotDeleteEvery / time.Duration(*hotKeys))
			t := time.NewTicker(*hotDeleteEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_ = cms.Delete(cctx, name, metav1.DeleteOptions{})
				cancel()
			}
		}(k)
	}
}

func (s *server) watchCacheCount() (int64, error) {
	req, err := http.NewRequest("GET", s.host+"/metrics", nil)
	if err != nil {
		return 0, err
	}
	resp, err := (&http.Client{Transport: s.roundTripper, Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	needle := fmt.Sprintf("apiserver_watch_cache_events_received_total{resource=%q}", *resource)
	for line := range strings.SplitSeq(string(body), "\n") {
		if strings.HasPrefix(line, needle) {
			f := strings.Fields(line)
			if len(f) == 2 {
				v, err := strconv.ParseFloat(f[1], 64)
				return int64(v), err
			}
		}
	}
	return 0, fmt.Errorf("metric not found")
}

// cacheVsDirectRead compares a LIST at resourceVersion=0 (served from the watch
// cache) against a quorum LIST (served from kine). Divergence is the stall.
func (s *server) cacheVsDirectRead(ctx context.Context) (cache, quorum int64) {
	cms := s.client.CoreV1().ConfigMaps(*namespace)
	if l, err := cms.List(ctx, metav1.ListOptions{ResourceVersion: "0", Limit: 1}); err == nil {
		cache, _ = strconv.ParseInt(l.ResourceVersion, 10, 64)
	}
	if l, err := cms.List(ctx, metav1.ListOptions{Limit: 1}); err == nil {
		quorum, _ = strconv.ParseInt(l.ResourceVersion, 10, 64)
	}
	return
}

func monitor(ctx context.Context, servers []*server) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	start := time.Now()
	prevW := make([]int64, len(servers))
	prevT := start
	dumped := false

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nrun complete")
			return
		case <-tick.C:
		}

		elapsed := time.Since(start)
		phase := "baseline"
		switch {
		case elapsed >= *burstAfter && elapsed < *burstAfter+*burstDuration:
			phase = "BURST   "
		case elapsed >= *burstAfter+*burstDuration:
			phase = "recovery"
		}

		dt := time.Since(prevT).Seconds()
		prevT = time.Now()
		fmt.Printf("[%5.0fs %s]", elapsed.Seconds(), phase)

		anyFlat := false
		for i, s := range servers {
			w := s.writes.Load()
			rate := float64(w-prevW[i]) / dt
			prevW[i] = w

			wc, err := s.watchCacheCount()
			if err != nil {
				fmt.Printf(" | %s metrics-err", s.name)
				continue
			}
			if wc == s.lastWatchCacheCounter {
				s.flatFor += 5 * time.Second
			} else {
				s.flatFor = 0
			}
			s.lastWatchCacheCounter = wc

			pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cache, directRead := s.cacheVsDirectRead(pctx)
			cancel()

			fmt.Printf(" | %s base-write=%.0f/s hot(ok=%d exists=%d) cache-count=%d flat=%.0fs lag=%d",
				s.name, rate, s.creates.Load(), s.exists.Load(), wc, s.flatFor.Seconds(), directRead-cache)

			if s.flatFor >= 30*time.Second {
				anyFlat = true
			}
		}
		fmt.Println()

		if anyFlat && !dumped {
			dumped = true
			fmt.Printf("\n*** STALL: watch-cache counter flat >=30s while writes commit ***\n")
			for _, s := range servers {
				if p := s.dumpPprof(); p != "" {
					fmt.Printf("*** goroutine dump %s -> %s\n", s.name, p)
				}
			}
			fmt.Println()
		}
	}
}

func (s *server) dumpPprof() string {
	req, err := http.NewRequest("GET", s.host+"/debug/pprof/goroutine?debug=2", nil)
	if err != nil {
		return ""
	}
	resp, err := (&http.Client{Transport: s.roundTripper, Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		fmt.Printf("pprof %s: %v\n", s.name, err)
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	path := filepath.Join(*outDir, fmt.Sprintf("goroutine-%s-%s.txt", s.name, time.Now().Format("150405")))
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return ""
	}
	return path
}

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", a...)
	os.Exit(1)
}
