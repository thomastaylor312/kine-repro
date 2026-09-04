// Command k3sload drives write load against two real k3s v1.33.13+k3s1 servers
// sharing one Postgres datastore (kine v0.16.1), and watches for the signature
// of the kine poll-loop stall:
//
//	apiserver_watch_cache_events_received_total goes FLAT on every resource and
//	every replica, while writes keep committing and quorum reads stay current.
//
// It also runs the discriminating probe that separates "stale watch cache" from
// "silent watch": a LIST at resourceVersion=0 (served from the watch cache)
// compared against a quorum LIST (served from kine). Under this bug the two
// diverge without bound.
//
// On detection it pulls a full goroutine dump from the apiserver's pprof
// endpoint. Because kine runs in-process inside `k3s server`, that dump
// contains kine's poll/broadcaster goroutines -- which a SIGQUIT aimed at the
// container's pid 1 will miss, since that is the k3s supervisor, not the child
// that hosts the apiserver.
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

var (
	kubeconfigs = flag.String("kubeconfigs", "kubeconfig-a.yaml,kubeconfig-b.yaml", "comma-separated kubeconfigs, one per k3s server")
	namespace   = flag.String("namespace", "loadtest", "namespace for the churn objects")
	objects     = flag.Int("objects", 300, "number of ConfigMaps to churn")
	writers     = flag.Int("writers", 16, "writer goroutines per server")
	payloadKB   = flag.Int("payload-kb", 8, "approximate size of each object")
	duration    = flag.Duration("duration", 10*time.Minute, "run duration")
	stallAfter  = flag.Duration("stall-after", 45*time.Second, "watch-cache counter flat this long (while writes commit) = stall")
	outDir      = flag.String("out", "dumps-k3s", "directory for goroutine dumps")
	resource    = flag.String("resource", "configmaps", "resource label in apiserver_watch_cache_events_received_total")
	stopOnStall = flag.Bool("stop-on-stall", true, "exit after first confirmed stall + dump")
)

type server struct {
	name                  string
	client                *kubernetes.Clientset
	roundTripper          http.RoundTripper
	host                  string
	writes                atomic.Int64
	conflicts             atomic.Int64
	errs                  atomic.Int64
	lastWatchCacheCounter int64 // last watch-cache counter
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

	fmt.Printf("k3sload: %d servers, %d objects, %d writers/server, ~%dKB payload\n",
		len(servers), *objects, *writers, *payloadKB)
	fmt.Printf("  watching apiserver_watch_cache_events_received_total{resource=%q}\n\n", *resource)

	seed(ctx, servers[0])

	for _, s := range servers {
		startWriters(ctx, s)
	}
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

// startWriters does read-modify-write updates, which is what my production workload does.
func startWriters(ctx context.Context, s *server) {
	for w := 0; w < *writers; w++ {
		go func(id int) {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			cms := s.client.CoreV1().ConfigMaps(*namespace)
			for ctx.Err() == nil {
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

// watchCacheCount scrapes the apiserver's own watch-cache counter and parses it out
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

// cacheVsDirectRead is the way we can check between what is in the cache vs a direct read. A resource
// version of 0 is served from the apiserver watch cache, an unset rv forces a read through kine.
// Different numbers means it is a stale cache
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
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	start := time.Now()
	prevW := make([]int64, len(servers))
	dumped := false

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nrun complete, no stall detected")
			return
		case <-tick.C:
		}

		fmt.Printf("[%5.0fs]", time.Since(start).Seconds())
		stalled := false
		for i, s := range servers {
			w := s.writes.Load()
			// Simple calculation of writes per second over the last 3s tick. This is not perfect,
			// but good enough for our purposes.
			rate := float64(w-prevW[i]) / 3.0
			prevW[i] = w

			wc, err := s.watchCacheCount()
			if err != nil {
				fmt.Printf(" | %s metrics-err", s.name)
				continue
			}
			if wc == s.lastWatchCacheCounter {
				s.flatFor += 3 * time.Second
			} else {
				s.flatFor = 0
			}
			s.lastWatchCacheCounter = wc

			pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cache, directRead := s.cacheVsDirectRead(pctx)
			cancel()

			fmt.Printf(" | %s write=%.0f/s conflicts=%d cache-count=%d flat-for=%.0fs lag-between-read-and-cache=%d",
				s.name, rate, s.conflicts.Load(), wc, s.flatFor.Seconds(), directRead-cache)

			// Stall: the watch cache counter has not moved for stallAfter while
			// this server is still committing writes.
			if s.flatFor >= *stallAfter && rate > 0 {
				stalled = true
			}
		}
		fmt.Println()

		if stalled && !dumped {
			dumped = true
			fmt.Printf("\n*** STALL: watch-cache counter flat while writes commit ***\n")
			for _, s := range servers {
				if p := s.dumpPprof(); p != "" {
					fmt.Printf("*** goroutine dump %s -> %s\n", s.name, p)
				}
			}
			fmt.Println()
			if *stopOnStall {
				return
			}
		}
	}
}

// dumpPprof pulls a full goroutine dump from the apiserver. kine runs
// in-process in `k3s server`, so this includes sqllog.poll and the broadcaster.
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
