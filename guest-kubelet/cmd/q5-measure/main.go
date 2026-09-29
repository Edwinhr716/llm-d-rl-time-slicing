// Command q5-measure times the Ready edges of one guest (M2, Q5 spans e1, e2, e3 and
// Ready -> InferencePool).
//
// It runs as a host-network pod on the guest kubelet's host, so every timestamp comes from the
// same clock as the guest kubelet's. For each edge it records:
//
//	trigger  the readiness change is caused (force: debug override; toggle: the guest's own probe
//	         endpoint is switched)
//	notify   the guest kubelet hands the new status to the pod controller (from /debug/ready-edges)
//	pod      the pod watch shows the guest's Ready condition changed
//	slice    the EndpointSlice watch shows the guest's endpoint ready flag changed
//	cip      the first ClusterIP request started after the change that has the new outcome
//	         (success = any HTTP response)
//	pool     the first request through the router started after the change that has the new
//	         outcome (200 = the guest is in the InferencePool; anything else = not)
//
// and prints e1 = pod-notify, e2 = slice-pod, e3 = cip-slice and pool-pod, one JSON line per
// edge, then a summary per span and direction.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/edwinhr716/guest-kubelet/internal/probe"
)

type config struct {
	mode        string // force or toggle
	namespace   string
	guest       string
	service     string
	servicePort int
	vkDebug     string
	routerURL   string
	model       string
	toggleURL   string // toggle mode: http://<guest IP>:<port>, filled from the pod when empty
	togglePort  int
	edges       int
	settle      time.Duration
	jitter      time.Duration
	timeout     time.Duration
	cipEvery    time.Duration
	poolEvery   time.Duration
	kubeconfig  string
}

// history is a series of state changes of one observer.
type history struct {
	mu      sync.Mutex
	changes []change
}

type change struct {
	at    time.Time
	state bool
}

func (h *history) set(at time.Time, state bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if n := len(h.changes); n > 0 && h.changes[n-1].state == state {
		return
	}
	h.changes = append(h.changes, change{at: at, state: state})
}

func (h *history) current() (change, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.changes) == 0 {
		return change{}, false
	}
	return h.changes[len(h.changes)-1], true
}

// firstSince returns the time of the first change to state at or after since.
func (h *history) firstSince(since time.Time, state bool) (time.Time, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, chg := range h.changes {
		if !chg.at.Before(since) && chg.state == state {
			return chg.at, true
		}
	}
	return time.Time{}, false
}

type observers struct {
	pod, slice, cip, pool *history
	podIP                 func() string
}

// edgeResult is one measured edge. Times are Unix nanoseconds; spans are seconds.
type edgeResult struct {
	Index   int                `json:"index"`
	Ready   bool               `json:"ready"`
	Trigger int64              `json:"trigger"`
	Spans   map[string]float64 `json:"spans"`
	Missing []string           `json:"missing,omitempty"`
}

func main() {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", "force",
		"force (debug override on the guest kubelet) or toggle (switch the guest's own probe endpoint)")
	flag.StringVar(&cfg.namespace, "namespace", "guest-kubelet-proto", "guest namespace")
	flag.StringVar(&cfg.guest, "guest", "q5-sim", "guest pod name")
	flag.StringVar(&cfg.service, "service", "q5-sim", "Service selecting only the guest")
	flag.IntVar(&cfg.servicePort, "service-port", 8000, "Service port")
	flag.StringVar(&cfg.vkDebug, "vk-debug", "http://127.0.0.1:10261", "guest kubelet debug endpoint (same host)")
	flag.StringVar(&cfg.routerURL, "router-url", "", "router completions URL; empty skips the pool span")
	flag.StringVar(&cfg.model, "model", "test-model", "model name sent to the router")
	flag.StringVar(&cfg.toggleURL, "toggle-url", "",
		"toggle mode: base URL of the guest's switch; default http://<guest IP>:<toggle-port>")
	flag.IntVar(&cfg.togglePort, "toggle-port", 8080, "toggle mode: guest port serving /cgi-bin/up and /cgi-bin/down")
	flag.IntVar(&cfg.edges, "edges", 25, "edges each way")
	flag.DurationVar(&cfg.settle, "settle", 3*time.Second, "wait after every observer has seen an edge")
	flag.DurationVar(&cfg.jitter, "jitter", time.Second, "random extra wait, so edges do not line up with periodic work")
	flag.DurationVar(&cfg.timeout, "timeout", 60*time.Second, "give up on an edge after this long")
	flag.DurationVar(&cfg.cipEvery, "cip-every", 10*time.Millisecond, "ClusterIP poll interval")
	flag.DurationVar(&cfg.poolEvery, "pool-every", 20*time.Millisecond, "router poll interval")
	flag.StringVar(&cfg.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig; empty means in-cluster")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, &cfg)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "q5-measure:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config) error {
	if cfg.mode != "force" && cfg.mode != "toggle" {
		return fmt.Errorf("--mode must be force or toggle, got %q", cfg.mode)
	}
	restCfg, err := restConfig(cfg.kubeconfig)
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	svc, err := client.CoreV1().Services(cfg.namespace).Get(ctx, cfg.service, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get service: %w", err)
	}
	obs, err := watch(ctx, client, cfg)
	if err != nil {
		return err
	}
	cipURL := "http://" + net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(cfg.servicePort)) + "/"
	go poll(ctx, cfg.cipEvery, obs.cip, func(ctx context.Context) bool { return httpAny(ctx, cipURL) })
	if cfg.routerURL != "" {
		go poll(ctx, cfg.poolEvery, obs.pool, func(ctx context.Context) bool { return routerOK(ctx, cfg.routerURL, cfg.model) })
	}

	trig := trigger(cfg, obs)
	defer func() {
		if cfg.mode == "force" {
			// Hand readiness back to the probes, whatever happened.
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer stop()
			if err := post(cleanupCtx, cfg.vkDebug+"/debug/readiness?pod="+cfg.namespace+"/"+cfg.guest+"&ready=clear"); err != nil {
				fmt.Fprintln(os.Stderr, "clear override:", err)
			}
		}
	}()

	// Start from Ready, with every observer agreeing.
	if err := trig(ctx, true); err != nil {
		return err
	}
	if err := waitAll(ctx, obs, cfg, time.Now().Add(-time.Hour), true); err != nil {
		return fmt.Errorf("initial Ready state: %w", err)
	}
	var results []edgeResult
	enc := json.NewEncoder(os.Stdout)
	for i := range 2 * cfg.edges {
		sleep(ctx, cfg.settle+rand.N(cfg.jitter+1)) //nolint:gosec // jitter, not security
		if ctx.Err() != nil {
			return ctx.Err()
		}
		want := i%2 == 1 // falling first
		res, err := measureEdge(ctx, cfg, obs, trig, i, want)
		if err != nil {
			return err
		}
		results = append(results, res)
		if err := enc.Encode(res); err != nil {
			return fmt.Errorf("write result: %w", err)
		}
	}
	printSummary(os.Stdout, results)
	return nil
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster config: %w", err)
		}
		return cfg, nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	return cfg, nil
}

// watch starts the pod and EndpointSlice watches and returns the observers.
func watch(ctx context.Context, client kubernetes.Interface, cfg *config) (*observers, error) {
	obs := &observers{pod: &history{}, slice: &history{}, cip: &history{}, pool: &history{}}
	podFactory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithNamespace(cfg.namespace),
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.FieldSelector = fields.OneTermEqualSelector("metadata.name", cfg.guest).String()
		}))
	// The pod IP comes from the lister: handlers may still be catching up after the cache sync.
	podLister := podFactory.Core().V1().Pods().Lister()
	obs.podIP = func() string {
		pod, err := podLister.Pods(cfg.namespace).Get(cfg.guest)
		if err != nil {
			return ""
		}
		return pod.Status.PodIP
	}
	onPod := func(obj any) {
		now := time.Now()
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return
		}
		obs.pod.set(now, probe.PodReady(pod))
	}
	if _, err := podFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onPod,
		UpdateFunc: func(_, obj any) { onPod(obj) },
	}); err != nil {
		return nil, fmt.Errorf("pod watch: %w", err)
	}

	sliceFactory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithNamespace(cfg.namespace),
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.LabelSelector = labels.Set{discoveryv1.LabelServiceName: cfg.service}.String()
		}))
	sliceLister := sliceFactory.Discovery().V1().EndpointSlices().Lister()
	onSlice := func(any) {
		now := time.Now()
		all, err := sliceLister.List(labels.Everything())
		if err != nil {
			return
		}
		obs.slice.set(now, endpointReady(all, cfg.guest))
	}
	if _, err := sliceFactory.Discovery().V1().EndpointSlices().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onSlice,
		UpdateFunc: func(_, obj any) { onSlice(obj) },
		DeleteFunc: onSlice,
	}); err != nil {
		return nil, fmt.Errorf("EndpointSlice watch: %w", err)
	}
	podFactory.Start(ctx.Done())
	sliceFactory.Start(ctx.Done())
	podFactory.WaitForCacheSync(ctx.Done())
	sliceFactory.WaitForCacheSync(ctx.Done())
	return obs, nil
}

// endpointReady reports whether any slice has a ready endpoint for the guest pod.
func endpointReady(all []*discoveryv1.EndpointSlice, guest string) bool {
	for _, slice := range all {
		for _, ep := range slice.Endpoints {
			if ep.TargetRef == nil || ep.TargetRef.Kind != "Pod" || ep.TargetRef.Name != guest {
				continue
			}
			if ep.Conditions.Ready == nil || *ep.Conditions.Ready {
				return true
			}
		}
	}
	return false
}

// poll records the outcome of check every interval, stamped with the time the attempt started.
func poll(ctx context.Context, interval time.Duration, hist *history, check func(context.Context) bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		started := time.Now()
		hist.set(started, check(ctx))
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

var noKeepAlive = &http.Client{
	Transport: &http.Transport{DisableKeepAlives: true},
	Timeout:   500 * time.Millisecond,
}

func httpAny(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return false
	}
	resp, err := noKeepAlive.Do(req)
	if err != nil {
		return false
	}
	drain(resp)
	return true
}

var routerClient = &http.Client{Timeout: 2 * time.Second}

// drain reads and closes a response body. Errors only mean the connection is not reused.
func drain(resp *http.Response) {
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)); err != nil {
		fmt.Fprintln(os.Stderr, "drain:", err)
	}
	if err := resp.Body.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "close:", err)
	}
}

func routerOK(ctx context.Context, url, model string) bool {
	body := fmt.Sprintf(`{"model":%q,"prompt":"q5","max_tokens":1}`, model)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := routerClient.Do(req)
	if err != nil {
		return false
	}
	drain(resp)
	return resp.StatusCode == http.StatusOK
}

func post(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", url, err)
	}
	defer resp.Body.Close()
	msg, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return fmt.Errorf("post %s: read: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("post %s: %d %s", url, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// trigger returns the function that causes a readiness change.
func trigger(cfg *config, obs *observers) func(context.Context, bool) error {
	if cfg.mode == "force" {
		return func(ctx context.Context, ready bool) error {
			return post(ctx, fmt.Sprintf("%s/debug/readiness?pod=%s/%s&ready=%t", cfg.vkDebug, cfg.namespace, cfg.guest, ready))
		}
	}
	return func(ctx context.Context, ready bool) error {
		base := cfg.toggleURL
		if base == "" {
			ip := obs.podIP()
			if ip == "" {
				return errors.New("guest has no pod IP yet")
			}
			base = "http://" + net.JoinHostPort(ip, strconv.Itoa(cfg.togglePort))
		}
		verb := "down"
		if ready {
			verb = "up"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/cgi-bin/"+verb, http.NoBody)
		if err != nil {
			return fmt.Errorf("request: %w", err)
		}
		resp, err := noKeepAlive.Do(req)
		if err != nil {
			return fmt.Errorf("toggle %s: %w", verb, err)
		}
		drain(resp)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("toggle %s: HTTP %d", verb, resp.StatusCode)
		}
		return nil
	}
}

func (o *observers) active(cfg *config) map[string]*history {
	out := map[string]*history{"pod": o.pod, "slice": o.slice, "cip": o.cip}
	if cfg.routerURL != "" {
		out["pool"] = o.pool
	}
	return out
}

// waitAll waits until every observer has changed to state at or after since, or is already
// there without a change since (the initial state).
func waitAll(ctx context.Context, obs *observers, cfg *config, since time.Time, state bool) error {
	deadline := time.Now().Add(cfg.timeout)
	for time.Now().Before(deadline) {
		done := true
		for _, hist := range obs.active(cfg) {
			last, known := hist.current()
			if !known || last.state != state {
				done = false
				break
			}
			if _, seen := hist.firstSince(since, state); !seen {
				done = false
				break
			}
		}
		if done {
			return nil
		}
		sleep(ctx, 10*time.Millisecond)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	var lagging []string
	for name, hist := range obs.active(cfg) {
		if last, _ := hist.current(); last.state != state {
			lagging = append(lagging, name)
		}
	}
	sort.Strings(lagging)
	return fmt.Errorf("timed out waiting for ready=%t; not there: %v", state, lagging)
}

func measureEdge(
	ctx context.Context, cfg *config, obs *observers, trig func(context.Context, bool) error, index int, want bool,
) (edgeResult, error) {
	start := time.Now()
	if err := trig(ctx, want); err != nil {
		return edgeResult{}, err
	}
	res := edgeResult{Index: index, Ready: want, Trigger: start.UnixNano(), Spans: map[string]float64{}}
	waitErr := waitAll(ctx, obs, cfg, start, want)
	stamps := map[string]time.Time{}
	for name, hist := range obs.active(cfg) {
		if at, ok := hist.firstSince(start, want); ok {
			stamps[name] = at
		} else {
			res.Missing = append(res.Missing, name)
		}
	}
	if at, ok := notifyTime(ctx, cfg, start, want); ok {
		stamps["notify"] = at
	} else {
		res.Missing = append(res.Missing, "notify")
	}
	sort.Strings(res.Missing)
	span := func(name, from, to string) {
		t0, ok0 := stamps[from]
		t1, ok1 := stamps[to]
		if ok0 && ok1 {
			res.Spans[name] = t1.Sub(t0).Seconds()
		}
	}
	stamps["trigger"] = start
	span("trigger_notify", "trigger", "notify")
	span("e1", "notify", "pod")
	span("e2", "pod", "slice")
	span("e3", "slice", "cip")
	span("e2_e3", "pod", "cip")
	span("ready_pool", "pod", "pool")
	span("trigger_pool", "trigger", "pool")
	if waitErr != nil && ctx.Err() != nil {
		return res, ctx.Err()
	}
	return res, nil
}

// notifyTime reads the guest kubelet's edge log and returns the first edge to want at or after
// since.
func notifyTime(ctx context.Context, cfg *config, since time.Time, want bool) (time.Time, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/debug/ready-edges?pod=%s/%s", cfg.vkDebug, cfg.namespace, cfg.guest), http.NoBody)
	if err != nil {
		return time.Time{}, false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return time.Time{}, false
	}
	defer resp.Body.Close()
	var edges []probe.Edge
	if err := json.NewDecoder(resp.Body).Decode(&edges); err != nil {
		return time.Time{}, false
	}
	for _, edge := range edges {
		if edge.Ready == want && !edge.Time.Before(since) {
			return edge.Time, true
		}
	}
	return time.Time{}, false
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

var spanOrder = []string{"trigger_notify", "e1", "e2", "e3", "e2_e3", "ready_pool", "trigger_pool"}

// printSummary prints n, min, p50, mean, p90 and max per span and direction, in seconds.
func printSummary(out io.Writer, results []edgeResult) {
	fmt.Fprintf(out, "%-8s %-15s %4s %8s %8s %8s %8s %8s\n", "edge", "span", "n", "min", "p50", "mean", "p90", "max")
	for _, dir := range []bool{true, false} {
		name := "rising"
		if !dir {
			name = "falling"
		}
		for _, span := range spanOrder {
			var vals []float64
			for _, res := range results {
				if val, ok := res.Spans[span]; ok && res.Ready == dir {
					vals = append(vals, val)
				}
			}
			if len(vals) == 0 {
				continue
			}
			slices.Sort(vals)
			var sum float64
			for _, val := range vals {
				sum += val
			}
			fmt.Fprintf(out, "%-8s %-15s %4d %8.3f %8.3f %8.3f %8.3f %8.3f\n", name, span, len(vals),
				vals[0], quantile(vals, 0.5), sum/float64(len(vals)), quantile(vals, 0.9), vals[len(vals)-1])
		}
	}
}

// quantile is the nearest-rank quantile of sorted vals.
func quantile(vals []float64, q float64) float64 {
	idx := int(q*float64(len(vals))+0.5) - 1
	idx = max(0, min(idx, len(vals)-1))
	return vals[idx]
}
