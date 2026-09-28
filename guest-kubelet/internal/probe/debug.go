package probe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Edge is one change of a guest's Ready condition, as handed to the pod controller.
type Edge struct {
	Time  time.Time `json:"time"`
	Ready bool      `json:"ready"`
}

// EdgeLog remembers, per guest, when the guest kubelet handed the pod controller a status whose
// Ready condition differed from the previous one. That call is where the Q5 span e1 starts
// (e1 = notify call -> status visible in the API); the measurement tool reads the times back
// through the debug endpoint. Only the last size edges per guest are kept.
type EdgeLog struct {
	size int

	mu    sync.Mutex
	last  map[string]bool
	edges map[string][]Edge
}

// NewEdgeLog keeps up to size edges per guest.
func NewEdgeLog(size int) *EdgeLog {
	return &EdgeLog{size: size, last: map[string]bool{}, edges: map[string][]Edge{}}
}

// Observe records an edge if pod's Ready condition differs from the last one seen for it.
// It returns the edge and whether one was recorded.
func (l *EdgeLog) Observe(pod *corev1.Pod) (Edge, bool) {
	now := time.Now()
	ready := PodReady(pod)
	key := podKey(pod.Namespace, pod.Name)
	l.mu.Lock()
	defer l.mu.Unlock()
	if prev, seen := l.last[key]; seen && prev == ready {
		return Edge{}, false
	}
	l.last[key] = ready
	edge := Edge{Time: now, Ready: ready}
	l.edges[key] = append(l.edges[key], edge)
	if extra := len(l.edges[key]) - l.size; extra > 0 {
		l.edges[key] = l.edges[key][extra:]
	}
	return edge, true
}

// Edges returns a copy of the edges recorded for namespace/name, oldest first.
func (l *EdgeLog) Edges(namespace, name string) []Edge {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Edge(nil), l.edges[podKey(namespace, name)]...)
}

// PodReady reports whether the pod's Ready condition is True.
func PodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// DebugHandler serves the M2 test hooks:
//
//	POST /debug/readiness?pod=<ns>/<name>&ready=true|false|clear  force a guest's readiness, or clear it
//	GET  /debug/ready-edges?pod=<ns>/<name>                        the EdgeLog for that guest (JSON)
//
// Forcing readiness changes only what the guest kubelet reports; the process is untouched. It
// exists so the Ready -> endpoint -> route path can be measured 20+ times quickly.
func DebugHandler(mgr *Manager, edges *EdgeLog) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/readiness", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		pod, err := splitPod(r.URL.Query().Get("pod"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var forced *bool
		if val := r.URL.Query().Get("ready"); val != "clear" {
			parsed, err := strconv.ParseBool(val)
			if err != nil {
				http.Error(w, "ready must be true, false or clear", http.StatusBadRequest)
				return
			}
			forced = &parsed
		}
		at := time.Now()
		mgr.SetOverride(pod.Namespace, pod.Name, forced)
		writeJSON(r.Context(), w, map[string]any{"pod": pod.String(), "ready": r.URL.Query().Get("ready"), "time": at})
	})
	mux.HandleFunc("/debug/ready-edges", func(w http.ResponseWriter, r *http.Request) {
		pod, err := splitPod(r.URL.Query().Get("pod"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(r.Context(), w, edges.Edges(pod.Namespace, pod.Name))
	})
	return mux
}

func writeJSON(ctx context.Context, w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.G(ctx).WithError(err).Warn("debug endpoint: write response")
	}
}

func splitPod(val string) (types.NamespacedName, error) {
	namespace, name, found := strings.Cut(val, "/")
	if !found || namespace == "" || name == "" {
		return types.NamespacedName{}, fmt.Errorf("pod must be <namespace>/<name>, got %q", val)
	}
	return types.NamespacedName{Namespace: namespace, Name: name}, nil
}

// CheckLoopback returns an error unless addr (host:port) listens on a loopback address only.
// The debug endpoint can force a guest Ready, so it must not be reachable from the network.
func CheckLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("debug address %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("debug address %q must be a loopback address (127.0.0.1, ::1 or localhost)", addr)
}

// Serve runs handler on addr until ctx ends. A bind failure is retried every second: with
// leader election and host networking, the previous leader may still hold the port briefly.
func Serve(ctx context.Context, addr string, handler http.Handler) {
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.G(ctx).WithError(err).Warn("debug endpoint: shutdown")
		}
	}()
	var lc net.ListenConfig
	for ctx.Err() == nil {
		ln, err := lc.Listen(ctx, "tcp", addr)
		if err != nil {
			log.G(ctx).WithError(err).WithField("addr", addr).Warn("debug endpoint: listen failed; retrying")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		log.G(ctx).WithField("addr", addr).Info("debug endpoint listening")
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.G(ctx).WithError(err).Warn("debug endpoint stopped")
		}
		return
	}
}
