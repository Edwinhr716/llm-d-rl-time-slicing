package probe_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/edwinhr716/guest-kubelet/internal/probe"
)

func podWithReady(ready corev1.ConditionStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "g", Namespace: "ns"},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: ready}}},
	}
}

func TestEdgeLog(t *testing.T) {
	t.Parallel()
	log := probe.NewEdgeLog(2)
	steps := []struct {
		status corev1.ConditionStatus
		edge   bool
	}{
		{corev1.ConditionFalse, true}, // first status seen is an edge
		{corev1.ConditionFalse, false},
		{corev1.ConditionTrue, true},
		{corev1.ConditionTrue, false},
		{corev1.ConditionFalse, true},
	}
	for i, step := range steps {
		if _, edge := log.Observe(podWithReady(step.status)); edge != step.edge {
			t.Fatalf("step %d: edge %t, want %t", i, edge, step.edge)
		}
	}
	edges := log.Edges("ns", "g")
	if len(edges) != 2 || !edges[0].Ready || edges[1].Ready {
		t.Errorf("want the last two edges (true, false), got %+v", edges)
	}
}

func TestDebugHandler(t *testing.T) {
	t.Parallel()
	mgr := probe.NewManager(t.Context(), probe.Options{})
	edges := probe.NewEdgeLog(8)
	edges.Observe(podWithReady(corev1.ConditionTrue))
	handler := probe.DebugHandler(mgr, edges)
	guest := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "g", Namespace: "ns"}}

	do := func(method, target string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, target, http.NoBody))
		return rec
	}
	if code := do(http.MethodPost, "/debug/readiness?pod=ns/g&ready=false").Code; code != http.StatusOK {
		t.Fatalf("force false: %d", code)
	}
	if verdict, has := mgr.ContainerReady(guest, "any"); verdict || !has {
		t.Error("override false not applied")
	}
	if code := do(http.MethodPost, "/debug/readiness?pod=ns/g&ready=clear").Code; code != http.StatusOK {
		t.Fatalf("clear: %d", code)
	}
	if _, has := mgr.ContainerReady(guest, "any"); has {
		t.Error("override not cleared")
	}
	for _, bad := range []struct {
		method, target string
		code           int
	}{
		{http.MethodGet, "/debug/readiness?pod=ns/g&ready=true", http.StatusMethodNotAllowed},
		{http.MethodPost, "/debug/readiness?pod=g&ready=true", http.StatusBadRequest},
		{http.MethodPost, "/debug/readiness?pod=ns/g&ready=maybe", http.StatusBadRequest},
		{http.MethodGet, "/debug/ready-edges?pod=", http.StatusBadRequest},
	} {
		if code := do(bad.method, bad.target).Code; code != bad.code {
			t.Errorf("%s %s: %d, want %d", bad.method, bad.target, code, bad.code)
		}
	}
	rec := do(http.MethodGet, "/debug/ready-edges?pod=ns/g")
	var got []probe.Edge
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Ready {
		t.Errorf("edges: %+v", got)
	}
}

func TestCheckLoopback(t *testing.T) {
	t.Parallel()
	for addr, wantOK := range map[string]bool{
		"127.0.0.1:10261": true, "[::1]:10261": true, "localhost:10261": true,
		":10261": false, "0.0.0.0:10261": false, "10.0.0.1:10261": false, "no-port": false,
	} {
		if err := probe.CheckLoopback(addr); (err == nil) != wantOK {
			t.Errorf("CheckLoopback(%q) = %v, want ok %t", addr, err, wantOK)
		}
	}
}
