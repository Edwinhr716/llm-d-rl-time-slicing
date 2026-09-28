//go:build evalwire

package evalwire

import (
	"bytes"
	"context"
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// mainFlagNames returns the flag names declared in cmd/timesliceorchestrator/main.go.
func mainFlagNames(t *testing.T) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "../../../cmd/timesliceorchestrator/main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	var names []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "flag" {
			return true
		}
		switch sel.Sel.Name {
		case "Int", "String", "Duration", "Bool", "Float64", "Int64", "Uint", "Uint64":
		default:
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			t.Fatalf("main.go flag with a non-literal name at %v", call.Pos())
		}
		name, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Fatalf("unquote %s: %v", lit.Value, err)
		}
		names = append(names, name)
		return true
	})
	slices.Sort(names)
	return names
}

func TestEvalwire_FlagsMatchMain(t *testing.T) {
	fs, fv := newFlagSet()
	var got []string
	fs.VisitAll(func(f *flag.Flag) { got = append(got, f.Name) })
	slices.Sort(got)
	want := mainFlagNames(t)
	if len(want) == 0 {
		t.Fatal("found no flags in main.go")
	}
	if !slices.Equal(got, want) {
		t.Errorf("evalwire flags = %v\nmain.go flags  = %v", got, want)
	}
	if fv.nodeSelectorExemptBackground {
		t.Error("--node-selector-exempt-background default = true, want false")
	}
}

func TestEvalwire_RejectsBadArgs(t *testing.T) {
	cs := fake.NewClientset()
	for _, args := range [][]string{
		{"--node-selector=pool==("},
		{"--watch-namespaces=Bad_NS"},
		{"--node-selector-exempt-background=maybe"},
		{"--dispatch-budget-redis-addr=127.0.0.1:1"},
		{"--no-such-flag"},
		{"stray"},
	} {
		if orch, err := Start(context.Background(), Config{Clientset: cs, Args: args}); err == nil {
			orch.Stop()
			t.Errorf("Start(%v) succeeded, want an error", args)
		}
	}
	if _, err := Start(context.Background(), Config{}); err == nil {
		t.Error("Start without a clientset succeeded, want an error")
	}
}

// waitForGroup polls GetGroupStatus until the orchestrator knows the group,
// which needs the informers, the infrastructure orchestrator and the group
// store wired together.
func waitForGroup(t *testing.T, addr, groupID string) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("close conn: %v", err)
		}
	}()
	client := pb.NewTimeSliceOrchestratorServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "no-such-group"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetGroupStatus(no-such-group) = %v, want NotFound", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err = client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: groupID})
		if status.Code(err) != codes.NotFound {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("group %s still unknown after 15s: %v", groupID, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func checkMetrics(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Logf("close body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", resp.StatusCode)
	}
}

func TestEvalwire_StartStopRestart(t *testing.T) {
	const groupID = "g-eval"
	cs := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "127.0.0.1",
		Labels: map[string]string{infrastructure.NodeLabelPrefix + groupID: "true", "pool": "demo"},
	}})
	args := []string{
		"--controller-workers=4",
		"--resync-period=30s",
		"--lock-namespace=eval",
		"--lock-configmap=eval-locks",
		"--watch-namespaces=demo",
		"--node-selector=pool=demo",
		"--node-selector-exempt-background=true",
	}

	// The second Start on the same clientset models an orchestrator restart.
	for i := range 2 {
		orch, err := Start(context.Background(), Config{Clientset: cs, AgentPort: 1, Args: args})
		if err != nil {
			t.Fatalf("Start #%d: %v", i+1, err)
		}
		waitForGroup(t, orch.Addr, groupID)
		checkMetrics(t, orch.MetricsAddr)
		orch.Stop()
		orch.Stop() // idempotent
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent writers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func evalPod(name, job, node, role string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "demo",
			Name:      name,
			UID:       types.UID("uid-" + name),
			Labels:    map[string]string{infrastructure.PodLabelKey: "g1", infrastructure.JobLabelKey: job},
		},
		Spec: corev1.PodSpec{NodeName: node},
	}
	if role != "" {
		pod.Labels["timeslice.io/role"] = role
	}
	return pod
}

// TestEvalwire_GroupJobs checks GroupJobs for both values of
// --node-selector-exempt-background, on the D-ORCH-4 C4 shape: the trainer's
// host is selected, the host of a background pod is not.
func TestEvalwire_GroupJobs(t *testing.T) {
	trainer := JobInfo{JobID: "job-trainer", Role: "foreground"}
	mirror := JobInfo{JobID: "vk/node-other", Role: "background"}
	for _, tc := range []struct {
		exempt string
		want   []JobInfo
	}{
		{exempt: "false", want: []JobInfo{trainer}},
		{exempt: "true", want: []JobInfo{trainer, mirror}},
	} {
		t.Run("exempt="+tc.exempt, func(t *testing.T) {
			cs := fake.NewClientset(
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name:   "node-demo",
					Labels: map[string]string{infrastructure.NodeLabelPrefix + "g1": "true", "pool": "demo"},
				}},
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name:   "node-other",
					Labels: map[string]string{infrastructure.NodeLabelPrefix + "g1": "true", "pool": "other"},
				}},
				evalPod("trainer", "job-trainer", "node-demo", ""),
				evalPod("mirror", "vk/node-other", "node-other", "background"),
			)
			logs := &syncBuffer{}
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
			defer slog.SetDefault(prev)

			orch, err := Start(context.Background(), Config{Clientset: cs, AgentPort: 1, Args: []string{
				"--controller-workers=4",
				"--node-selector=pool=demo",
				"--node-selector-exempt-background=" + tc.exempt,
			}})
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer orch.Stop()
			if want := "nodeSelectorExemptBackground=" + tc.exempt; !strings.Contains(logs.String(), want) {
				t.Errorf("startup log lacks %q", want)
			}

			deadline := time.Now().Add(10 * time.Second)
			var got []JobInfo
			for {
				got = orch.GroupJobs("g1")
				if slices.Equal(got, tc.want) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("GroupJobs(g1) = %v, want %v", got, tc.want)
				}
				time.Sleep(50 * time.Millisecond)
			}
			// Steady state: the set does not change on later reconciles.
			time.Sleep(500 * time.Millisecond)
			if got = orch.GroupJobs("g1"); !slices.Equal(got, tc.want) {
				t.Errorf("GroupJobs(g1) later = %v, want %v", got, tc.want)
			}
			if got := orch.GroupJobs("no-such-group"); len(got) != 0 {
				t.Errorf("GroupJobs(no-such-group) = %v, want none", got)
			}
		})
	}
}
