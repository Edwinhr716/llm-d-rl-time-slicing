package server_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// foregroundWaitSamples returns the sample count of
// timeslice_foreground_wait_seconds for group.
func foregroundWaitSamples(t *testing.T, group string) uint64 {
	t.Helper()
	n, _ := foregroundWaitSeries(t, group)
	return n
}

// foregroundWaitSeries also reports whether the series exists.
func foregroundWaitSeries(t *testing.T, group string) (uint64, bool) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(metrics.ForegroundWaitSeconds)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "group_id" && l.GetValue() == group {
					return m.GetHistogram().GetSampleCount(), true
				}
			}
		}
	}
	return 0, false
}

// TestORCHA5_ForegroundWaitSeconds: a granted foreground Acquire observes its
// wait once; one that times out observes nothing.
func TestORCHA5_ForegroundWaitSeconds(t *testing.T) {
	ctx := context.Background()
	lockStore := store.NewMemLockStore()
	const granted, waiting = "orcha5-fg", "orcha5-fg-wait"
	if err := lockStore.Lock(ctx, granted, "job-1"); err != nil {
		t.Fatal(err)
	}
	gs := store.NewGroupStore(lockStore)
	g, _, err := gs.GetOrCreate(ctx, granted)
	if err != nil {
		t.Fatal(err)
	}
	g.Status().SetLoadedJob("job-1")
	if _, _, err := gs.GetOrCreate(ctx, waiting); err != nil {
		t.Fatal(err)
	}

	_, _, cleanup := server.InitGRPCServer(gs, store.NewJobStore())
	defer cleanup()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(server.BufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewTimeSliceOrchestratorServiceClient(conn)

	before := foregroundWaitSamples(t, granted)
	resp, err := client.Acquire(ctx, &pb.AcquireRequest{GroupId: granted, JobId: "job-1"})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Acquire = %v, %v; want success", resp, err)
	}
	if got := foregroundWaitSamples(t, granted) - before; got != 1 {
		t.Errorf("foreground_wait_seconds samples = %d, want 1", got)
	}

	beforeWait := foregroundWaitSamples(t, waiting)
	tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := client.Acquire(tctx, &pb.AcquireRequest{GroupId: waiting, JobId: "job-1"}); err == nil {
		t.Fatal("Acquire on a group that never loads succeeded")
	}
	if got := foregroundWaitSamples(t, waiting) - beforeWait; got != 0 {
		t.Errorf("foreground_wait_seconds samples = %d after a failed Acquire, want 0", got)
	}
	// The series exists at zero from the first Acquire, so a backend that takes
	// the first sample as the baseline counts the first grant.
	if _, ok := foregroundWaitSeries(t, waiting); !ok {
		t.Error("no foreground_wait_seconds series at zero for a group with a waiting Acquire")
	}
}
