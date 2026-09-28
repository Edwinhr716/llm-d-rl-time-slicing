package server_test

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Tests for decision D-NS-4, option "keep": server log lines and faults.

type ns4Logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *ns4Logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *ns4Logs) contains(parts ...string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for line := range strings.SplitSeq(l.buf.String(), "\n") {
		all := true
		for _, part := range parts {
			all = all && strings.Contains(line, part)
		}
		if all {
			return true
		}
	}
	return false
}

func captureNS4Logs(t *testing.T) *ns4Logs {
	t.Helper()
	logs := &ns4Logs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logs
}

func ns4Client(t *testing.T, gs server.GroupStore, js server.JobStore) pb.TimeSliceOrchestratorServiceClient {
	t.Helper()
	_, _, cleanup := server.InitGRPCServer(gs, js,
		server.WithBackgroundRole(true), server.WithNoticeTiming(30*time.Second, 3*time.Second))
	t.Cleanup(cleanup)
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(server.BufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewTimeSliceOrchestratorServiceClient(conn)
}

// TestNS4_Keep_BackgroundFaultNeverFaultsGroup: a FAULTED background guest
// does not make the group FAULTED; the foreground Acquire succeeds and logs
// "Foreground granted".
func TestNS4_Keep_BackgroundFaultNeverFaultsGroup(t *testing.T) {
	logs := captureNS4Logs(t)
	gs, group := backgroundGroup(t, "trainer", true)
	js := store.NewJobStore()
	guest := store.NewJob(bgGroup, "guest-1")
	guest.SetBackground(true)
	guest.UpdateContextState(bgNode, pb.SnapshotAgentJobState_STATE_FAULTED)
	if err := js.Put(context.Background(), guest); err != nil {
		t.Fatalf("Put: %v", err)
	}
	client := ns4Client(t, gs, js)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Acquire(ctx, &pb.AcquireRequest{JobId: "trainer", GroupId: bgGroup})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatal("Acquire not successful")
	}
	if group.Spec().LockingJob() != "trainer" {
		t.Fatalf("locking job = %q", group.Spec().LockingJob())
	}
	if !logs.contains(`"msg":"Foreground granted"`, `"group":"group-1"`, `"job":"trainer"`, `"waited_ms"`) {
		t.Error(`missing "Foreground granted" log line`)
	}
}

// TestNS4_Keep_ForegroundAcquireLogsVacateStarted: a foreground Acquire while
// a participant holds a grant starts the notice, logs "Vacate started" with
// the held hosts, and stays blocked (fail closed).
func TestNS4_Keep_ForegroundAcquireLogsVacateStarted(t *testing.T) {
	logs := captureNS4Logs(t)
	gs, group := backgroundGroup(t, "", false)
	group.Spec().RegisterParticipant(bgNode, bgParticipant, time.Now())
	group.Spec().Grant(bgNode)
	client := ns4Client(t, gs, store.NewJobStore())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := client.Acquire(ctx, &pb.AcquireRequest{JobId: "trainer", GroupId: bgGroup}); err == nil {
		t.Fatal("foreground Acquire succeeded while the background holds a grant")
	}
	if !logs.contains(`"msg":"Vacate started"`, `"hosts":["node-a"]`, `"deadline"`) {
		t.Error(`missing "Vacate started" log line`)
	}
	if group.Spec().NoticeAt().IsZero() {
		t.Error("no notice running")
	}
	if !slices.Contains(group.Spec().NoticeHosts(), bgNode) {
		t.Errorf("notice hosts = %v, want %s", group.Spec().NoticeHosts(), bgNode)
	}

	// The VK's Yield(ROLE_BACKGROUND) during the notice is remembered for the
	// "Host clear" line.
	yctx, ycancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ycancel()
	if _, err := client.Yield(yctx, &pb.YieldRequest{
		JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND,
	}); err != nil {
		t.Fatalf("background Yield: %v", err)
	}
	if !group.Spec().YieldedInNotice(bgNode) {
		t.Error("background Yield during the notice not recorded")
	}
}
