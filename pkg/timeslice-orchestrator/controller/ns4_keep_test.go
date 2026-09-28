package controller_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/protobuf/encoding/protowire"
	"k8s.io/client-go/util/workqueue"
)

// Tests for decision D-NS-4, option "keep": the minimal lend / notice /
// vacate / grant path with the VK as a lock participant.

const (
	ns4Group = "group-1"
	ns4Node  = "node-1"
	ns4VK    = "vk/node-1"
	ns4Fg    = "trainer"
	ns4Guest = "guest-1"
)

// ns4Agent is a scripted snapshot agent for one node. Snapshot and Restore
// complete at once.
type ns4Agent struct {
	mu        sync.Mutex
	states    map[string]agentpb.JobState
	killed    map[string]bool
	snapshots int
	restores  int
}

func newNS4Agent() *ns4Agent {
	return &ns4Agent{states: make(map[string]agentpb.JobState), killed: make(map[string]bool)}
}

func (a *ns4Agent) set(job string, state agentpb.JobState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.states[job] = state
}

func (a *ns4Agent) setKilled(job string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.killed[job] = true
}

// ns4Counts is how many Snapshot and Restore calls the agent received.
type ns4Counts struct{ snapshots, restores int }

func (a *ns4Agent) counts() ns4Counts {
	a.mu.Lock()
	defer a.mu.Unlock()
	return ns4Counts{snapshots: a.snapshots, restores: a.restores}
}

func (a *ns4Agent) store() *controller.MockSnapshotAgentStore {
	return &controller.MockSnapshotAgentStore{
		GetStatusFunc: func(context.Context, string) (*agentpb.StatusResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			resp := &agentpb.StatusResponse{}
			for job, state := range a.states {
				js := &agentpb.JobStatus{JobId: job, State: state}
				if a.killed[job] {
					// JobStatus.last_outcome (field 3) = OUTCOME_KILLED (4), as a
					// newer agent sends it.
					raw := protowire.AppendTag(nil, 3, protowire.VarintType)
					js.ProtoReflect().SetUnknown(protowire.AppendVarint(raw, 4))
				}
				resp.JobStatuses = append(resp.JobStatuses, js)
			}
			return resp, nil
		},
		SnapshotFunc: func(_ context.Context, _, jobID, _ string) (*agentpb.SnapshotResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.snapshots++
			a.states[jobID] = agentpb.JobState_JOB_STATE_SAVED
			return &agentpb.SnapshotResponse{OperationId: "snap-" + jobID}, nil
		},
		RestoreFunc: func(_ context.Context, _, jobID, _ string) (*agentpb.RestoreResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.restores++
			a.states[jobID] = agentpb.JobState_JOB_STATE_RUNNING
			return &agentpb.RestoreResponse{OperationId: "restore-" + jobID}, nil
		},
		OperationFunc: func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
			return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE}, nil
		},
	}
}

// ns4Logs captures slog output for the duration of a test.
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
			if !strings.Contains(line, part) {
				all = false
				break
			}
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

type ns4Fixture struct {
	ctrl     *controller.Controller
	group    *store.Group
	jobStore *store.JobStore
	agent    *ns4Agent
}

// newNS4Fixture builds a one-node group with a foreground job in fgState.
func newNS4Fixture(t *testing.T, ctx context.Context, fgState agentpb.JobState) *ns4Fixture {
	t.Helper()
	groupStore := store.NewGroupStore(store.NewMemLockStore())
	jobStore := store.NewJobStore()
	queue := &trackQueue{
		TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "ns4"},
		),
	}
	group, _, err := groupStore.GetOrCreate(ctx, ns4Group)
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	group.Status().SetNodes([]string{ns4Node})
	if err := jobStore.Put(ctx, store.NewJob(ns4Group, ns4Fg)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	agent := newNS4Agent()
	agent.set(ns4Fg, fgState)
	infra := &mockInfrastructureOrchestrator{observeFunc: func(context.Context, string) error { return nil }}
	c := controller.NewController(groupStore, jobStore, queue, infra, agent.store())
	return &ns4Fixture{ctrl: c, group: group, jobStore: jobStore, agent: agent}
}

// addGuest adds a background (mirror) job with a pod on the node.
func (fx *ns4Fixture) addGuest(t *testing.T, ctx context.Context, state agentpb.JobState) {
	t.Helper()
	job := store.NewJob(ns4Group, ns4Guest)
	job.SetBackground(true)
	job.SetPodNodes([]string{ns4Node})
	if err := fx.jobStore.Put(ctx, job); err != nil {
		t.Fatalf("Put: %v", err)
	}
	fx.agent.set(ns4Guest, state)
}

func (fx *ns4Fixture) run(t *testing.T, ctx context.Context) {
	t.Helper()
	go func() {
		if err := fx.ctrl.Run(ctx, 1); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	fx.ctrl.EnqueueWork(ns4Group)
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	if err := waitWithTimeout(cond, timeout); err != nil {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestNS4_Keep_LendSnapshotsForegroundThenGrants: after a foreground Yield
// with a lend hint and a participant blocked in Acquire(ROLE_BACKGROUND), the
// foreground is snapshotted off the accelerator and then the node is granted.
func TestNS4_Keep_LendSnapshotsForegroundThenGrants(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	fx.group.Spec().SetActiveJob(ns4Fg)
	fx.group.Spec().SetLend(true)
	fx.group.Spec().RegisterParticipant(ns4Node, ns4VK, time.Now())
	fx.run(t, ctx)

	eventually(t, "the grant", 10*time.Second, func() bool { return fx.group.Spec().Granted(ns4Node) })
	if snaps := fx.agent.counts().snapshots; snaps != 1 {
		t.Errorf("snapshots = %d, want 1", snaps)
	}
	if fx.group.Spec().Lend() {
		t.Error("lend hint still set after every node was granted")
	}
	eventually(t, "Resume started", 2*time.Second, func() bool {
		return logs.contains(`"msg":"Resume started"`, `"group":"group-1"`, `"node":"node-1"`)
	})
}

// TestNS4_Keep_NoLendWithoutParticipant: the lend hint alone lends nothing.
func TestNS4_Keep_NoLendWithoutParticipant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	fx.group.Spec().SetActiveJob(ns4Fg)
	fx.group.Spec().SetLend(true)
	fx.run(t, ctx)

	time.Sleep(1500 * time.Millisecond)
	if snaps := fx.agent.counts().snapshots; snaps != 0 {
		t.Errorf("snapshots = %d, want 0", snaps)
	}
	if got := fx.group.Spec().ActiveJob(); got != ns4Fg {
		t.Errorf("active job = %q, want %q", got, ns4Fg)
	}
	if !fx.group.Spec().Lend() {
		t.Error("lend hint cleared although no foreground job asked for the lock")
	}
}

// TestNS4_Keep_ForegroundAcquireCancelsLend: a foreground job waiting for the
// lock ends the lend; nothing is granted.
func TestNS4_Keep_ForegroundAcquireCancelsLend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	fx.group.Spec().SetActiveJob(ns4Fg)
	fx.group.Spec().RegisterParticipant(ns4Node, ns4VK, time.Now())
	fx.group.Spec().RequestLock(ns4Fg)
	fx.group.Spec().SetLend(true)
	fx.run(t, ctx)

	eventually(t, "the foreground lock", 5*time.Second, func() bool { return fx.group.Spec().LockingJob() == ns4Fg })
	time.Sleep(500 * time.Millisecond)
	if fx.group.Spec().Granted(ns4Node) {
		t.Error("node granted while a foreground job wants the lock")
	}
	if fx.group.Spec().Lend() {
		t.Error("lend hint still set while a foreground job wants the lock")
	}
}

// TestNS4_Keep_NoticeHoldsRestoreUntilYield: a foreground Acquire while the
// node is granted starts a notice; the trainer is not restored until the
// participant yields, and the notice ends when the host is clear.
func TestNS4_Keep_NoticeHoldsRestoreUntilYield(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	spec := fx.group.Spec()
	spec.RegisterParticipant(ns4Node, ns4VK, time.Now())
	spec.Grant(ns4Node)
	spec.RequestLock(ns4Fg)
	fx.run(t, ctx)

	eventually(t, "the notice", 5*time.Second, func() bool { return !spec.NoticeAt().IsZero() })
	eventually(t, "Vacate started", 2*time.Second, func() bool {
		return logs.contains(`"msg":"Vacate started"`, `"hosts":["node-1"]`, `"deadline"`)
	})
	time.Sleep(1500 * time.Millisecond)
	if restores := fx.agent.counts().restores; restores != 0 {
		t.Fatalf("restores = %d while the node is granted, want 0", restores)
	}
	if got := fx.group.Status().LoadedJob(); got != "" {
		t.Fatalf("loaded job = %q while the node is granted, want none", got)
	}

	// The VK vacates and hands the grant back (Yield(ROLE_BACKGROUND)).
	spec.ClearGrant(ns4Node)
	fx.ctrl.EnqueueWork(ns4Group)

	eventually(t, "the restore", 10*time.Second, func() bool { return fx.agent.counts().restores == 1 })
	eventually(t, "the trainer loaded", 5*time.Second, func() bool { return fx.group.Status().LoadedJob() == ns4Fg })
	if !spec.NoticeAt().IsZero() {
		t.Error("notice still running after the host cleared")
	}
	if !logs.contains(`"msg":"Host clear"`, `"node":"node-1"`, `"how":"yield"`) {
		t.Error(`missing "Host clear" how=yield`)
	}
}

// TestNS4_Keep_LiveGuestHoldsRestoreUntilSuspended: a guest the agent reports
// RUNNING keeps the node busy even with no grant held (fail closed); the
// agent's JOB_STATE_SUSPENDED (6) clears it.
func TestNS4_Keep_LiveGuestHoldsRestoreUntilSuspended(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	fx.addGuest(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	fx.group.Spec().RequestLock(ns4Fg)
	fx.run(t, ctx)

	eventually(t, "the notice", 5*time.Second, func() bool { return !fx.group.Spec().NoticeAt().IsZero() })
	time.Sleep(1500 * time.Millisecond)
	if cnt := fx.agent.counts(); cnt.restores != 0 || cnt.snapshots != 0 {
		t.Fatalf("snapshots, restores = %d, %d while the guest is live, want 0, 0", cnt.snapshots, cnt.restores)
	}

	fx.agent.set(ns4Guest, agentpb.JobState(6))
	eventually(t, "the restore", 10*time.Second, func() bool { return fx.agent.counts().restores == 1 })
	guest, err := fx.jobStore.Get(ctx, ns4Group, ns4Guest)
	if err != nil {
		t.Fatalf("Get guest: %v", err)
	}
	if got := guest.ContextState()[ns4Node]; got != pb.SnapshotAgentJobState_STATE_SUSPENDED {
		t.Errorf("guest state = %v, want STATE_SUSPENDED", got)
	}
	if !logs.contains(`"msg":"Host clear"`, `"how":"agent-status"`) {
		t.Error(`missing "Host clear" how=agent-status`)
	}
}

// TestNS4_Keep_KilledGuestCountsAsVacated: last_outcome = OUTCOME_KILLED on
// the agent counts as vacated.
func TestNS4_Keep_KilledGuestCountsAsVacated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	fx.addGuest(t, ctx, agentpb.JobState_JOB_STATE_IDLE)
	fx.agent.setKilled(ns4Guest)
	fx.group.Spec().RequestLock(ns4Fg)
	fx.run(t, ctx)

	eventually(t, "the restore", 10*time.Second, func() bool { return fx.agent.counts().restores == 1 })
}

// TestNS4_Keep_HostNotClearAtDeadline: a participant that never yields is
// reported once at T; there is no kill and the trainer stays unrestored.
func TestNS4_Keep_HostNotClearAtDeadline(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	fx.ctrl.NoticeWindow = 2 * time.Second
	fx.ctrl.KillBudget = time.Second
	spec := fx.group.Spec()
	spec.RegisterParticipant(ns4Node, ns4VK, time.Now())
	spec.Grant(ns4Node)
	spec.RequestLock(ns4Fg)
	fx.run(t, ctx)

	eventually(t, "Host not clear at deadline", 6*time.Second, func() bool {
		return logs.contains(`"level":"WARN"`, `"msg":"Host not clear at deadline"`, `"node":"node-1"`)
	})
	if restores := fx.agent.counts().restores; restores != 0 {
		t.Errorf("restores = %d while the node is still granted, want 0", restores)
	}
}

// TestNS4_Keep_BackgroundJobNeverSnapshotted: the foreground loop never
// snapshots or restores a background guest.
func TestNS4_Keep_BackgroundJobNeverSnapshotted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_IDLE)
	fx.addGuest(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	spec := fx.group.Spec()
	spec.RegisterParticipant(ns4Node, ns4VK, time.Now())
	spec.Grant(ns4Node)
	fx.run(t, ctx)

	time.Sleep(1500 * time.Millisecond)
	if cnt := fx.agent.counts(); cnt.snapshots != 0 || cnt.restores != 0 {
		t.Errorf("snapshots, restores = %d, %d, want 0, 0", cnt.snapshots, cnt.restores)
	}
	if got := spec.ActiveJob(); got == ns4Guest {
		t.Errorf("background job deduced as the active job")
	}
}
