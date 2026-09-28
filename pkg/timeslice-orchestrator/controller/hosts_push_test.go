package controller_test

import (
	"context"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"k8s.io/client-go/util/workqueue"
)

// fakeHosts is a HostCommander whose clear state the test sets.
type fakeHosts struct {
	mu       sync.Mutex
	clear    bool
	vacates  int
	resumes  int
	noticeAt time.Time
	events   *eventLog
}

func (f *fakeHosts) Clear(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clear
}

func (f *fakeHosts) Vacate(_ context.Context, _ string, _ []string, noticeAt time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.clear {
		f.vacates++
		f.noticeAt = noticeAt
	}
	return f.clear
}

func (f *fakeHosts) Resume(context.Context, string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumes++
	f.events.add("resume")
}

func (f *fakeHosts) setClear(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clear = v
}

//nolint:gocritic // The project configuration bans named returns, conflicting with unnamedResult
func (f *fakeHosts) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.vacates, f.resumes
}

// agentSim is a one-node snapshot agent whose operations complete at once.
type agentSim struct {
	mu     sync.Mutex
	states map[string]agentpb.JobState
	events *eventLog
}

func (a *agentSim) store() *controller.MockSnapshotAgentStore {
	return &controller.MockSnapshotAgentStore{
		GetStatusFunc: func(context.Context, string) (*agentpb.StatusResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			resp := &agentpb.StatusResponse{}
			for job, st := range a.states {
				resp.JobStatuses = append(resp.JobStatuses, &agentpb.JobStatus{JobId: job, State: st})
			}
			return resp, nil
		},
		SnapshotFunc: func(_ context.Context, _, jobID, _ string) (*agentpb.SnapshotResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.states[jobID] = agentpb.JobState_JOB_STATE_SAVED
			a.events.add("snapshot:" + jobID)
			return &agentpb.SnapshotResponse{OperationId: "op-s"}, nil
		},
		RestoreFunc: func(_ context.Context, _, jobID, _ string) (*agentpb.RestoreResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.states[jobID] = agentpb.JobState_JOB_STATE_RUNNING
			a.events.add("restore:" + jobID)
			return &agentpb.RestoreResponse{OperationId: "op-r"}, nil
		},
		OperationFunc: func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
			return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE}, nil
		},
	}
}

// eventLog records agent and host calls in order.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(ev string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

type pushFixture struct {
	ctrl   *controller.Controller
	group  *store.Group
	queue  *trackQueue
	hosts  *fakeHosts
	agent  *agentSim
	events *eventLog
}

func (f *pushFixture) eventList() []string {
	f.events.mu.Lock()
	defer f.events.mu.Unlock()
	return append([]string(nil), f.events.events...)
}

func newPushFixture(t *testing.T, ctx context.Context, trainer agentpb.JobState, withHosts bool) *pushFixture {
	t.Helper()
	groupStore := store.NewGroupStore(store.NewMemLockStore())
	jobStore := store.NewJobStore()
	queue := &trackQueue{
		TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "test"},
		),
	}
	group, _, err := groupStore.GetOrCreate(ctx, "g")
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	group.Status().SetNodes([]string{"node-1"})
	if err := jobStore.Put(ctx, store.NewJob("g", "trainer")); err != nil {
		t.Fatalf("put job: %v", err)
	}

	f := &pushFixture{group: group, queue: queue, events: &eventLog{}}
	f.agent = &agentSim{states: map[string]agentpb.JobState{"trainer": trainer}, events: f.events}
	f.hosts = &fakeHosts{events: f.events}
	f.ctrl = controller.NewController(groupStore, jobStore, queue, &mockInfrastructureOrchestrator{}, f.agent.store())
	if withHosts {
		f.ctrl.Hosts = f.hosts
	}
	go func() {
		if err := f.ctrl.Run(ctx, 1); err != nil {
			t.Errorf("controller Run: %v", err)
		}
	}()
	return f
}

// TestNS4_Push_ControllerHoldsRestoreUntilHostsClear: a foreground Acquire
// while the hosts are not clear starts the notice and a vacate round and
// restores nothing; once every host acked, the foreground is restored.
func TestNS4_Push_ControllerHoldsRestoreUntilHostsClear(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newPushFixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED, true)

	fx.group.Spec().RequestLock("trainer")
	fx.queue.Add("g")
	if err := waitWithTimeout(func() bool { v, _ := fx.hosts.counts(); return v >= 1 }, 3*time.Second); err != nil {
		t.Fatalf("no vacate round: %v", err)
	}
	if err := waitWithTimeout(func() bool { return fx.queue.getDoneCount() >= 1 }, 3*time.Second); err != nil {
		t.Fatalf("no reconcile: %v", err)
	}
	if ev := fx.eventList(); len(ev) != 0 {
		t.Fatalf("agent called while hosts not clear: %v", ev)
	}
	if fx.group.Spec().NoticeAt().IsZero() {
		t.Fatal("no notice started while hosts not clear")
	}
	if fx.ctrl.HostsClear("g") {
		t.Fatal("HostsClear true before the acks")
	}

	fx.hosts.setClear(true)
	fx.queue.Add("g")
	if err := waitWithTimeout(func() bool {
		return fx.group.Status().LoadedJob() == "trainer"
	}, 5*time.Second); err != nil {
		t.Fatalf("trainer not restored after hosts clear: %v (events %v)", err, fx.eventList())
	}
	if ev := fx.eventList(); len(ev) != 1 || ev[0] != "restore:trainer" {
		t.Fatalf("events = %v, want [restore:trainer]", ev)
	}
	if !fx.group.Spec().NoticeAt().IsZero() {
		t.Fatal("notice still running once every host is clear")
	}
}

// TestNS4_Push_ControllerLendSavesForegroundThenResumesHosts: a lending
// Yield saves the foreground first and only then tells the hosts to resume.
func TestNS4_Push_ControllerLendSavesForegroundThenResumesHosts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newPushFixture(t, ctx, agentpb.JobState_JOB_STATE_RUNNING, true)
	fx.hosts.setClear(true)

	fx.group.Spec().SetActiveJob("trainer")
	fx.group.Spec().SetLend(true)
	fx.queue.Add("g")
	if err := waitWithTimeout(func() bool { _, r := fx.hosts.counts(); return r >= 1 }, 5*time.Second); err != nil {
		t.Fatalf("hosts never resumed: %v (events %v)", err, fx.eventList())
	}
	ev := fx.eventList()
	if len(ev) < 2 || ev[0] != "snapshot:trainer" || ev[1] != "resume" {
		t.Fatalf("events = %v, want snapshot:trainer before resume", ev)
	}
	if got := fx.group.Spec().ActiveJob(); got != "" {
		t.Errorf("active job = %q while lent, want none", got)
	}
}

// TestNS4_Push_ControllerNoHostsNoChange: without a HostCommander the
// foreground is restored at once (base behaviour).
func TestNS4_Push_ControllerNoHostsNoChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newPushFixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED, false)

	if !fx.ctrl.HostsClear("g") {
		t.Fatal("HostsClear must be true without a HostCommander")
	}
	fx.group.Spec().RequestLock("trainer")
	fx.queue.Add("g")
	if err := waitWithTimeout(func() bool {
		return fx.group.Status().LoadedJob() == "trainer"
	}, 5*time.Second); err != nil {
		t.Fatalf("trainer not restored: %v", err)
	}
	if !fx.group.Spec().NoticeAt().IsZero() {
		t.Fatal("notice started without a HostCommander")
	}
}
