package controller_test

import (
	"context"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"k8s.io/client-go/util/workqueue"
)

const pushGroup = "group-1"

// fakeHostCommander is a scripted controller.HostCommander that records the
// order of its calls in events.
type fakeHostCommander struct {
	mu       sync.Mutex
	allClear bool
	lent     bool
	synced   []string
	noticeAt []time.Time
	resumes  int
	forgot   []string
	events   *eventLog
}

func (f *fakeHostCommander) Forget(group string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forgot = append(f.forgot, group)
}

func (f *fakeHostCommander) forgotten() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.forgot...)
}

func (f *fakeHostCommander) SyncHosts(_ string, nodes []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synced = nodes
}

func (f *fakeHostCommander) AllClear(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.allClear
}

func (f *fakeHostCommander) Lent(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lent
}

func (f *fakeHostCommander) StartVacate(_ string, noticeAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.noticeAt = append(f.noticeAt, noticeAt)
	f.events.add("vacate")
}

func (f *fakeHostCommander) Resume(string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumes++
	f.lent = true
	f.allClear = false
	f.events.add("resume")
}

func (f *fakeHostCommander) setClear(allClear bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allClear = allClear
}

func (f *fakeHostCommander) vacateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.noticeAt)
}

func (f *fakeHostCommander) resumeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resumes
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (e *eventLog) add(ev string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, ev)
}

func (e *eventLog) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

type pushFixture struct {
	ctrl   *controller.Controller
	group  *store.Group
	hosts  *fakeHostCommander
	events *eventLog
	queue  *trackQueue
	infra  *mockInfrastructureOrchestrator
	groups *store.GroupStore
}

// newPushFixture builds group-1 on node-1 with the trainer job in the given
// context state, and a controller with host commands enabled.
func newPushFixture(
	t *testing.T, ctx context.Context, trainerState pb.SnapshotAgentJobState_State,
) *pushFixture {
	t.Helper()
	groupStore := store.NewGroupStore(store.NewMemLockStore())
	jobStore := store.NewJobStore()
	queue := &trackQueue{
		TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "test"},
		),
	}
	group, _, err := groupStore.GetOrCreate(ctx, pushGroup)
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	group.Status().SetNodes([]string{"node-1"})
	trainer := store.NewJob(pushGroup, "trainer")
	trainer.UpdateContextState("node-1", trainerState)
	if err := jobStore.Put(ctx, trainer); err != nil {
		t.Fatalf("failed to put trainer: %v", err)
	}
	events := &eventLog{}
	agent := &controller.MockSnapshotAgentStore{
		SnapshotFunc: func(_ context.Context, node, jobID, _ string) (*agentpb.SnapshotResponse, error) {
			events.add("snapshot " + jobID)
			trainer.UpdateContextState(node, pb.SnapshotAgentJobState_STATE_SAVED)
			return &agentpb.SnapshotResponse{OperationId: "op-s"}, nil
		},
		RestoreFunc: func(_ context.Context, node, jobID, _ string) (*agentpb.RestoreResponse, error) {
			events.add("restore " + jobID)
			trainer.UpdateContextState(node, pb.SnapshotAgentJobState_STATE_RUNNING)
			return &agentpb.RestoreResponse{OperationId: "op-r"}, nil
		},
		OperationResponses: []*agentpb.GetOperationResponse{
			{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE},
		},
	}
	infra := &mockInfrastructureOrchestrator{
		observeFunc: func(context.Context, string) error { return nil },
	}
	ctrl := controller.NewController(groupStore, jobStore, queue, infra, agent)
	hosts := &fakeHostCommander{events: events}
	ctrl.Hosts = hosts
	return &pushFixture{
		ctrl: ctrl, group: group, hosts: hosts, events: events, queue: queue,
		infra: infra, groups: groupStore,
	}
}

func (f *pushFixture) run(t *testing.T, ctx context.Context) {
	t.Helper()
	go func() {
		if err := f.ctrl.Run(ctx, 1); err != nil {
			t.Errorf("Controller Run failed: %v", err)
		}
	}()
}

func TestHostCommand_ControllerHoldsPromotionUntilHostsClear(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fix := newPushFixture(t, ctx, pb.SnapshotAgentJobState_STATE_SAVED)
	fix.group.Spec().RequestLock("trainer")
	fix.run(t, ctx)
	fix.queue.Add(pushGroup)

	if err := waitWithTimeout(func() bool { return fix.queue.getDoneCount() >= 1 }, 3*time.Second); err != nil {
		t.Fatalf("timed out waiting for a reconcile: %v", err)
	}
	if got := fix.group.Spec().LockingJob(); got != "" {
		t.Fatalf("trainer promoted to %q while hosts are not clear", got)
	}
	if fix.hosts.vacateCount() == 0 {
		t.Fatal("no vacate started while a foreground job waits")
	}
	if fix.group.Spec().NoticeAt().IsZero() {
		t.Fatal("no notice recorded while a foreground job waits")
	}
	for _, ev := range fix.events.list() {
		if ev == "restore trainer" {
			t.Fatal("trainer restored while hosts are not clear")
		}
	}

	fix.hosts.setClear(true)
	fix.queue.Add(pushGroup) // the barrier enqueues the group when it completes
	if err := waitWithTimeout(func() bool { return fix.group.Spec().LockingJob() == "trainer" }, 3*time.Second); err != nil {
		t.Fatalf("trainer not promoted once hosts are clear: %v", err)
	}
	if err := waitWithTimeout(func() bool { return fix.group.Status().LoadedJob() == "trainer" }, 5*time.Second); err != nil {
		t.Fatalf("trainer not restored once hosts are clear: %v", err)
	}
}

func TestHostCommand_ControllerLendSavesThenResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fix := newPushFixture(t, ctx, pb.SnapshotAgentJobState_STATE_RUNNING)
	fix.hosts.setClear(true)
	spec := fix.group.Spec()
	spec.RequestLock("trainer")
	if _, err := spec.TryPromote(ctx); err != nil {
		t.Fatal(err)
	}
	if err := spec.Yield(ctx, "trainer"); err != nil {
		t.Fatal(err)
	}
	spec.SetLend(true)
	fix.run(t, ctx)
	fix.queue.Add(pushGroup)

	if err := waitWithTimeout(func() bool { return fix.hosts.resumeCount() == 1 }, 5*time.Second); err != nil {
		t.Fatalf("hosts never resumed after a lend: %v", err)
	}
	got := fix.events.list()
	if len(got) < 2 || got[0] != "snapshot trainer" || got[1] != "resume" {
		t.Fatalf("events = %v, want the trainer saved before the hosts resume", got)
	}
	if spec.ActiveJob() != "" {
		t.Errorf("active job = %q, want none while lent", spec.ActiveJob())
	}

	// Later reconciles while lent resume nothing more.
	fix.queue.Add(pushGroup)
	done := fix.queue.getDoneCount()
	if err := waitWithTimeout(func() bool { return fix.queue.getDoneCount() > done }, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if resumes := fix.hosts.resumeCount(); resumes != 1 {
		t.Errorf("resumes = %d, want 1", resumes)
	}
}

func TestHostCommand_ControllerSweepsUnknownHostsUnderHeldLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fix := newPushFixture(t, ctx, pb.SnapshotAgentJobState_STATE_RUNNING)
	spec := fix.group.Spec()
	spec.RequestLock("trainer")
	if _, err := spec.TryPromote(ctx); err != nil {
		t.Fatal(err)
	}
	fix.run(t, ctx)
	fix.queue.Add(pushGroup)

	if err := waitWithTimeout(func() bool { return fix.hosts.vacateCount() >= 1 }, 3*time.Second); err != nil {
		t.Fatalf("unknown hosts not swept while the lock is held: %v", err)
	}
	if !spec.NoticeAt().IsZero() {
		t.Error("a notice was recorded although no foreground job waits")
	}
	fix.hosts.mu.Lock()
	synced := fix.hosts.synced
	fix.hosts.mu.Unlock()
	if len(synced) != 1 || synced[0] != "node-1" {
		t.Errorf("synced hosts = %v, want [node-1] from the group nodes", synced)
	}
}

func TestHostCommand_ControllerDisabledByDefault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fix := newPushFixture(t, ctx, pb.SnapshotAgentJobState_STATE_SAVED)
	fix.ctrl.Hosts = nil
	fix.group.Spec().RequestLock("trainer")
	fix.run(t, ctx)
	fix.queue.Add(pushGroup)
	if err := waitWithTimeout(func() bool { return fix.group.Spec().LockingJob() == "trainer" }, 3*time.Second); err != nil {
		t.Fatalf("trainer not promoted with host commands disabled: %v", err)
	}
	if vacates, resumes := fix.hosts.vacateCount(), fix.hosts.resumeCount(); vacates != 0 || resumes != 0 {
		t.Errorf("host commands sent while disabled: %d vacates, %d resumes", vacates, resumes)
	}
}

// When the observe step deletes the group (no nodes and no pods left), the
// controller forgets its hosts, so host state and commands do not leak.
func TestHostCommand_ControllerForgetsHostsOfDeletedGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fix := newPushFixture(t, ctx, pb.SnapshotAgentJobState_STATE_SAVED)
	fix.infra.observeFunc = func(ctx context.Context, groupID string) error {
		return fix.groups.Delete(ctx, groupID)
	}
	fix.run(t, ctx)
	fix.queue.Add(pushGroup)

	if err := waitWithTimeout(func() bool { return len(fix.hosts.forgotten()) >= 1 }, 3*time.Second); err != nil {
		t.Fatalf("hosts of a deleted group were never forgotten: %v", err)
	}
	if got := fix.hosts.forgotten(); got[0] != pushGroup {
		t.Errorf("forgot %v, want [%s]", got, pushGroup)
	}
}

// A group that still exists is never forgotten.
func TestHostCommand_ControllerKeepsHostsOfLiveGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fix := newPushFixture(t, ctx, pb.SnapshotAgentJobState_STATE_SAVED)
	fix.hosts.setClear(true)
	fix.run(t, ctx)
	fix.queue.Add(pushGroup)
	if err := waitWithTimeout(func() bool { return fix.queue.getDoneCount() >= 1 }, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := fix.hosts.forgotten(); len(got) != 0 {
		t.Errorf("forgot %v for a live group, want none", got)
	}
}
