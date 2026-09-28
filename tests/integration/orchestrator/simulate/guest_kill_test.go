package simulate_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/tests/integration/orchestrator/simulate"
	google_grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"k8s.io/client-go/util/workqueue"
)

// The guest kill tests run the real controller and server over loopback gRPC
// against the fake snapshot agent and a small fake VK. The trainer lends its
// node, takes it back, and the orchestrator must send the trainer's Restore
// within N; for a guest that vacates in time (ok, slow) before T = notice +
// N - K. (The fake agent completes a Restore on the controller's first 1 s
// operation poll; that time is logged, not asserted.)
// For a guest that never vacates (hung), or an agent that cannot be reached
// around T (unreachable), the orchestrator kills the guest at T, retrying
// while the agent is unreachable.

const (
	gkGroup   = "guests"
	gkNode    = "node-guest-1"
	gkTrainer = "trainer-1"
	gkGuest   = "guest-1"
	gkVK      = "vk/" + gkNode

	gkNotice  = 6 * time.Second
	gkKill    = 2 * time.Second
	gkT       = gkNotice - gkKill
	gkPoll    = 500 * time.Millisecond
	gkGrace   = 200 * time.Millisecond
	gkSlowFor = 2 * time.Second
)

type noopInfra struct{}

func (noopInfra) Init(context.Context) error                      { return nil }
func (noopInfra) ObserveGroupState(context.Context, string) error { return nil }

type gkKillCall struct {
	at      time.Time
	job     string
	reason  string
	reached bool
}

// gkEvents records what the fake agent saw.
type gkEvents struct {
	mu         sync.Mutex
	kills      []gkKillCall
	restoreAt  time.Time // Restore of the trainer sent
	restoredAt time.Time // Restore of the trainer complete
}

func (e *gkEvents) onKill(_, job, reason string, _ time.Time, reached bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.kills = append(e.kills, gkKillCall{at: time.Now(), job: job, reason: reason, reached: reached})
}

func (e *gkEvents) onComplete(_, job, opType string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if opType == "restore" && job == gkTrainer && e.restoredAt.IsZero() {
		e.restoredAt = time.Now()
	}
}

func (e *gkEvents) onRestore(_, job string) {
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if job == gkTrainer && e.restoreAt.IsZero() {
		e.restoreAt = now
	}
}

// gkSeen is a copy of what gkEvents recorded.
type gkSeen struct {
	kills      []gkKillCall
	restoreAt  time.Time
	restoredAt time.Time
}

func (e *gkEvents) snapshot() gkSeen {
	e.mu.Lock()
	defer e.mu.Unlock()
	return gkSeen{kills: append([]gkKillCall(nil), e.kills...), restoreAt: e.restoreAt, restoredAt: e.restoredAt}
}

// gkVacate is how the fake VK answers a notice.
type gkVacate int

const (
	vacateNow gkVacate = iota
	vacateSlow
	vacateNever
)

// fakeVK is the background participant of gkNode. One loop: Acquire(ROLE_
// BACKGROUND) -> on grant start the guest -> on a notice suspend it and Yield
// -> Acquire again. A separate loop polls GetGroupStatus every 0.5 s; it sends
// participant_id while the VK holds the node, or once its Acquire has been
// in flight for more than 200 ms (so a poll never lands between a Yield and
// the next Acquire and registers a claim).
type fakeVK struct {
	client pb.TimeSliceOrchestratorServiceClient
	agent  *simulate.FakeSnapshotAgentStore
	jobs   *store.JobStore
	vacate gkVacate

	mu          sync.Mutex
	holding     bool
	acquiringAt time.Time
	notice      bool
	granted     chan struct{}
	grantOnce   sync.Once
}

func (v *fakeVK) sendParticipant() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.holding || (!v.acquiringAt.IsZero() && time.Since(v.acquiringAt) > gkGrace)
}

func (v *fakeVK) setPhase(holding bool, acquiringAt time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.holding = holding
	v.acquiringAt = acquiringAt
}

func (v *fakeVK) noticeSeen() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.notice
}

func (v *fakeVK) poll(ctx context.Context) {
	ticker := time.NewTicker(gkPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		req := &pb.GetGroupStatusRequest{GroupId: gkGroup}
		if v.sendParticipant() {
			req.ParticipantId = gkVK
		}
		resp, err := v.client.GetGroupStatus(ctx, req)
		if err != nil {
			continue
		}
		if resp.GetGroup().GetVacateWithin() != nil {
			v.mu.Lock()
			v.notice = true
			v.mu.Unlock()
		}
	}
}

func (v *fakeVK) run(ctx context.Context, t *testing.T) {
	t.Helper()
	go v.poll(ctx)
	for ctx.Err() == nil {
		v.setPhase(false, time.Now())
		resp, err := v.client.Acquire(ctx, &pb.AcquireRequest{
			JobId: gkVK, GroupId: gkGroup, Role: pb.Role_ROLE_BACKGROUND, NodeName: gkNode,
		})
		if err != nil || !resp.GetSuccess() {
			if ctx.Err() == nil {
				t.Errorf("background Acquire: %v", err)
			}
			return
		}
		v.setPhase(true, time.Time{})
		v.startGuest(ctx, t)
		v.grantOnce.Do(func() { close(v.granted) })

		// Hold until a notice.
		for ctx.Err() == nil && !v.noticeSeen() {
			time.Sleep(50 * time.Millisecond)
		}
		switch v.vacate {
		case vacateNever:
			<-ctx.Done()
			return
		case vacateSlow:
			select {
			case <-ctx.Done():
				return
			case <-time.After(gkSlowFor):
			}
		default:
		}
		v.agent.SetJobState(gkNode, gkGuest, agentpb.JobState_JOB_STATE_SUSPENDED)
		v.setPhase(false, time.Time{})
		if _, err := v.client.Yield(ctx, &pb.YieldRequest{
			JobId: gkVK, GroupId: gkGroup, Role: pb.Role_ROLE_BACKGROUND,
		}); err != nil && ctx.Err() == nil {
			t.Errorf("background Yield: %v", err)
		}
		v.mu.Lock()
		v.notice = false
		v.mu.Unlock()
	}
}

// startGuest creates the guest's mirror job (as the infrastructure observer
// would from the mirror pod) and has the agent report it running.
func (v *fakeVK) startGuest(ctx context.Context, t *testing.T) {
	t.Helper()
	if _, err := v.jobs.Get(ctx, gkGroup, gkGuest); err == nil {
		return
	}
	job := store.NewJob(gkGroup, gkGuest)
	job.SetRole(store.RoleBackground)
	job.SetPodNodes([]string{gkNode})
	if err := v.jobs.Put(ctx, job); err != nil {
		t.Errorf("Put guest: %v", err)
	}
	v.agent.SetJobState(gkNode, gkGuest, agentpb.JobState_JOB_STATE_RUNNING)
}

type gkResult struct {
	noticeAt time.Time
	// restore is when the orchestrator sent the trainer's Restore, restored
	// when the fake agent completed it (on the first 1 s operation poll).
	restore  time.Duration
	restored time.Duration
	kills    []gkKillCall
	resp     *pb.AcquireResponse
}

// runGuestKill runs one lend-and-take-back cycle. unreachable makes every
// agent call on the node fail from the notice until T + 0.5 s.
func runGuestKill(t *testing.T, vacate gkVacate, unreachable bool) gkResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lockStore := store.NewMemLockStore()
	groupStore := store.NewGroupStore(lockStore)
	jobStore := store.NewJobStore()
	group, _, err := groupStore.GetOrCreate(ctx, gkGroup)
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	group.Status().SetNodes([]string{gkNode})
	if err := jobStore.Put(ctx, store.NewJob(gkGroup, gkTrainer)); err != nil {
		t.Fatalf("Put trainer: %v", err)
	}

	events := &gkEvents{}
	agent := simulate.NewFakeSnapshotAgentStore()
	agent.SetJobState(gkNode, gkTrainer, agentpb.JobState_JOB_STATE_RUNNING)
	agent.OnKill = events.onKill
	agent.OnOperationComplete = events.onComplete
	agent.OnRestore = events.onRestore

	queue := &simulate.TrackQueue{
		TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueueWithConfig(
			controller.NewRateLimiter(time.Second, 30*time.Second),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "guest-kill"},
		),
	}
	ctrl := controller.NewController(groupStore, jobStore, queue, noopInfra{}, agent)
	ctrl.NoticeWindow = gkNotice
	ctrl.KillBudget = gkKill
	go func() {
		if err := ctrl.Run(ctx, controller.DefaultWorkers); err != nil {
			t.Errorf("controller Run: %v", err)
		}
	}()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := google_grpc.NewServer()
	pb.RegisterTimeSliceOrchestratorServiceServer(grpcServer, server.NewServer(ctrl, groupStore, jobStore,
		server.WithBackgroundRole(true),
		server.WithMinBubble(time.Second),
		server.WithNoticeTiming(gkNotice, gkKill),
	))
	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, google_grpc.ErrServerStopped) {
			t.Errorf("serve: %v", err)
		}
	}()
	defer grpcServer.Stop()
	conn, err := google_grpc.NewClient(lis.Addr().String(), google_grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewTimeSliceOrchestratorServiceClient(conn)
	ctrl.EnqueueWork(gkGroup)

	// The trainer takes the node.
	fg := &pb.AcquireRequest{JobId: gkTrainer, GroupId: gkGroup, Role: pb.Role_ROLE_FOREGROUND}
	if resp, err := client.Acquire(ctx, fg); err != nil || !resp.GetSuccess() {
		t.Fatalf("first trainer Acquire = %v, %v", resp, err)
	}

	// The VK waits for the node; the trainer yields with a long bubble.
	vk := &fakeVK{client: client, agent: agent, jobs: jobStore, vacate: vacate, granted: make(chan struct{})}
	vkCtx, vkCancel := context.WithCancel(ctx)
	defer vkCancel()
	go vk.run(vkCtx, t)
	waitFor(t, "the VK blocked in Acquire", 5*time.Second, func() bool { return group.Spec().ParticipantBlocked(gkNode) })
	if _, err := client.Yield(ctx, &pb.YieldRequest{
		JobId: gkTrainer, GroupId: gkGroup, Role: pb.Role_ROLE_FOREGROUND, ExpectedIdle: durationpb.New(time.Minute),
	}); err != nil {
		t.Fatalf("trainer Yield: %v", err)
	}
	select {
	case <-vk.granted:
	case <-time.After(10 * time.Second):
		t.Fatal("the VK was not granted the node")
	}
	waitFor(t, "the guest observed running", 5*time.Second, func() bool {
		job, err := jobStore.Get(ctx, gkGroup, gkGuest)
		return err == nil && job.ContextState()[gkNode] == pb.SnapshotAgentJobState_STATE_RUNNING
	})

	// The trainer wants the node back: the notice starts.
	type acquired struct {
		resp *pb.AcquireResponse
		err  error
	}
	done := make(chan acquired, 1)
	go func() {
		resp, err := client.Acquire(ctx, fg)
		done <- acquired{resp, err}
	}()
	waitFor(t, "the notice", 5*time.Second, func() bool { return !group.Spec().NoticeAt().IsZero() })
	noticeAt := group.Spec().NoticeAt()
	if unreachable {
		agent.SetUnreachable(gkNode, true)
		time.AfterFunc(time.Until(noticeAt.Add(gkT+500*time.Millisecond)), func() {
			agent.SetUnreachable(gkNode, false)
		})
	}

	var got acquired
	select {
	case got = <-done:
	case <-time.After(gkNotice + 5*time.Second):
		t.Fatal("the trainer Acquire did not return")
	}
	if got.err != nil || !got.resp.GetSuccess() {
		t.Fatalf("trainer Acquire after the notice = %v, %v", got.resp, got.err)
	}
	seen := events.snapshot()
	kills, restoreAt, restoredAt := seen.kills, seen.restoreAt, seen.restoredAt
	if restoreAt.IsZero() || restoredAt.IsZero() {
		t.Fatal("the trainer was not restored")
	}
	return gkResult{
		noticeAt: noticeAt, restore: restoreAt.Sub(noticeAt), restored: restoredAt.Sub(noticeAt),
		kills: kills, resp: got.resp,
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGuestKill_Restore(t *testing.T) {
	for _, tc := range []struct {
		name        string
		vacate      gkVacate
		unreachable bool
		wantKill    bool
	}{
		{"guest-ok", vacateNow, false, false},
		{"guest-slow", vacateSlow, false, false},
		{"guest-hung", vacateNever, false, true},
		{"agent-unreachable", vacateNever, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := runGuestKill(t, tc.vacate, tc.unreachable)
			t.Logf("case=%s N=%v K=%v T=%v restore_sent=%v restore_complete=%v after the notice kills=%d waited_ms=%d",
				tc.name, gkNotice, gkKill, gkT, res.restore.Round(time.Millisecond), res.restored.Round(time.Millisecond),
				len(res.kills), res.resp.GetWaitedMs())
			for i, k := range res.kills {
				t.Logf("case=%s kill[%d] at=%v reason=%s reached=%v",
					tc.name, i, k.at.Sub(res.noticeAt).Round(time.Millisecond), k.reason, k.reached)
			}

			if res.restore > gkNotice {
				t.Errorf("Restore sent %v after the notice, want within N = %v", res.restore, gkNotice)
			}
			if res.resp.GetVramUnconfirmed() {
				t.Error("vram_unconfirmed set, want a confirmed vacate")
			}
			if !tc.wantKill {
				if res.restore >= gkT {
					t.Errorf("Restore sent %v after the notice, want before T = %v", res.restore, gkT)
				}
				if len(res.kills) != 0 {
					t.Errorf("kills = %+v, want none", res.kills)
				}
				return
			}

			if len(res.kills) == 0 {
				t.Fatal("no Kill sent")
			}
			first, last := res.kills[0], res.kills[len(res.kills)-1]
			if at := first.at.Sub(res.noticeAt); at < gkT-50*time.Millisecond {
				t.Errorf("first Kill %v after the notice, before T = %v", at, gkT)
			}
			for _, k := range res.kills {
				if k.job != gkGuest || k.reason != "deadline" {
					t.Errorf("kill %+v, want a deadline kill of %s", k, gkGuest)
				}
			}
			if !last.reached {
				t.Errorf("last Kill did not reach the agent: %+v", res.kills)
			}
			if tc.unreachable {
				if first.reached {
					t.Errorf("first Kill reached the agent while it was unreachable: %+v", first)
				}
			} else if len(res.kills) != 1 {
				t.Errorf("kills = %+v, want one", res.kills)
			}
		})
	}
}
