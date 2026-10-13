package server_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	bgGroup       = "group-1"
	bgNode        = "node-a"
	bgParticipant = "vk/node-a"
)

// backgroundGroup builds group-1 on node-a and node-b. With a holder, the
// holder holds the lock (and has its context loaded when loaded is true) and
// the recorded state is LOCKED; without one the group is IDLE_YIELDED.
func backgroundGroup(t *testing.T, holder string, loaded bool) (*store.GroupStore, *store.Group) {
	t.Helper()
	ctx := context.Background()
	gs := store.NewGroupStore(store.NewMemLockStore())
	group, _, err := gs.GetOrCreate(ctx, bgGroup)
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	group.Status().SetNodes([]string{bgNode, "node-b"})
	group.Status().SetState(pb.GroupStatus_STATE_IDLE_YIELDED)
	if holder != "" {
		group.Spec().RequestLock(holder)
		if _, err := group.Spec().TryPromote(ctx); err != nil {
			t.Fatalf("failed to promote %s: %v", holder, err)
		}
		if loaded {
			group.Status().SetLoadedJob(holder)
		}
		group.Status().SetState(pb.GroupStatus_STATE_LOCKED)
	}
	return gs, group
}

// dialServer starts a server on the shared bufconn listener and returns a
// connection to it.
func dialServer(t *testing.T, gs server.GroupStore, opts ...server.Option) *grpc.ClientConn {
	t.Helper()
	_, _, cleanup := server.InitGRPCServer(gs, store.NewJobStore(), opts...)
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
	return conn
}

func backgroundClient(t *testing.T, gs server.GroupStore, opts ...server.Option) pb.TimeSliceOrchestratorServiceClient {
	t.Helper()
	return pb.NewTimeSliceOrchestratorServiceClient(dialServer(t, gs, opts...))
}

func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status code = %v, want %v (err: %v)", got, want, err)
	}
}

func groupStatus(t *testing.T, client pb.TimeSliceOrchestratorServiceClient, participantID string) *pb.GroupStatus {
	t.Helper()
	resp, err := client.GetGroupStatus(context.Background(), &pb.GetGroupStatusRequest{
		GroupId:       bgGroup,
		ParticipantId: participantID,
	})
	if err != nil {
		t.Fatalf("GetGroupStatus failed: %v", err)
	}
	return resp.GetGroup()
}

// waitRegistered waits until node's background participant is registered and
// grants it the node.
func waitRegistered(t *testing.T, group *store.Group, node string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !group.Spec().Grant(node) {
		if time.Now().After(deadline) {
			t.Fatalf("background participant of %s never registered", node)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestServer_BackgroundRoleDisabled(t *testing.T) {
	gs, group := backgroundGroup(t, "", false)
	client := backgroundClient(t, gs)
	ctx := context.Background()

	_, err := client.Acquire(ctx, &pb.AcquireRequest{
		JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND, NodeName: bgNode,
	})
	assertCode(t, err, codes.FailedPrecondition)

	_, err = client.Yield(ctx, &pb.YieldRequest{JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND})
	assertCode(t, err, codes.FailedPrecondition)

	// participant_id is ignored, exactly as on a server without the field.
	got := groupStatus(t, client, bgParticipant)
	if got.GetBackgroundProtocol() != 0 {
		t.Errorf("background_protocol = %d, want 0 with the role disabled", got.GetBackgroundProtocol())
	}
	if got.GetGroupState() != pb.GroupStatus_STATE_IDLE_YIELDED {
		t.Errorf("group_state = %v, want STATE_IDLE_YIELDED", got.GetGroupState())
	}
	if got.GetVacateWithin() != nil {
		t.Errorf("vacate_within = %v, want unset", got.GetVacateWithin())
	}
	if group.Spec().BackgroundHeld() {
		t.Error("a poll registered a participant with the role disabled")
	}
}

func TestServer_BackgroundProtocolAdvertised(t *testing.T) {
	gs, _ := backgroundGroup(t, "", false)
	client := backgroundClient(t, gs, server.WithBackgroundRole(true))
	if got := groupStatus(t, client, "").GetBackgroundProtocol(); got != server.BackgroundProtocolVersion {
		t.Errorf("background_protocol = %d, want %d", got, server.BackgroundProtocolVersion)
	}
}

func TestServer_RoleValidation(t *testing.T) {
	tests := []struct {
		name string
		req  *pb.AcquireRequest
		want codes.Code
	}{
		{
			name: "unknown role",
			req:  &pb.AcquireRequest{JobId: "job-1", GroupId: bgGroup, Role: pb.Role(9)},
			want: codes.InvalidArgument,
		},
		{
			name: "background without node_name",
			req:  &pb.AcquireRequest{JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND},
			want: codes.InvalidArgument,
		},
		{
			name: "background job_id not vk/<node>",
			req: &pb.AcquireRequest{
				JobId: "guest-1", GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND, NodeName: bgNode,
			},
			want: codes.InvalidArgument,
		},
		{
			name: "background node outside the group",
			req: &pb.AcquireRequest{
				JobId: "vk/node-z", GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND, NodeName: "node-z",
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "background unknown group",
			req: &pb.AcquireRequest{
				JobId: bgParticipant, GroupId: "group-x", Role: pb.Role_ROLE_BACKGROUND, NodeName: bgNode,
			},
			want: codes.NotFound,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gs, group := backgroundGroup(t, "", false)
			client := backgroundClient(t, gs, server.WithBackgroundRole(true))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := client.Acquire(ctx, tc.req)
			assertCode(t, err, tc.want)
			if group.Spec().LockingJob() != "" || group.Spec().GetWaitingJobQueue().Len() != 0 {
				t.Error("a refused Acquire touched the foreground lock or queue")
			}
		})
	}

	t.Run("yield unknown role", func(t *testing.T) {
		gs, _ := backgroundGroup(t, "", false)
		client := backgroundClient(t, gs, server.WithBackgroundRole(true))
		_, err := client.Yield(context.Background(), &pb.YieldRequest{JobId: "job-1", GroupId: bgGroup, Role: pb.Role(9)})
		assertCode(t, err, codes.InvalidArgument)
	})
	t.Run("yield background bad participant", func(t *testing.T) {
		gs, _ := backgroundGroup(t, "", false)
		client := backgroundClient(t, gs, server.WithBackgroundRole(true))
		_, err := client.Yield(context.Background(), &pb.YieldRequest{
			JobId: "vk/", GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND,
		})
		assertCode(t, err, codes.InvalidArgument)
	})
}

func TestServer_AcquireBackground_BlocksUntilGranted(t *testing.T) {
	gs, group := backgroundGroup(t, "", false)
	client := backgroundClient(t, gs, server.WithBackgroundRole(true))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type result struct {
		resp *pb.AcquireResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := client.Acquire(ctx, &pb.AcquireRequest{
			JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND, NodeName: bgNode,
		})
		done <- result{resp: resp, err: err}
	}()

	select {
	case res := <-done:
		t.Fatalf("background Acquire returned before a grant: %v, %v", res.resp, res.err)
	case <-time.After(50 * time.Millisecond):
	}
	if group.Spec().GetWaitingJobQueue().Len() != 0 {
		t.Fatal("background participant entered the foreground queue")
	}

	waitRegistered(t, group, bgNode)
	res := <-done
	if res.err != nil || !res.resp.GetSuccess() {
		t.Fatalf("background Acquire = %v, %v; want success", res.resp, res.err)
	}

	if got := groupStatus(t, client, bgParticipant).GetGroupState(); got != pb.GroupStatus_STATE_BACKGROUND {
		t.Fatalf("group_state = %v, want STATE_BACKGROUND while granted", got)
	}

	// Background Yield hands the grant back and is idempotent.
	for i := range 2 {
		resp, err := client.Yield(ctx, &pb.YieldRequest{JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND})
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("background Yield #%d = %v, %v; want success", i+1, resp, err)
		}
	}
	if got := groupStatus(t, client, "").GetGroupState(); got != pb.GroupStatus_STATE_IDLE_YIELDED {
		t.Fatalf("group_state = %v, want STATE_IDLE_YIELDED after the background Yield", got)
	}
}

func TestServer_AcquireBackground_CancelUnregisters(t *testing.T) {
	gs, group := backgroundGroup(t, "", false)
	client := backgroundClient(t, gs, server.WithBackgroundRole(true))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.Acquire(ctx, &pb.AcquireRequest{
		JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND, NodeName: bgNode,
	})
	assertCode(t, err, codes.DeadlineExceeded)

	deadline := time.Now().Add(5 * time.Second)
	for group.Spec().Grant(bgNode) {
		// The server may not have seen the cancellation yet; undo and retry.
		group.Spec().ClearGrant(bgNode)
		if time.Now().After(deadline) {
			t.Fatal("participant still registered after its Acquire was cancelled")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestServer_GetGroupStatus_ParticipantHeartbeat(t *testing.T) {
	gs, group := backgroundGroup(t, "", false)
	client := backgroundClient(t, gs, server.WithBackgroundRole(true))

	// After an orchestrator restart the participant's first poll is a claim.
	if got := groupStatus(t, client, bgParticipant).GetGroupState(); got != pb.GroupStatus_STATE_BACKGROUND {
		t.Fatalf("group_state = %v, want STATE_BACKGROUND for a claim (fail closed)", got)
	}
	if group.Spec().Granted(bgNode) {
		t.Fatal("a poll must never grant")
	}

	for _, tc := range []struct {
		id   string
		want codes.Code
	}{
		{id: "node-a", want: codes.InvalidArgument},
		{id: "vk/a/b", want: codes.InvalidArgument},
		{id: "vk/node-z", want: codes.FailedPrecondition},
	} {
		_, err := client.GetGroupStatus(context.Background(), &pb.GetGroupStatusRequest{GroupId: bgGroup, ParticipantId: tc.id})
		assertCode(t, err, tc.want)
	}

	if _, err := client.Yield(context.Background(), &pb.YieldRequest{
		JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND,
	}); err != nil {
		t.Fatalf("background Yield failed: %v", err)
	}
	if group.Spec().BackgroundHeld() {
		t.Fatal("background Yield did not clear the claim")
	}
}

func TestServer_ForegroundAcquire_NoticeAndVacating(t *testing.T) {
	gs, group := backgroundGroup(t, "job-1", true)
	client := backgroundClient(t, gs, server.WithBackgroundRole(true), server.WithNoticeTiming(10*time.Second, 2*time.Second))
	group.Spec().RegisterParticipant(bgNode, bgParticipant, time.Now())
	group.Spec().Grant(bgNode)
	group.Spec().UnregisterParticipant(bgNode)

	// Option A: the foreground Acquire blocks while the guest holds the node,
	// even though the holder's context is loaded.
	shortCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := client.Acquire(shortCtx, &pb.AcquireRequest{JobId: "job-1", GroupId: bgGroup})
	assertCode(t, err, codes.DeadlineExceeded)

	got := groupStatus(t, client, "")
	if got.GetGroupState() != pb.GroupStatus_STATE_VACATING {
		t.Fatalf("group_state = %v, want STATE_VACATING while a notice runs", got.GetGroupState())
	}
	within := got.GetVacateWithin().AsDuration()
	if got.GetVacateWithin() == nil || within <= 0 || within > 8*time.Second {
		t.Fatalf("vacate_within = %v, want in (0, N-K = 8s]", got.GetVacateWithin())
	}

	ctx, cancelAll := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelAll()
	done := make(chan error, 1)
	go func() {
		resp, err := client.Acquire(ctx, &pb.AcquireRequest{JobId: "job-1", GroupId: bgGroup})
		if err == nil && !resp.GetSuccess() {
			err = status.Error(codes.Unknown, "success = false")
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("foreground Acquire returned over a held grant: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := client.Yield(ctx, &pb.YieldRequest{
		JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND,
	}); err != nil {
		t.Fatalf("background Yield failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("foreground Acquire after the guest vacated: %v", err)
	}

	got = groupStatus(t, client, "")
	if got.GetGroupState() != pb.GroupStatus_STATE_LOCKED || got.GetVacateWithin() != nil {
		t.Fatalf("after restore: group_state = %v, vacate_within = %v; want STATE_LOCKED and unset",
			got.GetGroupState(), got.GetVacateWithin())
	}
}

func TestServer_Yield_ExpectedIdle(t *testing.T) {
	tests := []struct {
		name      string
		minBubble time.Duration
		idle      *durationpb.Duration
		wantCode  codes.Code
		wantLend  bool
	}{
		{name: "no hint", minBubble: 30 * time.Second, wantCode: codes.OK},
		{name: "bubble above minimum", minBubble: 30 * time.Second, idle: durationpb.New(45 * time.Second), wantLend: true},
		{name: "bubble at minimum", minBubble: 30 * time.Second, idle: durationpb.New(30 * time.Second), wantLend: true},
		{name: "bubble below minimum", minBubble: 30 * time.Second, idle: durationpb.New(10 * time.Second)},
		{name: "min-bubble unset never lends", idle: durationpb.New(time.Hour)},
		{name: "negative", minBubble: 30 * time.Second, idle: durationpb.New(-time.Second), wantCode: codes.InvalidArgument},
		{
			name:      "invalid duration",
			minBubble: 30 * time.Second,
			idle:      &durationpb.Duration{Seconds: 1, Nanos: -1},
			wantCode:  codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gs, group := backgroundGroup(t, "job-1", true)
			client := backgroundClient(t, gs, server.WithMinBubble(tc.minBubble))
			_, err := client.Yield(context.Background(), &pb.YieldRequest{
				JobId: "job-1", GroupId: bgGroup, ExpectedIdle: tc.idle,
			})
			assertCode(t, err, tc.wantCode)
			if tc.wantCode != codes.OK {
				if group.Spec().LockingJob() != "job-1" {
					t.Fatal("a refused Yield released the lock")
				}
				return
			}
			if group.Spec().LockingJob() != "" {
				t.Fatal("Yield did not release the lock")
			}
			if got := group.Spec().Lend(); got != tc.wantLend {
				t.Errorf("Lend() = %v, want %v", got, tc.wantLend)
			}
		})
	}
}
