package server

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

func TestHostOp_GetOperationReturnsTargets(t *testing.T) {
	lister := func(role string) []string {
		if role == "background" {
			return []string{"guest-a", "guest-b"}
		}
		return nil
	}
	srv := NewServer(nil, backends.BackendNoop, "standalone", backends.NewChannelRegistry(), nil,
		sm.WithTargetLister(lister))
	for _, id := range []string{"guest-a", "guest-b"} {
		srv.state.RegisterJob(id, "group-1")
		if err := srv.state.TransitionToRunning(id, []int{1}); err != nil {
			t.Fatal(err)
		}
	}
	workerFor := func(jobID string) sm.GuestWorker {
		return func(context.Context) (sm.GuestResult, error) {
			if jobID == "guest-b" {
				return sm.GuestResult{}, sm.NewOpError(pb.ErrorReason_PRECONDITION_READINESS, errors.New("guest pod is Ready"))
			}
			return sm.GuestResult{Outcome: pb.Outcome_OUTCOME_SUSPENDED}, nil
		}
	}
	opID, err := srv.state.StartHostOp("background", sm.OpTypeSuspend, 1, time.Now().Add(time.Minute), workerFor)
	if err != nil {
		t.Fatal(err)
	}

	resp := waitGuestOp(t, srv, opID)
	if resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED ||
		resp.GetErrorReason() != pb.ErrorReason_PRECONDITION_READINESS {
		t.Errorf("unexpected host response: %v", resp)
	}
	targets := resp.GetTargets()
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %v", targets)
	}
	a, b := targets[0], targets[1]
	if a.GetJobId() != "guest-a" || a.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE ||
		a.GetOutcome() != pb.Outcome_OUTCOME_SUSPENDED || a.GetError() != "" {
		t.Errorf("unexpected result for guest-a: %v", a)
	}
	if b.GetJobId() != "guest-b" || b.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED ||
		b.GetErrorReason() != pb.ErrorReason_PRECONDITION_READINESS || b.GetError() == "" {
		t.Errorf("unexpected result for guest-b: %v", b)
	}

	// A per-job operation has no targets.
	opID, err = srv.state.StartGuestOp("guest-a", sm.OpTypeResume, 2, time.Now().Add(time.Minute),
		func(context.Context) (sm.GuestResult, error) { return sm.GuestResult{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if resp := waitGuestOp(t, srv, opID); len(resp.GetTargets()) != 0 {
		t.Errorf("per-job operation has targets: %v", resp.GetTargets())
	}
}
