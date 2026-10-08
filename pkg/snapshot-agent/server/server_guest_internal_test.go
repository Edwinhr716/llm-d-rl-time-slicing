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

func waitGuestOp(t *testing.T, srv *Server, opID string) *pb.GetOperationResponse {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := srv.GetOperation(context.Background(), &pb.GetOperationRequest{OperationId: opID})
		if err != nil {
			t.Fatalf("GetOperation: %v", err)
		}
		if resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_PENDING {
			return resp
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("operation %s did not finish", opID)
	return nil
}

func TestServer_GuestOperationFields(t *testing.T) {
	const jobID = "guest-1"
	suspended := func(context.Context) (sm.GuestResult, error) {
		return sm.GuestResult{Outcome: pb.Outcome_OUTCOME_SUSPENDED, DeviceBytes: 5 << 30, HostBytesPinned: 6 << 30}, nil
	}

	t.Run("suspend outcome and bytes", func(t *testing.T) {
		srv := NewServer(nil, backends.BackendNoop, "standalone", backends.NewChannelRegistry(), nil)
		srv.state.RegisterJob(jobID, "group-1")
		if err := srv.state.TransitionToRunning(jobID, []int{1}); err != nil {
			t.Fatal(err)
		}
		opID, err := srv.state.StartGuestOp(jobID, sm.OpTypeSuspend, 4, time.Now().Add(time.Minute), suspended)
		if err != nil {
			t.Fatal(err)
		}
		resp := waitGuestOp(t, srv, opID)
		if resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE ||
			resp.GetOutcome() != pb.Outcome_OUTCOME_SUSPENDED ||
			resp.GetErrorReason() != pb.ErrorReason_ERROR_REASON_UNSPECIFIED ||
			resp.GetHostBytesPinned() != 6<<30 {
			t.Errorf("unexpected response: %v", resp)
		}

		st, err := srv.Status(context.Background(), &pb.StatusRequest{})
		if err != nil {
			t.Fatal(err)
		}
		job := st.GetJobStatuses()[0]
		if job.GetState() != pb.JobState_JOB_STATE_SUSPENDED || job.GetLastOutcome() != pb.Outcome_OUTCOME_SUSPENDED ||
			job.GetDeviceBytes() != 5<<30 || job.GetHostBytesPinned() != 6<<30 || job.GetEpoch() != 4 {
			t.Errorf("unexpected job status: %v", job)
		}
	})

	t.Run("kill error reason", func(t *testing.T) {
		srv := NewServer(nil, backends.BackendNoop, "standalone", backends.NewChannelRegistry(), nil)
		srv.state.RegisterJob(jobID, "group-1")
		opID, err := srv.state.StartKill(jobID, time.Now().Add(time.Minute), "test",
			func(context.Context) error { return errors.New("processes remain") })
		if err != nil {
			t.Fatal(err)
		}
		resp := waitGuestOp(t, srv, opID)
		if resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED ||
			resp.GetErrorReason() != pb.ErrorReason_KILL_UNCONFIRMED || resp.GetError() == "" {
			t.Errorf("unexpected response: %v", resp)
		}
	})

	t.Run("state options reach the state manager", func(t *testing.T) {
		srv := NewServer(nil, backends.BackendNoop, "standalone", backends.NewChannelRegistry(), nil,
			sm.WithReportResumedOutcome(false))
		srv.state.RegisterJob(jobID, "group-1")
		if err := srv.state.TransitionToRunning(jobID, []int{1}); err != nil {
			t.Fatal(err)
		}
		opID, err := srv.state.StartGuestOp(jobID, sm.OpTypeResume, 1, time.Now().Add(time.Minute), suspended)
		if err != nil {
			t.Fatal(err)
		}
		if resp := waitGuestOp(t, srv, opID); resp.GetOutcome() != pb.Outcome_OUTCOME_UNSPECIFIED {
			t.Errorf("expected no outcome with the flag off, got %s", resp.GetOutcome())
		}
	})
}
