// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// TestKillHostOp_HungTargetDoesNotBlockKill: a SuspendAll runs its targets
// in parallel and every target's pipeline takes the node lock with the
// host operation's deadline. One target hangs inside a checkpoint holding
// the lock. Kill of that target still confirms well within its deadline,
// the target queued behind the lock gives up at the host deadline with
// DEADLINE_EXCEEDED instead of waiting for ever, GetOperation answers
// throughout, the host operation reports one result per target, and the
// hung worker returning later does not overwrite the killed job.
func TestKillHostOp_HungTargetDoesNotBlockKill(t *testing.T) {
	const other = "guest-queued"
	env := newKillEnv(t, kernelOpts{})
	env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
	st := env.srv.state
	st.RegisterJob(other, killGroup)
	if err := st.TransitionToRunning(other, []int{201}); err != nil {
		t.Fatal(err)
	}
	sm.WithTargetLister(func(role string) []string {
		if role != "background" {
			return nil
		}
		return []string{killJob, other}
	})(st)

	cuda := backends.NewCudaCheckpoint()
	hung := make(chan struct{})
	holding := make(chan struct{})
	returned := make(chan struct{})
	queuedErr := make(chan error, 1)
	workerFor := func(jobID string) sm.GuestWorker {
		if jobID == killJob {
			return func(ctx context.Context) (sm.GuestResult, error) {
				defer close(returned)
				if err := cuda.Acquire(ctx); err != nil {
					return sm.GuestResult{}, err
				}
				defer cuda.Release()
				close(holding)
				<-hung // Stuck in the driver: cancellation does not reach it.
				return sm.GuestResult{Outcome: pb.Outcome_OUTCOME_SUSPENDED}, nil
			}
		}
		return func(ctx context.Context) (sm.GuestResult, error) {
			<-holding
			err := cuda.Acquire(ctx)
			queuedErr <- err
			if err != nil {
				return sm.GuestResult{}, err
			}
			defer cuda.Release()
			return sm.GuestResult{Outcome: pb.Outcome_OUTCOME_SUSPENDED}, nil
		}
	}

	hostDeadline := time.Now().Add(400 * time.Millisecond)
	hostID, err := st.StartHostOp("background", sm.OpTypeSuspend, 1, hostDeadline, workerFor)
	if err != nil {
		t.Fatalf("SuspendAll: %v", err)
	}
	<-holding

	start := time.Now()
	killID := env.kill(t, 3*time.Second)
	if resp, err := env.srv.GetOperation(context.Background(), &pb.GetOperationRequest{OperationId: hostID}); err != nil {
		t.Fatalf("GetOperation of the host operation while a target hangs: %v", err)
	} else if len(resp.GetTargets()) != 2 {
		t.Fatalf("expected live results for 2 targets, got %v", resp.GetTargets())
	}
	env.checkKilled(t, killID)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Kill took %s behind a hung SuspendAll target", took)
	}
	env.checkKillWritten(t)

	select {
	case err := <-queuedErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued target: want DeadlineExceeded from the node lock, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the queued target never gave up on the node lock")
	}

	host := waitGuestOp(t, env.srv, hostID)
	if host.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED {
		t.Fatalf("SuspendAll: want FAILED, got %s", host.GetStatus())
	}
	results := make(map[string]*pb.TargetResult)
	for _, r := range host.GetTargets() {
		results[r.GetJobId()] = r
	}
	if r := results[killJob]; r.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED ||
		!strings.Contains(r.GetError(), "superseded by Kill") {
		t.Errorf("killed target: want FAILED superseded by Kill, got %v", r)
	}
	if r := results[other]; r.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED ||
		r.GetErrorReason() != pb.ErrorReason_DEADLINE_EXCEEDED {
		t.Errorf("queued target: want FAILED/DEADLINE_EXCEEDED, got %v", r)
	}

	close(hung)
	<-returned
	time.Sleep(50 * time.Millisecond)
	if js := env.jobStatus(t); js.GetState() != pb.JobState_JOB_STATE_IDLE || js.GetLastOutcome() != pb.Outcome_OUTCOME_KILLED {
		t.Fatalf("the hung target overwrote the kill: %s/%s", js.GetState(), js.GetLastOutcome())
	}
}
