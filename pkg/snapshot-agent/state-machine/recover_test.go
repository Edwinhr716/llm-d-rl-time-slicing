package statemachine_test

import (
	"reflect"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

func rec(state pb.JobState, hostBytes int64) statemachine.Recovered {
	return statemachine.Recovered{State: state, PIDs: []int{7}, HostBytesPinned: hostBytes}
}

func TestRecoverJob(t *testing.T) {
	t.Run("sets each observed state", func(t *testing.T) {
		cases := []struct {
			rec     statemachine.Recovered
			outcome pb.Outcome
			pids    []int
			host    int64
		}{
			{rec(pb.JobState_JOB_STATE_SUSPENDED, 99), pb.Outcome_OUTCOME_SUSPENDED, []int{7}, 99},
			{rec(pb.JobState_JOB_STATE_SAVED, 0), pb.Outcome_OUTCOME_UNSPECIFIED, []int{7}, 0},
			{rec(pb.JobState_JOB_STATE_RUNNING, 0), pb.Outcome_OUTCOME_UNSPECIFIED, []int{7}, 0},
			{rec(pb.JobState_JOB_STATE_FAULTED, 0), pb.Outcome_OUTCOME_KILLED, []int{7}, 0},
			{rec(pb.JobState_JOB_STATE_IDLE, 0), pb.Outcome_OUTCOME_KILLED, nil, 0},
		}
		for _, c := range cases {
			t.Run(c.rec.State.String(), func(t *testing.T) {
				sm := statemachine.NewStateManager()
				setJob(t, sm, pb.JobState_JOB_STATE_IDLE, pb.Outcome_OUTCOME_KILLED)
				sm.SeedEpoch(guestJob, 5)
				ok, err := sm.RecoverJob(guestJob, c.rec)
				if err != nil || !ok {
					t.Fatalf("RecoverJob: %v, %v", ok, err)
				}
				st := jobStatus(t, sm)
				if st.GetState() != c.rec.State || st.GetLastOutcome() != c.outcome ||
					st.GetHostBytesPinned() != c.host || st.GetEpoch() != 5 {
					t.Errorf("got %v", st)
				}
				pids, err := sm.GetJobPIDs(guestJob)
				if err != nil {
					pids = nil // a job without PIDs
				}
				if !reflect.DeepEqual(pids, c.pids) {
					t.Errorf("pids %v, want %v", pids, c.pids)
				}
			})
		}
	})

	t.Run("a recovered SUSPENDED job resumes; a recovered SAVED job suspends", func(t *testing.T) {
		sm := statemachine.NewStateManager()
		sm.RegisterJob(guestJob, guestGroup)
		if ok, err := sm.RecoverJob(guestJob, statemachine.Recovered{State: pb.JobState_JOB_STATE_SUSPENDED}); !ok || err != nil {
			t.Fatalf("RecoverJob: %v, %v", ok, err)
		}
		opID := startGuest(t, sm, statemachine.OpTypeResume, 1, instant(statemachine.GuestResult{}, nil))
		waitForOperation(t, sm, opID)
		if st := jobStatus(t, sm); st.GetState() != pb.JobState_JOB_STATE_RUNNING {
			t.Fatalf("after resume: %v", st)
		}

		if ok, err := sm.RecoverJob(guestJob, statemachine.Recovered{State: pb.JobState_JOB_STATE_SAVED}); !ok || err != nil {
			t.Fatalf("RecoverJob: %v, %v", ok, err)
		}
		opID = startGuest(t, sm, statemachine.OpTypeSuspend, 2, instant(suspendedResult, nil))
		waitForOperation(t, sm, opID)
		if st := jobStatus(t, sm); st.GetState() != pb.JobState_JOB_STATE_SUSPENDED {
			t.Fatalf("after suspend: %v", st)
		}
	})

	t.Run("leaves a job with a running operation alone", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(suspendedResult)
		startGuest(t, sm, statemachine.OpTypeSuspend, 1, stub.run)
		waitStarted(t, stub)
		ok, err := sm.RecoverJob(guestJob, statemachine.Recovered{State: pb.JobState_JOB_STATE_IDLE})
		if ok || err != nil {
			t.Fatalf("RecoverJob: %v, %v", ok, err)
		}
		if st := jobStatus(t, sm); st.GetState() != pb.JobState_JOB_STATE_TRANSITIONING {
			t.Errorf("got %v", st)
		}
		close(stub.release)
		waitReturned(t, stub)
	})

	t.Run("unknown job and bad state", func(t *testing.T) {
		sm := statemachine.NewStateManager()
		if ok, err := sm.RecoverJob("nope", statemachine.Recovered{State: pb.JobState_JOB_STATE_IDLE}); ok || err != nil {
			t.Errorf("unknown job: %v, %v", ok, err)
		}
		sm.RegisterJob(guestJob, guestGroup)
		if _, err := sm.RecoverJob(guestJob, statemachine.Recovered{State: pb.JobState_JOB_STATE_TRANSITIONING}); err == nil {
			t.Error("TRANSITIONING accepted")
		}
	})
}

// TestSeedHostEpoch_RestartMidSuspendAll is a SuspendAll with epoch 3 that
// was interrupted by a restart: one target finished (SUSPENDED), one was
// checkpointed but not frozen (SAVED). Recovery re-seeds both fences.
func TestSeedHostEpoch_RestartMidSuspendAll(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-b")
	sm := statemachine.NewStateManager(statemachine.WithTargetLister(host.list))
	for id, state := range map[string]pb.JobState{
		"job-a": pb.JobState_JOB_STATE_SUSPENDED, "job-b": pb.JobState_JOB_STATE_SAVED,
	} {
		sm.RegisterJob(id, guestGroup)
		sm.SeedEpoch(id, 3)
		if ok, err := sm.RecoverJob(id, statemachine.Recovered{State: state, PIDs: []int{7}}); !ok || err != nil {
			t.Fatalf("RecoverJob %s: %v, %v", id, ok, err)
		}
	}
	sm.SeedHostEpoch("", 9) // ignored
	sm.SeedHostEpoch(hostRole, 3)
	sm.SeedHostEpoch(hostRole, 1) // never lowers

	recorder := &workers{worker: instant(suspendedResult, nil)}
	if _, err := sm.StartHostOp(hostRole, statemachine.OpTypeSuspend, 2, future(), recorder.forJob); err == nil ||
		statemachine.ErrorReasonOf(err) != pb.ErrorReason_STALE_EPOCH {
		t.Fatalf("late SuspendAll: got %v, want STALE_EPOCH", err)
	}
	// The re-issued call with the same epoch runs every target again: its
	// operation record did not survive the restart.
	opID := startHost(t, sm, statemachine.OpTypeSuspend, 3, recorder.forJob)
	op := waitForOperation(t, sm, opID)
	checkComplete(t, op, pb.Outcome_OUTCOME_SUSPENDED)
	for _, id := range []string{"job-a", "job-b"} {
		if st := jobState(t, sm, id); st != pb.JobState_JOB_STATE_SUSPENDED {
			t.Errorf("%s: %s, want SUSPENDED", id, st)
		}
	}
	opID = startHost(t, sm, statemachine.OpTypeResume, 4, perJob(map[string]statemachine.GuestWorker{
		"job-a": instant(statemachine.GuestResult{}, nil), "job-b": instant(statemachine.GuestResult{}, nil),
	}))
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_RESUMED)
}
