// Copyright 2025 The llm-d Authors.
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

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// runMainEnv makes the test binary run main() with the arguments in
// mainArgsEnv (separated by newlines) instead of the tests. It lets a test
// check flag parsing, which exits the process, in a child process.
const (
	runMainEnv  = "SNAPSHOT_AGENT_TEST_RUN_MAIN"
	mainArgsEnv = "SNAPSHOT_AGENT_TEST_MAIN_ARGS"
)

func TestMain(m *testing.M) {
	if os.Getenv(runMainEnv) == "1" {
		// A fresh flag set keeps the test flags out of the agent's usage.
		flag.CommandLine = flag.NewFlagSet("snapshot-agent", flag.ExitOnError)
		os.Args = append([]string{"snapshot-agent"}, strings.Split(os.Getenv(mainArgsEnv), "\n")...)
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// mainRun is what runMain returns: the child's stderr and exit code.
type mainRun struct {
	stderr string
	code   int
}

// runMain runs main() in a child process with args and returns its stderr
// and exit code. Only use it with arguments that make flag parsing exit.
func runMain(t *testing.T, args ...string) mainRun {
	t.Helper()
	//nolint:gosec // re-executes this test binary; args come from the test.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), runMainEnv+"=1", mainArgsEnv+"="+strings.Join(args, "\n"))
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return mainRun{stderr: errBuf.String(), code: 0}
	case errors.As(err, &exitErr):
		return mainRun{stderr: errBuf.String(), code: exitErr.ExitCode()}
	default:
		t.Fatalf("running main %v: %v", args, err)
		return mainRun{code: -1}
	}
}

// TestNoFlag_ReportResumedOutcomeRejected checks that the removed flag is
// now an unknown flag: flag parsing exits with code 2.
func TestNoFlag_ReportResumedOutcomeRejected(t *testing.T) {
	for _, arg := range []string{"-report-resumed-outcome", "-report-resumed-outcome=false"} {
		res := runMain(t, arg)
		if res.code != 2 {
			t.Errorf("%s: exit code %d, want 2; stderr:\n%s", arg, res.code, res.stderr)
		}
		if !strings.Contains(res.stderr, "flag provided but not defined: -report-resumed-outcome") {
			t.Errorf("%s: stderr does not name the unknown flag:\n%s", arg, res.stderr)
		}
	}
	res := runMain(t, "-h")
	if res.code != 0 {
		t.Fatalf("-h: exit code %d, want 0; stderr:\n%s", res.code, res.stderr)
	}
	if strings.Contains(res.stderr, "report-resumed-outcome") {
		t.Errorf("-report-resumed-outcome is still in the usage:\n%s", res.stderr)
	}
}

// TestNoFlag_ResumeOutcome checks the Resume outcome of the agent as
// shipped, with the StateManager options main passes to the server: a
// Suspend then a Resume, and a Resume of a running job, both report
// OUTCOME_RESUMED.
func TestNoFlag_ResumeOutcome(t *testing.T) {
	const jobID = "guest-job"
	sm := statemachine.NewStateManager(stateMachineOptions()...)
	sm.RegisterJob(jobID, "group")
	if err := sm.TransitionToRunning(jobID, []int{42}); err != nil {
		t.Fatalf("TransitionToRunning: %v", err)
	}
	deadline := time.Now().Add(time.Minute)

	suspendID, err := sm.StartGuestOp(jobID, statemachine.OpTypeSuspend, 1, deadline,
		func(context.Context) (statemachine.GuestResult, error) {
			return statemachine.GuestResult{Outcome: pb.Outcome_OUTCOME_SUSPENDED}, nil
		})
	if err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	checkOutcome(t, sm, suspendID, pb.Outcome_OUTCOME_SUSPENDED)

	resume := func(context.Context) (statemachine.GuestResult, error) {
		return statemachine.GuestResult{}, nil
	}
	resumeID, err := sm.StartGuestOp(jobID, statemachine.OpTypeResume, 2, deadline, resume)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	checkOutcome(t, sm, resumeID, pb.Outcome_OUTCOME_RESUMED)

	// A Resume of a RUNNING job completes at once with the same outcome.
	resumeID, err = sm.StartGuestOp(jobID, statemachine.OpTypeResume, 3, deadline, resume)
	if err != nil {
		t.Fatalf("second Resume: %v", err)
	}
	checkOutcome(t, sm, resumeID, pb.Outcome_OUTCOME_RESUMED)
}

// checkOutcome waits for an operation to finish and checks that it
// completed with the given outcome.
func checkOutcome(t *testing.T, sm *statemachine.StateManager, opID string, want pb.Outcome) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		op, ok := sm.GetOperation(opID)
		if ok && !op.FinishedAt.IsZero() {
			if op.Status != pb.OperationStatus_OPERATION_STATUS_COMPLETE || op.Outcome != want {
				t.Fatalf("operation %s (%s): got %s %s (%s: %s), want COMPLETE %s",
					opID, op.Type, op.Status, op.Outcome, op.ErrorReason, op.Error, want)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation %s did not finish", opID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
