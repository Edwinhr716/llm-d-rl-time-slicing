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
	"errors"
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"
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

// flagUsage returns the usage lines of one flag from the -h output.
func flagUsage(usage, name string) string {
	lines := strings.Split(usage, "\n")
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "-"+name) {
			if i+1 < len(lines) {
				return line + "\n" + lines[i+1]
			}
			return line
		}
	}
	return ""
}

// TestKeepFlag_ReportResumedOutcomeDefaultTrue pins the keep-flag option of
// D-AGENT-7: the agent still has -report-resumed-outcome and its default is
// true, so a successful Resume reports OUTCOME_RESUMED unless it is set.
func TestKeepFlag_ReportResumedOutcomeDefaultTrue(t *testing.T) {
	res := runMain(t, "-h")
	if res.code != 0 {
		t.Fatalf("-h: exit code %d, want 0; stderr:\n%s", res.code, res.stderr)
	}
	usage := flagUsage(res.stderr, "report-resumed-outcome")
	if usage == "" {
		t.Fatalf("-report-resumed-outcome is missing from the usage:\n%s", res.stderr)
	}
	if !strings.Contains(usage, "(default true)") {
		t.Errorf("-report-resumed-outcome default is not true:\n%s", usage)
	}
}

// TestKeepFlag_ReportResumedOutcomeAccepted checks that the flag still parses
// with an explicit value.
func TestKeepFlag_ReportResumedOutcomeAccepted(t *testing.T) {
	res := runMain(t, "-report-resumed-outcome=false", "-h")
	if res.code != 0 {
		t.Fatalf("-report-resumed-outcome=false -h: exit code %d, want 0; stderr:\n%s", res.code, res.stderr)
	}
}
