package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Decision D-NS-17 (and D-ORCH-8): --lend-policy=hint|always. Each arm of
// the evaluation is one set of server options; each Yield below is sent the
// way the verl hooks send it (without expected_idle, as an old client does,
// or with a hint of 10, 30 or 60 s) and the recorded lend hint is checked.

// lendArm is one evaluation arm's server options.
type lendArm struct {
	name string
	opts []server.Option
}

var (
	armNever   = lendArm{name: "never", opts: []server.Option{server.WithLendPolicy(server.LendPolicyHint)}}
	armGated30 = lendArm{name: "gated30", opts: []server.Option{
		server.WithLendPolicy(server.LendPolicyHint), server.WithMinBubble(30 * time.Second),
	}}
	armAlways = lendArm{name: "always", opts: []server.Option{server.WithLendPolicy(server.LendPolicyAlways)}}
	// armAlwaysWithMinBubble sets both flags: --min-bubble is ignored.
	armAlwaysWithMinBubble = lendArm{name: "always+min-bubble", opts: []server.Option{
		server.WithLendPolicy(server.LendPolicyAlways), server.WithMinBubble(30 * time.Second),
	}}
)

// yieldHints are the Yields of the stage-0 matrix, nil = no expected_idle.
var yieldHints = []*time.Duration{nil, durp(10 * time.Second), durp(30 * time.Second), durp(60 * time.Second)}

func durp(d time.Duration) *time.Duration { return &d }

func hintName(idle *time.Duration) string {
	if idle == nil {
		return "no hint"
	}
	return "hint " + idle.String()
}

// yieldLends sends one foreground Yield from the lock holder and returns the
// recorded lend hint.
func yieldLends(t *testing.T, arm lendArm, idle *time.Duration) bool {
	t.Helper()
	gs, group := backgroundGroup(t, "job-1", true)
	client := backgroundClient(t, gs, arm.opts...)
	req := &pb.YieldRequest{JobId: "job-1", GroupId: bgGroup}
	if idle != nil {
		req.ExpectedIdle = durationpb.New(*idle)
	}
	if _, err := client.Yield(context.Background(), req); err != nil {
		t.Fatalf("Yield: %v", err)
	}
	if group.Spec().LockingJob() != "" {
		t.Fatal("Yield did not release the lock")
	}
	return group.Spec().Lend()
}

func runLendMatrix(t *testing.T, arm lendArm, want []bool) {
	t.Helper()
	for i, idle := range yieldHints {
		t.Run(hintName(idle), func(t *testing.T) {
			if got := yieldLends(t, arm, idle); got != want[i] {
				t.Errorf("arm %s, %s: Lend() = %v, want %v", arm.name, hintName(idle), got, want[i])
			}
		})
	}
}

// TestLendPolicy_Hint_MinBubbleZeroNeverLends is arm "never" (D-ORCH-8 unset).
func TestLendPolicy_Hint_MinBubbleZeroNeverLends(t *testing.T) {
	runLendMatrix(t, armNever, []bool{false, false, false, false})
}

// TestLendPolicy_Hint_Gated30 is arm "gated30" (D-NS-17 keep, D-ORCH-8 30s).
func TestLendPolicy_Hint_Gated30(t *testing.T) {
	runLendMatrix(t, armGated30, []bool{false, false, true, true})
}

// TestLendPolicy_Always_EveryYieldLends is arm "always" (D-NS-17 ns-always).
func TestLendPolicy_Always_EveryYieldLends(t *testing.T) {
	runLendMatrix(t, armAlways, []bool{true, true, true, true})
}

func TestLendPolicy_Always_IgnoresMinBubble(t *testing.T) {
	runLendMatrix(t, armAlwaysWithMinBubble, []bool{true, true, true, true})
}

// TestLendPolicy_Hint_OldClientUnchanged checks that a server built with no
// lend option behaves as --lend-policy=hint: a Yield without the field (an
// old client) never lends, with or without --min-bubble.
func TestLendPolicy_Hint_OldClientUnchanged(t *testing.T) {
	t.Setenv(server.EnvLendPolicy, "")
	for _, arm := range []lendArm{
		{name: "defaults"},
		{name: "default policy, min-bubble 30s", opts: []server.Option{server.WithMinBubble(30 * time.Second)}},
	} {
		t.Run(arm.name, func(t *testing.T) {
			if yieldLends(t, arm, nil) {
				t.Error("a Yield without expected_idle lent under the default policy")
			}
		})
	}
}

// TestLendPolicy_Always_StillRefusesAnInvalidHint checks that "always" does
// not skip the expected_idle validation: a bad hint is refused and the lock
// is kept.
func TestLendPolicy_Always_StillRefusesAnInvalidHint(t *testing.T) {
	gs, group := backgroundGroup(t, "job-1", true)
	client := backgroundClient(t, gs, armAlways.opts...)
	_, err := client.Yield(context.Background(), &pb.YieldRequest{
		JobId: "job-1", GroupId: bgGroup, ExpectedIdle: durationpb.New(-time.Second),
	})
	assertCode(t, err, codes.InvalidArgument)
	if group.Spec().LockingJob() != "job-1" {
		t.Fatal("a refused Yield released the lock")
	}
	if group.Spec().Lend() {
		t.Fatal("a refused Yield recorded a lend hint")
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent log writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Split(b.buf.String(), "\n")
}

// lendDecisionRecords returns the attributes of every "Lend decision" record.
func lendDecisionRecords(t *testing.T, buf *syncBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range buf.lines() {
		if !strings.Contains(line, `"msg":"Lend decision"`) {
			continue
		}
		rec := map[string]any{}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// TestLendPolicy_Hint_LogsLendDecision and its "always" twin check the
// evaluation's log hook: one "Lend decision" line per foreground Yield with
// group, job, policy, expected_idle, min_bubble and lend.
func TestLendPolicy_Hint_LogsLendDecision(t *testing.T) {
	checkLendDecisionLog(t, armGated30, durp(45*time.Second), map[string]any{
		"group": bgGroup, "job": "job-1", "policy": "hint", "expected_idle": "45s", "min_bubble": "30s", "lend": true,
	})
}

func TestLendPolicy_Always_LogsLendDecision(t *testing.T) {
	checkLendDecisionLog(t, armAlways, nil, map[string]any{
		"group": bgGroup, "job": "job-1", "policy": "always", "expected_idle": "none", "min_bubble": "0s", "lend": true,
	})
}

func checkLendDecisionLog(t *testing.T, arm lendArm, idle *time.Duration, want map[string]any) {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	yieldLends(t, arm, idle)

	recs := lendDecisionRecords(t, buf)
	if len(recs) != 1 {
		t.Fatalf("got %d Lend decision lines, want 1: %v", len(recs), recs)
	}
	for k, v := range want {
		if recs[0][k] != v {
			t.Errorf("Lend decision %s = %v, want %v (line %v)", k, recs[0][k], v, recs[0])
		}
	}
}

// TestLendPolicy_Always_BackgroundYieldLogsNoDecision checks that only
// foreground Yields are decided: a background Yield hands its grant back
// without a Lend decision line.
func TestLendPolicy_Always_BackgroundYieldLogsNoDecision(t *testing.T) {
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	gs, _ := backgroundGroup(t, "", false)
	client := backgroundClient(t, gs, append([]server.Option{server.WithBackgroundRole(true)}, armAlways.opts...)...)
	_, err := client.Yield(context.Background(), &pb.YieldRequest{
		JobId: bgParticipant, GroupId: bgGroup, Role: pb.Role_ROLE_BACKGROUND,
	})
	assertCode(t, err, codes.OK)
	if recs := lendDecisionRecords(t, buf); len(recs) != 0 {
		t.Fatalf("background Yield logged %d Lend decision lines, want 0: %v", len(recs), recs)
	}
}
