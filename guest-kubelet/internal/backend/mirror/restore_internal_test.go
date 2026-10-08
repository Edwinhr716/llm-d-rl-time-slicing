package mirror

import (
	"context"
	"testing"
	"time"
)

// reconcileRig is a suspend harness whose mirror has the recorded state and whose agent
// lists the job frozen or running, as the agent path finds them after a restart.
func reconcileRig(t *testing.T, recorded string, frozen bool) *harness {
	t.Helper()
	h, fa := suspendHarness(t)
	job := h.mirror("vllm" + Suffix).Labels[LabelJobID]
	if frozen {
		fa.setJob(job, "SUSPENDED")
	} else {
		fa.setJob(job, "RUNNING")
	}
	if recorded != "" {
		m, err := h.b.mirrors.Pods("ns").Get("vllm-m")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := h.b.SetMirrorSuspendState(context.Background(), m, recorded, EpochBump); err != nil {
			t.Fatal(err)
		}
	}
	h.waitMirrorState(recorded)
	return h
}

func (h *harness) waitMirrorState(want string) {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if m, err := h.b.mirrors.Pods("ns").Get("vllm-m"); err == nil {
			if got, _ := SuspendState(m); got == want {
				return
			}
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("informer never saw suspend state %q", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The host wins: after a restart the recorded state follows the freezer.
func TestReconcileSuspend_HostWins(t *testing.T) {
	for _, tc := range []struct {
		name, recorded string
		frozen         bool
		want           string
		changed        bool
	}{
		{"crash after freeze, before Suspended was written", StateSuspending, true, StateSuspended, true},
		{"crash before freeze", StateSuspending, false, "", true},
		{"crash before thaw", StateResuming, true, StateSuspended, true},
		{"crash after thaw, before the clear", StateResuming, false, "", true},
		{"suspended and frozen", StateSuspended, true, StateSuspended, false},
		{"running and thawed", "", false, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := reconcileRig(t, tc.recorded, tc.frozen)
			got, err := h.b.ReconcileSuspend(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if changed := len(got) == 1; changed != tc.changed {
				t.Fatalf("reconciled %+v, want changed=%t", got, tc.changed)
			}
			m := h.mirror("vllm-m")
			if state, _ := SuspendState(m); state != tc.want {
				t.Fatalf("state %q, want %q", state, tc.want)
			}
			h.waitMirrorState(tc.want)
			if held, _ := h.b.Hold(); held != (tc.want == StateSuspended) {
				t.Fatalf("Hold %t with state %q", held, tc.want)
			}
		})
	}
}
