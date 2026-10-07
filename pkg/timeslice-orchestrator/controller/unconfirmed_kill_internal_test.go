package controller

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// Tests of the D-NS-6 Kubernetes event. The other grant tests are in
// kill_internal_test.go.

// kubeRecord is one UnconfirmedKube call.
type kubeRecord struct {
	kind, reason, job, node string
	at                      time.Time
}

// fakeKube records UnconfirmedKube calls.
type fakeKube struct {
	mu    sync.Mutex
	calls []kubeRecord
}

func (k *fakeKube) record(r *kubeRecord) {
	k.mu.Lock()
	defer k.mu.Unlock()
	r.at = time.Now()
	k.calls = append(k.calls, *r)
}

func (k *fakeKube) GuestEvent(_ context.Context, _, job, node, reason, _ string) error {
	k.record(&kubeRecord{kind: "guest-event", reason: reason, job: job, node: node})
	return nil
}

// of returns the recorded calls of one kind.
func (k *fakeKube) of(kind string) []kubeRecord {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(k.calls), func(r kubeRecord) bool { return r.kind != kind })
}

// agentUnconfirmed makes the fixture's agent fail every Kill with
// KILL_UNCONFIRMED.
func agentUnconfirmed(fx *killFixture) {
	fx.agent.OperationFunc = func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		msg := "device memory still mapped"
		return &agentpb.GetOperationResponse{
			Status: agentpb.OperationStatus_OPERATION_STATUS_FAILED, Error: &msg,
			ErrorReason: agentpb.ErrorReason_KILL_UNCONFIRMED,
		}, nil
	}
}

// TestUnconfirmedKill_Grant_RecordsEvent: an unconfirmed Kill is granted with
// vram_unconfirmed and records one KillUnconfirmed event on the guest.
func TestUnconfirmedKill_Grant_RecordsEvent(t *testing.T) {
	noticeAt := time.Now()
	fx := newKillFixture(t, noticeAt, unconfirmedN, unconfirmedK)
	agentUnconfirmed(fx)
	kube := &fakeKube{}
	fx.ctrl.Kube = kube
	unconfirmedCase(t, fx, noticeAt, unconfirmedKillAgent)
	events := kube.of("guest-event")
	if len(events) != 1 || events[0].reason != eventKillUnconfirmed || events[0].node != killNode {
		t.Errorf("guest events = %+v, want one %s on %s", events, eventKillUnconfirmed, killNode)
	}
}
