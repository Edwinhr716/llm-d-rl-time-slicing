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

//go:build evalwire

package host_test

import (
	"context"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire/host"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type recExec struct {
	mu     sync.Mutex
	guests []string
	calls  []string
}

func (e *recExec) record(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, s)
}

func (e *recExec) Guests() []string { return e.guests }

func (e *recExec) SetNotReady(_ context.Context, g string) error {
	e.record("notready:" + g)
	return nil
}

func (e *recExec) Suspend(_ context.Context, g string, _ time.Time) error {
	e.record("suspend:" + g)
	return nil
}

func (e *recExec) Resume(_ context.Context, g string, _ time.Time) error {
	e.record("resume:" + g)
	return nil
}

func (e *recExec) SetReady(_ context.Context, g string) error {
	e.record("ready:" + g)
	return nil
}

func (e *recExec) take() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.calls
	e.calls = nil
	return out
}

// startHost starts a host whose orchestrator address accepts nothing: the
// command endpoint works on its own.
func startHost(t *testing.T, exec host.Executor) hcpb.HostCommandServiceClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dead, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	orchAddr := dead.Addr().String()
	_ = dead.Close()
	h, err := host.Start(ctx, host.Config{Node: "node-a", OrchAddr: orchAddr, ListenAddr: "127.0.0.1:0", Exec: exec})
	if err != nil {
		t.Fatalf("host.Start: %v", err)
	}
	t.Cleanup(h.Stop)
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial host: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return hcpb.NewHostCommandServiceClient(conn)
}

func TestNS4_Hybrid_HostCommandOrderAndEpochs(t *testing.T) {
	exec := &recExec{guests: []string{"g1", "g2"}}
	client := startHost(t, exec)
	ctx := context.Background()
	deadline := timestamppb.New(time.Now().Add(10 * time.Second))

	// An unknown guest may be live: Vacate suspends it, NotReady first.
	ack, err := client.Vacate(ctx, &hcpb.VacateRequest{GroupId: "g", NodeName: "node-a", Epoch: 10, Deadline: deadline})
	if err != nil || ack.GetOutcome() != hcpb.Outcome_OUTCOME_VACATED || ack.GetEpoch() != 10 {
		t.Fatalf("Vacate(10) = %v, %v; want VACATED epoch 10", ack, err)
	}
	if got, want := exec.take(), []string{"notready:g1", "suspend:g1", "notready:g2", "suspend:g2"}; !slices.Equal(got, want) {
		t.Fatalf("vacate calls = %v, want %v", got, want)
	}

	// A repeated Vacate acks at once and touches no guest.
	ack, err = client.Vacate(ctx, &hcpb.VacateRequest{GroupId: "g", NodeName: "node-a", Epoch: 10, Deadline: deadline})
	if err != nil || ack.GetOutcome() != hcpb.Outcome_OUTCOME_VACATED {
		t.Fatalf("repeated Vacate(10) = %v, %v; want VACATED", ack, err)
	}
	if got := exec.take(); len(got) != 0 {
		t.Fatalf("repeated vacate touched guests: %v", got)
	}

	// A Resume older than the last command is refused.
	ack, err = client.Resume(ctx, &hcpb.ResumeRequest{GroupId: "g", NodeName: "node-a", Epoch: 5, Deadline: deadline})
	if err != nil || ack.GetOutcome() != hcpb.Outcome_OUTCOME_STALE_EPOCH || ack.GetCurrentEpoch() != 10 {
		t.Fatalf("Resume(5) = %v, %v; want STALE_EPOCH current 10", ack, err)
	}
	if got := exec.take(); len(got) != 0 {
		t.Fatalf("stale resume touched guests: %v", got)
	}

	// Resume, then Ready, per guest.
	ack, err = client.Resume(ctx, &hcpb.ResumeRequest{GroupId: "g", NodeName: "node-a", Epoch: 20, Deadline: deadline})
	if err != nil || ack.GetOutcome() != hcpb.Outcome_OUTCOME_RESUMED {
		t.Fatalf("Resume(20) = %v, %v; want RESUMED", ack, err)
	}
	if got, want := exec.take(), []string{"resume:g1", "ready:g1", "resume:g2", "ready:g2"}; !slices.Equal(got, want) {
		t.Fatalf("resume calls = %v, want %v", got, want)
	}

	// A late Vacate from an earlier cycle is refused.
	ack, err = client.Vacate(ctx, &hcpb.VacateRequest{GroupId: "g", NodeName: "node-a", Epoch: 10, Deadline: deadline})
	if err != nil || ack.GetOutcome() != hcpb.Outcome_OUTCOME_STALE_EPOCH {
		t.Fatalf("late Vacate(10) = %v, %v; want STALE_EPOCH", ack, err)
	}

	// A command for another node is refused.
	ack, err = client.Vacate(ctx, &hcpb.VacateRequest{GroupId: "g", NodeName: "node-b", Epoch: 30, Deadline: deadline})
	if err != nil || ack.GetOutcome() != hcpb.Outcome_OUTCOME_FAILED {
		t.Fatalf("Vacate for node-b = %v, %v; want FAILED", ack, err)
	}
	if got := exec.take(); len(got) != 0 {
		t.Fatalf("refused commands touched guests: %v", got)
	}
}
