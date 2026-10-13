package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
)

// --agent-fault-injection also covers the host-command freezer's agent client, so a fault armed
// through /debug/fault plays on the SuspendAll/ResumeAll path the host-command VK really uses.

func hostCommandAgentOpts() *options {
	// Nothing listens on the agent address: an un-faulted call only ends with the context.
	return &options{hostCommandPort: 9, freezer: freezerAgent, hcAgentAddr: "127.0.0.1:1"}
}

func TestHostCommandWiring_FaultInjectorPlaysOnAgentClient(t *testing.T) {
	faults := hostcmd.NewFaultInjector()
	if err := faults.Arm("Status", hostcmd.FaultUnimplemented, 1); err != nil {
		t.Fatal(err)
	}
	w, err := newHostCommandWiring(hostCommandAgentOpts(), faults)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.agent.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = w.agent.Status(ctx)
	if err == nil || !strings.Contains(err.Error(), "injected fault") {
		t.Fatalf("Status error = %v, want the injected fault", err)
	}
	if got := faults.List(); len(got) != 1 || got[0].Hits != 1 {
		t.Fatalf("faults = %+v, want one hit", got)
	}
}

func TestHostCommandWiring_NoFaultInjector(t *testing.T) {
	w, err := newHostCommandWiring(hostCommandAgentOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.agent.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := w.agent.Status(ctx); err == nil || strings.Contains(err.Error(), "injected fault") {
		t.Fatalf("Status error = %v, want a plain connection failure", err)
	}
}
