//go:build evalwire

package evalwire

import (
	"context"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// recordingAgentStore records the address each call was made to.
type recordingAgentStore struct {
	addrs []string
}

func (r *recordingAgentStore) GetStatus(_ context.Context, addr string) (*agentpb.StatusResponse, error) {
	r.addrs = append(r.addrs, addr)
	return &agentpb.StatusResponse{}, nil
}

func (r *recordingAgentStore) CloseClient(addr string) error {
	r.addrs = append(r.addrs, addr)
	return nil
}

func (r *recordingAgentStore) Snapshot(_ context.Context, addr, _, _ string) (*agentpb.SnapshotResponse, error) {
	r.addrs = append(r.addrs, addr)
	return &agentpb.SnapshotResponse{}, nil
}

func (r *recordingAgentStore) GetOperation(_ context.Context, addr, _ string) (*agentpb.GetOperationResponse, error) {
	r.addrs = append(r.addrs, addr)
	return &agentpb.GetOperationResponse{}, nil
}

func (r *recordingAgentStore) Restore(_ context.Context, addr, _, _ string) (*agentpb.RestoreResponse, error) {
	r.addrs = append(r.addrs, addr)
	return &agentpb.RestoreResponse{}, nil
}

func (r *recordingAgentStore) Kill(_ context.Context, addr, _, _ string, _ time.Time) (*agentpb.KillResponse, error) {
	r.addrs = append(r.addrs, addr)
	return &agentpb.KillResponse{}, nil
}

// TestAddressedAgentStore_DialsInternalIP checks that the evalwire agent store
// dials a node's agent at its InternalIP, as the host endpoint is dialed, so a
// harness pod that cannot resolve node names still reaches the agent.
func TestAddressedAgentStore_DialsInternalIP(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, n := range []*corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeHostName, Address: "node-1"},
				{Type: corev1.NodeInternalIP, Address: "10.0.0.7"},
			}},
		},
		{ObjectMeta: metav1.ObjectMeta{Name: "no-ip"}},
	} {
		if err := indexer.Add(n); err != nil {
			t.Fatalf("add node: %v", err)
		}
	}
	inner := &recordingAgentStore{}
	addressed := &addressedAgentStore{inner: inner, nodes: corev1listers.NewNodeLister(indexer), port: 39101}

	for _, tc := range []struct{ node, want string }{
		{"node-1", "10.0.0.7:39101"},
		{"no-ip", "no-ip"},
		{"unknown", "unknown"},
		{"127.0.0.1:5000", "127.0.0.1:5000"},
	} {
		if got := addressed.address(tc.node); got != tc.want {
			t.Errorf("address(%q) = %q, want %q", tc.node, got, tc.want)
		}
	}

	ctx := context.Background()
	calls := []func() error{
		func() error { _, err := addressed.GetStatus(ctx, "node-1"); return err },
		func() error { return addressed.CloseClient("node-1") },
		func() error { _, err := addressed.Snapshot(ctx, "node-1", "j", "g"); return err },
		func() error { _, err := addressed.GetOperation(ctx, "node-1", "op"); return err },
		func() error { _, err := addressed.Restore(ctx, "node-1", "j", "g"); return err },
		func() error { _, err := addressed.Kill(ctx, "node-1", "j", "deadline", time.Now()); return err },
	}
	for i, call := range calls {
		if err := call(); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if len(inner.addrs) != 6 {
		t.Fatalf("calls = %d, want 6", len(inner.addrs))
	}
	for i, addr := range inner.addrs {
		if addr != "10.0.0.7:39101" {
			t.Errorf("call %d dialed %q, want 10.0.0.7:39101", i, addr)
		}
	}
}
