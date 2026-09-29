package mirror

import (
	"context"
	"sync/atomic"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"
)

// A create that clashes with a mirror that is gone by the time it is read (a vacate's delete that
// just finished) creates again, instead of failing with "get existing mirror: not found".
func TestCreate_ClashGoneCreatesAgain(t *testing.T) {
	h := newHarness(t, ref(testOptions()))
	var creates atomic.Int32
	h.client.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if creates.Add(1) == 1 {
			return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, "vllm-m")
		}
		return false, nil, nil
	})
	g := cpuGuest("g1")
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if n := creates.Load(); n != 2 {
		t.Fatalf("creates = %d, want 2", n)
	}
	if m := h.mirror("vllm-m"); m == nil || m.Labels[LabelMirrorOf] != "g1" {
		t.Fatalf("mirror not created for g1: %v", m)
	}
}
