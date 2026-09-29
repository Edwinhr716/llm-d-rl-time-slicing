package donorcontroller //nolint:testpackage // the evaluation hook needs the unexported controller seam

import (
	"context"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// evalTick is the real-time reconcile interval in tests. Every TTL decision reads the injected
// clock, so a test moves time by advancing its clock, not by sleeping.
const evalTick = 20 * time.Millisecond

// evalController is the evaluation hook (H10). It builds a controller on cs with the given lock
// source and clock, starts its informers and waits for them, and returns a function that runs
// the reconcile loop until its context is cancelled and then stops the informers.
//
//nolint:gocritic // hugeParam: the signature (Config by value) is fixed by the evaluation plan
func evalController(cs kubernetes.Interface, locks LockSource, clock func() time.Time, cfg Config) (
	func(context.Context) error, error,
) {
	cfg.Tick = evalTick
	ctl, err := New(cs, locks, clock, &cfg)
	if err != nil {
		return nil, err
	}
	base := countWatches(cs)
	stop := make(chan struct{})
	if err := ctl.start(stop); err != nil {
		close(stop)
		return nil, err
	}
	waitForFakeWatches(cs, base+1+len(ctl.podInformers))
	return func(ctx context.Context) error {
		defer close(stop)
		return ctl.loop(ctx)
	}, nil
}

// waitForFakeWatches closes the fake clientset's list/watch gap: an object created after an
// informer's list but before its watch is registered is never delivered. It waits until want
// watch calls in total are recorded, then a little longer for the watchers to be registered.
func waitForFakeWatches(cs kubernetes.Interface, want int) {
	if _, ok := cs.(*fake.Clientset); !ok {
		time.Sleep(100 * time.Millisecond)
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && countWatches(cs) < want {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
}

// countWatches counts the watch calls recorded by a fake clientset (0 for any other client).
func countWatches(cs kubernetes.Interface) int {
	fc, ok := cs.(*fake.Clientset)
	if !ok {
		return 0
	}
	n := 0
	for _, act := range fc.Actions() {
		if _, ok := act.(k8stesting.WatchAction); ok {
			n++
		}
	}
	return n
}
