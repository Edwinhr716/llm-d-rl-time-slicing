package era //nolint:testpackage // the evaluator's seam must see unexported names

// Test seam for the D-NS-13 evaluator (plan hook V7). The evaluator drops its own test file into
// this package and drives the era through these two functions only.

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// evalEra builds an era controller on cs with the given lock source and clock. There is no
// virtual-kubelet node controller and no agent: Running guests take the delete-mirror path,
// and a fresh era re-registers the virtual Node as it was when evalEra was called (same
// labels, taints and finalizers, so the NS and TODAY Node shapes both round-trip).
func evalEra(
	cs kubernetes.Interface, locks LockSource, clock func() time.Time,
	cfg Config, //nolint:gocritic // the evaluator's test calls evalEra with a Config value
) (func(context.Context) error, error) {
	if cfg.Poll == 0 {
		cfg.Poll = 10 * time.Millisecond // the evaluator steps its fake clock every 100 ms
	}
	if cfg.MirrorWait == 0 {
		cfg.MirrorWait = 2 * time.Second
	}
	snap, err := cs.CoreV1().Nodes().Get(context.Background(), cfg.VKNode, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("virtual Node %s must exist before evalEra: %w", cfg.VKNode, err)
	}
	snap = snap.DeepCopy()
	hooks := Hooks{
		Start: func(ctx context.Context) error {
			fresh := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: snap.Name, Labels: snap.Labels, Finalizers: snap.Finalizers, OwnerReferences: snap.OwnerReferences,
			}, Spec: snap.Spec}
			_, err := cs.CoreV1().Nodes().Create(ctx, fresh, metav1.CreateOptions{})
			return err
		},
	}
	c, err := New(cs, locks, clock, &cfg, hooks)
	if err != nil {
		return nil, err
	}
	return c.Run, nil
}

type scriptedLocks struct {
	mu sync.Mutex
	st *GroupStatus
}

func (s *scriptedLocks) GroupStatus(_ context.Context, group string) (*GroupStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st == nil {
		return nil, nil //nolint:nilnil // a nil status is LockSource's unknown group
	}
	out := *s.st
	out.GroupID = group
	return &out, nil
}

// evalLocks returns a scripted lock source and its setter. States: IDLE, IDLE_YIELDED, LOCKED,
// BACKGROUND (anything else: the group is unknown). lockingJob is both locking_job and
// active_job; for BACKGROUND pass the VK's own job id (vk/<host>).
func evalLocks() (LockSource, func(state, lockingJob string)) { //nolint:gocritic // shape fixed by the evaluator
	s := &scriptedLocks{}
	return s, func(state, lockingJob string) {
		states := map[string]GroupState{
			"IDLE": StateIdle, "IDLE_YIELDED": StateIdleYielded, "LOCKED": StateLocked, "BACKGROUND": StateBackground,
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		gs, ok := states[state]
		if !ok {
			s.st = nil
			return
		}
		s.st = &GroupStatus{GroupState: gs, LockingJob: lockingJob, ActiveJob: lockingJob}
	}
}
