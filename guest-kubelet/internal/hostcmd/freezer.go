package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Freezer suspends and resumes one guest's mirror. The deadline comes from ctx. epoch is the
// value just written to timeslice.io/guest-epoch on the mirror; an implementation passes it to
// the snapshot-agent (M4) or ignores it (M3 cgroup freeze).
//
// VK-A3's freeze.Backend has this method set and plugs in as is; VK-A4 swaps it for the agent.
type Freezer interface {
	Suspend(ctx context.Context, mirror *corev1.Pod, epoch int64) error
	Resume(ctx context.Context, mirror *corev1.Pod, epoch int64) error
}

// Killer is the first step of the kill sequence (agent Kill, M4). A Freezer that also
// implements it is asked to kill before the mirror is deleted.
type Killer interface {
	Kill(ctx context.Context, mirror *corev1.Pod, reason string) error
}

// ErrNoContext is returned by Suspend when the guest has no accelerator context yet. The host
// then deletes the mirror (normal grace) instead of suspending it.
var ErrNoContext = errors.New("guest has no accelerator context yet")

// AnnotationFakeFreezer is written by FakeFreezer on the mirror: "suspended" or "running". A
// test snapshot-agent reads it to report the guest's job state.
const AnnotationFakeFreezer = "timeslice.io/fake-freezer"

// Values of AnnotationFakeFreezer.
const (
	FakeSuspended = "suspended"
	FakeRunning   = "running"
)

// FakeCall is one call FakeFreezer received.
type FakeCall struct {
	Op     string // "suspend" or "resume"
	Mirror string
	Epoch  int64
	At     time.Time
	Err    error
}

// FakeFreezer stands in for the real freezer until VK-A3 lands. It takes SuspendDelay or
// ResumeDelay (the cuda-checkpoint timings on L4 are about 12.5 s and 6 s), honours the ctx
// deadline, and then records the new state on the mirror through Annotate, if set. The
// process keeps running: it proves the protocol, not the freeze.
type FakeFreezer struct {
	SuspendDelay time.Duration
	ResumeDelay  time.Duration
	// Annotate writes one annotation on the mirror; nil writes nothing.
	Annotate func(ctx context.Context, mirror *corev1.Pod, key, value string) error
	// Fail, if set, is returned by the next call of that op ("suspend" or "resume").
	Fail map[string]error

	mu    sync.Mutex
	calls []FakeCall
}

// Suspend implements Freezer.
func (f *FakeFreezer) Suspend(ctx context.Context, mirror *corev1.Pod, epoch int64) error {
	return f.do(ctx, "suspend", f.SuspendDelay, FakeSuspended, mirror, epoch)
}

// Resume implements Freezer.
func (f *FakeFreezer) Resume(ctx context.Context, mirror *corev1.Pod, epoch int64) error {
	return f.do(ctx, "resume", f.ResumeDelay, FakeRunning, mirror, epoch)
}

// Calls returns every call so far.
func (f *FakeFreezer) Calls() []FakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeCall(nil), f.calls...)
}

func (f *FakeFreezer) do(
	ctx context.Context, op string, delay time.Duration, state string, mirror *corev1.Pod, epoch int64,
) error {
	err := f.run(ctx, op, delay, state, mirror)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, FakeCall{Op: op, Mirror: mirror.Name, Epoch: epoch, At: time.Now(), Err: err})
	return err
}

func (f *FakeFreezer) run(ctx context.Context, op string, delay time.Duration, state string, mirror *corev1.Pod) error {
	f.mu.Lock()
	failErr := f.Fail[op]
	delete(f.Fail, op)
	f.mu.Unlock()
	if failErr != nil {
		return failErr
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("fake %s: %w", op, ctx.Err())
	case <-timer.C:
	}
	if f.Annotate == nil {
		return nil
	}
	if err := f.Annotate(ctx, mirror, AnnotationFakeFreezer, state); err != nil {
		return fmt.Errorf("fake %s: %w", op, err)
	}
	return nil
}
