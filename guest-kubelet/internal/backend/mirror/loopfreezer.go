package mirror

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// ErrNoFreezer means no freeze backend is configured (Options.Suspend.Freezer is nil).
var ErrNoFreezer = errors.New("no freeze backend configured")

// killRecordTimeout bounds the Killing write before the agent Kill.
const killRecordTimeout = 5 * time.Second

// ErrNoHostFact means the freeze backend cannot say whether a mirror is suspended now.
var ErrNoHostFact = errors.New("the freeze backend reports no host state")

// HostFact is implemented by a freeze backend that can report whether a mirror's job is
// suspended now (the agent's Status, M4). Recovery (M5) trusts it over the annotation.
type HostFact interface {
	Frozen(ctx context.Context, pod *corev1.Pod) (bool, error)
}

// LoopFreezer is the orchestrator loop's freezer for --freezer=agent: the M4 agent client
// (Options.Suspend.Freezer) driven by the loop. Like Backend.Suspend and Backend.Resume it
// records the suspend state on the mirror, so the guest shows Suspended while suspended and a
// restarted guest kubelet finds the state again (M5). The loop has already held the guest
// NotReady, confirmed it, and raised the epoch before it calls Suspend or Resume; the
// deadline is the context's. Any error leaves the state for the loop's kill sequence, which
// calls Kill (agent Kill) and then deletes the mirror.
type LoopFreezer struct{ b *Backend }

// LoopFreezer returns the orchestrator loop's freezer.
func (b *Backend) LoopFreezer() (*LoopFreezer, error) {
	if b.opts.Suspend.Freezer == nil {
		return nil, ErrNoFreezer
	}
	return &LoopFreezer{b: b}, nil
}

// Suspend records Suspending, has the agent suspend the mirror and records Suspended. A failed
// agent Suspend returns the error with the guest still Suspending (NotReady): Q6 never puts a
// guest whose state is unknown back to Running, and the loop kills it.
func (f *LoopFreezer) Suspend(ctx context.Context, m *corev1.Pod, epoch int64) error {
	guest, err := f.b.lockMirrorGuest(m)
	if err != nil {
		return err
	}
	defer f.b.locks.unlock(guest.UID)
	cur, err := f.b.setSuspendState(ctx, guest, StateSuspending, false)
	if err != nil {
		return err
	}
	f.b.emit(f.b.translate(guest, cur))
	if err := f.b.opts.Suspend.Freezer.Suspend(ctx, cur, epoch); err != nil {
		f.b.event(guest, corev1.EventTypeWarning, EventSuspendFailed, "agent suspend: "+err.Error())
		return fmt.Errorf("agent suspend: %w", err)
	}
	// The readiness verdict from before the suspend must not outlive it: the prober starts over
	// from not ready, so after the resume the guest is released only once the engine answers.
	f.b.forgetProbes(cur)
	if cur, err = f.b.setSuspendState(ctx, guest, StateSuspended, false); err != nil {
		return fmt.Errorf("suspended, but recording %s failed: %w", StateSuspended, err)
	}
	f.b.emit(f.b.translate(guest, cur))
	f.b.event(guest, corev1.EventTypeNormal, EventSuspended, fmt.Sprintf("suspended by the orchestrator loop (epoch %d)", epoch))
	return nil
}

// Resume records Resuming, has the agent resume the mirror and clears the suspend state. A
// failed resume leaves the guest Resuming (NotReady) and returns the error; the loop kills it.
// The loop runs the engine check before it releases Ready.
func (f *LoopFreezer) Resume(ctx context.Context, m *corev1.Pod, epoch int64) error {
	guest, err := f.b.lockMirrorGuest(m)
	if err != nil {
		return err
	}
	defer f.b.locks.unlock(guest.UID)
	cur, err := f.b.setSuspendState(ctx, guest, StateResuming, false)
	if err != nil {
		return err
	}
	f.b.emit(f.b.translate(guest, cur))
	if err := f.b.opts.Suspend.Freezer.Resume(ctx, cur, epoch); err != nil {
		f.b.event(guest, corev1.EventTypeWarning, EventResumeFailed, "agent resume: "+err.Error())
		return fmt.Errorf("agent resume: %w", err)
	}
	if cur, err = f.b.setSuspendState(ctx, guest, "", false); err != nil {
		return fmt.Errorf("resumed, but clearing %s failed: %w", StateResuming, err)
	}
	f.b.emit(f.b.translate(guest, cur))
	f.b.event(guest, corev1.EventTypeNormal, EventResumed, fmt.Sprintf("resumed by the orchestrator loop (epoch %d)", epoch))
	return nil
}

// Kill is the first step of the loop's kill sequence: record Killing (the guest turns
// NotReady and, once the mirror stops or is deleted, Failed with ReasonKilled), then agent
// Kill. The loop deletes the mirror afterwards whatever this returns. It does not take the
// guest's operation lock: a hung Suspend may still hold it.
func (f *LoopFreezer) Kill(ctx context.Context, m *corev1.Pod, reason string) error {
	if guest := f.b.guestFor(m); guest != nil {
		kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), killRecordTimeout)
		if cur, err := f.b.setSuspendState(kctx, guest, StateKilling, false); err == nil {
			m = cur
			f.b.emit(f.b.translate(guest, cur))
		} else {
			log.G(ctx).WithError(err).WithField("mirror", m.Name).Warn("could not record Killing; killing anyway")
		}
		cancel()
		f.b.event(guest, corev1.EventTypeWarning, EventKilled, "orchestrator loop kill: "+reason)
	}
	if err := f.b.opts.Suspend.Freezer.Kill(ctx, m, reason); err != nil {
		return fmt.Errorf("agent kill: %w", err)
	}
	return nil
}

// lockMirrorGuest finds the live guest of a mirror and takes the guest's operation lock.
func (b *Backend) lockMirrorGuest(m *corev1.Pod) (*corev1.Pod, error) {
	guest := b.guestFor(m)
	if guest == nil {
		return nil, errdefs.NotFoundf("mirror %s/%s has no live guest", m.Namespace, m.Name)
	}
	if !b.locks.tryLock(guest.UID) {
		return nil, ErrBusy
	}
	return guest, nil
}

// HostFrozen reports whether the host has the mirror's job suspended now (the freeze backend's
// HostFact: the agent's Status). It returns ErrNoFreezer when no freeze backend is configured
// and ErrNoHostFact when the backend cannot tell.
func (b *Backend) HostFrozen(m *corev1.Pod) (bool, error) {
	return b.hostFrozen(context.Background(), m)
}

// hostFrozen is HostFrozen under the caller's context (bounded by killRecordTimeout).
func (b *Backend) hostFrozen(parent context.Context, m *corev1.Pod) (bool, error) {
	if b.opts.Suspend.Freezer == nil {
		return false, ErrNoFreezer
	}
	hf, ok := b.opts.Suspend.Freezer.(HostFact)
	if !ok {
		return false, ErrNoHostFact
	}
	ctx, cancel := context.WithTimeout(parent, killRecordTimeout)
	defer cancel()
	return hf.Frozen(ctx, m)
}

// Attempt returns the attempt counter in a mirror's job id (<guest UID>-<attempt>).
func Attempt(m *corev1.Pod) (int, bool) {
	uid, job := m.Labels[LabelMirrorOf], m.Labels[LabelJobID]
	rest, ok := strings.CutPrefix(job, uid+"-")
	if uid == "" || !ok {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// ListMirrors lists this virtual node's mirrors from the API (not the informer).
func (b *Backend) ListMirrors(ctx context.Context) ([]*corev1.Pod, error) {
	sel := labels.Set{LabelMirrorNode: b.opts.VirtualNode}.String()
	list, err := b.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, fmt.Errorf("list mirrors: %w", err)
	}
	out := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, &list.Items[i])
	}
	return out, nil
}

// GuestNow reads a mirror's guest from the API. It returns nil, nil when the guest is gone or
// is another pod of the same name.
func (b *Backend) GuestNow(ctx context.Context, m *corev1.Pod) (*corev1.Pod, error) {
	name := m.Annotations[AnnotationGuestName]
	if name == "" {
		return nil, nil //nolint:nilnil // no guest recorded on the mirror
	}
	g, err := b.client.CoreV1().Pods(m.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil //nolint:nilnil // guest gone
	}
	if err != nil {
		return nil, fmt.Errorf("get guest %s/%s: %w", m.Namespace, name, err)
	}
	if string(g.UID) != m.Labels[LabelMirrorOf] {
		return nil, nil //nolint:nilnil // another pod of the same name
	}
	return g, nil
}

// RecordSuspendState writes the suspend state on the guest's mirror without raising the epoch
// ("" clears it). Recovery uses it to make the annotation match the host.
func (b *Backend) RecordSuspendState(ctx context.Context, guest *corev1.Pod, state string) (*corev1.Pod, error) {
	return b.setSuspendState(ctx, guest, state, false)
}

// Adopt rebuilds the orchestrator mode's view of a guest after a restart (M5): the attempt
// counter of its current mirror, and whether its Ready follows the mirror (released) or is
// held with reason.
func (b *Backend) Adopt(guest types.UID, attempt int, released bool, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if attempt > b.gate.attempts[guest] {
		b.gate.attempts[guest] = attempt
	}
	if released {
		b.gate.released[guest] = true
		delete(b.gate.reason, guest)
		return
	}
	delete(b.gate.released, guest)
	if reason != "" {
		b.gate.reason[guest] = reason
	}
}

// FreezeState is what GET /debug/freeze-state reports for a guest.
type FreezeState struct {
	Mirror    string `json:"mirror"`
	MirrorUID string `json:"mirrorUID"`
	JobID     string `json:"jobID"`
	State     string `json:"state"`
	Epoch     int64  `json:"epoch"`
	Frozen    bool   `json:"frozen"`
	HostError string `json:"hostError,omitempty"`
}

// FreezeStateOf reads a guest's mirror from the API and the host's freeze state of it.
func (b *Backend) FreezeStateOf(ctx context.Context, namespace, name string) (FreezeState, error) {
	m, err := b.client.CoreV1().Pods(namespace).Get(ctx, Name(name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return FreezeState{}, errdefs.NotFoundf("guest %s/%s has no mirror", namespace, name)
	}
	if err != nil {
		return FreezeState{}, err
	}
	state, epoch := SuspendState(m)
	fs := FreezeState{Mirror: m.Name, MirrorUID: string(m.UID), JobID: m.Labels[LabelJobID], State: state, Epoch: epoch}
	frozen, ferr := b.hostFrozen(ctx, m)
	fs.Frozen = frozen
	if ferr != nil {
		fs.HostError = ferr.Error()
		log.G(ctx).WithError(ferr).WithField("mirror", m.Name).Debug("freeze state: host read failed")
	}
	return fs, nil
}
