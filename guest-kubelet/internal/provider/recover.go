package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// RecoverHost is what recovery needs from the mirror backend. *mirror.Backend implements it.
type RecoverHost interface {
	// ListMirrors lists this virtual node's mirrors from the API.
	ListMirrors(ctx context.Context) ([]*corev1.Pod, error)
	// GuestNow reads a mirror's guest from the API; nil, nil when it is gone.
	GuestNow(ctx context.Context, m *corev1.Pod) (*corev1.Pod, error)
	// RecordSuspendState writes the suspend state on the guest's mirror ("" clears it).
	RecordSuspendState(ctx context.Context, guest *corev1.Pod, state string) (*corev1.Pod, error)
	// Adopt rebuilds the orchestrator mode's view of a guest.
	Adopt(guest types.UID, attempt int, released bool, reason string)
}

// RecoverOptions configures Recover.
type RecoverOptions struct {
	// Frozen reports whether the host has a mirror's processes stopped now. Nil means there is
	// no host fact (no freezer), and the recorded state is trusted.
	Frozen func(m *corev1.Pod) (bool, error)
	// RecordState makes the mirror's suspend-state annotation match the host when they differ.
	// Set it when the freezer is the one that writes that annotation (--freezer=agent).
	RecordState bool
	// OnAdopt is called for each adopted guest, for example to seed the orchestrator loop.
	OnAdopt func(Adopted)
}

// Adopted is what Recover found for one guest.
type Adopted struct {
	Guest, Mirror *corev1.Pod
	// Attempt is the attempt counter in the mirror's job id.
	Attempt int
	// State is the suspend state after recovery ("" = running).
	State string
	// Suspended: the mirror is frozen; it is resumed at the next grant.
	Suspended bool
	// Released: the guest is serving and Ready; its Ready keeps following the mirror.
	Released bool
	// Mismatch describes a recorded state the host contradicted ("" = none).
	Mismatch string
}

// Recover is M5: after a restart it relists this node's mirrors from the API, before the pod
// controller and the orchestrator loop start, and rebuilds the state a restart lost. The host
// wins over the recorded annotation: a frozen mirror is Suspended whatever the annotation says
// (a crash between the freeze and the write leaves Suspending), and a thawed one is running
// (a crash mid-thaw leaves Resuming). Mirrors whose guest is gone are left to the orphan loop.
// Nothing is created, deleted, frozen or thawed here.
func Recover(ctx context.Context, host RecoverHost, opts RecoverOptions) ([]Adopted, error) {
	mirrors, err := host.ListMirrors(ctx)
	if err != nil {
		return nil, err
	}
	var out []Adopted
	var errs []error
	for _, m := range mirrors {
		a, ok, err := recoverOne(ctx, host, opts, m)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !ok {
			continue
		}
		out = append(out, a)
		if opts.OnAdopt != nil {
			opts.OnAdopt(a)
		}
	}
	return out, errors.Join(errs...)
}

func recoverOne(ctx context.Context, host RecoverHost, opts RecoverOptions, mp *corev1.Pod) (Adopted, bool, error) {
	logger := log.G(ctx).WithField("mirror", mp.Namespace+"/"+mp.Name)
	if mp.DeletionTimestamp != nil || mp.Status.Phase == corev1.PodSucceeded || mp.Status.Phase == corev1.PodFailed {
		logger.Info("recover: mirror terminal or deleting; skipped")
		return Adopted{}, false, nil
	}
	guest, err := host.GuestNow(ctx, mp)
	if err != nil {
		return Adopted{}, false, err
	}
	if guest == nil {
		logger.Info("recover: guest gone; left to the orphan loop")
		return Adopted{}, false, nil
	}
	attempt, _ := mirror.Attempt(mp)
	recorded, _ := mirror.SuspendState(mp)
	adopted := Adopted{Guest: guest, Mirror: mp, Attempt: attempt, State: recorded}

	adopted.Suspended = recorded == mirror.StateSuspended
	if state := hostFact(ctx, opts, mp); state != hostUnknown {
		if err := reconcileState(ctx, host, opts, &adopted, state == hostFrozen); err != nil {
			return Adopted{}, false, err
		}
	}
	reason := ""
	switch {
	case adopted.Suspended:
		reason = mirror.ReasonSuspended
	case adopted.State == "" && adopted.Mismatch == "" && mp.Status.Phase == corev1.PodRunning && mirror.IsReady(guest):
		// Only a mirror that was running all along keeps its Ready. One the host showed thawed
		// mid-transition waits for the loop's engine check before it is released again.
		adopted.Released = true
	}
	host.Adopt(guest.UID, attempt, adopted.Released, reason)
	logger.WithField("guest", guest.Namespace+"/"+guest.Name).WithField("attempt", attempt).
		WithField("state", adopted.State).WithField("released", adopted.Released).Info("recover: guest adopted")
	return adopted, true, nil
}

// hostState is what the host says about a mirror's processes.
type hostState int

const (
	// hostUnknown: no host fact (no Frozen func) or it cannot be read; the recorded state is trusted.
	hostUnknown hostState = iota
	hostThawed
	hostFrozen
)

// hostFact reads whether the host has the mirror frozen now.
func hostFact(ctx context.Context, opts RecoverOptions, mp *corev1.Pod) hostState {
	if opts.Frozen == nil {
		return hostUnknown
	}
	frozen, err := opts.Frozen(mp)
	switch {
	case err == nil && frozen:
		return hostFrozen
	case err == nil:
		// The agent does not list the job as suspended (or not at all): nothing is frozen.
		return hostThawed
	default:
		log.G(ctx).WithField("mirror", mp.Namespace+"/"+mp.Name).WithError(err).
			Warn("recover: cannot read the host freeze state; trusting the annotation")
		return hostUnknown
	}
}

// reconcileState makes the adopted state follow the host: frozen is Suspended, thawed is running.
// A recorded state the host contradicts is a mismatch, rewritten when opts.RecordState is set.
func reconcileState(ctx context.Context, host RecoverHost, opts RecoverOptions, adopted *Adopted, frozen bool) error {
	want := ""
	if frozen {
		want = mirror.StateSuspended
	}
	recorded := adopted.State
	adopted.State, adopted.Suspended = want, frozen
	if recorded == want {
		return nil
	}
	mp := adopted.Mirror
	adopted.Mismatch = fmt.Sprintf("recorded %q, host frozen=%t", recorded, frozen)
	log.G(ctx).WithField("mirror", mp.Namespace+"/"+mp.Name).WithField("mismatch", adopted.Mismatch).
		Warn("recover: recorded state differs from the host; the host wins")
	if !opts.RecordState {
		return nil
	}
	if _, err := host.RecordSuspendState(ctx, adopted.Guest, want); err != nil {
		return fmt.Errorf("record %q on %s/%s: %w", want, mp.Namespace, mp.Name, err)
	}
	return nil
}
