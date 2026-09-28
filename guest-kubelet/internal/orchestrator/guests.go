package orchestrator

import (
	"context"
	"errors"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// guestLive reports whether a guest still wants to run: not being deleted, not finished.
func (l *Loop) guestLive(pod *corev1.Pod) bool {
	return l.cfg.IsGuest(pod) && pod.DeletionTimestamp == nil &&
		pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

// mirrorGone reports whether a mirror no longer holds the accelerator, or soon will not:
// finished, or being deleted (a deletion is not waited for here).
func mirrorGone(m *corev1.Pod) bool {
	return m.DeletionTimestamp != nil || m.Status.Phase == corev1.PodSucceeded || m.Status.Phase == corev1.PodFailed
}

// serve is one pass of step 3 under a grant. It creates missing mirrors, resumes suspended
// guests one at a time, and releases Ready for guests whose engine serves. It re-checks the
// grant before each create and resume, and stops as soon as the grant is gone.
func (l *Loop) serve(ctx context.Context, p *participant) {
	l.guestMu.Lock()
	defer l.guestMu.Unlock()
	guests, err := l.cfg.Host.Guests()
	if err != nil {
		log.G(ctx).WithError(err).Warn("orchestrator loop: cannot list guests")
		return
	}
	for _, guest := range guests {
		if !l.guestLive(guest.Pod) {
			continue
		}
		if !p.mayStart() {
			return
		}
		l.serveGuest(ctx, guest)
	}
}

func (l *Loop) serveGuest(ctx context.Context, guest mirror.Guest) {
	pod, mir := guest.Pod, guest.Mirror
	uid := pod.UID
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name)
	switch {
	case mir == nil:
		delete(l.suspended, uid)
		delete(l.released, uid)
		if err := l.cfg.Host.Create(ctx, pod); err != nil {
			logger.WithError(err).Warn("orchestrator loop: create mirror failed")
			return
		}
		logger.Info("orchestrator loop: mirror created under grant")
	case mirrorGone(mir):
		return
	case l.suspended[uid]:
		l.resume(ctx, pod, mir)
	case !l.released[uid] && l.cfg.EngineReady(pod, mir):
		l.cfg.Host.ReleaseReady(pod)
		l.released[uid] = true
		logger.Info("orchestrator loop: guest Ready")
	}
}

// resume bumps the epoch and resumes the guest within the resume budget. It stays NotReady
// until a later pass sees the engine serve. A failed resume runs the kill sequence.
func (l *Loop) resume(ctx context.Context, pod, mir *corev1.Pod) {
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name)
	start := time.Now()
	rctx, cancel := context.WithTimeout(ctx, l.cfg.ResumeBudget)
	defer cancel()
	upd, epoch, err := l.cfg.Host.BumpEpoch(rctx, mir)
	if err == nil {
		err = l.cfg.Freezer.Resume(rctx, upd, epoch)
	}
	delete(l.suspended, pod.UID)
	if err != nil {
		logger.WithError(err).Warn("orchestrator loop: resume failed; killing the guest")
		l.kill(ctx, pod, mir, "resume failed")
		return
	}
	l.cfg.Host.HoldNotReady(pod, mirror.ReasonWaitingForGrant)
	logger.WithField("epoch", epoch).WithField("took", time.Since(start).String()).Info("orchestrator loop: guest resumed")
}

// vacateGuests is step 4 for every guest that is up: hold NotReady and confirm it, then
// suspend by deadline (or delete a mirror that has no accelerator context). Any failure runs
// the kill sequence. Guests are handled one at a time.
func (l *Loop) vacateGuests(ctx context.Context, deadline time.Time) {
	l.guestMu.Lock()
	defer l.guestMu.Unlock()
	guests, err := l.cfg.Host.Guests()
	if err != nil {
		log.G(ctx).WithError(err).Warn("orchestrator loop: cannot list guests to vacate")
		return
	}
	for _, guest := range guests {
		if !l.cfg.IsGuest(guest.Pod) || l.suspended[guest.Pod.UID] {
			continue
		}
		// Read the mirror from the API: one created just before the notice may not be in the
		// informer yet, and it must be vacated too.
		mir, found, err := l.cfg.Host.MirrorNow(ctx, guest.Pod)
		if err != nil {
			log.G(ctx).WithError(err).WithField("guest", guest.Pod.Name).Warn("orchestrator loop: cannot read mirror")
			continue
		}
		if !found || mirrorGone(mir) {
			continue
		}
		l.vacateGuest(ctx, guest.Pod, mir, deadline)
	}
}

func (l *Loop) vacateGuest(ctx context.Context, pod, mir *corev1.Pod, deadline time.Time) {
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name)
	start := time.Now()
	delete(l.released, pod.UID)
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// NotReady first, always: the router must stop sending before the process stops.
	l.cfg.Host.HoldNotReady(pod, mirror.ReasonSuspending)
	if err := l.cfg.Host.ConfirmNotReady(dctx, pod); err != nil {
		logger.WithError(err).Warn("orchestrator loop: NotReady not confirmed by the deadline; killing the guest")
		l.kill(ctx, pod, mir, "NotReady not confirmed")
		return
	}
	if l.cfg.Freezer == nil || mir.Status.Phase != corev1.PodRunning {
		l.vacateByDelete(ctx, pod, mir, "no freezer or mirror not running")
		return
	}
	upd, epoch, err := l.cfg.Host.BumpEpoch(dctx, mir)
	if err == nil {
		err = l.cfg.Freezer.Suspend(dctx, upd, epoch)
	}
	switch {
	case errors.Is(err, ErrNoContext):
		l.vacateByDelete(ctx, pod, mir, "no accelerator context")
	case err != nil:
		logger.WithError(err).Warn("orchestrator loop: suspend failed; killing the guest")
		l.kill(ctx, pod, mir, "suspend failed")
	default:
		l.suspended[pod.UID] = true
		l.cfg.Host.HoldNotReady(pod, mirror.ReasonSuspended)
		logger.WithField("epoch", epoch).WithField("took", time.Since(start).String()).
			WithField("left", time.Until(deadline).String()).Info("orchestrator loop: guest suspended")
	}
}

func (l *Loop) vacateByDelete(ctx context.Context, pod, mir *corev1.Pod, why string) {
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name).WithField("why", why)
	if err := l.cfg.Host.VacateMirror(ctx, pod, mir); err != nil {
		logger.WithError(err).Warn("orchestrator loop: deleting the mirror to vacate failed")
		return
	}
	logger.Info("orchestrator loop: mirror deleted to vacate")
}

// kill is the kill sequence: Kill if the freezer can, then delete the mirror with normal
// grace. The guest then shows Failed.
func (l *Loop) kill(ctx context.Context, pod, mir *corev1.Pod, reason string) {
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name).WithField("reason", reason)
	delete(l.suspended, pod.UID)
	delete(l.released, pod.UID)
	if k, ok := l.cfg.Freezer.(Killer); ok {
		kctx, cancel := context.WithTimeout(ctx, l.cfg.RPCTimeout)
		if err := k.Kill(kctx, mir, reason); err != nil {
			logger.WithError(err).Warn("orchestrator loop: kill failed; deleting the mirror anyway")
		}
		cancel()
	}
	if err := l.cfg.Host.KillMirror(ctx, mir); err != nil {
		logger.WithError(err).Error("orchestrator loop: deleting the mirror failed")
		return
	}
	logger.Warn("orchestrator loop: guest killed")
}
