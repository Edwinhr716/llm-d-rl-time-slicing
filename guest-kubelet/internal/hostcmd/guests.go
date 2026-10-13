package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// guestLive reports whether a guest still wants to run: not being deleted, not finished.
func guestLive(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil &&
		pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

// mirrorTerminal reports whether a mirror has finished and no longer holds the accelerator.
func mirrorTerminal(m *corev1.Pod) bool {
	return m.Status.Phase == corev1.PodSucceeded || m.Status.Phase == corev1.PodFailed
}

func (s *Server) isReleased(uid types.UID) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.released[uid]
}

func (s *Server) setReleased(uid types.UID, released bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if released {
		s.released[uid] = true
	} else {
		delete(s.released, uid)
	}
}

// markServed records that the mirror's engine served; forgetServed drops a mirror that is gone.
func (s *Server) markServed(mir *corev1.Pod) {
	if mir == nil {
		return
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.served[mir.UID] = true
}

func (s *Server) forgetServed(mir *corev1.Pod) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	delete(s.served, mir.UID)
}

// neverServed reports whether a running, not suspended mirror has not served yet: its Ready
// was never released by this process and its engine does not serve now. Its process may have
// initialized the accelerator without a context the agent can checkpoint and verify (an engine
// that is still loading), so a vacate deletes it instead of suspending it; the guest stays
// Pending and starts again on the next Resume.
func (s *Server) neverServed(pod, mir *corev1.Pod) bool {
	if suspendState(mir) != "" || s.isReleased(pod.UID) || s.cfg.EngineReady(pod, mir) {
		return false
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return !s.served[mir.UID]
}

// suspendState is the guest's suspend state as recorded on its mirror: "" (running),
// Suspending, Suspended or Resuming. The mirror is the record, so it survives a restart.
func suspendState(m *corev1.Pod) string {
	state, _ := mirror.SuspendState(m)
	return state
}

// hostFrozen asks the freezer whether the mirror is stopped now, when the freezer can tell
// (freeze.Backend and FakeFreezer can). After a restart the host wins over a half-written
// record: a Suspending mirror that is frozen is Suspended; a Resuming one that runs is resumed.
func (s *Server) hostFrozen(m *corev1.Pod) (bool, bool) { //nolint:gocritic // see nonamedreturns
	f, ok := s.cfg.Freezer.(interface {
		Frozen(pod *corev1.Pod) (bool, error)
	})
	if !ok {
		return false, false
	}
	frozen, err := f.Frozen(m)
	if err != nil {
		return false, false
	}
	return frozen, true
}

// ---- Vacate ----

// vacateGuest leaves the guest suspended or its mirror gone, or returns why not. The mirror
// is read from the API: one created a moment ago may not be in the informer yet.
func (s *Server) vacateGuest(ctx context.Context, pod *corev1.Pod, deadline time.Time) error {
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name)
	start := time.Now()
	dctx, cancel := context.WithDeadline(ctx, deadline.Add(-s.cfg.VacateMargin))
	defer cancel()

	// Not bounded by the deadline: a retry after it still has to find and kill the mirror.
	rctx, rcancel := context.WithTimeout(ctx, s.cfg.KillTimeout)
	mir, found, err := s.cfg.Host.MirrorNow(rctx, pod)
	rcancel()
	if err != nil {
		return fmt.Errorf("read mirror: %w", err)
	}
	if !found || mirrorTerminal(mir) {
		s.setReleased(pod.UID, false)
		return nil
	}
	if mir.DeletionTimestamp != nil {
		return s.waitGone(ctx, dctx, pod, mir, "mirror already being deleted")
	}
	switch state := suspendState(mir); {
	case state == mirror.StateSuspended:
		s.setReleased(pod.UID, false)
		s.cfg.Host.HoldNotReady(pod, mirror.ReasonSuspended)
		return nil // suspended by an earlier vacate (maybe before a restart) and not resumed since
	case state != "":
		// A freeze or thaw was cut off (a restart, or an aborted resume). The host wins: a frozen
		// mirror is Suspended; otherwise it runs, and the normal path below suspends it.
		if frozen, known := s.hostFrozen(mir); known && frozen {
			s.setReleased(pod.UID, false)
			s.cfg.Host.HoldNotReady(pod, mirror.ReasonSuspended)
			if _, _, err := s.cfg.Host.SetMirrorSuspendState(dctx, mir, mirror.StateSuspended, mirror.EpochKeep); err != nil {
				logger.WithError(err).Warn("host command: frozen mirror not marked Suspended")
			}
			logger.WithField("recorded", state).Info("host command: guest found frozen; counted as suspended")
			return nil
		}
	}

	// NotReady first, always: the router must stop sending before the process stops.
	s.setReleased(pod.UID, false)
	s.cfg.Host.HoldNotReady(pod, mirror.ReasonSuspending)
	if err := s.cfg.Host.ConfirmNotReady(dctx, pod); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return s.kill(ctx, pod, mir, "NotReady not confirmed by the deadline")
	}
	if s.cfg.Freezer == nil || mir.Status.Phase != corev1.PodRunning {
		return s.vacateByDelete(ctx, dctx, pod, mir, "no freezer or mirror not running")
	}
	// Suspending and the new epoch in one compare-and-swap, before the freeze: a restart after it
	// finds the guest mid-freeze and asks the freezer (above).
	upd, epoch, err := s.cfg.Host.SetMirrorSuspendState(dctx, mir, mirror.StateSuspending, mirror.EpochBump)
	if err == nil {
		err = s.cfg.Freezer.Suspend(dctx, upd, epoch)
	}
	switch {
	case errors.Is(err, ErrNoContext):
		return s.vacateByDelete(ctx, dctx, pod, mir, "no accelerator context")
	case err != nil && ctx.Err() != nil:
		return err
	case err != nil:
		logger.WithError(err).Warn("host command: suspend failed; killing the guest")
		return s.kill(ctx, pod, mir, "suspend failed: "+err.Error())
	}
	wctx := context.WithoutCancel(ctx)
	if _, _, err := s.cfg.Host.SetMirrorSuspendState(wctx, upd, mirror.StateSuspended, mirror.EpochKeep); err != nil {
		// The guest is frozen: that is success. Suspending on the mirror still counts as not
		// running (Hold, the next vacate asks the freezer).
		logger.WithError(err).Warn("host command: suspended guest not marked Suspended")
	}
	s.setReleased(pod.UID, false)
	s.cfg.Host.HoldNotReady(pod, mirror.ReasonSuspended)
	logger.WithField("epoch", epoch).WithField("took", time.Since(start).String()).
		WithField("left", time.Until(deadline).String()).Info("host command: guest suspended")
	return nil
}

// vacateByDelete deletes the mirror with its normal grace and waits until it is gone.
func (s *Server) vacateByDelete(ctx, dctx context.Context, pod, mir *corev1.Pod, why string) error {
	if err := s.cfg.Host.VacateMirror(dctx, pod, mir); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return s.kill(ctx, pod, mir, "deleting the mirror failed: "+err.Error())
	}
	return s.waitGone(ctx, dctx, pod, mir, why)
}

// waitGone waits by the deadline until the mirror is gone; past it, the kill sequence runs.
func (s *Server) waitGone(ctx, dctx context.Context, pod, mir *corev1.Pod, why string) error {
	start := time.Now()
	if err := s.cfg.Host.WaitMirrorGone(dctx, mir); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return s.kill(ctx, pod, mir, "mirror not gone by the deadline")
	}
	s.setReleased(pod.UID, false)
	s.forgetServed(mir)
	log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name).WithField("why", why).
		WithField("took", time.Since(start).String()).Info("host command: mirror gone")
	return nil
}

// kill is the kill sequence: Kill if the freezer can, then delete the mirror with normal
// grace and wait until it is gone. The guest then shows Failed. It returns nil once the
// mirror is gone: the accelerator is clear.
func (s *Server) kill(ctx context.Context, pod, mir *corev1.Pod, reason string) error {
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name).WithField("reason", reason)
	s.setReleased(pod.UID, false)
	kctx, cancel := context.WithTimeout(ctx, s.cfg.KillTimeout)
	defer cancel()
	var killErr error
	if s.cfg.Agent != nil {
		killErr = s.cfg.Agent.Kill(kctx, jobID(mir), reason)
	} else if k, ok := s.cfg.Freezer.(Killer); ok {
		killErr = k.Kill(kctx, mir, reason)
	}
	if killErr != nil {
		logger.WithError(killErr).Warn("host command: kill failed; deleting the mirror anyway")
	}
	if err := s.cfg.Host.KillMirror(kctx, mir); err != nil {
		return fmt.Errorf("%s; deleting the mirror failed: %w", reason, err)
	}
	if err := s.cfg.Host.WaitMirrorGone(kctx, mir); err != nil {
		return fmt.Errorf("%s; killed mirror not gone: %w", reason, err)
	}
	s.forgetServed(mir)
	logger.Warn("host command: guest killed")
	return nil
}

// ---- Resume ----

// resumeGuest brings one guest back: it creates a missing mirror or resumes a suspended one,
// then releases Ready once the engine serves, all by the deadline.
func (s *Server) resumeGuest(ctx context.Context, guest mirror.Guest, deadline time.Time) error {
	pod := guest.Pod
	if !guestLive(pod) {
		return nil
	}
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := s.startGuest(dctx, guest); err != nil {
		return err
	}
	return s.waitServing(ctx, pod, deadline)
}

// startGuest makes the guest's mirror run: create it, or resume it if this process
// suspended it. A running mirror is left alone.
func (s *Server) startGuest(ctx context.Context, guest mirror.Guest) error {
	pod := guest.Pod
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name)
	mir, found, err := s.cfg.Host.MirrorNow(ctx, pod)
	if err != nil {
		return fmt.Errorf("read mirror: %w", err)
	}
	switch {
	case found && mirrorTerminal(mir):
		return nil // the guest's workload finished; the guest reports it
	case found && mir.DeletionTimestamp != nil:
		if err := s.cfg.Host.WaitMirrorGone(ctx, mir); err != nil {
			return err
		}
		found = false
	}
	if !found {
		s.setReleased(pod.UID, false)
		if err := s.cfg.Host.Create(ctx, pod); err != nil {
			return fmt.Errorf("create mirror: %w", err)
		}
		logger.Info("host command: mirror created")
		return nil
	}
	if suspendState(mir) == "" {
		return nil // running
	}
	if s.cfg.Freezer == nil {
		return errors.New("guest is suspended and was not resumed")
	}
	return s.resumeMirror(ctx, pod, mir)
}

// resumeMirror bumps the epoch and resumes the guest within the resume budget. It stays
// NotReady until the engine serves. A failed resume runs the kill sequence. The mirror shows
// Resuming during the thaw, and no suspend state once it runs, so a restart in between knows.
func (s *Server) resumeMirror(ctx context.Context, pod, mir *corev1.Pod) error {
	start := time.Now()
	logger := log.G(ctx).WithField("guest", pod.Namespace+"/"+pod.Name)
	rctx, cancel := context.WithTimeout(ctx, s.cfg.ResumeBudget)
	defer cancel()
	if state := suspendState(mir); state == mirror.StateResuming {
		// A thaw was cut off (a restart). The host wins: a running mirror is resumed already.
		if frozen, known := s.hostFrozen(mir); known && !frozen {
			if _, _, err := s.cfg.Host.SetMirrorSuspendState(rctx, mir, "", mirror.EpochKeep); err != nil {
				return fmt.Errorf("clear the suspend state of a running mirror: %w", err)
			}
			s.setReleased(pod.UID, false)
			s.cfg.Host.HoldNotReady(pod, mirror.ReasonWaitingForGrant)
			logger.Info("host command: guest found running; counted as resumed")
			return nil
		}
	}
	upd, epoch, err := s.cfg.Host.SetMirrorSuspendState(rctx, mir, mirror.StateResuming, mirror.EpochBump)
	if err == nil {
		err = s.cfg.Freezer.Resume(rctx, upd, epoch)
	}
	if err == nil {
		_, _, err = s.cfg.Host.SetMirrorSuspendState(rctx, upd, "", mirror.EpochKeep)
		if err != nil {
			err = fmt.Errorf("clear the suspend state: %w", err)
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			return err // aborted: leave it suspended for the vacate that follows
		}
		kerr := s.kill(context.WithoutCancel(ctx), pod, mir, "resume failed: "+err.Error())
		return errors.Join(fmt.Errorf("resume: %w", err), kerr)
	}
	s.setReleased(pod.UID, false)
	s.cfg.Host.HoldNotReady(pod, mirror.ReasonWaitingForGrant)
	logger.WithField("epoch", epoch).
		WithField("took", time.Since(start).String()).Info("host command: guest resumed")
	return nil
}

// waitServing waits for the guest's engine to serve, up to EngineStartBudget past the deadline:
// an engine that starts cold (a new mirror: image pull, model load) can take minutes, and a
// Resume that fails only because the engine is still loading makes the orchestrator retry it for
// nothing. The wait still ends at once when the mirror ends (nothing to wait for) or a newer
// command aborts it.
func (s *Server) waitServing(ctx context.Context, pod *corev1.Pod, deadline time.Time) error {
	wctx, cancel := context.WithDeadline(ctx, deadline.Add(s.cfg.EngineStartBudget))
	defer cancel()
	return s.waitReady(wctx, pod)
}

// waitReady releases the guest's Ready once its engine serves, or fails at ctx's end.
func (s *Server) waitReady(ctx context.Context, pod *corev1.Pod) error {
	tick := time.NewTicker(s.cfg.PollInterval)
	defer tick.Stop()
	for {
		ok, err := s.releaseIfReady(pod)
		if err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("engine not serving yet: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// releaseIfReady releases Ready if the guest's mirror (from the informer) serves.
func (s *Server) releaseIfReady(pod *corev1.Pod) (bool, error) {
	if s.isReleased(pod.UID) {
		return true, nil
	}
	guests, err := s.cfg.Host.Guests()
	if err != nil {
		return false, err
	}
	for _, g := range guests {
		if g.Pod.UID != pod.UID {
			continue
		}
		if !guestLive(g.Pod) || (g.Mirror != nil && mirrorTerminal(g.Mirror)) {
			return true, nil // nothing to release
		}
		// A suspended (or mid-freeze or mid-thaw) guest is released only after its resume.
		if g.Mirror == nil || suspendState(g.Mirror) != "" || !s.cfg.EngineReady(g.Pod, g.Mirror) {
			return false, nil
		}
		s.cfg.Host.ReleaseReady(g.Pod)
		s.setReleased(pod.UID, true)
		s.markServed(g.Mirror)
		log.G(s.ctx).WithField("guest", pod.Namespace+"/"+pod.Name).Info("host command: guest Ready")
		return true, nil
	}
	return true, nil // the guest is gone
}

// serveLoop runs while the node is lent: guests that arrived after the Resume get a mirror,
// and guests whose engine starts serving get their Ready.
func (s *Server) serveLoop(ctx context.Context) {
	tick := time.NewTicker(s.cfg.ServeInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-s.nudge:
		}
		s.servePass(ctx)
	}
}

func (s *Server) servePass(ctx context.Context) {
	if !s.ready() {
		return // the guest list is not complete yet
	}
	s.guestMu.Lock()
	defer s.guestMu.Unlock()
	if ctx.Err() != nil {
		return // a vacate took over
	}
	guests, err := s.cfg.Host.Guests()
	if err != nil {
		log.G(ctx).WithError(err).Warn("host command: cannot list guests")
		return
	}
	for _, g := range guests {
		if ctx.Err() != nil {
			return
		}
		if !s.cfg.IsGuest(g.Pod) || !guestLive(g.Pod) {
			continue
		}
		// No mirror: create one. A mirror still marked suspended while lent (a Resume cut off
		// by a restart and then aborted, or a lost state write) is resumed: it must not stay
		// frozen on a lent node.
		if g.Mirror == nil || (s.cfg.Agent == nil && suspendState(g.Mirror) != "" && !mirrorTerminal(g.Mirror)) {
			if err := s.startGuest(ctx, g); err != nil {
				log.G(ctx).WithError(err).WithField("guest", g.Pod.Name).Warn("host command: starting a guest failed")
			}
			continue
		}
		if _, err := s.releaseIfReady(g.Pod); err != nil {
			log.G(ctx).WithError(err).Warn("host command: release check failed")
		}
	}
}

// ready reports whether Config.Ready is closed (or not set).
func (s *Server) ready() bool {
	if s.cfg.Ready == nil {
		return true
	}
	select {
	case <-s.cfg.Ready:
		return true
	default:
		return false
	}
}
