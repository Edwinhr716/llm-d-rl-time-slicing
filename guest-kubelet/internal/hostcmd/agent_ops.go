package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"

	hcpb "github.com/edwinhr716/guest-kubelet/api/hostcommand/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// agentGuest is one guest a host-level agent call acts on.
type agentGuest struct {
	pod, mir *corev1.Pod
	err      error // set once the guest is finished (nil: done)
	finished bool
}

func (g *agentGuest) finish(err error) {
	g.err, g.finished = err, true
}

// runAgent carries out the command with one host-level agent call (D-NS-5 ns-host).
func (s *Server) runAgent(ctx context.Context, op *operation, guests []mirror.Guest, deadline time.Time) []*hcpb.GuestResult {
	var items []*agentGuest
	for _, g := range guests {
		if s.cfg.IsGuest(g.Pod) {
			items = append(items, &agentGuest{pod: g.Pod})
		}
	}
	if op.command == hcpb.Command_COMMAND_VACATE {
		s.agentVacate(ctx, op.epoch, deadline, items)
	} else {
		s.agentResume(ctx, op.epoch, deadline, items)
	}
	results := make([]*hcpb.GuestResult, 0, len(items))
	for _, it := range items {
		results = append(results, guestResult(ctx, op.command, it.pod, it.err))
	}
	return results
}

// parallel runs fn for every item not finished yet, in parallel.
func parallel(items []*agentGuest, fn func(*agentGuest)) {
	var wg sync.WaitGroup
	for _, it := range items {
		if it.finished {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(it)
		}()
	}
	wg.Wait()
}

// agentVacate: per guest, NotReady and confirm it (the agent does not) and write the epoch on
// the mirror; then one SuspendAll with the host command epoch; then kill each guest the agent
// did not suspend. A guest is vacated when it is suspended (or released) or its mirror is gone.
func (s *Server) agentVacate(ctx context.Context, epoch int64, deadline time.Time, items []*agentGuest) {
	dctx, cancel := context.WithDeadline(ctx, deadline.Add(-s.cfg.VacateMargin))
	defer cancel()
	parallel(items, func(it *agentGuest) {
		pod := it.pod
		rctx, rcancel := context.WithTimeout(ctx, s.cfg.KillTimeout)
		mir, found, err := s.cfg.Host.MirrorNow(rctx, pod)
		rcancel()
		switch {
		case err != nil:
			it.finish(fmt.Errorf("read mirror: %w", err))
			return
		case !found || mirrorTerminal(mir):
			s.setReleased(pod.UID, false)
			it.finish(nil)
			return
		case mir.DeletionTimestamp != nil:
			it.finish(s.waitGone(ctx, dctx, pod, mir, "mirror already being deleted"))
			return
		}
		it.mir = mir
		next := mirror.StateSuspending
		if suspendState(mir) == mirror.StateSuspended {
			next = mirror.StateSuspended // suspended before (maybe before a restart); the agent call is idempotent
			s.setReleased(pod.UID, false)
			s.cfg.Host.HoldNotReady(pod, mirror.ReasonSuspended)
		} else {
			s.setReleased(pod.UID, false)
			s.cfg.Host.HoldNotReady(pod, mirror.ReasonSuspending)
		}
		if err := s.cfg.Host.ConfirmNotReady(dctx, pod); err != nil {
			it.finish(s.killUnlessAborted(ctx, pod, mir, err, "NotReady not confirmed by the deadline"))
			return
		}
		upd, _, err := s.cfg.Host.SetMirrorSuspendState(dctx, mir, next, epoch)
		if err != nil {
			it.finish(s.killUnlessAborted(ctx, pod, mir, err, "writing the guest epoch failed"))
			return
		}
		it.mir = upd
	})
	if !anyPending(items) {
		return
	}
	start := time.Now()
	agentDeadline, _ := dctx.Deadline()
	res, err := s.cfg.Agent.SuspendAll(dctx, epoch, agentDeadline)
	logAgent(ctx, "SuspendAll", epoch, start, res, err)
	parallel(items, func(it *agentGuest) {
		if why := targetFailure(res, err, it.mir); why != "" {
			it.finish(s.killUnlessAborted(ctx, it.pod, it.mir, err, "agent did not suspend the guest: "+why))
			return
		}
		wctx := context.WithoutCancel(ctx)
		if _, _, err := s.cfg.Host.SetMirrorSuspendState(wctx, it.mir, mirror.StateSuspended, mirror.EpochKeep); err != nil {
			log.G(ctx).WithError(err).WithField("guest", it.pod.Name).Warn("host command: suspended guest not marked Suspended")
		}
		s.setReleased(it.pod.UID, false)
		s.cfg.Host.HoldNotReady(it.pod, mirror.ReasonSuspended)
		it.finish(nil)
	})
}

// agentResume: write the epoch on each suspended guest's mirror, one ResumeAll with the host
// command epoch, kill each guest the agent did not resume; then create the mirrors that are
// missing and release each guest's Ready once its engine serves.
func (s *Server) agentResume(ctx context.Context, epoch int64, deadline time.Time, items []*agentGuest) {
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var suspended []*agentGuest
	parallel(items, func(it *agentGuest) {
		if !guestLive(it.pod) {
			it.finish(nil)
			return
		}
		mir, found, err := s.cfg.Host.MirrorNow(dctx, it.pod)
		switch {
		case err != nil:
			it.finish(fmt.Errorf("read mirror: %w", err))
			return
		case !found || mirrorTerminal(mir) || mir.DeletionTimestamp != nil:
			s.setReleased(it.pod.UID, false) // startGuest below handles it
			return
		case suspendState(mir) == "":
			return // running
		}
		upd, _, err := s.cfg.Host.SetMirrorSuspendState(dctx, mir, mirror.StateResuming, epoch)
		if err != nil {
			it.finish(s.killUnlessAborted(ctx, it.pod, mir, err, "writing the guest epoch failed"))
			return
		}
		it.mir = upd
	})
	for _, it := range items {
		if !it.finished && it.mir != nil {
			suspended = append(suspended, it)
		}
	}
	if len(suspended) > 0 {
		start := time.Now()
		res, err := s.cfg.Agent.ResumeAll(dctx, epoch, deadline)
		logAgent(ctx, "ResumeAll", epoch, start, res, err)
		parallel(suspended, func(it *agentGuest) {
			if why := targetFailure(res, err, it.mir); why != "" {
				if ctx.Err() != nil {
					it.finish(ctx.Err()) // aborted: leave it suspended for the vacate that follows
					return
				}
				kerr := s.kill(context.WithoutCancel(ctx), it.pod, it.mir, "agent did not resume the guest: "+why)
				it.finish(errors.Join(errors.New("resume: "+why), kerr))
				return
			}
			if _, _, err := s.cfg.Host.SetMirrorSuspendState(dctx, it.mir, "", mirror.EpochKeep); err != nil {
				it.finish(fmt.Errorf("resumed, but clearing the suspend state failed: %w", err))
				return
			}
			s.setReleased(it.pod.UID, false)
			s.cfg.Host.HoldNotReady(it.pod, mirror.ReasonWaitingForGrant)
		})
	}
	parallel(items, func(it *agentGuest) {
		err := s.startGuest(dctx, mirror.Guest{Pod: it.pod})
		if err == nil {
			err = s.waitReady(dctx, it.pod)
		}
		it.finish(err)
	})
}

// targetFailure says why the agent did not act on the mirror's job, or "" when it did.
func targetFailure(res *AgentResult, callErr error, mir *corev1.Pod) string {
	if callErr != nil {
		return callErr.Error()
	}
	t, ok := res.Targets[jobID(mir)]
	switch {
	case !ok:
		return fmt.Sprintf("job %s not among the agent's targets", jobID(mir))
	case !t.Done:
		return t.Error
	default:
		return ""
	}
}

func (s *Server) killUnlessAborted(ctx context.Context, pod, mir *corev1.Pod, cause error, reason string) error {
	if ctx.Err() != nil {
		if cause == nil {
			cause = ctx.Err()
		}
		return cause
	}
	return s.kill(ctx, pod, mir, reason)
}

func anyPending(items []*agentGuest) bool {
	for _, it := range items {
		if !it.finished {
			return true
		}
	}
	return false
}

func logAgent(ctx context.Context, call string, epoch int64, start time.Time, res *AgentResult, err error) {
	logger := log.G(ctx).WithField("call", call).WithField("epoch", epoch).WithField("took", time.Since(start).String())
	if err != nil {
		logger.WithError(err).Warn("host command: agent call failed")
		return
	}
	logger.WithField("complete", res.Complete).WithField("targets", len(res.Targets)).
		WithField("error", res.Error).Info("host command: agent call done")
}
