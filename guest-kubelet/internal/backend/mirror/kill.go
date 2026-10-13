package mirror

import (
	"context"
	"strings"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// AnnotationKilled is set on a mirror the kill sequence is removing; its value is why. The
	// guest shows Failed from then on, even while the mirror is still terminating.
	AnnotationKilled = "timeslice.io/killed"
	// AnnotationKilling is set on a mirror before the kill sequence's agent Kill; its value is the
	// cause. The agent's Kill can stop the mirror before the kill is recorded, and the library never
	// updates a Failed guest again, so a stopped mirror carrying it already reports the kill.
	AnnotationKilling = "timeslice.io/killing"

	// ReasonAgentKilled is the guest's reason after the kill sequence (Q6).
	ReasonAgentKilled = "SnapshotAgentKilled"
	// ReasonNoContext is the guest's reason when its mirror was deleted instead of suspended:
	// the agent does not list the job, so it holds no accelerator context to save.
	ReasonNoContext = "NoAcceleratorContext"

	maxKilledMessage = 1024
)

// killedReason returns the kill message recorded on a mirror, or "".
func killedReason(m *corev1.Pod) string {
	if m == nil {
		return ""
	}
	return m.Annotations[AnnotationKilled]
}

// killStarted: a kill sequence started on the mirror (its agent Kill may not have answered yet).
// No suspend, resume or repair touches it any more.
func killStarted(m *corev1.Pod) bool {
	return killedReason(m) != "" || (m != nil && m.Annotations[AnnotationKilling] != "")
}

// stoppedMirror: the mirror's processes are gone or going (terminal phase or being deleted).
func stoppedMirror(m *corev1.Pod) bool {
	if m == nil {
		return false
	}
	return m.DeletionTimestamp != nil || m.Status.Phase == corev1.PodFailed || m.Status.Phase == corev1.PodSucceeded
}

// killedStatus is the guest's Failed status for a killed mirror, or nil if the mirror was not
// killed. The in-memory records cover a mirror whose annotation could not be written. A mirror
// that stopped while a kill sequence runs (the agent's Kill came first), or while Suspended (a
// frozen process cannot exit by itself: the agent or the orchestrator killed it), also counts:
// virtual-kubelet never updates a Failed guest again, so the first Failed must carry the reason.
func (b *Backend) killedStatus(guest, mirrorPod *corev1.Pod) *corev1.Pod {
	msg := killedReason(mirrorPod)
	b.mu.Lock()
	if msg == "" {
		msg = b.killed[guest.UID]
	}
	cause := b.killing[guest.UID]
	b.mu.Unlock()
	if msg == "" && stoppedMirror(mirrorPod) {
		if cause == "" {
			cause = mirrorPod.Annotations[AnnotationKilling]
		}
		state, _ := SuspendState(mirrorPod)
		switch {
		case cause != "":
			msg = truncateMsg("killed after " + cause)
		case state == StateSuspended:
			msg = "mirror stopped while Suspended: killed by the snapshot agent"
		}
	}
	if msg == "" {
		return nil
	}
	reason := ReasonAgentKilled
	if strings.HasPrefix(msg, ReasonNoContext) {
		reason = ReasonNoContext
	}
	out := TerminalStatus(guest, mirrorPod, reason)
	out.Status.Message = msg
	return out
}

// killGuest is the Q6 kill sequence after an agent failure, deadline miss or Unimplemented:
//  1. agent Kill with deadline now + K (best effort: the delete below runs either way);
//  2. record the kill on the mirror, so every replica shows the guest Failed;
//  3. delete the mirror with normal grace (never force): the pod stays until its processes are
//     gone, and the hold counts it until then;
//  4. report the guest Failed.
//
// It returns the message recorded, for Result.Killed.
func (b *Backend) killGuest(ctx context.Context, guest, m *corev1.Pod, cause string) string {
	so := &b.opts.Suspend
	job := m.Labels[LabelJobID]
	logger := log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).WithField("job", job)
	msg := truncateMsg("killed after " + cause)
	b.markKilling(ctx, guest, cause)

	start := time.Now()
	deadline := start.Add(so.KillBudget)
	kctx, cancel := waitCtx(context.WithoutCancel(ctx), deadline)
	kerr := so.Agent.Kill(kctx, job, deadline, cause)
	cancel()
	if kerr != nil {
		msg = truncateMsg(msg + "; agent Kill not confirmed: " + kerr.Error())
		logger.WithError(kerr).WithField("killMs", time.Since(start).Milliseconds()).
			Warn("agent Kill not confirmed; deleting the mirror anyway")
	} else {
		logger.WithField("killMs", time.Since(start).Milliseconds()).Info("agent killed the guest")
	}
	b.removeKilled(ctx, guest, m, msg)
	return msg
}

// markKilling records, before the agent's Kill, that a kill sequence runs: in memory and on the
// mirror, so a mirror the Kill stops first already translates to the kill (killedStatus).
func (b *Backend) markKilling(ctx context.Context, guest *corev1.Pod, cause string) {
	cause = truncateMsg(cause)
	b.mu.Lock()
	b.killing[guest.UID] = cause
	b.mu.Unlock()
	if _, err := b.mutateMirror(context.WithoutCancel(ctx), guest, func(_ *corev1.Pod, a map[string]string) {
		a[AnnotationKilling] = cause
	}); err != nil {
		log.G(ctx).WithError(err).WithField("guest", guest.Namespace+"/"+guest.Name).
			Warn("could not record the running kill on the mirror; the in-memory record covers this replica")
	}
}

// deleteNoContext deletes the mirror of a guest the agent does not list, instead of suspending
// it (Q6: suspend only the jobs the agent's Status lists). No agent call is made.
func (b *Backend) deleteNoContext(ctx context.Context, guest, m *corev1.Pod) string {
	msg := ReasonNoContext + ": the snapshot agent does not list job " + m.Labels[LabelJobID] + "; deleted instead of suspended"
	log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).Warn(msg)
	b.removeKilled(ctx, guest, m, msg)
	return msg
}

// removeKilled records msg on the mirror and in memory, deletes the mirror with normal grace
// and reports the guest Failed.
func (b *Backend) removeKilled(ctx context.Context, guest, m *corev1.Pod, msg string) {
	ctx = context.WithoutCancel(ctx) // finish even if the caller gave up
	logger := log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name)
	b.mu.Lock()
	b.killed[guest.UID] = msg
	b.mu.Unlock()
	if m2, err := b.mutateMirror(ctx, guest, func(_ *corev1.Pod, a map[string]string) { a[AnnotationKilled] = msg }); err == nil {
		m = m2
	} else {
		logger.WithError(err).Warn("could not record the kill on the mirror")
	}
	uid := m.UID
	err := b.client.CoreV1().Pods(m.Namespace).Delete(ctx, m.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		logger.WithError(err).Error("could not delete the killed mirror")
	}
	if st := b.killedStatus(guest, m); st != nil {
		b.emit(st)
	}
	b.event(guest, corev1.EventTypeWarning, EventKilled, msg)
	if f := b.opts.Suspend.OnHoldChange; f != nil {
		f()
	}
}

// forgetKilled drops the in-memory kill record and attempt count of a guest that is gone.
func (b *Backend) forgetKilled(uid types.UID) {
	b.mu.Lock()
	delete(b.killed, uid)
	delete(b.killing, uid)
	delete(b.attempts, uid)
	b.mu.Unlock()
}

// killBeforeDelete asks the agent to kill a mirror in any suspend state before it is deleted:
// a checkpointed or half-restored process may not act on SIGTERM, so it would sit out the whole
// grace period. Best effort, bounded by K; the guest kubelet never thaws a cgroup itself.
func (b *Backend) killBeforeDelete(ctx context.Context, m *corev1.Pod) {
	so := &b.opts.Suspend
	state, _ := SuspendState(m)
	job := m.Labels[LabelJobID]
	if so.Agent == nil || state == "" || job == "" {
		return
	}
	deadline := time.Now().Add(so.KillBudget)
	kctx, cancel := waitCtx(ctx, deadline)
	defer cancel()
	if err := so.Agent.Kill(kctx, job, deadline, "guest deleted while "+state); err != nil {
		log.G(ctx).WithError(err).WithField("mirror", m.Namespace+"/"+m.Name).Warn("agent Kill before delete not confirmed")
	}
}

// truncateMsg bounds a kill message (maxKilledMessage).
func truncateMsg(s string) string {
	if len(s) <= maxKilledMessage {
		return s
	}
	return s[:maxKilledMessage]
}
