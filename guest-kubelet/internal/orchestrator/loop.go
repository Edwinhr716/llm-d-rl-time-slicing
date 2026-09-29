// Package orchestrator is the VK side of the orchestrator guest protocol: the VK is a lock
// participant of its real node's group. It polls GetGroupStatus with its participant id every
// 0.5 s, waits in Acquire(ROLE_BACKGROUND) until the node is lent, then creates or resumes the
// guests' mirrors and releases their Ready once the engine serves. When vacate_within appears
// it holds each guest NotReady, confirms it, suspends it by the deadline and hands the grant
// back with Yield(ROLE_BACKGROUND).
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	pb "github.com/edwinhr716/guest-kubelet/api/timeslice_orchestrator/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// BackgroundProtocol is the GroupStatus.background_protocol value this loop speaks.
const BackgroundProtocol = 1

// participantGrace delays participant_id on polls until the first Acquire has registered, so
// the first poll does not make the orchestrator record a claim for this node.
const participantGrace = 200 * time.Millisecond

// Host is what the loop needs from the mirror backend. *mirror.Backend implements it.
type Host interface {
	SetGroup(group string)
	Guests() ([]mirror.Guest, error)
	Create(ctx context.Context, guest *corev1.Pod) error
	HoldNotReady(guest *corev1.Pod, reason string)
	ReleaseReady(guest *corev1.Pod)
	ConfirmNotReady(ctx context.Context, guest *corev1.Pod) error
	MirrorNow(ctx context.Context, guest *corev1.Pod) (*corev1.Pod, bool, error)
	VacateMirror(ctx context.Context, guest, m *corev1.Pod) error
	KillMirror(ctx context.Context, m *corev1.Pod) error
	BumpEpoch(ctx context.Context, m *corev1.Pod) (*corev1.Pod, int64, error)
}

// Config configures the loop. Durations left zero get the contract defaults.
type Config struct {
	// Node is the real node; the participant and job id is "vk/<Node>".
	Node   string
	Client pb.TimeSliceOrchestratorServiceClient
	Group  GroupSource
	Host   Host
	// Freezer suspends and resumes mirrors. Nil vacates by deleting the mirror.
	Freezer Freezer
	// IsGuest selects the pods on the virtual node that get a mirror.
	IsGuest func(*corev1.Pod) bool
	// EngineReady reports whether a running mirror really serves. The default is the mirror's
	// ContainersReady; the VK readiness prober (M2) plugs in here.
	EngineReady func(guest, m *corev1.Pod) bool

	PollInterval  time.Duration // GetGroupStatus period (0.5 s)
	Liveness      time.Duration // L: a grant is trusted only within L of a successful poll (3 s)
	VacateMargin  time.Duration // taken off vacate_within for the poll's latency (250 ms)
	ResumeBudget  time.Duration // bound on each Resume (30 s)
	RPCTimeout    time.Duration // timeout of each unary RPC (5 s)
	RetryInterval time.Duration // spacing of retries (200 ms)
	GroupRecheck  time.Duration // how often the group is read again (10 s)
}

func (c *Config) defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	set(&c.PollInterval, 500*time.Millisecond)
	set(&c.Liveness, 3*time.Second)
	set(&c.VacateMargin, 250*time.Millisecond)
	set(&c.ResumeBudget, 30*time.Second)
	set(&c.RPCTimeout, 5*time.Second)
	set(&c.RetryInterval, 200*time.Millisecond)
	set(&c.GroupRecheck, 10*time.Second)
	if c.EngineReady == nil {
		c.EngineReady = MirrorContainersReady
	}
	if c.IsGuest == nil {
		c.IsGuest = func(*corev1.Pod) bool { return true }
	}
}

// MirrorContainersReady is the default engine check: the mirror runs and its containers are
// ready. The mirror has no probes, so this only says the processes started.
func MirrorContainersReady(_, m *corev1.Pod) bool {
	if m.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range m.Status.Conditions {
		if c.Type == corev1.ContainersReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Loop is the orchestrator loop of one VK.
type Loop struct {
	cfg   Config
	nudge chan struct{}

	// guestMu serializes guest work; the maps are the loop's view since it started.
	guestMu   sync.Mutex
	suspended map[types.UID]bool
	released  map[types.UID]bool
}

// New returns a loop. Run starts it.
func New(config *Config) (*Loop, error) {
	cfg := *config
	if cfg.Node == "" || cfg.Client == nil || cfg.Group == nil || cfg.Host == nil {
		return nil, errors.New("orchestrator: Node, Client, Group and Host are required")
	}
	cfg.defaults()
	return &Loop{
		cfg: cfg, nudge: make(chan struct{}, 1),
		suspended: map[types.UID]bool{}, released: map[types.UID]bool{},
	}, nil
}

// ParticipantID is the VK's job id and participant id.
func (l *Loop) ParticipantID() string { return "vk/" + l.cfg.Node }

// Adopt records what a restarted guest kubelet found for a guest (M5), before Run: a suspended
// guest is resumed at the next grant, a released one is not released again. The mirrors in
// the API stay the source of truth; this only restores the loop's view of them.
func (l *Loop) Adopt(guest types.UID, suspended, released bool) {
	l.guestMu.Lock()
	defer l.guestMu.Unlock()
	delete(l.suspended, guest)
	delete(l.released, guest)
	if suspended {
		l.suspended[guest] = true
	} else if released {
		l.released[guest] = true
	}
}

// GuestWaiting implements provider.CreateOwner: a guest without a mirror arrived. The loop
// creates its mirror at the next serve pass under a grant.
func (l *Loop) GuestWaiting(*corev1.Pod) {
	select {
	case l.nudge <- struct{}{}:
	default:
	}
}

// Run reads the group, runs the participant loop for it, and starts over when the group
// changes. With no single group it starts nothing (fail closed). It returns when ctx ends.
func (l *Loop) Run(ctx context.Context) error {
	logger := log.G(ctx).WithField("node", l.cfg.Node)
	for {
		group, err := l.cfg.Group(ctx)
		if err != nil {
			logger.WithError(err).Warn("orchestrator loop: no group; starting no mirrors")
			if err := sleep(ctx, l.cfg.GroupRecheck); err != nil {
				return err
			}
			continue
		}
		l.cfg.Host.SetGroup(group)
		logger.WithField("group", group).Info("orchestrator loop: joining group")
		pctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- newParticipant(l, group).run(pctx) }()
		err = l.watchGroup(ctx, group, done)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		logger.WithField("group", group).WithError(err).Warn("orchestrator loop: participant stopped; starting over")
		if err := sleep(ctx, l.cfg.RetryInterval); err != nil {
			return err
		}
	}
}

// watchGroup returns when the participant ends or the node's group changes.
func (l *Loop) watchGroup(ctx context.Context, group string, done <-chan error) error {
	tick := time.NewTicker(l.cfg.GroupRecheck)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			<-done
			return ctx.Err()
		case err := <-done:
			return err
		case <-tick.C:
			cur, err := l.cfg.Group(ctx)
			if err == nil && cur == group {
				continue
			}
			if err == nil {
				err = fmt.Errorf("group changed from %s to %s", group, cur)
			}
			return err
		}
	}
}

// participant is the §4 loop for one group.
type participant struct {
	l     *Loop
	group string

	mu            sync.Mutex
	acquireSentAt time.Time // first background Acquire; polls carry participant_id from +grace
	lastOK        time.Time // when the last successful poll was sent
	protocol      int32
	deadline      time.Time // vacate deadline from the last poll; zero when no notice runs
	gone          bool
	granted       bool      // a grant is held: set by Acquire, cleared by a notice, Yield or lease loss
	grantedAt     time.Time // when the last grant arrived; notices from polls sent earlier are stale
	changed       chan struct{}
}

func newParticipant(l *Loop, group string) *participant {
	return &participant{l: l, group: group, changed: make(chan struct{}, 1)}
}

func (p *participant) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.poll(ctx)

	// 1. Check: start nothing until the server speaks the background protocol.
	for p.snapshot().protocol != BackgroundProtocol {
		if err := p.wait(ctx); err != nil {
			return err
		}
	}
	vacated := false
	for {
		// 2. Wait in Acquire; vacate on a notice meanwhile.
		if err := p.acquire(ctx, vacated); err != nil {
			return err
		}
		// 3. Serve until a notice (4. vacate) or the lease is lost.
		var err error
		if vacated, err = p.holdGrant(ctx); err != nil {
			return err
		}
	}
}

type pollState struct {
	lastOK   time.Time
	protocol int32
	deadline time.Time
	gone     bool
	granted  bool
}

func (p *participant) snapshot() pollState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return pollState{lastOK: p.lastOK, protocol: p.protocol, deadline: p.deadline, gone: p.gone, granted: p.granted}
}

// grant records a successful Acquire. The orchestrator grants only when no notice runs, so a
// deadline left from an earlier poll is stale and is dropped.
func (p *participant) grant() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.granted = true
	p.grantedAt = time.Now()
	p.deadline = time.Time{}
}

func (p *participant) setGranted(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.granted = v
}

// mayStart reports whether a mirror may be created or resumed now: a grant is held, no notice
// runs, and a poll sent within L succeeded. Checked just before each create and resume.
func (p *participant) mayStart() bool {
	st := p.snapshot()
	return st.granted && st.deadline.IsZero() && time.Since(st.lastOK) < p.l.cfg.Liveness
}

var errGroupGone = errors.New("group is gone")

func (p *participant) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.changed:
	}
	if p.snapshot().gone {
		return errGroupGone
	}
	return nil
}

func (p *participant) poll(ctx context.Context) {
	tick := time.NewTicker(p.l.cfg.PollInterval)
	defer tick.Stop()
	for {
		p.pollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (p *participant) pollOnce(ctx context.Context) {
	req := &pb.GetGroupStatusRequest{GroupId: p.group}
	p.mu.Lock()
	if !p.acquireSentAt.IsZero() && time.Since(p.acquireSentAt) >= participantGrace {
		req.ParticipantId = p.l.ParticipantID()
	}
	p.mu.Unlock()

	sent := time.Now()
	rctx, cancel := context.WithTimeout(ctx, p.l.cfg.RPCTimeout)
	resp, err := p.l.cfg.Client.GetGroupStatus(rctx, req)
	cancel()

	p.mu.Lock()
	switch {
	case err == nil:
		gs := resp.GetGroup()
		p.lastOK = sent
		p.protocol = gs.GetBackgroundProtocol()
		p.deadline = time.Time{}
		// A poll sent before the last grant may still carry the notice that grant ended.
		if gs.GetVacateWithin() != nil && !sent.Before(p.grantedAt) {
			p.deadline = sent.Add(gs.GetVacateWithin().AsDuration() - p.l.cfg.VacateMargin)
			p.granted = false // no mirror starts once a notice runs
		}
	case status.Code(err) == codes.NotFound:
		p.gone = true
	default:
		log.G(ctx).WithError(err).WithField("group", p.group).Debug("orchestrator loop: status poll failed")
	}
	p.mu.Unlock()

	select {
	case p.changed <- struct{}{}:
	default:
	}
}

var errNotMember = errors.New("node is not a member of the group")

// acquire waits in Acquire(ROLE_BACKGROUND). The call stays outstanding until it returns; a
// notice meanwhile vacates any guest still up.
func (p *participant) acquire(ctx context.Context, vacated bool) error {
	for {
		actx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		p.mu.Lock()
		if p.acquireSentAt.IsZero() {
			p.acquireSentAt = time.Now()
		}
		p.mu.Unlock()
		go func() {
			_, err := p.l.cfg.Client.Acquire(actx, &pb.AcquireRequest{
				JobId: p.l.ParticipantID(), GroupId: p.group, Role: pb.Role_ROLE_BACKGROUND, NodeName: p.l.cfg.Node,
			})
			done <- err
		}()
		res := p.waitAcquire(ctx, done, &vacated)
		cancel()
		if res.stop != nil {
			return res.stop
		}
		err := res.err
		if err == nil {
			p.grant()
			log.G(ctx).WithField("group", p.group).Info("orchestrator loop: node granted")
			return nil
		}
		switch status.Code(err) {
		case codes.FailedPrecondition:
			if strings.Contains(status.Convert(err).Message(), "is not a node of group") {
				return errNotMember
			}
		case codes.NotFound:
			return errGroupGone
		default:
		}
		log.G(ctx).WithError(err).WithField("group", p.group).Debug("orchestrator loop: background Acquire failed; retrying")
		if err := sleep(ctx, p.l.cfg.RetryInterval); err != nil {
			return err
		}
	}
}

// acquireResult is what Acquire returned (err), or why the loop must stop instead (stop).
type acquireResult struct {
	err  error
	stop error
}

// waitAcquire waits for the outstanding Acquire and vacates on a notice meanwhile.
func (p *participant) waitAcquire(ctx context.Context, done <-chan error, vacated *bool) acquireResult {
	for {
		select {
		case <-ctx.Done():
			return acquireResult{stop: ctx.Err()}
		case err := <-done:
			return acquireResult{err: err}
		case <-p.changed:
			st := p.snapshot()
			if st.gone {
				return acquireResult{stop: errGroupGone}
			}
			if st.deadline.IsZero() {
				*vacated = false
				continue
			}
			if !*vacated {
				*vacated = true
				p.vacate(ctx, st.deadline)
			}
		}
	}
}

// holdGrant serves while the node is lent. A notice makes it vacate and return true; losing
// the lease (no successful poll within L) makes it return false, and the loop re-enters
// Acquire so the orchestrator decides again.
func (p *participant) holdGrant(ctx context.Context) (bool, error) {
	for {
		st := p.snapshot()
		if !st.deadline.IsZero() {
			p.vacate(ctx, st.deadline)
			return true, nil
		}
		if time.Since(st.lastOK) >= p.l.cfg.Liveness {
			p.setGranted(false)
			log.G(ctx).WithField("group", p.group).Warn("orchestrator loop: lease lost; re-entering Acquire")
			return false, nil
		}
		p.l.serve(ctx, p)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-p.changed:
			if p.snapshot().gone {
				return false, errGroupGone
			}
		case <-p.l.nudge:
		}
	}
}

// vacate suspends every live guest by deadline, then hands the grant back.
func (p *participant) vacate(ctx context.Context, deadline time.Time) {
	p.setGranted(false)
	log.G(ctx).WithField("group", p.group).WithField("deadline", deadline).Info("orchestrator loop: vacating")
	p.l.vacateGuests(ctx, deadline)
	p.yield(ctx, deadline)
}

// yield sends Yield(ROLE_BACKGROUND), retrying until it succeeds or the deadline passes.
func (p *participant) yield(ctx context.Context, deadline time.Time) {
	for {
		rctx, cancel := context.WithTimeout(ctx, p.l.cfg.RPCTimeout)
		_, err := p.l.cfg.Client.Yield(rctx, &pb.YieldRequest{
			JobId: p.l.ParticipantID(), GroupId: p.group, Role: pb.Role_ROLE_BACKGROUND,
		})
		cancel()
		if err == nil {
			log.G(ctx).WithField("group", p.group).Info("orchestrator loop: background Yield sent")
			return
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			log.G(ctx).WithError(err).WithField("group", p.group).Warn("orchestrator loop: background Yield failed")
			return
		}
		if sleep(ctx, p.l.cfg.RetryInterval) != nil {
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
