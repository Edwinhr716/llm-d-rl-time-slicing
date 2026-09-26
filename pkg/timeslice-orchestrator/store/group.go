package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

// GroupLockStore defines the interface for persisting lock state for a specific group.
type GroupLockStore interface {
	GetLock(ctx context.Context) (string, error)
	Lock(ctx context.Context, jobID string) error
	Unlock(ctx context.Context, jobID string) error
}

// GroupSpec defines the specification of desired end state of a time-slice group.
type GroupSpec struct {
	mu        sync.RWMutex
	lockStore GroupLockStore
	// lockingJob is job we want to hold the lock.
	lockingJob string
	// lockedAt is when lockingJob was granted the lock. Zero when unlocked,
	// and also zero for a lock recovered from the lock store at startup,
	// because the store persists only the holder's identity. Consumers must
	// treat a zero value as "unknown, assume long held".
	lockedAt time.Time
	queue    *WaitingJobQueue
	// activeJob is job for which we want context loaded on nodes.
	// This can be non-empty when lockingJob is empty as an optimization
	// when there is no one waiting to be the locking job.
	activeJob string

	// The fields below are the background participant state. They live in
	// memory only and are never written to the lock store: the group states
	// derived from them (BACKGROUND, VACATING) are computed on read.
	//
	// lend records that the last foreground Yield carried an expected_idle
	// hint of at least the server's minimum bubble. It is only a hint; the
	// reconcile loop decides whether the accelerator is actually lent.
	lend bool
	// participants holds the background participant of each node, keyed by
	// node name.
	participants map[string]*participant
	// noticeAt is when the current notice to the background started. Zero
	// when no notice runs.
	noticeAt time.Time
}

// participant is the in-memory record of a node's background participant.
type participant struct {
	id       string
	lastSeen time.Time
	// blocked is true while the participant waits in Acquire(ROLE_BACKGROUND).
	blocked bool
	// granted is true while the node is lent to the participant.
	granted bool
	// claimed is true when the participant was first seen through a status
	// poll rather than an Acquire (for example after an orchestrator
	// restart), so it may still hold a grant from an earlier process. A claim
	// keeps the node busy like a grant (fail closed) but never satisfies a
	// background Acquire.
	claimed bool
}

func (p *participant) holds() bool {
	return p.granted || p.claimed
}

// GroupStatus represents the current status of a time-slice group.
// Updated by the controller reconcile loop.
type GroupStatus struct {
	mu             sync.RWMutex
	nodes          []string
	state          pb.GroupStatus_State
	stateTimestamp time.Time
	// loadedJob is a job that the controller has loaded all
	// the snapshotted context for on the nodes. Context for all
	// other jobs will have been offloaded as well.
	loadedJob string
	// loadedAt is when loadedJob last changed, i.e. when the current job's
	// context finished being restored on the nodes. Zero when nothing is
	// loaded.
	loadedAt time.Time
}

func (s *GroupStatus) Nodes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.nodes...)
}

func (s *GroupStatus) SetNodes(nodes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes = append([]string(nil), nodes...)
}

func (s *GroupStatus) State() (pb.GroupStatus_State, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, s.stateTimestamp
}

func (s *GroupStatus) SetState(state pb.GroupStatus_State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == state {
		return
	}
	s.state = state
	s.stateTimestamp = time.Now()
}

func (s *GroupStatus) LoadedJob() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadedJob
}

func (s *GroupStatus) SetLoadedJob(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadedJob == jobID {
		return
	}
	s.loadedJob = jobID
	if jobID == "" {
		s.loadedAt = time.Time{}
		return
	}
	s.loadedAt = time.Now()
}

// Group represents the in-memory and persistent state of a time-slice group.
type Group struct {
	id     string
	spec   *GroupSpec
	status *GroupStatus
}

// NewGroup creates a new Group and initializes its locking state from the lockStore if available.
func NewGroup(ctx context.Context, id string, lockStore GroupLockStore) (*Group, error) {
	group := &Group{
		id: id,
		spec: &GroupSpec{
			lockStore:    lockStore,
			queue:        NewWaitingJobQueue(),
			participants: make(map[string]*participant),
		},
		status: &GroupStatus{
			state:          pb.GroupStatus_STATE_UNSPECIFIED,
			stateTimestamp: time.Now(),
		},
	}

	if lockStore != nil {
		lockingJob, err := lockStore.GetLock(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get lock status for group %s: %w", id, err)
		}
		group.spec.lockingJob = lockingJob
		group.spec.activeJob = lockingJob
	}

	return group, nil
}

func (g *Group) ID() string {
	return g.id
}

// Spec returns the GroupSpec for the group.
func (g *Group) Spec() *GroupSpec {
	// No lock is safe here because the pointer is not
	// modifiable after the Group is created. Fields
	// on the spec itself are controlled by the internal mutex.
	return g.spec
}

// Status returns the GroupStatus.
func (g *Group) Status() *GroupStatus {
	return g.status
}

// Delete deletes the group by releasing its lock if it is currently held.
func (g *Group) Delete(ctx context.Context) error {
	return g.spec.Delete(ctx)
}

// Methods on GroupSpec

// ActiveJob returns the job ID for which the context should be loaded on nodes.
func (s *GroupSpec) ActiveJob() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeJob
}

// LockingJob returns the job ID that currently holds the lock, or empty if none.
func (s *GroupSpec) LockingJob() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lockingJob
}

// GetWaitingJobQueue returns the queue of jobs waiting for the lock.
func (s *GroupSpec) GetWaitingJobQueue() *WaitingJobQueue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queue
}

// Delete cleans up the spec information.
func (s *GroupSpec) Delete(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockingJob == "" {
		return nil
	}
	return s.unlock(ctx, s.lockingJob)
}

// lock assumes s.mu is held.
func (s *GroupSpec) lock(ctx context.Context, jobID string) error {
	if s.lockStore != nil {
		if err := s.lockStore.Lock(ctx, jobID); err != nil {
			return err
		}
	}
	s.lockingJob = jobID
	s.lockedAt = time.Now()
	s.activeJob = jobID
	return nil
}

// unlock assumes s.mu is held.
func (s *GroupSpec) unlock(ctx context.Context, jobID string) error {
	if s.lockStore != nil {
		if err := s.lockStore.Unlock(ctx, jobID); err != nil {
			return err
		}
	}
	s.lockingJob = ""
	s.lockedAt = time.Time{}
	// Notice we do not clear the active job. This is because
	// we actually want to leave the context on the machines until
	// there is a new job that wants to lock the group.
	return nil
}

// GroupSnapshot represents an immutable, point-in-time copy of a Group's state.
type GroupSnapshot struct {
	ID               string
	Nodes            []string
	State            pb.GroupStatus_State
	StateTimestamp   time.Time
	LockingJob       string
	LockedAt         time.Time
	ActiveJob        string
	WaiterQueueDepth int
	LoadedJob        string
	LoadedAt         time.Time
	// BackgroundHeld is true when any node's background participant holds a
	// grant or a claim.
	BackgroundHeld bool
	// NoticeAt is when the current notice to the background started, or zero.
	NoticeAt time.Time
}

// EffectiveState returns the group state to report to callers. It overlays
// the two background states, which are never stored, on the state the
// reconcile loop last recorded: VACATING while a notice runs, BACKGROUND while
// a background participant holds a grant or a claim. With no background
// participant it is exactly the recorded state.
func (s *GroupSnapshot) EffectiveState() pb.GroupStatus_State {
	switch {
	case !s.NoticeAt.IsZero():
		return pb.GroupStatus_STATE_VACATING
	case s.BackgroundHeld:
		return pb.GroupStatus_STATE_BACKGROUND
	default:
		return s.State
	}
}

// ServingSince reports when the current lock holder became able to actually use
// the accelerator: it holds the lock AND its context is loaded on the nodes. It
// returns the zero time when no job is in that state, which includes the window
// between a grant and the completion of the context restore, and any holder
// recovered from the lock store at startup (whose grant time is not persisted).
//
// The minimum serving quantum is measured from this instant rather than from the
// grant, because the grant is followed by a multi-second cuda-checkpoint restore
// during which the holder cannot serve anything.
func (s *GroupSnapshot) ServingSince() time.Time {
	if s.LockingJob == "" || s.LockingJob != s.LoadedJob {
		return time.Time{}
	}
	if s.LockedAt.IsZero() || s.LoadedAt.IsZero() {
		return time.Time{}
	}
	if s.LoadedAt.After(s.LockedAt) {
		return s.LoadedAt
	}
	return s.LockedAt
}

// Snapshot returns a consistent, point-in-time snapshot of the group's state.
func (g *Group) Snapshot() *GroupSnapshot {
	g.status.mu.RLock()
	defer g.status.mu.RUnlock()

	// Deep copy nodes slice
	nodes := make([]string, len(g.status.nodes))
	copy(nodes, g.status.nodes)

	loadedJob := g.status.loadedJob
	loadedAt := g.status.loadedAt

	g.spec.mu.RLock()
	defer g.spec.mu.RUnlock()

	return &GroupSnapshot{
		ID:               g.id,
		Nodes:            nodes,
		State:            g.status.state,
		StateTimestamp:   g.status.stateTimestamp,
		LockingJob:       g.spec.lockingJob,
		LockedAt:         g.spec.lockedAt,
		ActiveJob:        g.spec.activeJob,
		WaiterQueueDepth: g.spec.queue.Len(),
		LoadedJob:        loadedJob,
		LoadedAt:         loadedAt,
		BackgroundHeld:   g.spec.backgroundHeldLocked(),
		NoticeAt:         g.spec.noticeAt,
	}
}

// TryPromote attempts to promote the next waiting job in the queue to be the locking job
// if the group is currently unlocked.
func (s *GroupSpec) TryPromote(ctx context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lockingJob != "" {
		return false, nil
	}

	nextJobID, ok := s.queue.Peek()
	if !ok {
		return false, nil
	}

	if err := s.lock(ctx, nextJobID); err != nil {
		return false, fmt.Errorf("failed to lock promoted job %s: %w", nextJobID, err)
	}

	s.queue.Dequeue()
	return true, nil
}

// Yield releases the lock for the current job.
func (s *GroupSpec) Yield(ctx context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.unlock(ctx, jobID)

	// Note that promoting the next job to take the lock is
	// done in the reconiliation loop to not block a RL job trying
	// to yield their job if there is a temporary issue locking for next job.
}

// RequestLock requests a lock for the given job.
// If the job already holds the lock, it returns immediately.
// Otherwise, it enqueues the job in the waiting queue.
func (s *GroupSpec) RequestLock(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A foreground job asking for the lock ends any pending lend hint.
	s.lend = false

	if s.lockingJob == jobID {
		return
	}

	s.queue.Enqueue(jobID)
}

// SetLend records (or clears) the foreground lend hint. It decides nothing by
// itself; the reconcile loop reads it.
func (s *GroupSpec) SetLend(lend bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lend = lend
}

// Lend reports the foreground lend hint.
func (s *GroupSpec) Lend() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lend
}

// RegisterParticipant records that the background participant id of node is
// blocked in Acquire(ROLE_BACKGROUND). It never touches the foreground queue.
func (s *GroupSpec) RegisterParticipant(node, id string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.participants[node]
	if !ok {
		p = &participant{}
		s.participants[node] = p
	}
	p.id = id
	p.lastSeen = now
	p.blocked = true
}

// UnregisterParticipant records that node's participant stopped waiting in
// Acquire. A participant that holds a grant or a claim is kept (fail closed):
// only a background Yield, or the reconcile loop, clears those.
func (s *GroupSpec) UnregisterParticipant(node string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.participants[node]
	if !ok {
		return
	}
	if p.holds() {
		p.blocked = false
		return
	}
	delete(s.participants, node)
}

// Touch refreshes the last-seen time of node's participant. An unknown
// participant is registered with a claim, because the orchestrator cannot
// tell whether it holds a grant from an earlier process. It reports whether
// the participant was unknown.
func (s *GroupSpec) Touch(node, id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.participants[node]; ok {
		p.id = id
		p.lastSeen = now
		return false
	}
	s.participants[node] = &participant{id: id, lastSeen: now, claimed: true}
	return true
}

// Grant lends node to its participant. It replaces a claim with a grant and
// reports false if node has no participant.
func (s *GroupSpec) Grant(node string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.participants[node]
	if !ok {
		return false
	}
	p.granted = true
	p.claimed = false
	return true
}

// ClearGrant takes back node's grant or claim. A participant that is not
// waiting in Acquire is forgotten. It reports whether anything was held.
func (s *GroupSpec) ClearGrant(node string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.participants[node]
	if !ok {
		return false
	}
	held := p.holds()
	p.granted = false
	p.claimed = false
	if !p.blocked {
		delete(s.participants, node)
	}
	return held
}

// Granted reports whether node is lent to its participant. A claim is not a
// grant.
func (s *GroupSpec) Granted(node string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.participants[node]
	return ok && p.granted
}

// BackgroundHeld reports whether any node's participant holds a grant or a
// claim, i.e. whether background guests may be on the accelerator.
func (s *GroupSpec) BackgroundHeld() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.backgroundHeldLocked()
}

// backgroundHeldLocked assumes s.mu is held.
func (s *GroupSpec) backgroundHeldLocked() bool {
	for _, p := range s.participants {
		if p.holds() {
			return true
		}
	}
	return false
}

// EnsureNotice starts a notice at now unless one already runs, and returns
// the notice start time.
func (s *GroupSpec) EnsureNotice(now time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.noticeAt.IsZero() {
		s.noticeAt = now
	}
	return s.noticeAt
}

// ClearNotice ends the current notice, if any.
func (s *GroupSpec) ClearNotice() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.noticeAt = time.Time{}
}

// NoticeAt returns when the current notice started, or zero.
func (s *GroupSpec) NoticeAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.noticeAt
}

// SetActiveJob sets the active job.
// Primarily used for testing.
func (s *GroupSpec) SetActiveJob(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeJob = jobID
}
