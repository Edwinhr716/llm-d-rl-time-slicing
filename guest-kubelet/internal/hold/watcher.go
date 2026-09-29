// Package hold tells the guest kubelet whether the donor holds its group lock.
//
// It is a stand-in until the orchestrator loop exists: it only polls GetGroupStatus with the
// VK's participant ID and reports held or not held. The loop will take the hold from its own
// poll and replace this watcher.
package hold

import (
	"context"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"google.golang.org/grpc"

	pb "github.com/edwinhr716/guest-kubelet/api/timeslice_orchestrator/v1alpha1"
)

// Unreachable is the state reported when no poll has succeeded for UnreachableAfter.
const Unreachable = "UNREACHABLE"

// Defaults: the contract's 0.5 s heartbeat and L = 3 s background liveness.
const (
	DefaultInterval         = 500 * time.Millisecond
	DefaultUnreachableAfter = 3 * time.Second
	DefaultRPCTimeout       = 1 * time.Second
)

// IsHeld is the only place that decides which group states mean "the donor holds the lock".
// Held: LOCKED, SWITCHING, VACATING. Not held: everything else (IDLE, IDLE_YIELDED,
// BACKGROUND, UNKNOWN, UNSPECIFIED). An unreachable orchestrator counts as held (Watcher).
func IsHeld(state pb.GroupStatus_State) bool {
	switch state {
	case pb.GroupStatus_STATE_LOCKED, pb.GroupStatus_STATE_SWITCHING, pb.GroupStatus_STATE_VACATING:
		return true
	default:
		return false
	}
}

// StatusClient is the one orchestrator RPC the watcher needs.
type StatusClient interface {
	GetGroupStatus(ctx context.Context, in *pb.GetGroupStatusRequest, opts ...grpc.CallOption) (*pb.GetGroupStatusResponse, error)
}

// Watcher polls GetGroupStatus and calls OnPoll with the hold decision after every poll that
// has one. Until the first successful poll, or UnreachableAfter without one, there is no
// decision and OnPoll is not called, so a restarted VK leaves the Node as it found it.
type Watcher struct {
	Client      StatusClient
	Group       string
	Participant string // "vk/<real node name>"
	OnPoll      func(ctx context.Context, held bool, state string)

	Interval         time.Duration
	UnreachableAfter time.Duration
	RPCTimeout       time.Duration
}

// Run polls until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(orDefault(w.Interval, DefaultInterval))
	defer ticker.Stop()
	lastOK := time.Now() // unreachable is measured from start until the first success
	lastState := ""
	for {
		sent := time.Now()
		dec, err := w.decide(ctx, lastOK)
		state, held := dec.state, dec.held
		if err == nil {
			lastOK = sent
		}
		if state != "" && state != lastState {
			entry := log.G(ctx).WithField("group", w.Group).WithField("state", state).WithField("held", held)
			if err != nil {
				entry = entry.WithError(err)
			}
			entry.Info("hold watcher")
			lastState = state
		}
		if state != "" && w.OnPoll != nil {
			w.OnPoll(ctx, held, state)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// decision is one poll's outcome: the group state name (or Unreachable) and whether it is held.
type decision struct {
	state string
	held  bool
}

// decide polls once. It returns an empty state when the poll failed but the orchestrator has
// not yet been unreachable for UnreachableAfter (measured from the last successful poll's send
// time): no decision yet.
func (w *Watcher) decide(ctx context.Context, lastOK time.Time) (decision, error) {
	ctx, cancel := context.WithTimeout(ctx, orDefault(w.RPCTimeout, DefaultRPCTimeout))
	defer cancel()
	resp, err := w.Client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: w.Group, ParticipantId: w.Participant})
	switch {
	case err == nil:
		st := resp.GetGroup().GetGroupState()
		return decision{state: st.String(), held: IsHeld(st)}, nil
	case time.Since(lastOK) > orDefault(w.UnreachableAfter, DefaultUnreachableAfter):
		return decision{state: Unreachable, held: true}, err // fail closed
	default:
		return decision{}, err
	}
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}
