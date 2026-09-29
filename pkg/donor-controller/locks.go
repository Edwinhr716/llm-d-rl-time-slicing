package donorcontroller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

// BackgroundJobPrefix is the job id prefix of the background participant (the VK), "vk/<host>".
const BackgroundJobPrefix = "vk/"

// LockSource reads a group's status from the orchestrator. A nil status with a nil error means
// the orchestrator does not know the group (no lock activity). Any error means the status is not
// known this time; the controller then holds every countdown of that group (fail safe).
type LockSource interface {
	GroupStatus(ctx context.Context, group string) (*v1alpha1.GroupStatus, error)
}

// LastLockActivity returns now if the step from prev to cur is foreground lock activity, and the
// zero time otherwise. Lock activity is a change of the group's foreground lock (group_state,
// locking_job, active_job), or any status whose state is LOCKED, SWITCHING or VACATING. It never
// counts the background participant: a state comparison where either side is the computed
// STATE_BACKGROUND is skipped, and a locking_job or active_job change where the old or new value
// is a background participant (vk/<host>) is ignored. So the VK's Acquire, Yield and heartbeat
// polls never keep an era alive. A nil cur (group unknown) is no activity; a nil prev (first
// observation) is activity only if cur is in an active state.
//
// Shared definition with D-NS-13 (hook V2): same name and semantics.
func LastLockActivity(prev, cur *v1alpha1.GroupStatus, now time.Time) time.Time {
	if cur == nil {
		return time.Time{}
	}
	if IsLockActive(cur) {
		return now
	}
	if prev == nil {
		return time.Time{}
	}
	prevState, curState := prev.GetGroupState(), cur.GetGroupState()
	background := v1alpha1.GroupStatus_STATE_BACKGROUND
	if prevState != background && curState != background && prevState != curState {
		return now
	}
	if foregroundChange(prev.GetLockingJob(), cur.GetLockingJob()) ||
		foregroundChange(prev.GetActiveJob(), cur.GetActiveJob()) {
		return now
	}
	return time.Time{}
}

// IsLockActive reports whether the group's state is one that is lock activity by itself.
func IsLockActive(st *v1alpha1.GroupStatus) bool {
	switch st.GetGroupState() {
	case v1alpha1.GroupStatus_STATE_LOCKED, v1alpha1.GroupStatus_STATE_SWITCHING, v1alpha1.GroupStatus_STATE_VACATING:
		return true
	default:
		return false
	}
}

// foregroundChange reports a job field change in which neither side is a background participant.
func foregroundChange(before, after string) bool {
	if before == after {
		return false
	}
	return !strings.HasPrefix(before, BackgroundJobPrefix) && !strings.HasPrefix(after, BackgroundJobPrefix)
}

// GRPCLockSource reads GetGroupStatus from the orchestrator. It sends no participant_id: the
// controller is not a participant.
type GRPCLockSource struct {
	conn   *grpc.ClientConn
	client v1alpha1.TimeSliceOrchestratorServiceClient
}

// NewGRPCLockSource dials the orchestrator at addr (host:port, plaintext, as the in-cluster
// orchestrator serves). The connection is lazy: an unreachable orchestrator shows up as errors.
func NewGRPCLockSource(addr string) (*GRPCLockSource, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("orchestrator client for %s: %w", addr, err)
	}
	return &GRPCLockSource{conn: conn, client: v1alpha1.NewTimeSliceOrchestratorServiceClient(conn)}, nil
}

// GroupStatus implements LockSource. NotFound maps to a nil status and a nil error.
func (g *GRPCLockSource) GroupStatus(ctx context.Context, group string) (*v1alpha1.GroupStatus, error) {
	resp, err := g.client.GetGroupStatus(ctx, &v1alpha1.GetGroupStatusRequest{GroupId: group})
	if status.Code(err) == codes.NotFound {
		return nil, nil //nolint:nilnil // documented: nil, nil means the group is unknown
	}
	if err != nil {
		return nil, err
	}
	return resp.GetGroup(), nil
}

// Close closes the connection.
func (g *GRPCLockSource) Close() error { return g.conn.Close() }
