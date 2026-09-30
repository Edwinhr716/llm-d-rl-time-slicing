package hostcmd

import (
	"context"
	"fmt"
	"path"
	"sort"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sapb "github.com/edwinhr716/guest-kubelet/api/snapshotagent/v1alpha1"
)

// Fault kinds the injector plays on a snapshot-agent RPC.
const (
	// FaultHang: the agent never answers; the call blocks until its own timeout.
	FaultHang = "hang"
	// FaultCrash: the agent is gone; the call fails Unavailable without reaching it.
	FaultCrash = "crash"
	// FaultDropAck: the call reaches the agent, but the answer is lost (Unavailable).
	FaultDropAck = "drop-ack"
	// FaultPending: GetOperation only; the agent answers, but the operation shows PENDING, as
	// if it were stuck, so the caller's deadline runs out.
	FaultPending = "pending"
	// FaultUnimplemented: the agent answers Unimplemented.
	FaultUnimplemented = "unimplemented"
	// FaultRefuse: the agent refuses the call (FailedPrecondition, BACKEND_ERROR).
	FaultRefuse = "refuse"
)

// Fault is one armed fault.
type Fault struct {
	// RPC is the method name: Suspend, Resume, Kill, SuspendAll, ResumeAll, GetOperation,
	// Status.
	RPC  string `json:"rpc"`
	Kind string `json:"kind"`
	// Count is how many more calls it hits; negative means until cleared.
	Count int `json:"count"`
	// Hits counts the calls it hit so far.
	Hits int `json:"hits"`
}

// FaultInjector breaks snapshot-agent calls on purpose, for the VK-A4 fault tests. It is a
// gRPC client interceptor, so it sits under the client's retries and deadlines exactly where
// a real network or agent fault would. Nothing is armed until Arm is called (the debug
// endpoint, behind --agent-fault-injection).
type FaultInjector struct {
	mu     sync.Mutex
	faults map[string]*Fault
}

// NewFaultInjector returns an injector with nothing armed.
func NewFaultInjector() *FaultInjector {
	return &FaultInjector{faults: map[string]*Fault{}}
}

var faultKinds = map[string]bool{
	FaultHang: true, FaultCrash: true, FaultDropAck: true, FaultPending: true, FaultUnimplemented: true, FaultRefuse: true,
}

// Arm sets the fault for one RPC, replacing any fault armed on it.
func (f *FaultInjector) Arm(rpc, kind string, count int) error {
	if !faultKinds[kind] {
		return fmt.Errorf("unknown fault kind %q", kind)
	}
	if kind == FaultPending && rpc != "GetOperation" {
		return fmt.Errorf("fault %s applies to GetOperation only", FaultPending)
	}
	if count == 0 {
		count = 1
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[rpc] = &Fault{RPC: rpc, Kind: kind, Count: count}
	return nil
}

// Clear disarms every fault.
func (f *FaultInjector) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = map[string]*Fault{}
}

// List returns the armed faults, spent ones included (Count 0) so their hits can be read.
func (f *FaultInjector) List() []Fault {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Fault, 0, len(f.faults))
	for _, x := range f.faults {
		out = append(out, *x)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RPC < out[j].RPC })
	return out
}

// take returns the kind to play on this call of rpc, or "".
func (f *FaultInjector) take(rpc string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	x := f.faults[rpc]
	if x == nil || x.Count == 0 {
		return ""
	}
	if x.Count > 0 {
		x.Count--
	}
	x.Hits++
	return x.Kind
}

// Interceptor is the grpc.UnaryClientInterceptor that plays the armed faults.
func (f *FaultInjector) Interceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		switch f.take(path.Base(method)) {
		case FaultHang:
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		case FaultCrash:
			return status.Error(codes.Unavailable, "injected fault: agent unreachable")
		case FaultDropAck:
			if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "injected fault: answer lost")
		case FaultPending:
			if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
				return err
			}
			if op, ok := reply.(*sapb.GetOperationResponse); ok {
				op.Reset()
				op.Status = sapb.OperationStatus_OPERATION_STATUS_PENDING
			}
			return nil
		case FaultUnimplemented:
			return status.Error(codes.Unimplemented, "injected fault: not implemented")
		case FaultRefuse:
			return refusal(codes.FailedPrecondition, sapb.ErrorReason_BACKEND_ERROR.String(), "injected fault")
		default:
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}
}
