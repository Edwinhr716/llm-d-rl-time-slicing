package backends_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"gotest.tools/v3/assert"
)

// fakeDevice simulates the GPU-CR data path for one workload: "device
// memory" whose bytes cr_client -c dumps directly into the destination file
// named by -o (GEP-0001) and cr_client -r loads back from it.
type fakeDevice struct {
	mu     sync.Mutex
	memory []byte
	fail   error
}

func (d *fakeDevice) exec(ctx context.Context, name string, args ...string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail != nil {
		return []byte("cr_client: injected failure"), d.fail
	}
	var dest string
	for i, a := range args {
		if a == "-o" && i+1 < len(args) {
			dest = args[i+1]
		}
	}
	if dest == "" {
		return nil, fmt.Errorf("cr_client invoked without -o destination (args %v)", args)
	}
	switch args[0] {
	case "-c":
		return nil, os.WriteFile(dest, d.memory, 0o600)
	case "-r":
		data, err := os.ReadFile(dest)
		if err != nil {
			return nil, err
		}
		d.memory = data
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected cr_client mode %q", args[0])
	}
}

func (d *fakeDevice) setMemory(b []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.memory = append([]byte(nil), b...)
}

func (d *fakeDevice) getMemory() []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]byte(nil), d.memory...)
}

func (d *fakeDevice) setFail(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fail = err
}

// startAgent runs a real Server (standalone mode) over bufconn with a
// MemoryRegions backend whose exec is the fake device.
func startAgent(t *testing.T, dev *fakeDevice) pb.SnapshotAgentServiceClient {
	t.Helper()

	origHasGPU := utils.HasGPUProcesses
	utils.HasGPUProcesses = func(_ context.Context) (bool, error) { return true, nil }
	t.Cleanup(func() { utils.HasGPUProcesses = origHasGPU })

	backend := backends.NewMemoryRegions()
	backend.SetExecCommand(dev.exec)
	backend.SetLookPath(func(string) (string, error) { return "/usr/local/bin/cr_client", nil })

	backendsMap := map[backends.BackendType]backends.Backend{
		backends.BackendMemoryRegions: backend,
		backends.BackendNoop:          backends.NewNoopBackend(),
	}

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	srv := server.NewServer(backendsMap, backends.BackendNoop, "standalone", backends.NewChannelRegistry(), nil)
	pb.RegisterSnapshotAgentServiceServer(s, srv)
	go func() {
		if err := s.Serve(lis); err != nil {
			return
		}
	}()
	t.Cleanup(s.GracefulStop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NilError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewSnapshotAgentServiceClient(conn)
}

func waitOp(t *testing.T, client pb.SnapshotAgentServiceClient, opID string) *pb.GetOperationResponse {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.GetOperation(context.Background(), &pb.GetOperationRequest{OperationId: opID})
		assert.NilError(t, err)
		if resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_PENDING {
			return resp
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for operation %s", opID)
	return nil
}

const e2eJobID = "job-e2e"

func jobState(t *testing.T, client pb.SnapshotAgentServiceClient) pb.JobState {
	t.Helper()
	resp, err := client.Status(context.Background(), &pb.StatusRequest{})
	assert.NilError(t, err)
	for _, js := range resp.GetJobStatuses() {
		if js.GetJobId() == e2eJobID {
			return js.GetState()
		}
	}
	return pb.JobState_JOB_STATE_UNSPECIFIED
}

func integrationConfig(slot string) *pb.BackendConfig {
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_MemoryRegions{
			MemoryRegions: &pb.MemoryRegionsBackendConfig{
				Regions:      []*pb.MemoryRegion{{Pid: 4242, Address: 0x7f0000000000, SizeBytes: 64}},
				SnapshotName: slot,
			},
		},
	}
}

func snapshotSlotAndWait(t *testing.T, client pb.SnapshotAgentServiceClient, slot string) *pb.GetOperationResponse {
	t.Helper()
	resp, err := client.Snapshot(context.Background(), &pb.SnapshotRequest{
		JobId:         e2eJobID,
		BackendConfig: integrationConfig(slot),
	})
	assert.NilError(t, err)
	return waitOp(t, client, resp.GetOperationId())
}

func restoreSlotAndWait(t *testing.T, client pb.SnapshotAgentServiceClient, slot string) *pb.GetOperationResponse {
	t.Helper()
	resp, err := client.Restore(context.Background(), &pb.RestoreRequest{
		JobId:         e2eJobID,
		BackendConfig: integrationConfig(slot),
	})
	assert.NilError(t, err)
	return waitOp(t, client, resp.GetOperationId())
}

// TestMemoryRegionsEndToEnd drives the full RPC surface against a real
// server + state machine with a fake GPU-CR device: snapshot/restore round
// trip, two independent snapshot slots swapped on demand (bitwise-verified),
// job state transitions, and backend failure surfacing.
func TestMemoryRegionsEndToEnd(t *testing.T) {
	ctlDir := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctlDir)
	t.Setenv("GPU_CR_CTL_PATH", "")
	t.Setenv("GPU_CR_GROUP_STORE", "")

	dev := &fakeDevice{}
	// pid_map written by the (simulated) preloader at workload startup.
	assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "pid_map_4242"), []byte("42\n"), 0o600))

	client := startAgent(t, dev)

	contentA := bytes.Repeat([]byte{0xA1}, 64)
	contentB := bytes.Repeat([]byte{0xB2}, 64)

	// Snapshot slot-a: PENDING -> COMPLETE with storage bytes; job SAVED.
	dev.setMemory(contentA)
	op := snapshotSlotAndWait(t, client, "slot-a")
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_COMPLETE, "error: %s", op.GetError())
	assert.Assert(t, op.GetStorageBytes() > 0)
	assert.Equal(t, jobState(t, client), pb.JobState_JOB_STATE_SAVED)

	// Restore slot-a: job back to RUNNING.
	op = restoreSlotAndWait(t, client, "slot-a")
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_COMPLETE, "error: %s", op.GetError())
	assert.Equal(t, jobState(t, client), pb.JobState_JOB_STATE_RUNNING)

	// Snapshot slot-b with different device bytes, then restore slot-a and
	// verify slot-a's bytes land back in device memory (bitwise).
	dev.setMemory(contentB)
	op = snapshotSlotAndWait(t, client, "slot-b")
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_COMPLETE, "error: %s", op.GetError())

	op = restoreSlotAndWait(t, client, "slot-a")
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_COMPLETE, "error: %s", op.GetError())
	assert.Assert(t, bytes.Equal(dev.getMemory(), contentA), "slot-a bytes not restored")

	// Live slot swap while RUNNING: restore slot-b, then back to slot-a.
	op = restoreSlotAndWait(t, client, "slot-b")
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_COMPLETE, "error: %s", op.GetError())
	assert.Assert(t, bytes.Equal(dev.getMemory(), contentB), "slot-b bytes not restored")

	op = restoreSlotAndWait(t, client, "slot-a")
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_COMPLETE, "error: %s", op.GetError())
	assert.Assert(t, bytes.Equal(dev.getMemory(), contentA), "slot-a bytes not restored after swap")

	// Backend failure: operation FAILED with the backend's error, then a
	// fresh restore resets the FAULTED job.
	dev.setFail(fmt.Errorf("workload died"))
	op = restoreSlotAndWait(t, client, "slot-b")
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_FAILED)
	assert.Assert(t, op.GetError() != "")
	assert.Equal(t, jobState(t, client), pb.JobState_JOB_STATE_FAULTED)

	dev.setFail(nil)
	op = restoreSlotAndWait(t, client, "slot-b")
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_COMPLETE, "error: %s", op.GetError())
	assert.Assert(t, bytes.Equal(dev.getMemory(), contentB))
	assert.Equal(t, jobState(t, client), pb.JobState_JOB_STATE_RUNNING)
}

// TestMemoryRegionsInvalidRequests verifies RPC-level rejections.
func TestMemoryRegionsInvalidRequests(t *testing.T) {
	ctlDir := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctlDir)
	t.Setenv("GPU_CR_CTL_PATH", "")

	dev := &fakeDevice{}
	client := startAgent(t, dev)

	// Empty regions: the RPC is accepted (validation runs in the backend)
	// and the operation fails with the validation error.
	resp, err := client.Snapshot(context.Background(), &pb.SnapshotRequest{
		JobId: "job-invalid",
		BackendConfig: &pb.BackendConfig{
			Backend: &pb.BackendConfig_MemoryRegions{
				MemoryRegions: &pb.MemoryRegionsBackendConfig{SnapshotName: "slot-a"},
			},
		},
	})
	assert.NilError(t, err)
	op := waitOp(t, client, resp.GetOperationId())
	assert.Equal(t, op.GetStatus(), pb.OperationStatus_OPERATION_STATUS_FAILED)
	assert.Assert(t, op.GetError() != "")
}
