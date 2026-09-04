// Package memoryregions_test hosts the cross-language integration test for
// the memory-regions backend: a real gRPC server with the real backend
// exec'ing a stub cr_client shell script, driven end to end by the Python
// client library. This is the test that guards Go<->Python codegen drift
// (it would have caught the prototype's field-4 wire collision).
//
// It needs python3 with grpcio+protobuf on PATH, so it only runs when
// CROSS_LANG_TEST=1 (set by the Cloud Build test step).
package memoryregions_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/features"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
}

func TestCrossLanguageMemoryRegions(t *testing.T) {
	if os.Getenv("CROSS_LANG_TEST") != "1" {
		t.Skip("set CROSS_LANG_TEST=1 (needs python3 with grpcio+protobuf)")
	}
	root := repoRoot(t)

	// Shared dirs; the stub cr_client reads EXPORT_FILE_PATH from env.
	ctlDir := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctlDir)

	// The workload's preloader would write the pid map; simulate it. The
	// workload pid must be a real live process: the backend records slot
	// ownership as a <pid>-<starttime> owner dir and reads the starttime
	// from procfs, so a made-up pid fails the snapshot. The test process
	// itself is the one pid guaranteed alive for the duration.
	workloadPID := strconv.Itoa(os.Getpid())
	if err := os.WriteFile(filepath.Join(ctlDir, "pid_map_"+workloadPID), []byte("42\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The backend execs cr_client at its fixed install location only (PATH
	// is not consulted), so the stub must live there. The Cloud Build test
	// step runs as root; a local run that cannot write it skips instead.
	const pinnedCrClient = "/usr/local/bin/cr_client"
	if _, err := os.Lstat(pinnedCrClient); err == nil {
		t.Skipf("%s already exists; refusing to overwrite it with the stub", pinnedCrClient)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	stubSrc, err := os.ReadFile(filepath.Join(root, "tests", "integration", "memory-regions", "stub_cr_client.sh"))
	if err != nil {
		t.Fatal(err)
	}
	//nolint:gosec // the stub must be executable
	if err := os.WriteFile(pinnedCrClient, stubSrc, 0o700); err != nil {
		if os.IsPermission(err) {
			t.Skipf("cannot install the stub at %s: %v", pinnedCrClient, err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(pinnedCrClient) })

	// Standalone-mode auto-transition needs "GPU occupied"; no NVML here.
	origHasGPU := utils.HasGPUProcesses
	utils.HasGPUProcesses = func(_ context.Context) (bool, error) { return true, nil }
	t.Cleanup(func() { utils.HasGPUProcesses = origHasGPU })

	// Real server + real MemoryRegions backend (real exec) on localhost.
	backendsMap := map[backends.BackendType]backends.Backend{
		backends.BackendMemoryRegions: backends.NewMemoryRegions(),
		backends.BackendNoop:          backends.NewNoopBackend(),
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	srv := server.NewServer(backendsMap, backends.BackendNoop, "standalone", backends.NewChannelRegistry(),
		features.Gates{features.MemoryRegionsBackend: true})
	pb.RegisterSnapshotAgentServiceServer(s, srv)
	grpc_health_v1.RegisterHealthServer(s, server.NewHealthServer(backendsMap, backends.BackendNoop))
	go func() {
		if err := s.Serve(lis); err != nil {
			return
		}
	}()
	t.Cleanup(s.GracefulStop)

	endpoint := lis.Addr().String()

	// Drive it with the real Python client.
	python := os.Getenv("CROSS_LANG_PYTHON")
	if python == "" {
		python = "python3"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, filepath.Join(root, "tests", "integration", "memory-regions", "driver.py"))
	cmd.Env = append(os.Environ(),
		"AGENT_ENDPOINT="+endpoint,
		"CTL_DIR="+ctlDir,
		"WORKLOAD_PID="+workloadPID,
		"PYTHONPATH="+filepath.Join(root, "pkg", "client", "python"),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python driver failed: %v\n%s", err, string(out))
	}
	if !strings.Contains(string(out), "CROSS-LANG-OK") {
		t.Fatalf("driver output missing CROSS-LANG-OK:\n%s", string(out))
	}

	// The stub must have been invoked with the region spec.
	log, err := os.ReadFile(filepath.Join(ctlDir, "cr_client_calls.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(log), "-c -p "+workloadPID+" -s 0x7f0000000000:64") {
		t.Errorf("checkpoint call missing from calls log:\n%s", string(log))
	}
	if !strings.Contains(string(log), "-r -p "+workloadPID+" -s 0x7f0000000000:64") {
		t.Errorf("restore call missing from calls log:\n%s", string(log))
	}
	fmt.Println("cross-language driver output:", strings.TrimSpace(string(out)))
}
