package backends

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// MemoryRegions implements the Backend interface for selective checkpoint
// and restore of explicit device-memory regions of a running process, using
// the GPU-CR cr_client with destination-path dumps (GPU-CR GEP-0001/GEP-0006).
// Regions are provided by the caller through MemoryRegionsBackendConfig; the
// backend performs no discovery.
//
// Since GEP-0001 GA the agent moves NO dump bytes itself: each snapshot is
// written by the workload's preloader directly into a per-slot destination
// file (cr_client -o), and each restore reads straight from it. The agent
// only names files, pre-creates them (via cr_client), and garbage-collects —
// none of which faults a hugetlb page, which is what lets the DaemonSet run
// with no hugepages-2Mi request.
//
// Environment configuration:
//
//	EXPORT_FILE_PATH        GPU-CR data dir: dump/staging buffers and the
//	                        destination group store (default /mnt/huge-ckpt)
//	GPU_CR_CTL_PATH         GEP-0006 control-plane tmpfs (control-<pid>,
//	                        pid_map_<pid>, ctl-ready-<pid>); unset = legacy
//	                        layout sharing the data dir
//	GPU_CR_GROUP_STORE      destination store override (default <data>/groups)
//	SNAPSHOT_DIR            LEGACY pre-GEP copy store; only GC reads it
//	GPU_CR_OP_TIMEOUT_SEC   per-cr_client-invocation timeout (default 120)
type MemoryRegions struct {
	mu          sync.Mutex
	execCommand func(ctx context.Context, name string, args ...string) ([]byte, error)
	lookPath    func(string) (string, error)
	// procRoot is "/proc" in production; injectable for tests.
	procRoot string
}

// NewMemoryRegions creates a new MemoryRegions backend.
func NewMemoryRegions() *MemoryRegions {
	return &MemoryRegions{
		execCommand: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).CombinedOutput()
		},
		lookPath: exec.LookPath,
		procRoot: "/proc",
	}
}

// dataDir is where GPU-CR keeps dump/staging DATA files (hugetlbfs mount).
func dataDir() string {
	if d := os.Getenv("EXPORT_FILE_PATH"); d != "" {
		return d
	}
	return "/mnt/huge-ckpt"
}

// ctlFilesDir is where the control plane lives: control-<pid>, pid_map_<pid>,
// ctl-ready-<pid>. With GPU_CR_CTL_PATH set (GEP-0006) that's a tmpfs; unset
// means the legacy layout where control files share the data dir.
func ctlFilesDir() string {
	if d := os.Getenv("GPU_CR_CTL_PATH"); d != "" {
		return d
	}
	return dataDir()
}

// groupStoreDir is where per-slot destination dumps live. Defaults to
// <dataDir>/groups: on the hugepage mount the parked bytes consume the pool
// the workload pod already requested, and the path string resolves
// identically in the agent and workload mount namespaces (both mount the
// same hostPath at the same in-container path).
func groupStoreDir() string {
	if d := os.Getenv("GPU_CR_GROUP_STORE"); d != "" {
		return d
	}
	return filepath.Join(dataDir(), "groups")
}

// groupDir validates a snapshot slot name and returns its directory under
// the destination store. Slots must be a single path segment: nesting is
// rejected along with traversal because GC reaps store entries at the top
// level only (a nested slot's .owners metadata would be invisible to it).
func groupDir(slot string) (string, error) {
	store := groupStoreDir()
	dir := filepath.Join(store, filepath.Clean(slot))
	rel, err := filepath.Rel(store, dir)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") || strings.ContainsRune(rel, os.PathSeparator) {
		return "", fmt.Errorf("invalid snapshot slot (path traversal or nested path): %q", slot)
	}
	return dir, nil
}

// groupMetaFile records the owning workload processes of a slot:
// "pid starttime" per line. GC deletes a slot only when every recorded
// owner is gone (dead, or its PID recycled to a different starttime) —
// a parked slot's dump is meaningless once its process is, and never
// expendable before that.
const groupMetaName = ".owners"

func writeGroupMeta(dir string, owners map[string]string) error {
	var sb strings.Builder
	for pid := range owners {
		st, err := procStarttime(pid)
		if err != nil {
			return fmt.Errorf("starttime of owner pid %s: %w", pid, err)
		}
		fmt.Fprintf(&sb, "%s %d\n", pid, st)
	}
	return os.WriteFile(filepath.Join(dir, groupMetaName), []byte(sb.String()), 0o644)
}

// touchGroup bumps the slot dir mtime explicitly on every op.
func touchGroup(dir string) {
	now := time.Now()
	_ = os.Chtimes(dir, now, now)
}

// procStarttime returns field 22 of /proc/<pid>/stat — the PID-reuse guard.
func procStarttime(pid string) (int64, error) {
	data, err := os.ReadFile(filepath.Join("/proc", pid, "stat"))
	if err != nil {
		return 0, err
	}
	// comm may contain spaces/parens: parse after the LAST ')'.
	idx := strings.LastIndexByte(string(data), ')')
	if idx < 0 || idx+2 >= len(data) {
		return 0, fmt.Errorf("malformed /proc/%s/stat", pid)
	}
	fields := strings.Fields(string(data[idx+2:]))
	// fields[0] is field 3 (state); starttime is field 22.
	if len(fields) < 20 {
		return 0, fmt.Errorf("short /proc/%s/stat", pid)
	}
	return strconv.ParseInt(fields[19], 10, 64)
}

// regionSpecs validates the config's regions and groups them per PID,
// preserving order, each formatted as the cr_client "0xADDR:SIZE" spec.
func regionSpecs(cfg *pb.MemoryRegionsBackendConfig) (map[int32][]string, error) {
	regions := cfg.GetRegions()
	if len(regions) == 0 {
		return nil, fmt.Errorf("at least one memory region is required")
	}
	specs := make(map[int32][]string)
	for _, r := range regions {
		if r.GetPid() <= 0 {
			return nil, fmt.Errorf("memory region pid must be positive, got %d", r.GetPid())
		}
		if r.GetSizeBytes() == 0 {
			return nil, fmt.Errorf("memory region size_bytes must be positive (pid %d, address 0x%x)", r.GetPid(), r.GetAddress())
		}
		specs[r.GetPid()] = append(specs[r.GetPid()], fmt.Sprintf("0x%x:%d", r.GetAddress(), r.GetSizeBytes()))
	}
	return specs, nil
}

// regionPIDs returns the config's PIDs in first-appearance order, so
// cr_client invocations are deterministic.
func regionPIDs(cfg *pb.MemoryRegionsBackendConfig) []int32 {
	seen := make(map[int32]bool)
	var pids []int32
	for _, r := range cfg.GetRegions() {
		if !seen[r.GetPid()] {
			seen[r.GetPid()] = true
			pids = append(pids, r.GetPid())
		}
	}
	return pids
}

// snapshotSlot returns the destination-store slot for the request:
// MemoryRegionsBackendConfig.snapshot_name, falling back to the job ID when
// empty. The request's `group` is deliberately NOT used: group identifies a
// set of related jobs for the orchestrator and does not name agent-side
// storage. The returned slot is guaranteed to resolve inside the store.
func snapshotSlot(req Request) (string, error) {
	slot := req.Config.GetMemoryRegions().GetSnapshotName()
	if slot == "" {
		slot = req.JobID
	}
	if slot == "" {
		return "", fmt.Errorf("snapshot slot is empty: set snapshot_name or job_id")
	}
	if _, err := groupDir(slot); err != nil {
		return "", err
	}
	return slot, nil
}

// Snapshot triggers a selective snapshot of the configured memory regions,
// dumped by the preloader directly into <group store>/<slot>/<id>.
func (g *MemoryRegions) Snapshot(ctx context.Context, req Request) error {
	cfg := req.Config.GetMemoryRegions()
	if cfg == nil {
		return fmt.Errorf("memory-regions backend requires BackendConfig.memory_regions")
	}
	specs, err := regionSpecs(cfg)
	if err != nil {
		return err
	}
	slot, err := snapshotSlot(req)
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	storeMu.Lock()
	defer storeMu.Unlock()

	slog.InfoContext(ctx, "Snapshotting memory regions using GPU-CR",
		"jobID", req.JobID, "slot", slot, "pids", regionPIDs(cfg), "regions", len(cfg.GetRegions()))

	targetDir, err := groupDir(slot)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("failed to create slot dir %s: %w", targetDir, err)
	}

	t0 := time.Now()
	owners := make(map[string]string)
	for _, pid := range regionPIDs(cfg) {
		pidStr := strconv.Itoa(int(pid))
		id, err := g.resolvePidToID(pidStr)
		if err != nil {
			return fmt.Errorf("failed to resolve PID %d to ID: %w", pid, err)
		}
		dest := filepath.Join(targetDir, id)
		specStr := strings.Join(specs[pid], ",")
		if err := g.checkpointRegions(ctx, pid, specStr, dest); err != nil {
			return fmt.Errorf("cr_client checkpoint failed for PID %d with specs %s: %w", pid, specStr, err)
		}
		owners[pidStr] = id
	}
	slog.InfoContext(ctx, "GPU-CR selective checkpoint (direct-to-destination) took", "duration", time.Since(t0))

	// Slot metadata + explicit utimes: these files are the ONLY copy of a
	// parked slot, so GC decisions must never ride on mmap-driven mtimes
	// (which hugetlbfs does not reliably update) or on a blind TTL.
	if err := writeGroupMeta(targetDir, owners); err != nil {
		slog.WarnContext(ctx, "failed to write slot metadata (GC will be conservative)", "dir", targetDir, "err", err)
	}
	touchGroup(targetDir)

	return nil
}

// Restore triggers a selective restoration of the configured memory regions,
// read by the preloader directly from <group store>/<slot>/<id>.
func (g *MemoryRegions) Restore(ctx context.Context, req Request) error {
	cfg := req.Config.GetMemoryRegions()
	if cfg == nil {
		return fmt.Errorf("memory-regions backend requires BackendConfig.memory_regions")
	}
	specs, err := regionSpecs(cfg)
	if err != nil {
		return err
	}
	slot, err := snapshotSlot(req)
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	storeMu.Lock()
	defer storeMu.Unlock()

	slog.InfoContext(ctx, "Restoring memory regions using GPU-CR",
		"jobID", req.JobID, "slot", slot, "pids", regionPIDs(cfg), "regions", len(cfg.GetRegions()))

	targetDir, err := groupDir(slot)
	if err != nil {
		return err
	}
	if _, err := os.Stat(targetDir); err != nil {
		return fmt.Errorf("snapshot slot %q not found in group store %s: %w", slot, groupStoreDir(), err)
	}

	t0 := time.Now()
	for _, pid := range regionPIDs(cfg) {
		id, err := g.resolvePidToID(strconv.Itoa(int(pid)))
		if err != nil {
			return fmt.Errorf("failed to resolve PID %d to ID: %w", pid, err)
		}
		dest := filepath.Join(targetDir, id)
		specStr := strings.Join(specs[pid], ",")
		if err := g.restoreRegions(ctx, pid, specStr, dest); err != nil {
			return fmt.Errorf("cr_client restore failed for PID %d with specs %s: %w", pid, specStr, err)
		}
	}
	slog.InfoContext(ctx, "GPU-CR selective restore (direct-from-destination) took", "duration", time.Since(t0))
	touchGroup(targetDir)
	return nil
}

// HealthCheck reports whether the cr_client binary is resolvable, so
// grpc.health.v1.Health/Check with service "memory-regions" reflects backend
// readiness (an agent image built without cr_client reports NOT_SERVING).
func (g *MemoryRegions) HealthCheck(ctx context.Context) error {
	binaryPath := g.getCrClientPath()
	if _, err := g.lookPath(binaryPath); err != nil {
		return fmt.Errorf("cr_client executable not found: %w", err)
	}
	return nil
}

func (g *MemoryRegions) getCrClientPath() string {
	candidates := []string{
		"cr_client",
		"/usr/bin/cr_client",
		"/bin/cr_client",
		"/opt/bin/cr_client",
		"/usr/local/bin/cr_client",
	}
	for _, p := range candidates {
		if path, err := g.lookPath(p); err == nil {
			return path
		}
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "/usr/local/bin/cr_client"
}

func (g *MemoryRegions) runCommand(ctx context.Context, name string, args ...string) error {
	if out, err := g.execCommand(ctx, name, args...); err != nil {
		return fmt.Errorf("command failed: %w, output: %s", err, string(out))
	}
	return nil
}

func (g *MemoryRegions) checkpointRegions(ctx context.Context, pid int32, spec string, dest string) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout())
	defer cancel()
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-c", "-p", strconv.Itoa(int(pid)), "-s", spec, "-o", dest); err != nil {
		return fmt.Errorf("cr_client checkpoint (timeout %s): %w", opTimeout(), err)
	}
	return nil
}

func (g *MemoryRegions) restoreRegions(ctx context.Context, pid int32, spec string, dest string) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout())
	defer cancel()
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-r", "-p", strconv.Itoa(int(pid)), "-s", spec, "-o", dest); err != nil {
		return fmt.Errorf("cr_client restore (timeout %s): %w", opTimeout(), err)
	}
	return nil
}

// resolvePidToID maps a workload PID to its GPU-CR dump-buffer id.
func (g *MemoryRegions) resolvePidToID(pid string) (string, error) {
	// pid_map lives in the ctl dir since GEP-0006 (and is finally non-empty
	// there: the preloader writes it with write(2) on tmpfs). Check the
	// legacy data dir too for pre-GEP workloads.
	var lastErr error
	for _, dir := range []string{ctlFilesDir(), dataDir()} {
		mapPath := filepath.Join(dir, fmt.Sprintf("pid_map_%s", pid))
		data, err := os.ReadFile(mapPath)
		if err != nil {
			lastErr = err
			continue
		}
		// Strip NULs as well as whitespace: an mmap-written map file is
		// hugepage-sized with a zero-padded tail.
		id := strings.TrimSpace(strings.TrimRight(string(data), "\x00"))
		if isAllDigits(id) {
			return id, nil
		}
	}

	// Fallback: pre-GEP preloaders wrote pid_map via buffered stdio, which
	// silently produces an empty file on hugetlbfs. The dump buffer mapping
	// is visible in /proc/<pid>/maps and its basename IS the id.
	id, ferr := idFromProcMaps(g.procRoot, pid)
	if ferr != nil {
		readProblem := "contents are not a numeric id"
		if lastErr != nil {
			readProblem = lastErr.Error()
		}
		return "", fmt.Errorf("pid map for %s unusable (%s) and /proc fallback failed: %w", pid, readProblem, ferr)
	}
	slog.Info("Resolved GPU-CR id from /proc/<pid>/maps fallback", "pid", pid, "id", id)
	return id, nil
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// idFromProcMaps scans <procRoot>/<pid>/maps for the GPU-CR dump buffer
// mapping (a file named huge-ckpt/<id>, all digits) and returns the id.
func idFromProcMaps(procRoot, pid string) (string, error) {
	data, err := os.ReadFile(filepath.Join(procRoot, pid, "maps"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		idx := strings.IndexByte(line, '/')
		if idx < 0 {
			continue
		}
		path := line[idx:]
		if !strings.Contains(path, "huge-ckpt/") {
			continue
		}
		base := filepath.Base(path)
		if isAllDigits(base) {
			return base, nil
		}
	}
	return "", fmt.Errorf("no huge-ckpt/<id> mapping found in %s/%s/maps", procRoot, pid)
}

// snapshotStoreDir returns where LEGACY snapshot slot copies were kept
// (pre-GEP-0001 agent-side copies). Only GC still looks here, to reap
// leftovers from older agents.
func snapshotStoreDir(ctlDir string) string {
	if d := os.Getenv("SNAPSHOT_DIR"); d != "" {
		return d
	}
	return filepath.Join(ctlDir, "snapshots")
}

// opTimeout bounds a single cr_client invocation. Without it, a workload
// dying mid-operation leaves cr_client polling the shared-memory control file
// forever and the job wedged in TRANSITIONING (observed in Phase 0).
// cr_client now enforces the same deadline internally (GEP-0001); this is
// the outer belt to its braces.
func opTimeout() time.Duration {
	if v := os.Getenv("GPU_CR_OP_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 120 * time.Second
}
