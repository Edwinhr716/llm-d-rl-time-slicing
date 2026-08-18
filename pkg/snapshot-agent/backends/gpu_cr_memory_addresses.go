package backends

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// GpuCrMemoryAddresses implements the Backend interface using cr_client with
// memory addresses and destination-path dumps (GPU-CR GEP-0001/GEP-0006).
//
// Since GEP-0001 GA the agent moves NO dump bytes itself: each snapshot is
// written by the workload's preloader directly into a per-group destination
// file (cr_client -o), and each restore reads straight from it. The agent
// only names files, pre-creates them (via cr_client), and garbage-collects —
// none of which faults a hugetlb page, which is what lets the DaemonSet run
// with no hugepages-2Mi request.
type GpuCrMemoryAddresses struct {
	*GpuCr
}

// NewGpuCrMemoryAddresses creates a new GpuCrMemoryAddresses backend.
func NewGpuCrMemoryAddresses() *GpuCrMemoryAddresses {
	return &GpuCrMemoryAddresses{
		GpuCr: NewGpuCr(),
	}
}

// target format: "pid:addr:size"
func parseTargets(targets []string) (map[string][]string, error) {
	grouped := make(map[string][]string)
	for _, t := range targets {
		parts := strings.SplitN(t, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid target format %q, expected pid:spec", t)
		}
		pid := parts[0]
		spec := parts[1] // addr:size
		grouped[pid] = append(grouped[pid], spec)
	}
	return grouped, nil
}

// Snapshot triggers a selective snapshot of the accelerator context for a job,
// dumped by the preloader directly into <group store>/<groupID>/<id>.
func (g *GpuCrMemoryAddresses) Snapshot(ctx context.Context, groupID string, targets []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	storeMu.Lock()
	defer storeMu.Unlock()

	slog.InfoContext(ctx, "Snapshotting memory addresses using GPU-CR", "groupID", groupID, "targets", targets)

	grouped, err := parseTargets(targets)
	if err != nil {
		return err
	}

	targetDir, err := groupDir(groupID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create group dir %s: %w", targetDir, err)
	}

	t0 := time.Now()
	owners := make(map[string]string)
	for pid, specs := range grouped {
		id, err := g.resolvePidToId(pid)
		if err != nil {
			return fmt.Errorf("failed to resolve PID %s to ID: %w", pid, err)
		}
		dest := filepath.Join(targetDir, id)
		specStr := strings.Join(specs, ",")
		if err := g.checkpointMemoryAddresses(ctx, pid, specStr, dest); err != nil {
			return fmt.Errorf("cr_client checkpoint failed for PID %s with specs %s: %w", pid, specStr, err)
		}
		owners[pid] = id
	}
	slog.InfoContext(ctx, "GPU-CR selective checkpoint (direct-to-destination) took", "duration", time.Since(t0))

	// Group metadata + explicit utimes: these files are the ONLY copy of a
	// parked group, so GC decisions must never ride on mmap-driven mtimes
	// (which hugetlbfs does not reliably update) or on a blind TTL.
	if err := writeGroupMeta(targetDir, owners); err != nil {
		slog.WarnContext(ctx, "failed to write group metadata (GC will be conservative)", "dir", targetDir, "err", err)
	}
	touchGroup(targetDir)

	return nil
}

// Restore triggers a selective restoration of the accelerator context for a
// job, read by the preloader directly from <group store>/<groupID>/<id>.
func (g *GpuCrMemoryAddresses) Restore(ctx context.Context, groupID string, targets []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	storeMu.Lock()
	defer storeMu.Unlock()

	slog.InfoContext(ctx, "Restoring memory addresses using GPU-CR", "groupID", groupID, "targets", targets)

	grouped, err := parseTargets(targets)
	if err != nil {
		return err
	}

	targetDir, err := groupDir(groupID)
	if err != nil {
		return err
	}

	t0 := time.Now()
	for pid, specs := range grouped {
		id, err := g.resolvePidToId(pid)
		if err != nil {
			return fmt.Errorf("failed to resolve PID %s to ID: %w", pid, err)
		}
		dest := filepath.Join(targetDir, id)
		specStr := strings.Join(specs, ",")
		if err := g.restoreMemoryAddresses(ctx, pid, specStr, dest); err != nil {
			return fmt.Errorf("cr_client restore failed for PID %s with specs %s: %w", pid, specStr, err)
		}
	}
	slog.InfoContext(ctx, "GPU-CR selective restore (direct-from-destination) took", "duration", time.Since(t0))
	touchGroup(targetDir)
	return nil
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

// groupStoreDir is where per-group destination dumps live. Defaults to
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

// groupDir validates groupID and returns its directory under the store.
func groupDir(groupID string) (string, error) {
	store := groupStoreDir()
	dir := filepath.Join(store, filepath.Clean(groupID))
	rel, err := filepath.Rel(store, dir)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") || strings.ContainsRune(rel, os.PathSeparator) {
		return "", fmt.Errorf("invalid group ID: %s", groupID)
	}
	return dir, nil
}

// groupMetaFile records the owning workload processes of a group:
// "pid starttime" per line. GC deletes a group only when every recorded
// owner is gone (dead, or its PID recycled to a different starttime) —
// a parked group's dump is meaningless once its process is, and never
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
	return os.WriteFile(filepath.Join(dir, groupMetaName), []byte(sb.String()), 0644)
}

// touchGroup bumps the group dir mtime explicitly on every op.
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

func (g *GpuCrMemoryAddresses) resolvePidToId(pid string) (string, error) {
	// pid_map lives in the ctl dir since GEP-0006 (and is finally non-empty
	// there: the preloader writes it with write(2) on tmpfs). Check the
	// legacy data dir too for pre-GEP workloads.
	for _, dir := range []string{ctlFilesDir(), dataDir()} {
		mapPath := filepath.Join(dir, fmt.Sprintf("pid_map_%s", pid))
		data, err := os.ReadFile(mapPath)
		if err != nil {
			continue
		}
		// Strip NULs as well as whitespace: an mmap-written map file is
		// hugepage-sized with a zero-padded tail.
		id := strings.TrimSpace(strings.TrimRight(string(data), "\x00"))
		if isAllDigits(id) {
			return id, nil
		}
	}

	// Fallback: the dump buffer mapping is visible in /proc/<pid>/maps and
	// its basename IS the id.
	id, ferr := idFromProcMaps(pid)
	if ferr != nil {
		return "", fmt.Errorf("pid map for %s unusable and /proc fallback failed: %w", pid, ferr)
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

// idFromProcMaps scans /proc/<pid>/maps for the GPU-CR dump buffer mapping
// (a file named huge-ckpt/<id>, all digits) and returns the id.
func idFromProcMaps(pid string) (string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", pid, "maps"))
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
	return "", fmt.Errorf("no huge-ckpt/<id> mapping found in /proc/%s/maps", pid)
}

// snapshotStoreDir returns where LEGACY snapshot group copies were kept
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

func (g *GpuCrMemoryAddresses) checkpointMemoryAddresses(ctx context.Context, pid string, spec string, dest string) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout())
	defer cancel()
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-c", "-p", pid, "-s", spec, "-o", dest); err != nil {
		return fmt.Errorf("cr_client checkpoint (timeout %s): %w", opTimeout(), err)
	}
	return nil
}

func (g *GpuCrMemoryAddresses) restoreMemoryAddresses(ctx context.Context, pid string, spec string, dest string) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout())
	defer cancel()
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-r", "-p", pid, "-s", spec, "-o", dest); err != nil {
		return fmt.Errorf("cr_client restore (timeout %s): %w", opTimeout(), err)
	}
	return nil
}
