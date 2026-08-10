package backends

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// MemoryRegions implements the Backend interface for selective checkpoint
// and restore of explicit device-memory regions of a running process, using
// the GPU-CR cr_client (`-s addr:size,...` spec). Regions are provided by the
// caller through MemoryRegionsBackendConfig; the backend performs no
// discovery.
//
// Environment configuration:
//
//	EXPORT_FILE_PATH       shared checkpoint/control dir (default /mnt/huge-ckpt)
//	SNAPSHOT_DIR           disk-backed snapshot store (default <ctl>/snapshots)
//	GPU_CR_COPY_HOST_FILE  copy the -host staging file too when set to "1"
//	GPU_CR_OP_TIMEOUT_SEC  per-cr_client-invocation timeout (default 120)
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

// exportDir returns the shared GPU-CR checkpoint/control directory.
func exportDir() string {
	if d := os.Getenv("EXPORT_FILE_PATH"); d != "" {
		return d
	}
	return "/mnt/huge-ckpt"
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

// snapshotSlot returns the snapshot-store slot for the request:
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
	if _, err := slotDir(snapshotStoreDir(exportDir()), slot); err != nil {
		return "", err
	}
	return slot, nil
}

// slotDir resolves the on-disk directory for a snapshot slot, rejecting
// names that would escape the snapshot store (path traversal).
func slotDir(snapshotsDir, slot string) (string, error) {
	target := filepath.Join(snapshotsDir, filepath.Clean(slot))
	rel, err := filepath.Rel(snapshotsDir, target)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid snapshot name (path traversal attempt): %q", slot)
	}
	return target, nil
}

// Snapshot triggers a selective snapshot of the configured memory regions.
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

	slog.InfoContext(ctx, "Snapshotting memory regions using GPU-CR",
		"jobID", req.JobID, "slot", slot, "pids", regionPIDs(cfg), "regions", len(cfg.GetRegions()))

	// 1. Trigger checkpoint via cr_client, one invocation per PID.
	t0 := time.Now()
	for _, pid := range regionPIDs(cfg) {
		specStr := strings.Join(specs[pid], ",")
		if err := g.checkpointRegions(ctx, pid, specStr); err != nil {
			return fmt.Errorf("cr_client checkpoint failed for PID %d with specs %s: %w", pid, specStr, err)
		}
	}
	slog.InfoContext(ctx, "GPU-CR selective checkpoint took", "duration", time.Since(t0))

	// 2. Copy dump files into the snapshot store slot.
	ctlDir := exportDir()
	snapshotsDir := snapshotStoreDir(ctlDir)
	targetDir, err := slotDir(snapshotsDir, slot)
	if err != nil {
		return err
	}

	t1 := time.Now()
	for _, pid := range regionPIDs(cfg) {
		id, err := g.resolvePidToId(strconv.Itoa(int(pid)))
		if err != nil {
			return fmt.Errorf("failed to resolve PID %d to ID: %w", pid, err)
		}

		// Copy data file. Limit the copy to the dump's allocated extent
		// (shared_mem_fs.current_offset) so we don't scan the full 25GB
		// buffer, which on hugetlbfs is a zero-fill read of the whole pool.
		srcData := filepath.Join(ctlDir, id)
		dstData := filepath.Join(targetDir, id)
		slog.InfoContext(ctx, "Copying checkpoint file", "src", srcData, "dst", dstData)
		if err := copyFile(srcData, dstData, dumpDataLimit(srcData)); err != nil {
			return fmt.Errorf("failed to copy checkpoint file from %s to %s: %w", srcData, dstData, err)
		}

		// Copy host file. The -host file is the DMA staging double-buffer —
		// transient scratch, ~2GB of mostly-stale bytes; copying it both ways
		// dominated swap latency in Phase 1 measurements. Skipped unless
		// GPU_CR_COPY_HOST_FILE=1.
		if copyHostFileEnabled() {
			srcHost := filepath.Join(ctlDir, fmt.Sprintf("%s-host", id))
			dstHost := filepath.Join(targetDir, fmt.Sprintf("%s-host", id))
			if _, err := os.Stat(srcHost); err == nil {
				slog.InfoContext(ctx, "Copying host file", "src", srcHost, "dst", dstHost)
				if err := copyFile(srcHost, dstHost, 0); err != nil {
					return fmt.Errorf("failed to copy host file from %s to %s: %w", srcHost, dstHost, err)
				}
			}
		}
	}
	slog.InfoContext(ctx, "Copying snapshot files took", "duration", time.Since(t1))

	return nil
}

// Restore triggers a selective restoration of the configured memory regions.
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

	slog.InfoContext(ctx, "Restoring memory regions using GPU-CR",
		"jobID", req.JobID, "slot", slot, "pids", regionPIDs(cfg), "regions", len(cfg.GetRegions()))

	ctlDir := exportDir()
	snapshotsDir := snapshotStoreDir(ctlDir)
	targetDir, err := slotDir(snapshotsDir, slot)
	if err != nil {
		return err
	}
	if _, err := os.Stat(targetDir); err != nil {
		return fmt.Errorf("snapshot slot %q not found in snapshot store %s: %w", slot, snapshotsDir, err)
	}

	// 1. Copy files back from the snapshot store (overwriting the active
	// dump buffer). When ctlDir is a hugetlbfs mount, write(2) is not
	// supported and truncating would rip pages out of the workload's live
	// mapping, so we write through a shared mmap of the existing buffer file
	// instead.
	hugetlb := isHugetlbfs(ctlDir)
	t0 := time.Now()
	for _, pid := range regionPIDs(cfg) {
		id, err := g.resolvePidToId(strconv.Itoa(int(pid)))
		if err != nil {
			return fmt.Errorf("failed to resolve PID %d to ID: %w", pid, err)
		}

		// Restore data file.
		srcData := filepath.Join(targetDir, id)
		dstData := filepath.Join(ctlDir, id)
		slog.InfoContext(ctx, "Restoring checkpoint file", "src", srcData, "dst", dstData, "hugetlbfs", hugetlb)
		if err := restoreCopy(srcData, dstData, hugetlb); err != nil {
			return fmt.Errorf("failed to restore checkpoint file from %s to %s: %w", srcData, dstData, err)
		}

		// Restore host file (skipped by default; see Snapshot).
		if copyHostFileEnabled() {
			srcHost := filepath.Join(targetDir, fmt.Sprintf("%s-host", id))
			dstHost := filepath.Join(ctlDir, fmt.Sprintf("%s-host", id))
			if _, err := os.Stat(srcHost); err == nil {
				slog.InfoContext(ctx, "Restoring host file", "src", srcHost, "dst", dstHost, "hugetlbfs", hugetlb)
				if err := restoreCopy(srcHost, dstHost, hugetlb); err != nil {
					return fmt.Errorf("failed to restore host file from %s to %s: %w", srcHost, dstHost, err)
				}
			}
		}
	}
	slog.InfoContext(ctx, "Restoring snapshot files took", "duration", time.Since(t0))

	// 2. Trigger restore via cr_client.
	t1 := time.Now()
	for _, pid := range regionPIDs(cfg) {
		specStr := strings.Join(specs[pid], ",")
		if err := g.restoreRegions(ctx, pid, specStr); err != nil {
			return fmt.Errorf("cr_client restore failed for PID %d with specs %s: %w", pid, specStr, err)
		}
	}
	slog.InfoContext(ctx, "GPU-CR selective restore took", "duration", time.Since(t1))
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
	for _, p := range []string{"cr_client", "/usr/bin/cr_client", "/bin/cr_client", "/opt/bin/cr_client", "/usr/local/bin/cr_client"} {
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

func (g *MemoryRegions) checkpointRegions(ctx context.Context, pid int32, spec string) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout())
	defer cancel()
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-c", "-p", strconv.Itoa(int(pid)), "-s", spec); err != nil {
		return fmt.Errorf("cr_client checkpoint (timeout %s): %w", opTimeout(), err)
	}
	return nil
}

func (g *MemoryRegions) restoreRegions(ctx context.Context, pid int32, spec string) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout())
	defer cancel()
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-r", "-p", strconv.Itoa(int(pid)), "-s", spec); err != nil {
		return fmt.Errorf("cr_client restore (timeout %s): %w", opTimeout(), err)
	}
	return nil
}

// resolvePidToId maps a workload PID to its GPU-CR dump-buffer id.
func (g *MemoryRegions) resolvePidToId(pid string) (string, error) {
	ctlDir := exportDir()
	mapPath := filepath.Join(ctlDir, fmt.Sprintf("pid_map_%s", pid))
	data, err := os.ReadFile(mapPath)
	if err == nil {
		// Strip NULs as well as whitespace: an mmap-written map file is
		// hugepage-sized with a zero-padded tail.
		id := strings.TrimSpace(strings.TrimRight(string(data), "\x00"))
		if isAllDigits(id) {
			return id, nil
		}
	}

	// Fallback: the preloader writes pid_map via buffered stdio, which
	// silently produces an empty file on hugetlbfs. The dump buffer mapping
	// is visible in /proc/<pid>/maps and its basename IS the id.
	id, ferr := idFromProcMaps(g.procRoot, pid)
	if ferr != nil {
		return "", fmt.Errorf("pid map file %s unusable (%v) and /proc fallback failed: %w", mapPath, err, ferr)
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

// snapshotStoreDir returns where snapshot slot copies are kept. Defaults to
// <ctlDir>/snapshots for backwards compatibility, but should be pointed at a
// disk-backed path via SNAPSHOT_DIR when ctlDir is hugetlbfs (write(2) is not
// supported there, and copies would permanently consume hugepages).
func snapshotStoreDir(ctlDir string) string {
	if d := os.Getenv("SNAPSHOT_DIR"); d != "" {
		return d
	}
	return filepath.Join(ctlDir, "snapshots")
}

func copyHostFileEnabled() bool {
	return os.Getenv("GPU_CR_COPY_HOST_FILE") == "1"
}

const hugetlbfsMagic = 0x958458f6

func isHugetlbfs(path string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false
	}
	return uint64(st.Type) == hugetlbfsMagic
}

// dumpDataLimit reads the shared_mem_fs header of a GPU-CR dump file and
// returns current_offset — the end of allocated data — so copies can skip the
// untouched tail of the buffer. Falls back to 0 (copy everything) if the
// header is unreadable or implausible.
func dumpDataLimit(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return 0
	}

	var hdr [16]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return 0
	}
	currentOffset := int64(binary.LittleEndian.Uint64(hdr[8:16]))
	if currentOffset < 16 || currentOffset > st.Size() {
		return 0
	}
	return currentOffset
}

// restoreCopy copies a saved snapshot file back over the live dump buffer.
func restoreCopy(src, dst string, hugetlb bool) error {
	if hugetlb {
		return copyIntoHugetlbfs(src, dst)
	}
	return copyFile(src, dst, 0)
}

// copyFile copies src to dst (regular filesystem), sparse-aware. If limit > 0
// only the first limit bytes are copied; dst is still sized to match src.
func copyFile(src, dst string, limit int64) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	srcStat, err := srcFile.Stat()
	if err != nil {
		return err
	}
	srcSize := srcStat.Size()
	if limit <= 0 || limit > srcSize {
		limit = srcSize
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	// Open with O_RDWR | O_CREATE | O_TRUNC to ensure we can write and truncate
	dstFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	// Pre-set the size to match source (creates a sparse file of that size if we don't write to it)
	if err := dstFile.Truncate(srcSize); err != nil {
		return err
	}

	const bufSize = 4 * 1024 * 1024 // 4MB blocks: hugetlbfs zero-fill reads dominate copy time
	buf := make([]byte, bufSize)

	var writeOffset int64
	var currentOffset int64

	for currentOffset < limit {
		want := int64(bufSize)
		if remaining := limit - currentOffset; remaining < want {
			want = remaining
		}
		n, err := srcFile.Read(buf[:want])
		if n > 0 {
			if isAllZeros(buf[:n]) {
				currentOffset += int64(n)
			} else {
				if currentOffset != writeOffset {
					_, err = dstFile.Seek(currentOffset, io.SeekStart)
					if err != nil {
						return err
					}
					writeOffset = currentOffset
				}
				nw, err := dstFile.Write(buf[:n])
				if err != nil {
					return err
				}
				writeOffset += int64(nw)
				currentOffset += int64(n)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return dstFile.Sync()
}

// copyIntoHugetlbfs writes src back into the existing dump buffer file dst
// on hugetlbfs via a shared mmap (hugetlbfs has no write(2), and truncating
// would invalidate the workload's live mapping).
//
// HOLE-FAITHFUL BY CONTRACT: every byte of the dump extent is authoritative
// checkpoint payload, INCLUDING zeros — first-step optimizer moments are
// exactly zero by construction, and skipping them left a co-resident
// checkpoint's bytes beneath sparse holes (the 2026-08-04 silent-corruption
// incident; see GPU-CR GEP-0003). We therefore copy the full extent
// [0, current_offset) unconditionally: pread reads file holes as zeros, so
// zeros land in the buffer exactly like data. Hugepage faults stay bounded
// by the dump extent, not the buffer size.
func copyIntoHugetlbfs(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	srcStat, err := srcFile.Stat()
	if err != nil {
		return err
	}
	srcSize := srcStat.Size()

	limit := dumpDataLimit(src)
	if limit <= 0 || limit > srcSize {
		limit = srcSize
	}

	dstFile, err := os.OpenFile(dst, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("live dump buffer must already exist on hugetlbfs: %w", err)
	}
	defer dstFile.Close()

	dstStat, err := dstFile.Stat()
	if err != nil {
		return err
	}
	dstSize := dstStat.Size()
	if dstSize == 0 {
		return fmt.Errorf("dump buffer %s has zero size", dst)
	}
	if limit > dstSize {
		limit = dstSize
	}

	m, err := syscall.Mmap(int(dstFile.Fd()), 0, int(dstSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap of hugetlbfs dump buffer %s failed: %w", dst, err)
	}
	defer func() {
		_ = syscall.Munmap(m)
	}()

	buf := make([]byte, 4*1024*1024)
	for off := int64(0); off < limit; {
		want := int64(len(buf))
		if remaining := limit - off; remaining < want {
			want = remaining
		}
		n, err := srcFile.ReadAt(buf[:want], off)
		if n > 0 {
			copy(m[off:off+int64(n)], buf[:n])
			off += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func isAllZeros(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

// opTimeout bounds a single cr_client invocation. Without it, a workload
// dying mid-operation leaves cr_client polling the shared-memory control file
// forever and the job wedged in TRANSITIONING (observed in Phase 0).
func opTimeout() time.Duration {
	if v := os.Getenv("GPU_CR_OP_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 120 * time.Second
}
