package backends

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GpuCrMemoryAddresses implements the Backend interface using cr_client with memory addresses.
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

// Snapshot triggers a selective snapshot of the accelerator context for a job.
func (g *GpuCrMemoryAddresses) Snapshot(ctx context.Context, groupID string, targets []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	slog.InfoContext(ctx, "Snapshotting memory addresses using GPU-CR", "groupID", groupID, "targets", targets)

	grouped, err := parseTargets(targets)
	if err != nil {
		return err
	}

	t0 := time.Now()
	// 1. Trigger checkpoint via cr_client
	for pid, specs := range grouped {
		specStr := strings.Join(specs, ",")
		if err := g.checkpointMemoryAddresses(ctx, pid, specStr); err != nil {
			return fmt.Errorf("cr_client checkpoint failed for PID %s with specs %s: %w", pid, specStr, err)
		}
	}
	slog.InfoContext(ctx, "GPU-CR selective checkpoint took", "duration", time.Since(t0))

	// 2. Copy files to snapshot directory
	t1 := time.Now()
	ctlDir := os.Getenv("EXPORT_FILE_PATH")
	if ctlDir == "" {
		ctlDir = "/mnt/huge-ckpt"
	}
	snapshotsDir := filepath.Join(ctlDir, "snapshots")
	targetDir := filepath.Join(snapshotsDir, filepath.Clean(groupID))

	// Ensure targetDir is still under snapshotsDir (prevent path traversal)
	rel, err := filepath.Rel(snapshotsDir, targetDir)
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("invalid group ID (path traversal attempt): %s", groupID)
	}

	for pid := range grouped {
		id, err := g.resolvePidToId(pid)
		if err != nil {
			return fmt.Errorf("failed to resolve PID %s to ID: %w", pid, err)
		}

		// Copy data file
		srcData := filepath.Join(ctlDir, id)
		dstData := filepath.Join(targetDir, id)
		slog.InfoContext(ctx, "Copying checkpoint file", "src", srcData, "dst", dstData)
		if err := copyFile(srcData, dstData); err != nil {
			return fmt.Errorf("failed to copy checkpoint file from %s to %s: %w", srcData, dstData, err)
		}

		// Copy host file
		srcHost := filepath.Join(ctlDir, fmt.Sprintf("%s-host", id))
		dstHost := filepath.Join(targetDir, fmt.Sprintf("%s-host", id))
		if _, err := os.Stat(srcHost); err == nil {
			slog.InfoContext(ctx, "Copying host file", "src", srcHost, "dst", dstHost)
			if err := copyFile(srcHost, dstHost); err != nil {
				return fmt.Errorf("failed to copy host file from %s to %s: %w", srcHost, dstHost, err)
			}
		}
	}
	slog.InfoContext(ctx, "Copying snapshot files took", "duration", time.Since(t1))

	return nil
}

// Restore triggers a selective restoration of the accelerator context for a job.
func (g *GpuCrMemoryAddresses) Restore(ctx context.Context, groupID string, targets []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	slog.InfoContext(ctx, "Restoring memory addresses using GPU-CR", "groupID", groupID, "targets", targets)

	grouped, err := parseTargets(targets)
	if err != nil {
		return err
	}

	ctlDir := os.Getenv("EXPORT_FILE_PATH")
	if ctlDir == "" {
		ctlDir = "/mnt/huge-ckpt"
	}
	snapshotsDir := filepath.Join(ctlDir, "snapshots")
	targetDir := filepath.Join(snapshotsDir, filepath.Clean(groupID))

	// Ensure targetDir is still under snapshotsDir (prevent path traversal)
	rel, err := filepath.Rel(snapshotsDir, targetDir)
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("invalid group ID (path traversal attempt): %s", groupID)
	}

	// 1. Copy files back from snapshot directory (overwriting active slot)
	t0 := time.Now()
	for pid := range grouped {
		id, err := g.resolvePidToId(pid)
		if err != nil {
			return fmt.Errorf("failed to resolve PID %s to ID: %w", pid, err)
		}

		// Restore data file
		srcData := filepath.Join(targetDir, id)
		dstData := filepath.Join(ctlDir, id)
		slog.InfoContext(ctx, "Restoring checkpoint file", "src", srcData, "dst", dstData)
		if err := copyFile(srcData, dstData); err != nil {
			return fmt.Errorf("failed to restore checkpoint file from %s to %s: %w", srcData, dstData, err)
		}

		// Restore host file
		srcHost := filepath.Join(targetDir, fmt.Sprintf("%s-host", id))
		dstHost := filepath.Join(ctlDir, fmt.Sprintf("%s-host", id))
		if _, err := os.Stat(srcHost); err == nil {
			slog.InfoContext(ctx, "Restoring host file", "src", srcHost, "dst", dstHost)
			if err := copyFile(srcHost, dstHost); err != nil {
				return fmt.Errorf("failed to restore host file from %s to %s: %w", srcHost, dstHost, err)
			}
		}
	}
	slog.InfoContext(ctx, "Restoring snapshot files took", "duration", time.Since(t0))

	// 2. Trigger restore via cr_client
	t1 := time.Now()
	for pid, specs := range grouped {
		specStr := strings.Join(specs, ",")
		if err := g.restoreMemoryAddresses(ctx, pid, specStr); err != nil {
			return fmt.Errorf("cr_client restore failed for PID %s with specs %s: %w", pid, specStr, err)
		}
	}
	slog.InfoContext(ctx, "GPU-CR selective restore took", "duration", time.Since(t1))
	return nil
}

func (g *GpuCrMemoryAddresses) resolvePidToId(pid string) (string, error) {
	ctlDir := os.Getenv("EXPORT_FILE_PATH")
	if ctlDir == "" {
		ctlDir = "/mnt/huge-ckpt"
	}
	mapPath := filepath.Join(ctlDir, fmt.Sprintf("pid_map_%s", pid))
	data, err := os.ReadFile(mapPath)
	if err != nil {
		return "", fmt.Errorf("failed to read pid map file %s: %w", mapPath, err)
	}
	return strings.TrimSpace(string(data)), nil
}

func copyFile(src, dst string) error {
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

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	// Open with O_RDWR | O_CREATE | O_TRUNC to ensure we can write and truncate
	dstFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	// Pre-set the size to match source (creates a sparse file of that size if we don't write to it)
	if err := dstFile.Truncate(srcSize); err != nil {
		return err
	}

	const bufSize = 64 * 1024 // 64KB block size
	buf := make([]byte, bufSize)

	var writeOffset int64 = 0
	var currentOffset int64 = 0

	for {
		n, err := srcFile.Read(buf)
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

func isAllZeros(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func (g *GpuCrMemoryAddresses) checkpointMemoryAddresses(ctx context.Context, pid string, spec string) error {
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-c", "-p", pid, "-s", spec); err != nil {
		return err
	}
	return nil
}

func (g *GpuCrMemoryAddresses) restoreMemoryAddresses(ctx context.Context, pid string, spec string) error {
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-r", "-p", pid, "-s", spec); err != nil {
		return err
	}
	return nil
}
