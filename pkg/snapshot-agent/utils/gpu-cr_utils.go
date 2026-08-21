package utils

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// GPU-CR leaves per-process artifacts in the shared checkpoint dir:
//
//	<id>, <id>-host   dump + staging buffers (hugetlbfs: MAP_SHARED
//	                  reservations stick to the FILE, so a leaked pair pins
//	                  the reserved hugepages even after the process dies)
//	control-<pid>     shared-memory control channel
//	pid_map_<pid>     pid→id map (may be empty on hugetlbfs)
//
// Nothing deletes them on process exit, so the agent sweeps them.

var (
	dumpFileRe = regexp.MustCompile(`^(\d+)(-host)?$`)
	pidFileRe  = regexp.MustCompile(`^(?:control|pid_map)[-_](\d+)$`)
)

// gcMinAge guards against racing a process that created its files but hasn't
// mmap'd them yet (files appear before the mapping does).
const gcMinAge = 5 * time.Minute

// StartGPUCRSweeper sweeps stale GPU-CR artifacts at startup and every
// interval.
func StartGPUCRSweeper(ctx context.Context, ctlDir string, interval time.Duration) {
	go func() {
		sweep(ctlDir)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sweep(ctlDir)
			}
		}
	}()
}

func sweep(ctlDir string) {
	entries, err := os.ReadDir(ctlDir)
	if err != nil {
		slog.Warn("GC: cannot read checkpoint dir", "dir", ctlDir, "err", err)
		return
	}

	liveIds, complete := liveMappedIds()
	if !complete {
		slog.Warn("GC: procfs scan incomplete; keeping all dump files this sweep", "dir", ctlDir)
	}
	now := time.Now()
	var removed []string

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || now.Sub(info.ModTime()) < gcMinAge {
			continue
		}

		if m := dumpFileRe.FindStringSubmatch(name); m != nil {
			if complete && !liveIds[m[1]] {
				if os.Remove(filepath.Join(ctlDir, name)) == nil {
					removed = append(removed, name)
				}
			}
			continue
		}
		if m := pidFileRe.FindStringSubmatch(name); m != nil {
			if _, err := os.Stat("/proc/" + m[1]); os.IsNotExist(err) {
				if os.Remove(filepath.Join(ctlDir, name)) == nil {
					removed = append(removed, name)
				}
			}
		}
		// Never touch the bare "control" id-counter file or anything else.
	}
	if len(removed) > 0 {
		slog.Info("GC: removed stale GPU-CR artifacts", "count", len(removed), "files", removed)
	}
}

// procfsRoot is the procfs mount scanned for live mappings; a var so tests
// can point the scan at a fixture tree.
var procfsRoot = "/proc"

// pidDirRe matches the numeric process dirs under /proc.
var pidDirRe = regexp.MustCompile(`^\d+$`)

// liveMappedIds returns the set of dump-buffer ids currently mmap'd by any
// live process (agent runs with hostPID, so /proc covers the whole node),
// plus whether the scan was complete. PID dirs are enumerated with ReadDir
// rather than a glob: Glob silently drops paths it cannot traverse, which
// would under-report live mappings with no error to classify. The scan is
// incomplete when the enumeration or any maps read fails for a reason other
// than the process exiting mid-scan: a partial scan cannot prove a dump is
// unmapped, so the caller must skip dump deletion for that sweep.
func liveMappedIds() (map[string]bool, bool) {
	ids := make(map[string]bool)
	complete := true
	entries, err := os.ReadDir(procfsRoot)
	if err != nil {
		return ids, false
	}
	for _, entry := range entries {
		if !entry.IsDir() || !pidDirRe.MatchString(entry.Name()) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(procfsRoot, entry.Name(), "maps"))
		if err != nil {
			// Gone between enumerate and read is normal process churn;
			// anything else (EACCES, hidepid, ...) hides mappings from
			// the scan.
			if !os.IsNotExist(err) && !errors.Is(err, syscall.ESRCH) {
				complete = false
			}
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			idx := strings.Index(line, "huge-ckpt/")
			if idx < 0 {
				continue
			}
			base := filepath.Base(strings.Fields(line[idx:])[0])
			if m := dumpFileRe.FindStringSubmatch(base); m != nil {
				ids[m[1]] = true
			}
		}
	}
	return ids, complete
}
