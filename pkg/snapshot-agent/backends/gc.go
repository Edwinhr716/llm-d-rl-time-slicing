package backends

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GPU-CR leaves per-process artifacts behind; nothing deletes them on
// process exit, so the agent sweeps them.
//
// Data dir (hugetlbfs mount):
//   <id>, <id>-host   dump + staging buffers (MAP_SHARED reservations stick
//                     to the FILE, so a leaked pair pins hugepages even
//                     after the process dies)
//   control-<pid>     legacy control channel / v3 dual-flock lock files
//   groups/<group>/   destination dumps (GEP-0001) — the ONLY copy of each
//                     parked group; reaped by owner-liveness, never blind TTL
// Ctl dir (tmpfs, GEP-0006; same dir as data when GPU_CR_CTL_PATH unset):
//   control-<pid>, pid_map_<pid>, ctl-ready-<pid>

var (
	dumpFileRe = regexp.MustCompile(`^(\d+)(-host)?$`)
	pidFileRe  = regexp.MustCompile(`^(?:control-|pid_map_|ctl-ready-)(\d+)$`)
)

// storeMu serializes group-store mutation between backend ops and GC, so a
// sweep can never unlink a destination file mid-checkpoint/restore.
var storeMu sync.Mutex

// minAge guards against racing a process that created its files but hasn't
// mmap'd them yet (files appear before the mapping does).
const gcMinAge = 5 * time.Minute

// ctlMinAge can be shorter: ctl files are written completely by the ELF
// constructor / init_CR, and every LD_PRELOADed descendant (dataloader
// workers, launchers) leaves a set, so the small ctl tmpfs earns a faster
// sweep.
const ctlMinAge = time.Minute

// StartGC sweeps stale GPU-CR artifacts at startup and every interval.
func StartGC(ctx context.Context, ctlDir string, interval time.Duration) {
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

func sweep(dataDir string) {
	sweepDataDir(dataDir)
	if ctl := os.Getenv("GPU_CR_CTL_PATH"); ctl != "" && ctl != dataDir {
		sweepPidFiles(ctl, ctlMinAge)
	}

	storeMu.Lock()
	sweepGroupStore(time.Now())
	storeMu.Unlock()

	sweepLegacySnapshotStore(dataDir, time.Now())
}

func sweepDataDir(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn("GC: cannot read checkpoint dir", "dir", dir, "err", err)
		return
	}

	liveIds := liveMappedIds()
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
			if !liveIds[m[1]] {
				if os.Remove(filepath.Join(dir, name)) == nil {
					removed = append(removed, name)
				}
			}
			continue
		}
		if m := pidFileRe.FindStringSubmatch(name); m != nil {
			if pidGone(m[1], filepath.Join(dir, name)) {
				if os.Remove(filepath.Join(dir, name)) == nil {
					removed = append(removed, name)
				}
			}
		}
		// Never touch the bare "control" id-counter file or anything else.
	}
	if len(removed) > 0 {
		slog.Info("GC: removed stale GPU-CR artifacts", "dir", dir, "count", len(removed), "files", removed)
	}
}

// sweepPidFiles reaps per-PID control-plane files in the ctl dir.
func sweepPidFiles(dir string, minAge time.Duration) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	now := time.Now()
	var removed []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		m := pidFileRe.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		info, err := e.Info()
		if err != nil || now.Sub(info.ModTime()) < minAge {
			continue
		}
		if pidGone(m[1], filepath.Join(dir, name)) {
			if os.Remove(filepath.Join(dir, name)) == nil {
				removed = append(removed, name)
			}
		}
	}
	if len(removed) > 0 {
		slog.Info("GC: removed stale ctl files", "dir", dir, "count", len(removed), "files", removed)
	}
}

// pidGone reports whether the PID a per-process file belongs to is gone.
// For ctl-ready files the advertised starttime is compared too: a recycled
// PID (alive, different starttime) makes the file stale even though
// /proc/<pid> exists (GEP-0006 F3).
func pidGone(pid string, path string) bool {
	cur, err := procStarttime(pid)
	if err != nil {
		return os.IsNotExist(err) || !procExists(pid)
	}
	if strings.HasPrefix(filepath.Base(path), "ctl-ready-") {
		adv, ok := advertisedStarttime(path)
		if ok && adv != cur {
			return true
		}
	}
	return false
}

func procExists(pid string) bool {
	_, err := os.Stat(filepath.Join("/proc", pid))
	return err == nil
}

func advertisedStarttime(path string) (int64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	line, err := bufio.NewReader(f).ReadString('\n')
	if err != nil && line == "" {
		return 0, false
	}
	for _, field := range strings.Fields(line) {
		if v, ok := strings.CutPrefix(field, "starttime="); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			return n, err == nil
		}
	}
	return 0, false
}

// sweepGroupStore reaps destination-dump groups (GEP-0001). These are the
// SOLE copy of parked state, so deletion requires owner death, not a TTL:
// a group goes only when every owner recorded in .owners is dead or its PID
// was recycled (starttime mismatch) — at that point the dump is
// unrestorable anyway (the buffer VAs died with the process) — and the
// grace period after the last op has passed.
func sweepGroupStore(now time.Time) {
	grace := 1 * time.Hour
	if v := os.Getenv("GPU_CR_GROUP_GRACE_HOURS"); v != "" {
		if n, err := time.ParseDuration(v + "h"); err == nil && n > 0 {
			grace = n
		}
	}
	store := groupStoreDir()
	entries, err := os.ReadDir(store)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(store, e.Name())
		info, err := e.Info()
		if err != nil || now.Sub(info.ModTime()) < grace {
			continue
		}
		owners, err := readGroupMeta(dir)
		if err != nil || len(owners) == 0 {
			// No metadata: be conservative, never delete.
			continue
		}
		allGone := true
		for pid, st := range owners {
			cur, err := procStarttime(pid)
			if err == nil && cur == st {
				allGone = false
				break
			}
		}
		if !allGone {
			continue
		}
		if err := os.RemoveAll(dir); err == nil {
			slog.Info("GC: removed orphaned destination group (all owners dead)",
				"group", e.Name(), "idle", now.Sub(info.ModTime()).Round(time.Minute).String())
		}
	}
}

func readGroupMeta(dir string) (map[string]int64, error) {
	data, err := os.ReadFile(filepath.Join(dir, groupMetaName))
	if err != nil {
		return nil, err
	}
	owners := make(map[string]int64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		st, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		owners[fields[0]] = st
	}
	return owners, nil
}

// sweepLegacySnapshotStore drops pre-GEP-0001 snapshot COPY dirs untouched
// for longer than SNAPSHOT_TTL_HOURS (default 6). Those were copies — the
// authoritative bytes lived in the dump buffer — so a TTL is safe there.
func sweepLegacySnapshotStore(ctlDir string, now time.Time) {
	ttl := 6 * time.Hour
	if v := os.Getenv("SNAPSHOT_TTL_HOURS"); v != "" {
		if n, err := time.ParseDuration(v + "h"); err == nil && n > 0 {
			ttl = n
		}
	}
	snapDir := snapshotStoreDir(ctlDir)
	if snapDir == groupStoreDir() {
		return // misconfiguration guard: never TTL the destination store
	}
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || now.Sub(info.ModTime()) < ttl {
			continue
		}
		if err := os.RemoveAll(filepath.Join(snapDir, e.Name())); err == nil {
			slog.Info("GC: removed expired legacy snapshot copies", "group", e.Name(), "age", now.Sub(info.ModTime()).Round(time.Minute).String())
		}
	}
}

// liveMappedIds returns the set of dump-buffer ids currently mmap'd by any
// live process (agent runs with hostPID, so /proc covers the whole node).
func liveMappedIds() map[string]bool {
	ids := make(map[string]bool)
	procs, err := filepath.Glob("/proc/[0-9]*/maps")
	if err != nil {
		return ids
	}
	for _, mapsPath := range procs {
		data, err := os.ReadFile(mapsPath)
		if err != nil {
			continue // process exited or unreadable; skip
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
	return ids
}
