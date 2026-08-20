package utils

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// GPU-CR leaves per-process artifacts behind; nothing deletes them on
// process exit, so the agent sweeps them.
//
// Data dir (hugetlbfs mount):
//
//	<id>, <id>-host   dump + staging buffers (MAP_SHARED reservations stick
//	                  to the FILE, so a leaked pair pins hugepages even
//	                  after the process dies)
//	control-<pid>     legacy control channel / v3 dual-flock lock files
//	groups/<slot>/    destination dumps (GEP-0001) — the ONLY copy of each
//	                  parked slot; reaped by owner-liveness, never blind TTL
//
// Ctl dir (tmpfs, GEP-0006; same dir as data when GPU_CR_CTL_PATH unset):
//
//	control-<pid>, pid_map_<pid>, ctl-ready-<pid>
var (
	dumpFileRe = regexp.MustCompile(`^(\d+)(-host)?$`)
	pidFileRe  = regexp.MustCompile(`^(?:control-|pid_map_|ctl-ready-)(\d+)$`)
)

// StoreMu serializes group-store mutation between backend ops and GC, so a
// sweep can never unlink a destination file mid-checkpoint/restore.
var StoreMu sync.Mutex

// gcMinAge guards against racing a process that created its files but hasn't
// mmap'd them yet (files appear before the mapping does).
const gcMinAge = 5 * time.Minute

// ctlMinAge can be shorter: ctl files are written completely by the ELF
// constructor / init_CR, and every LD_PRELOADed descendant (dataloader
// workers, launchers) leaves a set, so the small ctl tmpfs earns a faster
// sweep.
const ctlMinAge = time.Minute

// DataDir is where GPU-CR keeps dump/staging DATA files (hugetlbfs mount).
func DataDir() string {
	if d := os.Getenv("EXPORT_FILE_PATH"); d != "" {
		return d
	}
	return "/mnt/huge-ckpt"
}

// GroupStoreDir is where per-slot destination dumps live. Defaults to
// <DataDir>/groups: on the hugepage mount the parked bytes consume the pool
// the workload pod already requested, and the path string resolves
// identically in the agent and workload mount namespaces (both mount the
// same hostPath at the same in-container path).
func GroupStoreDir() string {
	if d := os.Getenv("GPU_CR_GROUP_STORE"); d != "" {
		return d
	}
	return filepath.Join(DataDir(), "groups")
}

// SnapshotStoreDir returns where LEGACY snapshot slot copies were kept
// (pre-GEP-0001 agent-side copies). Only GC still looks here, to reap
// leftovers from older agents.
func SnapshotStoreDir(ctlDir string) string {
	if d := os.Getenv("SNAPSHOT_DIR"); d != "" {
		return d
	}
	return filepath.Join(ctlDir, "snapshots")
}

// GroupMetaName is the per-slot metadata file recording the owning workload
// processes of a slot: "pid starttime" per line. GC deletes a slot only when
// every recorded owner is gone (dead, or its PID recycled to a different
// starttime) — a parked slot's dump is meaningless once its process is, and
// never expendable before that.
const GroupMetaName = ".owners"

// ProcStarttime returns field 22 of /proc/<pid>/stat — the PID-reuse guard.
func ProcStarttime(pid string) (int64, error) {
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

func sweep(dataDir string) {
	sweepDataDir(dataDir)
	if ctl := os.Getenv("GPU_CR_CTL_PATH"); ctl != "" && ctl != dataDir {
		sweepPidFiles(ctl, ctlMinAge)
	}

	StoreMu.Lock()
	sweepGroupStore(time.Now())
	StoreMu.Unlock()

	sweepLegacySnapshotStore(dataDir, time.Now())
}

func sweepDataDir(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn("GC: cannot read checkpoint dir", "dir", dir, "err", err)
		return
	}

	liveIds, complete := liveMappedIds()
	if !complete {
		slog.Warn("GC: procfs scan incomplete; keeping all dump files this sweep", "dir", dir)
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
	cur, err := ProcStarttime(pid)
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

// sweepGroupStore reaps destination-dump slots (GEP-0001). These are the
// SOLE copy of parked state, so deletion requires owner death, not a TTL:
// a slot goes only when every owner recorded in .owners is dead or its PID
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
	store := GroupStoreDir()
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
		owners, err := ReadGroupMeta(dir)
		if err != nil || len(owners) == 0 {
			// No metadata: be conservative, never delete.
			continue
		}
		allGone := true
		for pid, st := range owners {
			cur, err := ProcStarttime(pid)
			if err == nil && cur == st {
				allGone = false
				break
			}
		}
		if !allGone {
			continue
		}
		if err := os.RemoveAll(dir); err == nil {
			slog.Info("GC: removed orphaned destination slot (all owners dead)",
				"slot", e.Name(), "idle", now.Sub(info.ModTime()).Round(time.Minute).String())
		}
	}
}

// ReadGroupMeta parses a slot's GroupMetaName file into pid -> starttime.
func ReadGroupMeta(dir string) (map[string]int64, error) {
	data, err := os.ReadFile(filepath.Join(dir, GroupMetaName))
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
	snapDir := SnapshotStoreDir(ctlDir)
	if snapDir == GroupStoreDir() {
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
			slog.Info("GC: removed expired legacy snapshot copies",
				"slot", e.Name(), "age", now.Sub(info.ModTime()).Round(time.Minute).String())
		}
	}
}

// procfsRoot is the procfs mount scanned for live mappings; a var so tests
// can point the scan at a fixture tree.
var procfsRoot = "/proc"

// liveMappedIds returns the set of dump-buffer ids currently mmap'd by any
// live process (agent runs with hostPID, so /proc covers the whole node).
// complete is false when any maps file was unreadable for a reason other
// than its process exiting mid-scan: a partial scan cannot prove a dump is
// unmapped, so the caller must skip dump deletion for that sweep.
func liveMappedIds() (ids map[string]bool, complete bool) {
	ids = make(map[string]bool)
	complete = true
	procs, err := filepath.Glob(filepath.Join(procfsRoot, "[0-9]*", "maps"))
	if err != nil {
		return ids, false
	}
	for _, mapsPath := range procs {
		data, err := os.ReadFile(mapsPath)
		if err != nil {
			// Gone between glob and read is normal process churn; anything
			// else (EACCES, hidepid, ...) hides mappings from the scan.
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
