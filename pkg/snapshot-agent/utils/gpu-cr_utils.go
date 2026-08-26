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
//	ckpt-<id>.data, ckpt-<id>-host.data
//	                  dump + staging buffers, file backend — the naming
//	                  GPU-CR uses whenever EXPORT_FILE_PATH is set, i.e. in
//	                  every agent-managed deployment (MAP_SHARED
//	                  reservations stick to the FILE, so a leaked pair pins
//	                  its whole extent even after the process dies)
//	<id>, <id>-host   the same pair in hugepage mode (EXPORT_FILE_PATH
//	                  unset, hardcoded /mnt/huge-ckpt)
//	control-<pid>     legacy control channel / v3 dual-flock lock files
//	groups/<slot>/    destination dumps (GEP-0001) — the ONLY copy of each
//	                  parked slot; reaped by owner-liveness, never blind TTL
//
// Ctl dir (tmpfs, GEP-0006; same dir as data when GPU_CR_CTL_PATH unset):
//
//	control-<pid>, pid_map_<pid>, ctl-ready-<pid>
var (
	fileDumpRe = regexp.MustCompile(`^ckpt-(\d+)(-host)?\.data$`)
	hugeDumpRe = regexp.MustCompile(`^(\d+)(-host)?$`)
	pidFileRe  = regexp.MustCompile(`^(?:control-|pid_map_|ctl-ready-)(\d+)$`)
)

// isDumpFile reports whether name is a GPU-CR dump/staging buffer file, in
// either naming scheme.
func isDumpFile(name string) bool {
	return fileDumpRe.MatchString(name) || hugeDumpRe.MatchString(name)
}

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
	data, err := os.ReadFile("/proc/" + pid + "/stat")
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

	liveInodes, complete := liveMappedInodes()
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

		if isDumpFile(name) {
			key, ok := fileDevIno(filepath.Join(dir, name))
			if complete && ok && !liveInodes[key] {
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
func pidGone(pid, path string) bool {
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
	_, err := os.Stat("/proc/" + pid)
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
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(store, entry.Name())
		info, err := entry.Info()
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
				"slot", entry.Name(), "idle", now.Sub(info.ModTime()).Round(time.Minute).String())
			if fp := GroupMetaFallbackPath(dir); fp != "" {
				_ = os.Remove(fp)
			}
		}
	}
	sweepOrphanedMetaFallbacks(store)
}

// sweepOrphanedMetaFallbacks removes ctl-tmpfs owners-<slot> files whose
// slot directory no longer exists (e.g. a slot deleted by hand). Runs under
// StoreMu like the rest of the group-store sweep, so it cannot race a
// Snapshot writing metadata for a slot it just created.
func sweepOrphanedMetaFallbacks(store string) {
	ctl := os.Getenv("GPU_CR_CTL_PATH")
	if ctl == "" {
		return
	}
	entries, err := os.ReadDir(ctl)
	if err != nil {
		return
	}
	for _, e := range entries {
		slot, ok := strings.CutPrefix(e.Name(), "owners-")
		if !ok || e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(store, slot)); os.IsNotExist(err) {
			_ = os.Remove(filepath.Join(ctl, e.Name()))
		}
	}
}

// GroupMetaFallbackPath is where a slot's metadata lives when the group
// store itself rejects write(2) — hugetlbfs. The ctl tmpfs is the one
// place guaranteed writable in that layout; only this agent process reads
// and writes slot metadata, so no cross-process path contract is needed.
// Empty when GPU_CR_CTL_PATH is unset (legacy layout, no fallback).
func GroupMetaFallbackPath(slotDir string) string {
	ctl := os.Getenv("GPU_CR_CTL_PATH")
	if ctl == "" {
		return ""
	}
	return filepath.Join(ctl, "owners-"+filepath.Base(slotDir))
}

// ReadGroupMeta parses a slot's GroupMetaName file into pid -> starttime.
// A slot on a hugetlbfs store carries a 0-byte in-slot stub (creation
// succeeds, write(2) does not), so when the in-slot file yields no owners
// the ctl-tmpfs fallback location is consulted before giving up.
func ReadGroupMeta(dir string) (map[string]int64, error) {
	owners, err := parseGroupMeta(filepath.Join(dir, GroupMetaName))
	if err == nil && len(owners) > 0 {
		return owners, nil
	}
	if fp := GroupMetaFallbackPath(dir); fp != "" {
		if fbOwners, fbErr := parseGroupMeta(fp); fbErr == nil {
			return fbOwners, nil
		}
	}
	return owners, err
}

func parseGroupMeta(path string) (map[string]int64, error) {
	data, err := os.ReadFile(path)
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

// pidDirRe matches the numeric process dirs under /proc.
var pidDirRe = regexp.MustCompile(`^\d+$`)

// liveMappedInodes returns the device:inode key of every file currently
// mmap'd by any live process (agent runs with hostPID, so /proc covers the
// whole node), plus whether the scan was complete. Liveness matches by
// inode, never by pathname: a maps line renders the path in the OWNING
// process's mount namespace, so the same checkpoint file can appear under a
// path the agent has never mounted — dev:inode is namespace- and
// name-independent. PID dirs are enumerated with ReadDir rather than a
// glob: Glob silently drops paths it cannot traverse, which would
// under-report live mappings with no error to classify. The scan is
// incomplete when the enumeration or any maps read fails for a reason other
// than the process exiting mid-scan: a partial scan cannot prove a dump is
// unmapped, so the caller must skip dump deletion for that sweep.
func liveMappedInodes() (map[string]bool, bool) {
	live := make(map[string]bool)
	complete := true
	entries, err := os.ReadDir(procfsRoot)
	if err != nil {
		return live, false
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
			// address perms offset dev inode [pathname]
			fields := strings.Fields(line)
			if len(fields) < 5 || fields[4] == "0" {
				continue // anonymous or malformed: nothing to key on
			}
			if key, ok := mapsDevIno(fields[3], fields[4]); ok {
				live[key] = true
			}
		}
	}
	return live, complete
}

// mapsDevIno turns a maps-line device field (hex "major:minor") and decimal
// inode field into the key fileDevIno produces for the same file.
func mapsDevIno(dev, ino string) (string, bool) {
	majField, minField, found := strings.Cut(dev, ":")
	if !found {
		return "", false
	}
	major, err := strconv.ParseUint(majField, 16, 64)
	if err != nil {
		return "", false
	}
	minor, err := strconv.ParseUint(minField, 16, 64)
	if err != nil {
		return "", false
	}
	inode, err := strconv.ParseUint(ino, 10, 64)
	if err != nil {
		return "", false
	}
	return fmt.Sprintf("%d:%d:%d", major, minor, inode), true
}

// fileDevIno stats path into the same key space as mapsDevIno.
func fileDevIno(path string) (string, bool) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return "", false
	}
	nums := devMajMin(st.Dev)
	return fmt.Sprintf("%d:%d:%d", nums.major, nums.minor, st.Ino), true
}

// devNums is a stat dev_t split into its device-number halves.
type devNums struct {
	major, minor uint64
}

// devMajMin splits a stat dev_t with the Linux huge-dev encoding — the same
// split the kernel uses to print the maps device column.
func devMajMin(dev uint64) devNums {
	return devNums{
		major: ((dev >> 8) & 0xfff) | ((dev >> 32) &^ 0xfff),
		minor: (dev & 0xff) | ((dev >> 12) &^ 0xff),
	}
}
