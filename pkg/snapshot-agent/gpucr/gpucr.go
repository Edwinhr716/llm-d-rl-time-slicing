package gpucr

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
//	groups/<slot>/<pid>-<starttime>/<id>
//	                  destination dumps — the ONLY copy of each
//	                  parked slot; owner identity is the dirname itself;
//	                  reaped by owner-liveness, never blind TTL
//
// Ctl dir (tmpfs; same dir as data when GPU_CR_CTL_PATH unset):
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

// Environment variables of the GPU-CR deployment surface, shared between
// the backends and this sweeper so a rename happens in one place.
const (
	// EnvDataDir overrides the GPU-CR data dir (dump/staging buffers and
	// the parent of the destination store).
	EnvDataDir = "EXPORT_FILE_PATH"
	// EnvGroupStore overrides the destination store location.
	EnvGroupStore = "GPU_CR_GROUP_STORE"
	// EnvCtlPath points at the control-plane tmpfs; unset means the
	// legacy layout where control files share the data dir.
	EnvCtlPath = "GPU_CR_CTL_PATH"
	// EnvGroupGraceHours overrides the store sweep grace period.
	EnvGroupGraceHours = "GPU_CR_GROUP_GRACE_HOURS"
	// EnvOpTimeoutSec bounds a single cr_client invocation.
	EnvOpTimeoutSec = "GPU_CR_OP_TIMEOUT_SEC"
)

// DataDir is where GPU-CR keeps dump/staging DATA files (hugetlbfs mount).
func DataDir() string {
	if d := os.Getenv(EnvDataDir); d != "" {
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
	if d := os.Getenv(EnvGroupStore); d != "" {
		return d
	}
	return filepath.Join(DataDir(), "groups")
}

// ownerDirRe matches a slot's per-owner subdirectory, <pid>-<starttime>:
// the dirname IS the slot's ownership metadata (path-encoded so it exists
// atomically with the dump destination and cannot fail to be written on a
// hugetlbfs store, which rejects write(2)). GC deletes a slot only when
// every owner so recorded is gone (dead, or its PID recycled to a different
// starttime) — a parked slot's dump is meaningless once its process is, and
// never expendable before that.
var ownerDirRe = regexp.MustCompile(`^(\d+)-(\d+)$`)

// /proc/<pid>/stat layout, per proc(5): fields are 1-indexed, and comm
// (field 2) may contain spaces and parens, so parsing starts after the
// LAST ')' — which leaves index 0 of the split holding field 3 (state).
const (
	statStarttimeField      = 22
	statFirstFieldAfterComm = 3
)

// ProcStarttime returns the starttime field of /proc/<pid>/stat — the
// PID-reuse guard.
func ProcStarttime(pid string) (int64, error) {
	data, err := os.ReadFile("/proc/" + pid + "/stat")
	if err != nil {
		return 0, err
	}
	idx := strings.LastIndexByte(string(data), ')')
	if idx < 0 || idx+len(") ") >= len(data) {
		return 0, fmt.Errorf("malformed /proc/%s/stat", pid)
	}
	fields := strings.Fields(string(data[idx+len(") "):]))
	starttimeIdx := statStarttimeField - statFirstFieldAfterComm
	if len(fields) <= starttimeIdx {
		return 0, fmt.Errorf("short /proc/%s/stat", pid)
	}
	return strconv.ParseInt(fields[starttimeIdx], 10, 64)
}

// StartSweeper sweeps stale GPU-CR artifacts at startup and every
// interval.
func StartSweeper(ctx context.Context, ctlDir string, interval time.Duration) {
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
	if ctl := os.Getenv(EnvCtlPath); ctl != "" && ctl != dataDir {
		sweepPidFiles(ctl, ctlMinAge)
	}

	sweepGroupStore(time.Now())
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
// /proc/<pid> exists.
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

// ownerGone reports whether a recorded slot owner (pid + starttime) is gone:
// the process exited, or its pid now belongs to a different process
// (starttime mismatch). A destination slot is the sole copy of parked state
// and its deletion is irreversible, so this demands POSITIVE evidence of
// death: when the starttime cannot be read, the owner counts as gone only if
// /proc/<pid> is confirmed absent. A malformed stat, or a permission/I/O
// error on either read, keeps the owner alive — stricter than pidGone,
// whose ctl-file deletions are recoverable.
func ownerGone(pid string, recorded int64) bool {
	cur, err := ProcStarttime(pid)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		_, serr := os.Stat("/proc/" + pid)
		return os.IsNotExist(serr)
	}
	return cur != recorded
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

// sweepGroupStore reaps destination-dump slots. These are the
// SOLE copy of parked state, so deletion requires owner death, not a TTL:
// a slot goes only when every owner recorded in its <pid>-<starttime>
// subdirectory names is dead or its PID was recycled (starttime mismatch) —
// at that point the dump is unrestorable anyway (the buffer VAs died with
// the process) — and the grace period after the last op has passed.
// A slot with no parseable owner dirs (including a pre-owner-dir legacy
// layout, where dumps sit flat in the slot) carries no license to delete
// and is kept forever.
func sweepGroupStore(now time.Time) {
	// The whole pass runs under StoreMu — the mutex the C/R path holds —
	// deferred so no code added below can leave the store locked.
	StoreMu.Lock()
	defer StoreMu.Unlock()

	grace := 1 * time.Hour
	if v := os.Getenv(EnvGroupGraceHours); v != "" {
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
		owners, ok := slotOwners(dir)
		if !ok || len(owners) == 0 {
			// No (or not only) well-formed ownership dirnames: be
			// conservative, never delete.
			continue
		}
		allGone := true
		for _, o := range owners {
			if !ownerGone(o.pid, o.starttime) {
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
		}
	}
}

// ownerRec is one parsed <pid>-<starttime> ownership record.
type ownerRec struct {
	pid       string
	starttime int64
}

// slotOwners parses a slot's ownership out of its subdirectory names
// (<pid>-<starttime>). The second return is false when any subdirectory
// fails to parse: a name this sweeper cannot account for means the slot's
// ownership is not fully known, so the caller must keep it. Non-directory
// children (dump files of a legacy flat slot) are not ownership records and
// are simply skipped.
//
// Every parsed pair is preserved as its own record — never keyed by pid:
// a slot can hold both a dead owner's directory and a recycled-pid
// successor's (same pid, different starttime), and collapsing them onto
// the pid could let the dead record's mismatch delete the live owner's
// dump.
func slotOwners(dir string) ([]ownerRec, bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false
	}
	var owners []ownerRec
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m := ownerDirRe.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, false
		}
		st, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			return nil, false
		}
		owners = append(owners, ownerRec{pid: m[1], starttime: st})
	}
	return owners, true
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
