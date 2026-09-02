package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// deadPID is a PID that will not exist on any test host (max pid is
// typically 4194304 on Linux).
const deadPID = "999999999"

func hasProcfs() bool {
	_, err := os.Stat("/proc/self/stat")
	return err == nil
}

// writeAged creates a GC-candidate file, backdated past gcMinAge when aged.
func writeAged(t *testing.T, dir, name string, aged bool) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", name, err)
	}
	if aged {
		old := time.Now().Add(-time.Hour)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("Chtimes(%s): %v", name, err)
		}
	}
}

func assertGone(t *testing.T, dir, name string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
		t.Errorf("%s should have been removed (stat err: %v)", name, err)
	}
}

func assertKept(t *testing.T, dir, name string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
		t.Errorf("%s should have been kept: %v", name, err)
	}
}

func TestGCFilePatterns(t *testing.T) {
	tests := []struct {
		name      string
		file      string
		dumpMatch bool
		pidMatch  bool
	}{
		{name: "dump id", file: "123", dumpMatch: true},
		{name: "host staging", file: "123-host", dumpMatch: true},
		{name: "file-backend dump", file: "ckpt-123.data", dumpMatch: true},
		{name: "file-backend staging", file: "ckpt-123-host.data", dumpMatch: true},
		{name: "file-backend prefix without suffix", file: "ckpt-123"},
		{name: "suffix without prefix", file: "123.data"},
		{name: "non-numeric file-backend id", file: "ckpt-abc.data"},
		{name: "control channel", file: "control-4567", pidMatch: true},
		{name: "pid map", file: "pid_map_4567", pidMatch: true},
		{name: "ctl readiness advertisement", file: "ctl-ready-4567", pidMatch: true},
		{name: "bare control counter untouched", file: "control"},
		{name: "unrelated file", file: "somefile.txt"},
		{name: "non-numeric id", file: "abc-host"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDumpFile(tc.file); got != tc.dumpMatch {
				t.Errorf("isDumpFile(%q) = %v, want %v", tc.file, got, tc.dumpMatch)
			}
			if got := pidFileRe.MatchString(tc.file); got != tc.pidMatch {
				t.Errorf("pidFileRe.MatchString(%q) = %v, want %v", tc.file, got, tc.pidMatch)
			}
		})
	}
}

func TestSweep(t *testing.T) {
	ctl := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctl)  // keep group-store sweep inside the tempdir
	t.Setenv("GPU_CR_CTL_PATH", "")    // legacy layout: ctl files share the data dir
	t.Setenv("GPU_CR_GROUP_STORE", "") // default <data>/groups

	// Deterministic maps scan: a fixture procfs whose one process maps the
	// two "live" dumps. Scanning the real /proc would make the test depend
	// on host privileges (an unprivileged runner cannot read other users'
	// maps, which correctly marks the scan incomplete and skips deletion).
	// The maps entries are written after the files exist, from their real
	// device:inode, under pathnames from a foreign mount namespace — the
	// scan must match by inode, never by name.
	fakeProc := t.TempDir()
	pidDir := filepath.Join(fakeProc, "1")
	if err := os.MkdirAll(pidDir, 0o755); err != nil {
		t.Fatal(err)
	}
	oldRoot := procfsRoot
	procfsRoot = fakeProc
	defer func() { procfsRoot = oldRoot }()

	writeAged(t, ctl, "123", true)           // stale dump, no live mapping -> removed
	writeAged(t, ctl, "123-host", true)      // stale staging -> removed
	writeAged(t, ctl, "456", false)          // fresh dump -> kept (min-age guard)
	writeAged(t, ctl, "789", true)           // stale dump, live mapping -> kept
	writeAged(t, ctl, "ckpt-321.data", true) // stale file-backend dump -> removed
	writeAged(t, ctl, "ckpt-321-host.data", true)
	writeAged(t, ctl, "ckpt-790.data", true)      // stale file-backend dump, live mapping -> kept
	writeAged(t, ctl, "control-"+deadPID, true)   // dead pid -> removed
	writeAged(t, ctl, "pid_map_"+deadPID, true)   // dead pid -> removed
	writeAged(t, ctl, "ctl-ready-"+deadPID, true) // dead pid -> removed
	writeAged(t, ctl, "control", true)            // bare counter -> kept
	writeAged(t, ctl, "unrelated.bin", true)      // unknown file -> kept

	// The live-PID guard needs a procfs (Linux); skip that piece elsewhere.
	ownPidMap := "pid_map_" + strconv.Itoa(os.Getpid())
	if hasProcfs() {
		writeAged(t, ctl, ownPidMap, true) // live pid -> kept
	}

	mapsLines := mapsEntry(t, filepath.Join(ctl, "789"), "/workload/ns/buf0") +
		mapsEntry(t, filepath.Join(ctl, "ckpt-790.data"), "/workload/ns/buf1")
	if err := os.WriteFile(filepath.Join(pidDir, "maps"), []byte(mapsLines), 0o600); err != nil {
		t.Fatal(err)
	}

	sweep(ctl)

	assertGone(t, ctl, "123")
	assertGone(t, ctl, "123-host")
	assertGone(t, ctl, "ckpt-321.data")
	assertGone(t, ctl, "ckpt-321-host.data")
	assertGone(t, ctl, "control-"+deadPID)
	assertGone(t, ctl, "pid_map_"+deadPID)
	assertGone(t, ctl, "ctl-ready-"+deadPID)
	assertKept(t, ctl, "456")
	assertKept(t, ctl, "789")
	assertKept(t, ctl, "ckpt-790.data")
	assertKept(t, ctl, "control")
	assertKept(t, ctl, "unrelated.bin")
	if hasProcfs() {
		assertKept(t, ctl, ownPidMap)
	}
}

// TestSweepCtlDir covers the second sweep target: per-PID control-plane
// files on the ctl tmpfs, swept with the shorter ctlMinAge.
func TestSweepCtlDir(t *testing.T) {
	data := t.TempDir()
	ctl := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", data)
	t.Setenv("GPU_CR_CTL_PATH", ctl)
	t.Setenv("GPU_CR_GROUP_STORE", "")

	writeAged(t, ctl, "control-"+deadPID, true)   // dead pid -> removed
	writeAged(t, ctl, "pid_map_"+deadPID, true)   // dead pid -> removed
	writeAged(t, ctl, "ctl-ready-"+deadPID, true) // dead pid -> removed
	writeAged(t, ctl, "pid_map_"+deadPID+"0", false)
	writeAged(t, ctl, "unrelated.bin", true) // unknown file -> kept

	sweep(data)

	assertGone(t, ctl, "control-"+deadPID)
	assertGone(t, ctl, "pid_map_"+deadPID)
	assertGone(t, ctl, "ctl-ready-"+deadPID)
	assertKept(t, ctl, "pid_map_"+deadPID+"0") // fresh -> kept (ctlMinAge guard)
	assertKept(t, ctl, "unrelated.bin")
}

// TestPidGoneStarttime covers the recycled-PID guard: a ctl-ready file whose
// advertised starttime differs from the live process's is stale even though
// /proc/<pid> exists.
func TestPidGoneStarttime(t *testing.T) {
	if !hasProcfs() {
		t.Skip("no procfs on this host")
	}
	dir := t.TempDir()
	pid := strconv.Itoa(os.Getpid())
	st, err := ProcStarttime(pid)
	if err != nil {
		t.Fatal(err)
	}

	stale := filepath.Join(dir, "ctl-ready-"+pid)
	if err := os.WriteFile(stale, []byte(fmt.Sprintf("pid=%s starttime=%d\n", pid, st+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if !pidGone(pid, stale) {
		t.Error("mismatched starttime must mark the advertisement stale")
	}

	current := filepath.Join(dir, "ctl-ready-"+pid)
	if err := os.WriteFile(current, []byte(fmt.Sprintf("pid=%s starttime=%d\n", pid, st)), 0o600); err != nil {
		t.Fatal(err)
	}
	if pidGone(pid, current) {
		t.Error("matching starttime must keep the advertisement")
	}

	// Files without an advertised starttime fall back to plain liveness.
	plain := filepath.Join(dir, "control-"+pid)
	if err := os.WriteFile(plain, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if pidGone(pid, plain) {
		t.Error("live pid without advertisement must be kept")
	}
	if !pidGone(deadPID, filepath.Join(dir, "control-"+deadPID)) {
		t.Error("dead pid must be reported gone")
	}
}

func TestAdvertisedStarttime(t *testing.T) {
	dir := t.TempDir()

	good := filepath.Join(dir, "ctl-ready-1")
	if err := os.WriteFile(good, []byte("pid=1 starttime=42 uid=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, ok := advertisedStarttime(good)
	if !ok || st != 42 {
		t.Errorf("advertisedStarttime() = %d, %v, want 42, true", st, ok)
	}

	bad := filepath.Join(dir, "ctl-ready-2")
	if err := os.WriteFile(bad, []byte("no starttime here\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := advertisedStarttime(bad); ok {
		t.Error("file without starttime must not parse")
	}

	if _, ok := advertisedStarttime(filepath.Join(dir, "missing")); ok {
		t.Error("missing file must not parse")
	}
}

// TestSweepGroupStore covers the owner-liveness reap of destination slots:
// deletion requires every recorded owner dead (or recycled) AND the grace
// period passed; slots without metadata are never deleted.
func TestSweepGroupStore(t *testing.T) {
	data := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", data)
	t.Setenv("GPU_CR_CTL_PATH", "")
	t.Setenv("GPU_CR_GROUP_STORE", "")
	t.Setenv("GPU_CR_GROUP_GRACE_HOURS", "")
	store := filepath.Join(data, "groups")

	makeGroup := func(name, meta string, aged bool) {
		t.Helper()
		dir := filepath.Join(store, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if meta != "" {
			if err := os.WriteFile(filepath.Join(dir, GroupMetaName), []byte(meta), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if aged {
			old := time.Now().Add(-2 * time.Hour) // default grace is 1h
			if err := os.Chtimes(dir, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}

	makeGroup("dead-owner", deadPID+" 123\n", true)     // all owners gone -> removed
	makeGroup("no-meta", "", true)                      // no metadata -> NEVER removed
	makeGroup("fresh", deadPID+" 123\n", false)         // inside grace -> kept
	makeGroup("garbage-meta", "not a pid line\n", true) // unparseable = no owners -> kept
	if hasProcfs() {
		pid := strconv.Itoa(os.Getpid())
		st, err := ProcStarttime(pid)
		if err != nil {
			t.Fatal(err)
		}
		makeGroup("live-owner", fmt.Sprintf("%s %d\n", pid, st), true) // live owner -> kept
	}

	sweepGroupStore(time.Now())

	assertGone(t, store, "dead-owner")
	assertKept(t, store, "no-meta")
	assertKept(t, store, "fresh")
	assertKept(t, store, "garbage-meta")
	if hasProcfs() {
		assertKept(t, store, "live-owner")
	}
}

// TestSweepGroupStoreMetaFallback: on a hugetlbfs group store the in-slot
// .owners is an unwritable 0-byte stub and the real metadata lives on the
// ctl tmpfs. The sweeper must read owners through the fallback, delete the
// slot when they are all dead, drop the fallback file with it, and reap
// orphaned fallback files whose slot is already gone.
func TestSweepGroupStoreMetaFallback(t *testing.T) {
	data := t.TempDir()
	ctl := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", data)
	t.Setenv("GPU_CR_CTL_PATH", ctl)
	t.Setenv("GPU_CR_GROUP_STORE", "")
	t.Setenv("GPU_CR_GROUP_GRACE_HOURS", "")
	store := filepath.Join(data, "groups")

	makeSlot := func(name, fallbackMeta string, aged bool) string {
		t.Helper()
		dir := filepath.Join(store, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		// 0-byte in-slot stub, as left behind by a failed write(2).
		if err := os.WriteFile(filepath.Join(dir, GroupMetaName), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if fallbackMeta != "" {
			if err := os.WriteFile(GroupMetaFallbackPath(dir), []byte(fallbackMeta), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if aged {
			old := time.Now().Add(-2 * time.Hour)
			if err := os.Chtimes(dir, old, old); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}

	deadDir := makeSlot("dead-owner", deadPID+" 123\n", true)
	makeSlot("stub-only", "", true) // no metadata anywhere -> kept
	freshDir := makeSlot("fresh", deadPID+" 123\n", false)

	// The reader must see the fallback owners through the empty stub.
	owners, err := ReadGroupMeta(deadDir)
	if err != nil {
		t.Fatalf("ReadGroupMeta(): %v", err)
	}
	if len(owners) != 1 {
		t.Fatalf("ReadGroupMeta() = %v, want the one fallback owner", owners)
	}

	orphan := filepath.Join(ctl, "owners-long-gone")
	if err := os.WriteFile(orphan, []byte(deadPID+" 123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(ctl, "pid_map_123")
	if err := os.WriteFile(unrelated, []byte("42\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	sweepGroupStore(time.Now())

	assertGone(t, store, "dead-owner")
	assertKept(t, store, "stub-only")
	assertKept(t, store, "fresh")
	if _, err := os.Stat(GroupMetaFallbackPath(deadDir)); !os.IsNotExist(err) {
		t.Errorf("dead slot's fallback meta must go with it (stat err: %v)", err)
	}
	if _, err := os.Stat(GroupMetaFallbackPath(freshDir)); err != nil {
		t.Errorf("kept slot's fallback meta must stay: %v", err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphaned fallback meta must be reaped (stat err: %v)", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Errorf("non-owners ctl files are not this sweep's business: %v", err)
	}
}

// TestSweepIncompleteProcScan: when a maps file is unreadable for a reason
// other than process exit, the scan cannot prove any dump is unmapped, so
// dump deletion is skipped; per-PID files are still reaped (their liveness
// check does not depend on the maps scan).
func TestSweepIncompleteProcScan(t *testing.T) {
	ctl := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctl)
	t.Setenv("GPU_CR_CTL_PATH", "")
	t.Setenv("GPU_CR_GROUP_STORE", "")

	fakeProc := t.TempDir()
	pidDir := filepath.Join(fakeProc, "42")
	if err := os.MkdirAll(pidDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A self-referencing symlink makes ReadFile fail with ELOOP — neither
	// IsNotExist nor ESRCH, so the scan must report incomplete.
	loop := filepath.Join(pidDir, "maps")
	if err := os.Symlink(loop, loop); err != nil {
		t.Skipf("cannot create symlink loop: %v", err)
	}
	oldRoot := procfsRoot
	procfsRoot = fakeProc
	defer func() { procfsRoot = oldRoot }()

	if _, complete := liveMappedInodes(); complete {
		t.Fatal("liveMappedInodes() reported a complete scan despite an unreadable maps file")
	}

	writeAged(t, ctl, "123", true)              // stale dump, but scan incomplete -> kept
	writeAged(t, ctl, "ckpt-123.data", true)    // file-backend dump, same protection -> kept
	writeAged(t, ctl, "control-"+deadPID, true) // dead pid -> still removed

	sweep(ctl)

	assertKept(t, ctl, "123")
	assertKept(t, ctl, "ckpt-123.data")
	assertGone(t, ctl, "control-"+deadPID)
}

// TestLiveMappedInodesExitedProcess: a numeric dir with no maps file is a
// process that exited between enumeration and read — benign churn, the
// scan stays complete.
func TestLiveMappedInodesExitedProcess(t *testing.T) {
	fakeProc := t.TempDir()
	if err := os.MkdirAll(filepath.Join(fakeProc, "7"), 0o755); err != nil {
		t.Fatal(err)
	}
	oldRoot := procfsRoot
	procfsRoot = fakeProc
	defer func() { procfsRoot = oldRoot }()

	if _, complete := liveMappedInodes(); !complete {
		t.Error("missing maps file (exited process) must not mark the scan incomplete")
	}
}

// mapsEntry renders a /proc/<pid>/maps line for path from its real
// device:inode, under a pathname the agent has never mounted.
func mapsEntry(t *testing.T, path, nsPath string) string {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	nums := devMajMin(st.Dev)
	return fmt.Sprintf("7f0000000000-7f0000200000 rw-s 00000000 %02x:%02x %d %s\n",
		nums.major, nums.minor, st.Ino, nsPath)
}
