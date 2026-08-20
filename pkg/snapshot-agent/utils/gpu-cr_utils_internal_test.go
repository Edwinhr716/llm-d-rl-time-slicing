package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

// deadPID is a PID that will not exist on any test host (max pid is
// typically 4194304 on Linux).
const deadPID = "999999999"

func hasProcfs() bool {
	_, err := os.Stat("/proc/self/stat")
	return err == nil
}

func TestGCFilePatterns(t *testing.T) {
	type testCase struct {
		name      string
		file      string
		dumpMatch bool
		pidMatch  bool
	}

	run := func(t *testing.T, tc testCase) {
		t.Helper()
		assert.Equal(t, dumpFileRe.MatchString(tc.file), tc.dumpMatch, "dumpFileRe")
		assert.Equal(t, pidFileRe.MatchString(tc.file), tc.pidMatch, "pidFileRe")
	}

	testCases := []testCase{
		{name: "dump id", file: "123", dumpMatch: true},
		{name: "host staging", file: "123-host", dumpMatch: true},
		{name: "control channel", file: "control-4567", pidMatch: true},
		{name: "pid map", file: "pid_map_4567", pidMatch: true},
		{name: "ctl readiness advertisement", file: "ctl-ready-4567", pidMatch: true},
		{name: "bare control counter untouched", file: "control"},
		{name: "unrelated file", file: "somefile.txt"},
		{name: "non-numeric id", file: "abc-host"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func writeAged(t *testing.T, dir, name string, aged bool) {
	t.Helper()
	path := filepath.Join(dir, name)
	assert.NilError(t, os.WriteFile(path, []byte("x"), 0o600))
	if aged {
		old := time.Now().Add(-time.Hour)
		assert.NilError(t, os.Chtimes(path, old, old))
	}
}

func assertGone(t *testing.T, dir, name string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	assert.Assert(t, os.IsNotExist(err), "%s should have been removed", name)
}

func assertKept(t *testing.T, dir, name string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	assert.NilError(t, err, "%s should have been kept", name)
}

func TestSweep(t *testing.T) {
	ctl := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctl)     // keep group-store sweep inside the tempdir
	t.Setenv("GPU_CR_CTL_PATH", "")       // legacy layout: ctl files share the data dir
	t.Setenv("GPU_CR_GROUP_STORE", "")    // default <data>/groups
	t.Setenv("SNAPSHOT_DIR", t.TempDir()) // keep legacy snapshot sweep away from ctl

	writeAged(t, ctl, "123", true)                // stale dump, no live mapping -> removed
	writeAged(t, ctl, "123-host", true)           // stale staging -> removed
	writeAged(t, ctl, "456", false)               // fresh dump -> kept (min-age guard)
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

	sweep(ctl)

	assertGone(t, ctl, "123")
	assertGone(t, ctl, "123-host")
	assertGone(t, ctl, "control-"+deadPID)
	assertGone(t, ctl, "pid_map_"+deadPID)
	assertGone(t, ctl, "ctl-ready-"+deadPID)
	assertKept(t, ctl, "456")
	assertKept(t, ctl, "control")
	assertKept(t, ctl, "unrelated.bin")
	if hasProcfs() {
		assertKept(t, ctl, ownPidMap)
	}
}

// TestSweepCtlDir covers the GEP-0006 second sweep target: per-PID control
// plane files on the ctl tmpfs, swept with the shorter ctlMinAge.
func TestSweepCtlDir(t *testing.T) {
	data := t.TempDir()
	ctl := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", data)
	t.Setenv("GPU_CR_CTL_PATH", ctl)
	t.Setenv("GPU_CR_GROUP_STORE", "")
	t.Setenv("SNAPSHOT_DIR", t.TempDir())

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
	assert.NilError(t, err)

	stale := filepath.Join(dir, "ctl-ready-"+pid)
	assert.NilError(t, os.WriteFile(stale, []byte(fmt.Sprintf("pid=%s starttime=%d\n", pid, st+1)), 0o600))
	assert.Assert(t, pidGone(pid, stale), "mismatched starttime must mark the advertisement stale")

	current := filepath.Join(dir, "ctl-ready-"+pid)
	assert.NilError(t, os.WriteFile(current, []byte(fmt.Sprintf("pid=%s starttime=%d\n", pid, st)), 0o600))
	assert.Assert(t, !pidGone(pid, current), "matching starttime must keep the advertisement")

	// Files without an advertised starttime fall back to plain liveness.
	plain := filepath.Join(dir, "control-"+pid)
	assert.NilError(t, os.WriteFile(plain, []byte("x"), 0o600))
	assert.Assert(t, !pidGone(pid, plain))
	assert.Assert(t, pidGone(deadPID, filepath.Join(dir, "control-"+deadPID)))
}

func TestAdvertisedStarttime(t *testing.T) {
	dir := t.TempDir()

	good := filepath.Join(dir, "ctl-ready-1")
	assert.NilError(t, os.WriteFile(good, []byte("pid=1 starttime=42 uid=0\n"), 0o600))
	st, ok := advertisedStarttime(good)
	assert.Assert(t, ok)
	assert.Equal(t, st, int64(42))

	bad := filepath.Join(dir, "ctl-ready-2")
	assert.NilError(t, os.WriteFile(bad, []byte("no starttime here\n"), 0o600))
	_, ok = advertisedStarttime(bad)
	assert.Assert(t, !ok)

	_, ok = advertisedStarttime(filepath.Join(dir, "missing"))
	assert.Assert(t, !ok)
}

// TestSweepGroupStore covers the owner-liveness reap of destination slots:
// deletion requires every recorded owner dead (or recycled) AND the grace
// period passed; slots without metadata are never deleted.
func TestSweepGroupStore(t *testing.T) {
	data := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", data)
	t.Setenv("GPU_CR_GROUP_STORE", "")
	t.Setenv("GPU_CR_GROUP_GRACE_HOURS", "")
	store := filepath.Join(data, "groups")

	makeGroup := func(name, meta string, aged bool) {
		t.Helper()
		dir := filepath.Join(store, name)
		assert.NilError(t, os.MkdirAll(dir, 0o755))
		if meta != "" {
			assert.NilError(t, os.WriteFile(filepath.Join(dir, GroupMetaName), []byte(meta), 0o600))
		}
		if aged {
			old := time.Now().Add(-2 * time.Hour) // default grace is 1h
			assert.NilError(t, os.Chtimes(dir, old, old))
		}
	}

	makeGroup("dead-owner", deadPID+" 123\n", true)     // all owners gone -> removed
	makeGroup("no-meta", "", true)                      // no metadata -> NEVER removed
	makeGroup("fresh", deadPID+" 123\n", false)         // inside grace -> kept
	makeGroup("garbage-meta", "not a pid line\n", true) // unparseable = no owners -> kept
	if hasProcfs() {
		pid := strconv.Itoa(os.Getpid())
		st, err := ProcStarttime(pid)
		assert.NilError(t, err)
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

func TestSweepLegacySnapshotStore(t *testing.T) {
	ctl := t.TempDir()
	snap := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctl)
	t.Setenv("GPU_CR_GROUP_STORE", "")
	t.Setenv("SNAPSHOT_DIR", snap)

	oldDir := filepath.Join(snap, "expired-slot")
	freshDir := filepath.Join(snap, "fresh-slot")
	assert.NilError(t, os.MkdirAll(oldDir, 0o755))
	assert.NilError(t, os.MkdirAll(freshDir, 0o755))
	stale := time.Now().Add(-7 * time.Hour) // default TTL is 6h
	assert.NilError(t, os.Chtimes(oldDir, stale, stale))

	sweepLegacySnapshotStore(ctl, time.Now())

	_, err := os.Stat(oldDir)
	assert.Assert(t, os.IsNotExist(err), "expired slot should be removed")
	_, err = os.Stat(freshDir)
	assert.NilError(t, err, "fresh slot should be kept")
}

// TestSweepLegacySnapshotStoreMisconfigGuard: if SNAPSHOT_DIR is pointed at
// the destination group store, the TTL sweep must refuse to run — those
// files are the sole copy of parked state.
func TestSweepLegacySnapshotStoreMisconfigGuard(t *testing.T) {
	data := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", data)
	t.Setenv("GPU_CR_GROUP_STORE", "")
	store := filepath.Join(data, "groups")
	t.Setenv("SNAPSHOT_DIR", store)

	dir := filepath.Join(store, "parked-group")
	assert.NilError(t, os.MkdirAll(dir, 0o755))
	stale := time.Now().Add(-100 * time.Hour)
	assert.NilError(t, os.Chtimes(dir, stale, stale))

	sweepLegacySnapshotStore(data, time.Now())

	assertKept(t, store, "parked-group")
}
