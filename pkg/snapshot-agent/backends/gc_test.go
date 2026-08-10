package backends

import (
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

func TestGCFilePatterns(t *testing.T) {
	type testCase struct {
		name      string
		file      string
		dumpMatch bool
		pidMatch  bool
	}

	run := func(t *testing.T, tc testCase) {
		assert.Equal(t, dumpFileRe.MatchString(tc.file), tc.dumpMatch, "dumpFileRe")
		assert.Equal(t, pidFileRe.MatchString(tc.file), tc.pidMatch, "pidFileRe")
	}

	testCases := []testCase{
		{name: "dump id", file: "123", dumpMatch: true},
		{name: "host staging", file: "123-host", dumpMatch: true},
		{name: "control channel", file: "control-4567", pidMatch: true},
		{name: "pid map", file: "pid_map_4567", pidMatch: true},
		{name: "bare control counter untouched", file: "control"},
		{name: "unrelated file", file: "somefile.txt"},
		{name: "non-numeric id", file: "abc-host"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestSweep(t *testing.T) {
	ctl := t.TempDir()
	t.Setenv("SNAPSHOT_DIR", t.TempDir()) // keep snapshot sweep away from ctl

	old := time.Now().Add(-time.Hour)

	writeAged := func(name string, aged bool) {
		path := filepath.Join(ctl, name)
		assert.NilError(t, os.WriteFile(path, []byte("x"), 0o644))
		if aged {
			assert.NilError(t, os.Chtimes(path, old, old))
		}
	}

	writeAged("123", true)              // stale dump, no live mapping -> removed
	writeAged("123-host", true)         // stale staging -> removed
	writeAged("456", false)             // fresh dump -> kept (min-age guard)
	writeAged("control-"+deadPID, true) // dead pid -> removed
	writeAged("pid_map_"+deadPID, true) // dead pid -> removed
	writeAged("control", true)          // bare counter -> kept
	writeAged("unrelated.bin", true)    // unknown file -> kept

	// The live-PID guard needs a procfs (Linux); skip that piece elsewhere.
	_, procErr := os.Stat("/proc/self")
	ownPidMap := "pid_map_" + strconv.Itoa(os.Getpid())
	if procErr == nil {
		writeAged(ownPidMap, true) // live pid -> kept
	}

	sweep(ctl)

	assertGone := func(name string) {
		_, err := os.Stat(filepath.Join(ctl, name))
		assert.Assert(t, os.IsNotExist(err), "%s should have been removed", name)
	}
	assertKept := func(name string) {
		_, err := os.Stat(filepath.Join(ctl, name))
		assert.NilError(t, err, "%s should have been kept", name)
	}

	assertGone("123")
	assertGone("123-host")
	assertGone("control-" + deadPID)
	assertGone("pid_map_" + deadPID)
	assertKept("456")
	assertKept("control")
	assertKept("unrelated.bin")
	if procErr == nil {
		assertKept(ownPidMap)
	}
}

func TestSweepSnapshotStore(t *testing.T) {
	ctl := t.TempDir()
	snap := t.TempDir()
	t.Setenv("SNAPSHOT_DIR", snap)

	oldDir := filepath.Join(snap, "expired-slot")
	freshDir := filepath.Join(snap, "fresh-slot")
	assert.NilError(t, os.MkdirAll(oldDir, 0o755))
	assert.NilError(t, os.MkdirAll(freshDir, 0o755))
	stale := time.Now().Add(-7 * time.Hour) // default TTL is 6h
	assert.NilError(t, os.Chtimes(oldDir, stale, stale))

	sweepSnapshotStore(ctl, time.Now())

	_, err := os.Stat(oldDir)
	assert.Assert(t, os.IsNotExist(err), "expired slot should be removed")
	_, err = os.Stat(freshDir)
	assert.NilError(t, err, "fresh slot should be kept")
}
