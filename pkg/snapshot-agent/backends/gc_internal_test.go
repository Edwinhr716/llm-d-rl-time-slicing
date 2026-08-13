package backends

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// deadPID is a PID that will not exist on any test host (max pid is
// typically 4194304 on Linux).
const deadPID = "999999999"

func TestGCFilePatterns(t *testing.T) {
	tests := []struct {
		name      string
		file      string
		dumpMatch bool
		pidMatch  bool
	}{
		{name: "dump id", file: "123", dumpMatch: true},
		{name: "host staging", file: "123-host", dumpMatch: true},
		{name: "control channel", file: "control-4567", pidMatch: true},
		{name: "pid map", file: "pid_map_4567", pidMatch: true},
		{name: "bare control counter untouched", file: "control"},
		{name: "unrelated file", file: "somefile.txt"},
		{name: "non-numeric id", file: "abc-host"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := dumpFileRe.MatchString(tc.file); got != tc.dumpMatch {
				t.Errorf("dumpFileRe.MatchString(%q) = %v, want %v", tc.file, got, tc.dumpMatch)
			}
			if got := pidFileRe.MatchString(tc.file); got != tc.pidMatch {
				t.Errorf("pidFileRe.MatchString(%q) = %v, want %v", tc.file, got, tc.pidMatch)
			}
		})
	}
}

func TestSweep(t *testing.T) {
	ctl := t.TempDir()

	old := time.Now().Add(-time.Hour)

	writeAged := func(name string, aged bool) {
		t.Helper()
		path := filepath.Join(ctl, name)
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
		if aged {
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatalf("Chtimes(%s): %v", name, err)
			}
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
		t.Helper()
		if _, err := os.Stat(filepath.Join(ctl, name)); !os.IsNotExist(err) {
			t.Errorf("%s should have been removed (stat err: %v)", name, err)
		}
	}
	assertKept := func(name string) {
		t.Helper()
		if _, err := os.Stat(filepath.Join(ctl, name)); err != nil {
			t.Errorf("%s should have been kept: %v", name, err)
		}
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
