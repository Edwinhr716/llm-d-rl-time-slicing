package hostcmd

import (
	"testing"
	"time"
)

func TestNS4_Push_EpochsStrictlyIncrease(t *testing.T) {
	var src epochSource
	last := src.Next()
	for range 1000 {
		next := src.Next()
		if next <= last {
			t.Fatalf("epoch %d after %d, want strictly increasing", next, last)
		}
		last = next
	}

	// An epoch observed from a host (for example set by a process whose
	// clock ran ahead) is passed.
	ahead := time.Now().Add(time.Hour).UnixNano()
	src.Observe(ahead)
	if next := src.Next(); next <= ahead {
		t.Fatalf("epoch %d after observing %d, want greater", next, ahead)
	}

	// A restarted process starts from the clock, above the old epochs.
	var restarted epochSource
	if first := restarted.Next(); first <= time.Now().Add(-time.Second).UnixNano() {
		t.Fatalf("first epoch %d is not from the clock", first)
	}
}
