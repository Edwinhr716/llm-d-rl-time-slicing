package server

import (
	"testing"
	"time"
)

func TestForegroundWait_AsyncPoll_Tracker(t *testing.T) {
	var tracker pollTracker
	t0 := time.Unix(1000, 0)

	first, isNew := tracker.mark("g", "j", t0)
	if !isNew || !first.Equal(t0) {
		t.Fatalf("first mark = %v, %v; want %v, true", first, isNew, t0)
	}
	first, isNew = tracker.mark("g", "j", t0.Add(time.Second))
	if isNew || !first.Equal(t0) {
		t.Fatalf("second mark = %v, %v; want %v, false", first, isNew, t0)
	}
	// Another job in the same group waits on its own.
	if _, isNew := tracker.mark("g", "other", t0.Add(time.Second)); !isNew {
		t.Error("another job's first mark is not new")
	}

	// A job that stops polling for longer than pollStaleAfter starts over.
	late := t0.Add(time.Second + pollStaleAfter + time.Millisecond)
	first, isNew = tracker.mark("g", "j", late)
	if !isNew || !first.Equal(late) {
		t.Fatalf("mark after a gap = %v, %v; want %v, true", first, isNew, late)
	}

	tracker.clear("g", "j")
	next := late.Add(time.Second)
	if first, isNew := tracker.mark("g", "j", next); !isNew || !first.Equal(next) {
		t.Fatalf("mark after clear = %v, %v; want %v, true", first, isNew, next)
	}
}
