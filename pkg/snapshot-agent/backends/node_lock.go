package backends

import (
	"context"
	"fmt"
)

// NodeLock is a one-slot lock whose Acquire honours a context. It replaces
// a mutex so that a caller waiting behind a hung holder (for example a
// cuda-checkpoint that does not return) gives up at its deadline or when
// it is cancelled, instead of waiting forever.
type NodeLock struct {
	slot chan struct{}
}

// NewNodeLock returns an unlocked NodeLock.
func NewNodeLock() *NodeLock {
	return &NodeLock{slot: make(chan struct{}, 1)}
}

// Acquire takes the lock, or returns the context's error if the context
// ends first. A context that has already ended never takes the lock.
func (l *NodeLock) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("waiting for the node lock: %w", err)
	}
	select {
	case l.slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for the node lock: %w", ctx.Err())
	}
}

// Release frees the lock. Releasing an unlocked NodeLock panics, as for
// sync.Mutex.
func (l *NodeLock) Release() {
	select {
	case <-l.slot:
	default:
		panic("backends: release of an unlocked NodeLock")
	}
}
