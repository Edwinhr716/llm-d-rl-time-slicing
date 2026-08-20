package statemachine

import (
	"sync"
)

// Exported for testing purposes only.
type (
	InternalJob       = Job
	InternalOperation = Operation
)

func (sm *StateManager) InternalJobs() map[string]*Job {
	return sm.jobs
}

func (sm *StateManager) InternalOperations() map[string]*Operation {
	return sm.operations
}

func (sm *StateManager) InternalMu() *sync.RWMutex {
	return &sm.mu
}

func (sm *StateManager) InternalGetOrCreateJob(jobID, group string) *Job {
	return sm.getOrCreateJob(jobID, group)
}

// InternalJobSlot reads the job's loaded slot under the locks the async
// operation goroutines write it under.
func (sm *StateManager) InternalJobSlot(jobID string) string {
	sm.mu.RLock()
	job := sm.jobs[jobID]
	sm.mu.RUnlock()
	if job == nil {
		return ""
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.Slot
}
