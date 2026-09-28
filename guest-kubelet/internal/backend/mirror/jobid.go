package mirror

import (
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

// LabelJobID is the mirror's job id: "<guest UID>-<attempt>", unique per mirror incarnation.
// The orchestrator and the snapshot-agent key their state on it.
const LabelJobID = "timeslice.io/job-id"

// JobID returns the job id of the attempt-th mirror of a guest. Attempts start at 1.
func JobID(guestUID types.UID, attempt int) string {
	return string(guestUID) + "-" + strconv.Itoa(attempt)
}

// AttemptOf returns the attempt in a job id written for this guest UID.
func AttemptOf(jobID string, guestUID types.UID) (int, bool) {
	rest, ok := strings.CutPrefix(jobID, string(guestUID)+"-")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}

// nextAttempt is the attempt for the next mirror created for this guest: one more than the
// highest attempt this process has retired for it (incarnation), else 1.
func (b *Backend) nextAttempt(guestUID types.UID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.attempts[guestUID] + 1
}
