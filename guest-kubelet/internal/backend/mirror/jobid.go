package mirror

import (
	"strconv"

	"k8s.io/apimachinery/pkg/types"
)

// LabelJobID is the mirror's job id: "<guest UID>-<attempt>". The orchestrator and the
// snapshot-agent key their state on it. A restarted guest kubelet keeps the mirrors it finds
// and their job id; a guest re-created with the same name that adopts an orphaned mirror keeps
// the orphan's job id.
const LabelJobID = "timeslice.io/job-id"

// JobID returns the job id of the attempt-th mirror of a guest. Attempts start at 1.
func JobID(guestUID types.UID, attempt int) string {
	return string(guestUID) + "-" + strconv.Itoa(attempt)
}
