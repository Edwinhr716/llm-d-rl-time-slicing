package mirror

import "fmt"

// Identity decides what a restarted guest kubelet does with the mirrors it finds
// (flag --mirror-identity). PENDING LEAD DECISION D-VK-6; readopt is today's behaviour.
type Identity string

const (
	// IdentityReadopt keeps every mirror it finds: same process, same job id. A guest
	// re-created with the same name and containers re-adopts an orphaned mirror.
	IdentityReadopt Identity = "readopt"
	// IdentityIncarnation deletes the mirrors of the previous incarnation (normal grace) and,
	// once each is gone, creates a new one with the next attempt in its job id. Orphans are
	// never re-adopted.
	IdentityIncarnation Identity = "incarnation"
)

// ParseIdentity validates a --mirror-identity value.
func ParseIdentity(s string) (Identity, error) {
	switch i := Identity(s); i {
	case IdentityReadopt, IdentityIncarnation:
		return i, nil
	default:
		return "", fmt.Errorf("--mirror-identity must be %q or %q, got %q", IdentityReadopt, IdentityIncarnation, s)
	}
}

// adopts reports whether orphaned mirrors may be re-adopted. The zero value means readopt.
func (i Identity) adopts() bool { return i != IdentityIncarnation }
