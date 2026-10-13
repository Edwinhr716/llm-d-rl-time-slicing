package group

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// TokenPrefix starts every opaque group token.
const TokenPrefix = "gt-"

// Token is the opaque value a mirror carries in LabelGroup instead of the group name when
// --mirror-group-label=token. The group name is <owner-namespace>.<job>.<group>, and a mirror
// lives in the guest's namespace, so the plain name tells a guest-namespace reader who owns
// the GPU. The orchestrator computes the same token (pkg/timeslice-orchestrator/infrastructure
// GroupToken; the shared test vector is in both packages' tests) to find the mirrors of a
// group. It is a plain hash: it hides the name from a reader who cannot guess it, not from one
// who can enumerate candidate names.
func Token(group string) string {
	sum := sha256.Sum256([]byte("timeslice.io/group-token/v1:" + group))
	return TokenPrefix + hex.EncodeToString(sum[:])[:32]
}

// IsToken reports whether a LabelGroup value is a Token rather than a group name.
func IsToken(v string) bool {
	return strings.HasPrefix(v, TokenPrefix) && len(v) == len(TokenPrefix)+32
}
