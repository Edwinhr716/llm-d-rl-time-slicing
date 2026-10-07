package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	// maxLabelValue is the Kubernetes limit on a label value.
	maxLabelValue = 63
	// groupHashLen is the number of hex characters of the hash suffix of a shortened group.
	groupHashLen = 10
)

// GroupName assembles the donor group <namespace>.<job-id>.<ray group>. A value longer than 63
// characters (the label value limit) is cut to 52 characters and gets "-" plus the first 10 hex
// characters of the SHA-256 of the full value, so it stays a valid, stable and distinct label value.
func GroupName(namespace, jobID, rayGroup string) string {
	full := namespace + "." + jobID + "." + rayGroup
	if len(full) <= maxLabelValue {
		return full
	}
	sum := sha256.Sum256([]byte(full))
	prefix := full[:maxLabelValue-groupHashLen-1]
	// A label value must end with an alphanumeric character; the hash does, the cut prefix may not.
	prefix = strings.TrimRight(prefix, "-_.")
	return prefix + "-" + hex.EncodeToString(sum[:])[:groupHashLen]
}
