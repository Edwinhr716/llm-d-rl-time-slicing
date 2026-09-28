package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// GroupLabelPrefix marks a real node as a member of a time-slice group:
// group.timeslice.io/<group>=true. The virtual node never carries it.
const GroupLabelPrefix = "group.timeslice.io/"

// ErrNoGroup means the real node's group cannot be told: no group label, or more than one.
// The loop then starts no mirrors (fail closed).
var ErrNoGroup = errors.New("real node has no single time-slice group")

// GroupSource returns the group of the real node. Decision O3 picks where it comes from; the
// default is the real node's group label (--group-source=node-label).
type GroupSource func(ctx context.Context) (string, error)

// GroupFromLabels returns the one group named by group.timeslice.io/<group>=true labels.
func GroupFromLabels(nodeLabels map[string]string) (string, error) {
	var groups []string
	for k, v := range nodeLabels {
		if name, ok := strings.CutPrefix(k, GroupLabelPrefix); ok && name != "" && v == "true" {
			groups = append(groups, name)
		}
	}
	switch len(groups) {
	case 1:
		return groups[0], nil
	case 0:
		return "", fmt.Errorf("%w: no %s<group>=true label", ErrNoGroup, GroupLabelPrefix)
	default:
		sort.Strings(groups)
		return "", fmt.Errorf("%w: several group labels %v", ErrNoGroup, groups)
	}
}

// NodeLabelGroup is the default GroupSource: it reads the real node's labels.
func NodeLabelGroup(client kubernetes.Interface, node string) GroupSource {
	return func(ctx context.Context) (string, error) {
		n, err := client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("get real node %s: %w", node, err)
		}
		return GroupFromLabels(n.Labels)
	}
}
