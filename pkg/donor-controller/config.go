package donorcontroller

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/labels"
)

// Defaults for the flags in cmd/donor-controller.
const (
	DefaultDonorSelector     = "timeslice.io/group,timeslice.io/role!=background"
	DefaultGroupFilter       = ".*"
	DefaultEraTTL            = 15 * time.Minute
	DefaultVKDeregisterGrace = 60 * time.Second
	DefaultLockPollInterval  = 2 * time.Second
	DefaultTick              = time.Second
	DefaultLockRPCTimeout    = time.Second
	// releaseRetry is how long H11 waits before retrying one virtual Node.
	releaseRetry = 5 * time.Second
)

// Config carries the controller's flag values.
type Config struct {
	// LabelKeys is the node label shape: LabelKeysPrefix or LabelKeysNS.
	LabelKeys string
	// EraTTL is how long a labelled node must have no donor pods and its group no lock activity
	// before its labels are removed. Must be > 0.
	EraTTL time.Duration
	// DonorSelector selects donor pods (label selector syntax). The group comes from the pod's
	// timeslice.io/group label.
	DonorSelector string
	// GroupFilter is a regular expression on the group value. Donor pods and owned nodes of other
	// groups are ignored.
	GroupFilter string
	// VKDeregisterGrace bounds how long era end waits for the host's virtual Node to go away
	// before the labels are removed anyway.
	VKDeregisterGrace time.Duration
	// WatchNamespaces limits the pod watch. Empty means all namespaces.
	WatchNamespaces []string
	// ReleaseDeadHosts turns on H11: release a virtual Node whose host Node is gone or recreated.
	ReleaseDeadHosts bool
	// LockPollInterval is how often GetGroupStatus is polled per labelled group (by the clock).
	LockPollInterval time.Duration
	// LockRPCTimeout bounds one GetGroupStatus call.
	LockRPCTimeout time.Duration
	// Tick is the real-time interval of the reconcile loop. Every decision reads the clock.
	Tick time.Duration
}

// compiled holds the parsed forms of a Config.
type compiled struct {
	selector labels.Selector
	filter   *regexp.Regexp
}

// withDefaults fills zero durations and strings with their defaults.
func (c *Config) withDefaults() Config {
	out := *c
	out.fill()
	return out
}

func (c *Config) fill() {
	if c.DonorSelector == "" {
		c.DonorSelector = DefaultDonorSelector
	}
	if c.GroupFilter == "" {
		c.GroupFilter = DefaultGroupFilter
	}
	if c.VKDeregisterGrace < 0 {
		c.VKDeregisterGrace = 0
	}
	if c.LockPollInterval <= 0 {
		c.LockPollInterval = DefaultLockPollInterval
	}
	if c.LockRPCTimeout <= 0 {
		c.LockRPCTimeout = DefaultLockRPCTimeout
	}
	if c.Tick <= 0 {
		c.Tick = DefaultTick
	}
}

// compile validates the Config and parses its selector and filter.
func (c *Config) compile() (compiled, error) {
	if err := ValidateLabelKeys(c.LabelKeys); err != nil {
		return compiled{}, err
	}
	if c.EraTTL <= 0 {
		return compiled{}, fmt.Errorf("--era-ttl must be > 0, got %s", c.EraTTL)
	}
	sel, err := labels.Parse(c.DonorSelector)
	if err != nil {
		return compiled{}, fmt.Errorf("--donor-selector: %w", err)
	}
	re, err := regexp.Compile(c.GroupFilter)
	if err != nil {
		return compiled{}, fmt.Errorf("--group-filter: %w", err)
	}
	return compiled{selector: sel, filter: re}, nil
}

// SplitNamespaces parses a comma-separated --watch-namespaces value.
func SplitNamespaces(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, ns := range parts {
		if ns = strings.TrimSpace(ns); ns != "" {
			out = append(out, ns)
		}
	}
	return out
}

// placeholder matches an unrendered manifest placeholder such as __ARGS__.
var placeholder = regexp.MustCompile(`^__[A-Z0-9_]+__$`)

// IsPlaceholder reports whether v is an unrendered manifest placeholder.
func IsPlaceholder(v string) bool { return placeholder.MatchString(v) }

// ArgsFromEnv splits the value of the args environment variable into flags. The manifest puts
// the harness's space-separated flag string there. An unrendered placeholder yields no flags.
func ArgsFromEnv(v string) []string {
	fields := strings.Fields(v)
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if !IsPlaceholder(f) {
			out = append(out, f)
		}
	}
	return out
}
