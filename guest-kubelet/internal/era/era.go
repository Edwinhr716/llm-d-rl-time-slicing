// Package era ends the guest kubelet's era (decision D-NS-13, option ns-era).
//
// An era is the time the host lends its GPU to guests for one RL job. It ends when, for
// --era-ttl, the host has had no donor pods and the group's foreground lock no activity
// (trigger ttl), or at once when the host loses its group label, meaning the donor controller
// ended it (trigger label). At the end the VK stops admitting guests, expires the guests it
// runs (mirror deleted with normal grace, guest Failed with reason EraEnded), hands back the
// background grant, stops its node controller and deregisters its virtual Node. It then stays
// up and idle, watching its host; a new donor pod starts a fresh era with a new Node.
//
// --era-ttl=0 turns all of this off (today's behaviour): the VK keeps its Node and guests until
// someone deletes the VK.
package era

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

// Triggers (--era-trigger).
const (
	TriggerTTL    = "ttl"    // only the TTL ends the era
	TriggerLabel  = "label"  // only the host losing its group label ends the era
	TriggerEither = "either" // whichever comes first
)

// States, logged as msg="era" state=<...>.
const (
	StateActive   = "active"   // donor pods on the host, or the lock held
	StateCounting = "counting" // no donor pods and no lock activity: the TTL runs
	StateEnded    = "ended"    // guests expired, Node deregistered; waiting for a donor pod
	StateFresh    = "fresh"    // a donor pod arrived after the end; re-registering
)

const (
	// ReasonEraEnded is the guest pod's status reason and Event reason at era end.
	ReasonEraEnded = "EraEnded"
	// AnnotationIdleSince on the virtual Node holds the era condition start (RFC 3339), so a
	// restarted VK resumes the countdown from it instead of starting over (or earlier).
	AnnotationIdleSince = "timeslice.io/era-idle-since"
	// DefaultDonorSelector matches the RL job's pods and never a mirror.
	DefaultDonorSelector = "timeslice.io/group,timeslice.io/role!=background"
)

// Config is the era's part of the VK configuration.
type Config struct {
	Host   string // real node the VK runs on
	VKNode string // the virtual Node
	// Group, when set, is the group the host yields to. Empty: read from the host's labels in
	// the LabelKeys form, again for every era.
	Group         string
	LabelKeys     string        // LabelKeysPrefix or LabelKeysNS
	EraTTL        time.Duration // 0 = no era (today)
	Trigger       string        // TriggerTTL, TriggerLabel or TriggerEither
	DonorSelector string        // label selector for donor pods on the host
	// Poll is how often the era condition is evaluated (default 500 ms, the VK-A6 poll period).
	Poll time.Duration
	// MirrorWait bounds how long the end waits for deleted mirrors to go before the guests are
	// marked Failed anyway (default 25 s). Mirrors are never force-deleted.
	MirrorWait time.Duration
}

// Hooks connect the era to the rest of the VK. Every hook may be nil.
type Hooks struct {
	// Stop stops the virtual-kubelet node and pod controllers and returns once they have
	// stopped, so nothing re-registers the Node after deregistration.
	Stop func(ctx context.Context)
	// Start registers the virtual Node again with a freshly computed budget and starts its
	// controllers (fresh era).
	Start func(ctx context.Context) error
	// Yield hands back the background grant if the VK holds one (VK-A6 loop).
	Yield func(ctx context.Context) error
	// Kill runs the kill sequence (agent Kill, delete mirror) for a guest that is suspended or
	// in doubt (VK-A4), and reports whether it did. false or nil: the delete-mirror path.
	Kill func(ctx context.Context, guest, mirror *corev1.Pod) (bool, error)
	// Recorder records the EraEnded Event on each expired guest.
	Recorder record.EventRecorder
}

// Controller runs one VK's eras. Run owns all state except closed, which the provider's
// admission gate reads from other goroutines.
type Controller struct {
	cs       kubernetes.Interface
	locks    LockSource
	clock    func() time.Time
	cfg      Config
	hooks    Hooks
	selector labels.Selector

	hostNodes corev1listers.NodeLister
	donorPods corev1listers.PodLister
	synced    []cache.InformerSynced
	factories []informers.SharedInformerFactory

	closed atomic.Bool // admission closed: the era is ending or has ended
	wg     sync.WaitGroup

	state        string
	idleSince    time.Time // when the host was first seen without donor pods (zero: has donors)
	lastActivity time.Time // latest LastLockActivity result
	prev         *GroupStatus
	sawLabel     bool      // the host carried the group label during this era
	persisted    time.Time // idle-since value last written to the Node
	deregPending bool      // deregistration failed; retried on every step while ended
	logged       string
}

// New validates cfg and builds a controller. locks may be nil (no lock activity is seen).
func New(cs kubernetes.Interface, locks LockSource, clock func() time.Time, in *Config, hooks Hooks) (*Controller, error) {
	if in == nil {
		return nil, fmt.Errorf("era: no config")
	}
	cfg := *in
	if clock == nil {
		clock = time.Now
	}
	if cfg.Host == "" || cfg.VKNode == "" {
		return nil, fmt.Errorf("era: host and virtual node names are required")
	}
	switch cfg.Trigger {
	case "":
		cfg.Trigger = TriggerEither
	case TriggerTTL, TriggerLabel, TriggerEither:
	default:
		return nil, fmt.Errorf("--era-trigger=%q: want ttl, label or either", cfg.Trigger)
	}
	switch cfg.LabelKeys {
	case "":
		cfg.LabelKeys = LabelKeysPrefix
	case LabelKeysPrefix, LabelKeysNS:
	default:
		return nil, fmt.Errorf("era label keys %q: want prefix or ns", cfg.LabelKeys)
	}
	if cfg.DonorSelector == "" {
		cfg.DonorSelector = DefaultDonorSelector
	}
	sel, err := labels.Parse(cfg.DonorSelector)
	if err != nil {
		return nil, fmt.Errorf("--era-donor-selector: %w", err)
	}
	if cfg.EraTTL < 0 {
		return nil, fmt.Errorf("--era-ttl must not be negative")
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 500 * time.Millisecond
	}
	if cfg.MirrorWait <= 0 {
		cfg.MirrorWait = 25 * time.Second
	}
	c := &Controller{cs: cs, locks: locks, clock: clock, cfg: cfg, hooks: hooks, selector: sel, state: StateActive}

	hf := informers.NewSharedInformerFactoryWithOptions(cs, 0, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.FieldSelector = fields.OneTermEqualSelector("metadata.name", cfg.Host).String()
	}))
	pf := informers.NewSharedInformerFactoryWithOptions(cs, 0, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", cfg.Host).String()
		o.LabelSelector = cfg.DonorSelector
	}))
	nodes, pods := hf.Core().V1().Nodes(), pf.Core().V1().Pods()
	c.hostNodes, c.donorPods = nodes.Lister(), pods.Lister()
	c.synced = []cache.InformerSynced{nodes.Informer().HasSynced, pods.Informer().HasSynced}
	c.factories = []informers.SharedInformerFactory{hf, pf}
	return c, nil
}

// Enabled reports whether eras are on (--era-ttl > 0).
func (c *Controller) Enabled() bool { return c.cfg.EraTTL > 0 }

// Admit is the provider's admission gate. While the era is ending or has ended, a guest bound
// to the Node gets no mirror: it is marked Failed with reason EraEnded instead.
func (c *Controller) Admit(ctx context.Context, pod *corev1.Pod) bool {
	if !c.closed.Load() {
		return true
	}
	go c.failGuest(context.WithoutCancel(ctx), pod, nil, "refused")
	return false
}

// MirrorDeletedReason is the reason the mirror backend writes on a guest whose mirror is
// deleted: EraEnded while the era ends, so the backend's and the era's writes agree.
func (c *Controller) MirrorDeletedReason() string {
	if c.closed.Load() {
		return ReasonEraEnded
	}
	return ""
}

// Run evaluates the era every Poll until ctx ends. A cancelled ctx (SIGTERM, restart, outage)
// never ends the era: only the era condition does.
func (c *Controller) Run(ctx context.Context) error {
	if !c.Enabled() {
		log.G(ctx).WithField("host", c.cfg.Host).Info("era off (--era-ttl=0): the virtual Node and its guests are kept")
		<-ctx.Done()
		return nil
	}
	for _, f := range c.factories {
		f.Start(ctx.Done())
	}
	defer func() {
		for _, f := range c.factories {
			f.Shutdown()
		}
	}()
	if !cache.WaitForCacheSync(ctx.Done(), c.synced...) {
		return ctx.Err()
	}
	c.restore(ctx)
	t := time.NewTicker(c.cfg.Poll)
	defer t.Stop()
	for {
		c.step(ctx)
		select {
		case <-ctx.Done():
			c.wg.Wait()
			return nil
		case <-t.C:
		}
	}
}

// restore reads the persisted condition start after a restart.
func (c *Controller) restore(ctx context.Context) {
	n, err := c.cs.CoreV1().Nodes().Get(ctx, c.cfg.VKNode, metav1.GetOptions{})
	if err != nil {
		return
	}
	v, ok := n.Annotations[AnnotationIdleSince]
	if !ok {
		return
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		log.G(ctx).WithField("value", v).Warn("era: unparsable idle-since annotation; ignored")
		return
	}
	c.idleSince, c.persisted = t, t
	log.G(ctx).WithField("idle_since", v).Info("era: countdown resumed from the virtual Node")
}

func (c *Controller) donorCount() int {
	pods, err := c.donorPods.List(labels.Everything())
	if err != nil {
		return 0
	}
	n := 0
	for _, p := range pods {
		if c.isDonor(p) {
			n++
		}
	}
	return n
}

func (c *Controller) host() *corev1.Node {
	h, err := c.hostNodes.Get(c.cfg.Host)
	if err != nil {
		return nil
	}
	return h
}

// step evaluates the era condition once.
func (c *Controller) step(ctx context.Context) {
	now := c.clock()
	host := c.host()
	group := c.cfg.Group
	if group == "" {
		group = GroupFromHost(host, c.cfg.LabelKeys)
	}
	labelled := hostCarries(host, c.cfg.LabelKeys, group)
	donors := c.donorCount()

	var st *GroupStatus
	if c.locks != nil && group != "" {
		if s, err := c.locks.GroupStatus(ctx, group); err == nil {
			st = s
		}
	}
	if a := LastLockActivity(c.prev, st, now); a.After(c.lastActivity) {
		c.lastActivity = a
	}
	held := lockHeld(st)
	c.prev = st

	if c.state == StateEnded {
		if c.deregPending {
			c.deregister(ctx)
		}
		if donors > 0 && (c.cfg.Trigger == TriggerTTL || labelled) {
			c.fresh(ctx, donors)
		}
		return
	}
	if labelled {
		c.sawLabel = true
	}
	if c.cfg.Trigger != TriggerTTL && c.sawLabel && !labelled {
		c.end(ctx, "label", donors)
		return
	}
	if donors > 0 || held {
		c.idleSince = time.Time{}
		if c.state != StateActive || c.persisted != (time.Time{}) {
			c.state = StateActive
			c.writeIdleSince(ctx, time.Time{})
		}
		c.logState(ctx, donors, time.Time{})
		return
	}
	if c.idleSince.IsZero() {
		c.idleSince = now
	}
	start := c.idleSince
	if c.lastActivity.After(start) {
		start = c.lastActivity
	}
	c.state = StateCounting
	if !start.Equal(c.persisted) {
		c.writeIdleSince(ctx, start)
	}
	expires := start.Add(c.cfg.EraTTL)
	c.logState(ctx, donors, expires)
	if c.cfg.Trigger != TriggerLabel && !now.Before(expires) {
		c.end(ctx, "ttl", donors)
	}
}

// logState writes msg="era" when the state, donor count or expiry changes.
func (c *Controller) logState(ctx context.Context, donors int, expires time.Time) {
	exp := "-"
	if !expires.IsZero() {
		exp = expires.UTC().Format(time.RFC3339)
	}
	act := "-"
	if !c.lastActivity.IsZero() {
		act = c.lastActivity.UTC().Format(time.RFC3339)
	}
	key := fmt.Sprintf("%s|%d|%s|%s", c.state, donors, act, exp)
	if key == c.logged {
		return
	}
	c.logged = key
	log.G(ctx).WithField("state", c.state).WithField("host", c.cfg.Host).WithField("donor_pods", donors).
		WithField("last_activity", act).WithField("expires_at", exp).Info("era")
}

// writeIdleSince persists the condition start on the virtual Node (zero removes it). A failed
// write is retried on the next step.
func (c *Controller) writeIdleSince(ctx context.Context, t time.Time) {
	var patch string
	if t.IsZero() {
		patch = fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, AnnotationIdleSince)
	} else {
		patch = fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, AnnotationIdleSince, t.UTC().Format(time.RFC3339))
	}
	_, err := c.cs.CoreV1().Nodes().Patch(ctx, c.cfg.VKNode, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		log.G(ctx).WithError(err).WithField("node", c.cfg.VKNode).Warn("era: could not persist idle-since; retrying")
		return
	}
	c.persisted = t
}

// fresh starts a new era after a donor pod arrived on the host.
func (c *Controller) fresh(ctx context.Context, donors int) {
	c.state = StateFresh
	c.logState(ctx, donors, time.Time{})
	c.closed.Store(false)
	if c.hooks.Start != nil {
		if err := c.hooks.Start(ctx); err != nil {
			log.G(ctx).WithError(err).WithField("node", c.cfg.VKNode).Warn("era: re-registration failed; retrying")
			c.closed.Store(true)
			c.state = StateEnded
			return
		}
	}
	c.state = StateActive
	c.idleSince, c.lastActivity, c.persisted, c.prev, c.sawLabel = time.Time{}, time.Time{}, time.Time{}, nil, false
	c.logState(ctx, donors, time.Time{})
}
