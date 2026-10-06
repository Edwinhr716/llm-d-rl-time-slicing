package donorcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

// How the wait for the host's virtual Node ended at era end (log key ended_by).
const (
	EndedByNoVKNode = "no-vk-node"
	EndedByVKGone   = "vk-node-gone"
	EndedByGrace    = "grace"
)

// Node event reasons.
const (
	EventDonorLabelled = "DonorLabelled"
	EventEraEnded      = "EraEnded"
	EventGroupConflict = "GroupConflict"
)

// nodeState is the controller's view of one real node. After a node is first seen it is the
// source of truth; the node's annotations are read only when the controller (re)starts.
type nodeState struct {
	owned bool
	// foreign: owned by a donor controller for a group outside --group-filter; left alone.
	foreign bool
	// released: unlabelled by this process; ignored until the cache shows the patch landed.
	released bool
	group    string
	keys     []string
	// idleSince is the era condition start (persisted); zero while a donor pod of the group is on
	// the node.
	idleSince time.Time
	// vkWaitSince is when era end started waiting for the host's virtual Node to go.
	vkWaitSince time.Time
	clockSig    string
	conflicts   map[string]bool
	note        string
	// ambiguous: more than one donor pod holds GPUs (lendable 0, event raised once).
	ambiguous bool
	// shadowSig is the last lendable/shadow-allocatable pair logged.
	shadowSig string
}

// groupState is what the controller knows about one group's lock.
type groupState struct {
	prev         *v1alpha1.GroupStatus
	lastActivity time.Time
	lastPoll     time.Time
	polled       bool
	active       bool
	holdErr      error
}

// Controller labels donor hosts and ends their eras.
type Controller struct {
	cs    kubernetes.Interface
	locks LockSource
	clock func() time.Time
	cfg   Config
	cmp   compiled
	log   *slog.Logger

	factories    []informers.SharedInformerFactory
	podInformers []cache.SharedIndexInformer
	nodeInformer cache.SharedIndexInformer
	wake         chan struct{}

	nodes    map[string]*nodeState
	groups   map[string]*groupState
	released map[types.UID]time.Time
	// fenceGone tracks hosts whose fence taint names a missing virtual Node (--clear-stale-fences).
	fenceGone map[string]fenceWait
	eventSeq  uint64
}

// New builds a Controller. locks may be nil (no orchestrator: donor pods alone drive the era).
// clock is used for every TTL decision; nil means time.Now.
func New(cs kubernetes.Interface, locks LockSource, clock func() time.Time, cfg *Config) (*Controller, error) {
	full := cfg.withDefaults()
	cmp, err := full.compile()
	if err != nil {
		return nil, err
	}
	if clock == nil {
		clock = time.Now
	}
	ctl := &Controller{
		cs:       cs,
		locks:    locks,
		clock:    clock,
		cfg:      full,
		cmp:      cmp,
		log:      slog.Default(),
		wake:     make(chan struct{}, 1),
		nodes:    map[string]*nodeState{},
		groups:   map[string]*groupState{},
		released:  map[types.UID]time.Time{},
		fenceGone: map[string]fenceWait{},
	}
	if err := ctl.setupInformers(); err != nil {
		return nil, err
	}
	return ctl, nil
}

func (c *Controller) setupInformers() error {
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { c.poke() },
		UpdateFunc: func(any, any) { c.poke() },
		DeleteFunc: func(any) { c.poke() },
	}
	nodeFactory := informers.NewSharedInformerFactory(c.cs, 0)
	c.nodeInformer = nodeFactory.Core().V1().Nodes().Informer()
	if _, err := c.nodeInformer.AddEventHandler(handler); err != nil {
		return fmt.Errorf("node informer: %w", err)
	}
	c.factories = append(c.factories, nodeFactory)

	namespaces := c.cfg.WatchNamespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}
	selector := c.cfg.DonorSelector
	for _, ns := range namespaces {
		factory := informers.NewSharedInformerFactoryWithOptions(c.cs, 0,
			informers.WithNamespace(ns),
			informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = selector }))
		inf := factory.Core().V1().Pods().Informer()
		if _, err := inf.AddEventHandler(handler); err != nil {
			return fmt.Errorf("pod informer: %w", err)
		}
		c.podInformers = append(c.podInformers, inf)
		c.factories = append(c.factories, factory)
	}
	return nil
}

func (c *Controller) poke() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// start starts the informers and waits for their caches.
func (c *Controller) start(stop <-chan struct{}) error {
	for _, f := range c.factories {
		f.Start(stop)
	}
	synced := make([]cache.InformerSynced, 0, 1+len(c.podInformers))
	synced = append(synced, c.nodeInformer.HasSynced)
	for _, inf := range c.podInformers {
		synced = append(synced, inf.HasSynced)
	}
	if !cache.WaitForCacheSync(stop, synced...) {
		return errors.New("informer caches did not sync")
	}
	return nil
}

// Run starts the informers and reconciles until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	if err := c.start(ctx.Done()); err != nil {
		return err
	}
	return c.loop(ctx)
}

func (c *Controller) loop(ctx context.Context) error {
	c.log.Info("donor controller started", "label_keys", c.cfg.LabelKeys, "era_ttl", c.cfg.EraTTL.String(),
		"donor_selector", c.cfg.DonorSelector, "group_filter", c.cfg.GroupFilter,
		"vk_deregister_grace", c.cfg.VKDeregisterGrace.String(), "watch_namespaces", strings.Join(c.cfg.WatchNamespaces, ","),
		"release_dead_hosts", c.cfg.ReleaseDeadHosts, "clear_stale_fences", c.cfg.ClearStaleFences,
		"lock_source", c.locks != nil)
	ticker := time.NewTicker(c.cfg.Tick)
	defer ticker.Stop()
	for {
		c.reconcileAll(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-c.wake:
		case <-ticker.C:
		}
	}
}

// donorSet is the donor pods on one node, by group.
type donorSet map[string][]*corev1.Pod

// reconcileAll runs one pass over every node in the cache.
func (c *Controller) reconcileAll(ctx context.Context) {
	now := c.clock()
	nodes := map[string]*corev1.Node{}
	for _, obj := range c.nodeInformer.GetStore().List() {
		if node, ok := obj.(*corev1.Node); ok {
			nodes[node.Name] = node
		}
	}
	for name := range c.nodes {
		if _, ok := nodes[name]; !ok {
			delete(c.nodes, name)
		}
	}
	donors := c.donorsByNode(nodes)
	names := make([]string, 0, len(nodes))
	for name, node := range nodes {
		if isVirtual(node) {
			continue
		}
		names = append(names, name)
		st, known := c.nodes[name]
		switch {
		case !known && node.Annotations[AnnotationLabelledBy] == LabelledByValue:
			c.nodes[name] = c.loadState(node)
		case known && st.released && node.Annotations[AnnotationLabelledBy] != LabelledByValue:
			delete(c.nodes, name) // our unlabel patch is in the cache now
		}
	}
	sort.Strings(names)
	c.pollGroups(ctx, now)
	for _, name := range names {
		st := c.nodes[name]
		if st == nil && len(donors[name]) == 0 {
			continue
		}
		if st == nil {
			st = &nodeState{}
			c.nodes[name] = st
		}
		c.reconcileNode(ctx, now, nodes[name], st, donors[name], nodes)
	}
	if c.cfg.ReleaseDeadHosts {
		c.releaseDeadHosts(ctx, now, nodes)
	}
	if c.cfg.ClearStaleFences {
		c.clearStaleFences(ctx, now, nodes)
	}
}

func isVirtual(node *corev1.Node) bool {
	_, ok := node.Labels[VirtualNodeLabel]
	return ok
}

// donorsByNode groups the cached donor pods by real node and group. Terminal pods, pods on
// virtual Nodes and pods outside the group filter are not donors.
func (c *Controller) donorsByNode(nodes map[string]*corev1.Node) map[string]donorSet {
	out := map[string]donorSet{}
	for _, inf := range c.podInformers {
		for _, obj := range inf.GetStore().List() {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				continue
			}
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				continue
			}
			if !c.cmp.selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			node, ok := nodes[pod.Spec.NodeName]
			if !ok || isVirtual(node) {
				continue
			}
			group := pod.Labels[GroupLabelKey]
			if group == "" || !c.cmp.filter.MatchString(group) {
				continue
			}
			if out[node.Name] == nil {
				out[node.Name] = donorSet{}
			}
			out[node.Name][group] = append(out[node.Name][group], pod)
		}
	}
	return out
}

// loadState reads an owned node's annotations (controller start, or a node first seen owned).
func (c *Controller) loadState(node *corev1.Node) *nodeState {
	ann := node.Annotations
	st := &nodeState{owned: true, group: ann[AnnotationLabelledGroup]}
	if keys := ann[AnnotationLabelledKeys]; keys != "" {
		st.keys = strings.Split(keys, ",")
	} else {
		st.keys = sortedKeys(NodeLabelsFor(c.cfg.LabelKeys, st.group))
	}
	if st.group == "" || !c.cmp.filter.MatchString(st.group) {
		st.foreign = true
		return st
	}
	if ts, err := time.Parse(time.RFC3339, ann[AnnotationEraIdleSince]); err == nil {
		st.idleSince = ts
	}
	c.log.Info("owned node loaded", "node", node.Name, "group", st.group, "keys", strings.Join(st.keys, ","),
		"idle_since", fmtTime(st.idleSince))
	return st
}

// pollGroups polls GetGroupStatus for every labelled group, at most once per LockPollInterval.
func (c *Controller) pollGroups(ctx context.Context, now time.Time) {
	want := map[string]bool{}
	for _, st := range c.nodes {
		if st.owned && !st.foreign {
			want[st.group] = true
		}
	}
	for group := range c.groups {
		if !want[group] {
			delete(c.groups, group)
		}
	}
	for group := range want {
		gs := c.groups[group]
		if gs == nil {
			gs = &groupState{}
			c.groups[group] = gs
		}
		if c.locks == nil || (gs.polled && now.Sub(gs.lastPoll) < c.cfg.LockPollInterval) {
			continue
		}
		gs.polled, gs.lastPoll = true, now
		rctx, cancel := context.WithTimeout(ctx, c.cfg.LockRPCTimeout)
		cur, err := c.locks.GroupStatus(rctx, group)
		cancel()
		if err != nil {
			if gs.holdErr == nil {
				c.log.Warn("lock status unavailable; holding the era clock", "group", group, "err", err)
			}
			gs.holdErr = err
			continue
		}
		if gs.holdErr != nil {
			c.log.Info("lock status available again", "group", group)
		}
		gs.holdErr = nil
		if t := LastLockActivity(gs.prev, cur, now); t.After(gs.lastActivity) {
			gs.lastActivity = t
		}
		gs.active = cur != nil && IsLockActive(cur)
		gs.prev = cur
	}
}

// reconcileNode handles one real node that is owned or has donor pods.
func (c *Controller) reconcileNode(ctx context.Context, now time.Time, node *corev1.Node, st *nodeState,
	donors donorSet, nodes map[string]*corev1.Node,
) {
	if st.foreign || st.released {
		return
	}
	if !st.owned {
		c.reconcileUnowned(ctx, node, st, donors)
		if !st.owned && len(donors) == 0 {
			delete(c.nodes, node.Name)
		}
		return
	}
	c.noteConflicts(ctx, node, st, donors)
	c.syncLendable(ctx, node, st, donors)
	gs := c.groups[st.group]
	if gs == nil {
		gs = &groupState{}
	}
	if mine := len(donors[st.group]); mine > 0 {
		st.vkWaitSince = time.Time{}
		if !st.idleSince.IsZero() {
			if err := c.patchNode(ctx, node.Name, nil, map[string]*string{AnnotationEraIdleSince: nil}); err != nil {
				c.log.Warn("clearing era idle-since failed", "node", node.Name, "err", err)
				return
			}
			st.idleSince = time.Time{}
		}
		c.logClock(node.Name, st, mine, gs.lastActivity, time.Time{}, "")
		return
	}
	switch {
	case st.idleSince.IsZero():
		c.setIdleSince(ctx, node.Name, st, now)
	case !gs.active && gs.holdErr == nil && gs.lastActivity.After(st.idleSince):
		c.setIdleSince(ctx, node.Name, st, gs.lastActivity)
	}
	if st.idleSince.IsZero() {
		return // the persist failed; retry next pass
	}
	expires := st.idleSince.Add(c.cfg.EraTTL)
	switch {
	case gs.active:
		c.logClock(node.Name, st, 0, gs.lastActivity, expires, "lock-active")
		return
	case gs.holdErr != nil:
		c.logClock(node.Name, st, 0, gs.lastActivity, expires, "lock-status-unavailable")
		return
	}
	c.logClock(node.Name, st, 0, gs.lastActivity, expires, "")
	if now.Before(expires) {
		st.vkWaitSince = time.Time{}
		return
	}
	c.endEra(ctx, now, node, st, nodes)
}

// setIdleSince persists the era condition start, rounded up to the second so that the RFC3339
// value read back after a restart is never earlier than the one in memory.
func (c *Controller) setIdleSince(ctx context.Context, name string, st *nodeState, t time.Time) {
	up := t.Truncate(time.Second)
	if up.Before(t) {
		up = up.Add(time.Second)
	}
	if up.Equal(st.idleSince) {
		return
	}
	val := up.UTC().Format(time.RFC3339)
	if err := c.patchNode(ctx, name, nil, map[string]*string{AnnotationEraIdleSince: &val}); err != nil {
		c.log.Warn("persisting era idle-since failed", "node", name, "err", err)
		return
	}
	st.idleSince = up
}

// logClock logs "era clock" whenever what it would print changes.
func (c *Controller) logClock(node string, st *nodeState, donorPods int, lastActivity, expires time.Time, held string) {
	sig := fmt.Sprintf("%d|%s|%s|%s", donorPods, fmtTime(lastActivity), fmtTime(expires), held)
	if sig == st.clockSig {
		return
	}
	st.clockSig = sig
	args := []any{
		"node", node, "group", st.group, "donor_pods", donorPods,
		"last_activity", fmtTime(lastActivity), "expires_at", fmtTime(expires),
	}
	if held != "" {
		args = append(args, "held", held)
	}
	c.log.Info("era clock", args...)
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "none"
	}
	return t.UTC().Format(time.RFC3339)
}

// endEra removes the labels once the host's virtual Node is gone or the grace has passed
// (guest kubelets deregister first, then the marks go).
func (c *Controller) endEra(ctx context.Context, now time.Time, node *corev1.Node, st *nodeState,
	nodes map[string]*corev1.Node,
) {
	vks := virtualNodesOf(node.Name, nodes)
	var endedBy string
	switch {
	case len(vks) == 0 && st.vkWaitSince.IsZero():
		endedBy = EndedByNoVKNode
	case len(vks) == 0:
		endedBy = EndedByVKGone
	default:
		if st.vkWaitSince.IsZero() {
			st.vkWaitSince = now
			c.log.Info("era end waiting for virtual node", "node", node.Name, "group", st.group,
				"vk_nodes", strings.Join(vks, ","), "grace", c.cfg.VKDeregisterGrace.String())
		}
		if now.Sub(st.vkWaitSince) < c.cfg.VKDeregisterGrace {
			return
		}
		endedBy = EndedByGrace
	}
	c.log.Info("era end wait over", "node", node.Name, "group", st.group, "ended_by", endedBy)
	c.unlabel(ctx, node, st, endedBy)
}

// virtualNodesOf lists the virtual Nodes whose host (ownerReference kind Node) is host.
func virtualNodesOf(host string, nodes map[string]*corev1.Node) []string {
	var out []string
	for _, node := range nodes {
		if !isVirtual(node) {
			continue
		}
		if ref := hostOwnerRef(node); ref != nil && ref.Name == host {
			out = append(out, node.Name)
		}
	}
	sort.Strings(out)
	return out
}

// hostOwnerRef returns a virtual Node's ownerReference to its real host Node, if any.
func hostOwnerRef(node *corev1.Node) *metav1.OwnerReference {
	for i := range node.OwnerReferences {
		ref := &node.OwnerReferences[i]
		if ref.Kind == "Node" && ref.APIVersion == "v1" {
			return ref
		}
	}
	return nil
}

// unlabel removes exactly the keys and annotations the controller wrote.
func (c *Controller) unlabel(ctx context.Context, node *corev1.Node, st *nodeState, endedBy string) {
	lbls := map[string]*string{}
	for _, k := range st.keys {
		lbls[k] = nil
	}
	anns := map[string]*string{
		AnnotationLabelledBy: nil, AnnotationLabelledGroup: nil, AnnotationLabelledKeys: nil, AnnotationEraIdleSince: nil,
		AnnotationLendableGPUs: nil,
	}
	if err := c.patchNode(ctx, node.Name, lbls, anns); err != nil {
		c.log.Warn("removing labels failed", "node", node.Name, "err", err)
		return
	}
	c.log.Info("node unlabelled", "node", node.Name, "group", st.group, "reason", "era-ended", "ended_by", endedBy,
		"keys", strings.Join(st.keys, ","))
	c.event(ctx, node, corev1.EventTypeNormal, EventEraEnded,
		fmt.Sprintf("era of group %s ended (%s); removed %s", st.group, endedBy, strings.Join(st.keys, ",")))
	*st = nodeState{released: true}
}

// reconcileUnowned labels a node that has donor pods and no donor-family labels.
func (c *Controller) reconcileUnowned(ctx context.Context, node *corev1.Node, st *nodeState, donors donorSet) {
	if len(donors) == 0 {
		return
	}
	groups := groupsByAge(donors)
	if hasFamilyKey(node.Labels) {
		have := FamilyGroups(node.Labels)
		for _, want := range groups {
			if !contains(have, want) {
				c.conflict(ctx, node, st, strings.Join(have, ","), want)
			}
		}
		c.noteOnce(st, "node has donor labels it did not write; not managed", "node", node.Name,
			"have", strings.Join(have, ","))
		return
	}
	want := groups[0]
	if err := ValidateGroup(c.cfg.LabelKeys, want); err != nil {
		c.noteOnce(st, "donor group cannot be written as a node label", "node", node.Name, "group", want, "err", err)
		return
	}
	lbls := NodeLabelsFor(c.cfg.LabelKeys, want)
	keys := sortedKeys(lbls)
	patchLabels := map[string]*string{}
	for k, v := range lbls {
		patchLabels[k] = &v
	}
	by, keyList := LabelledByValue, strings.Join(keys, ",")
	anns := map[string]*string{AnnotationLabelledBy: &by, AnnotationLabelledGroup: &want, AnnotationLabelledKeys: &keyList}
	if err := c.patchNode(ctx, node.Name, patchLabels, anns); err != nil {
		c.log.Warn("labelling node failed", "node", node.Name, "group", want, "err", err)
		return
	}
	*st = nodeState{owned: true, group: want, keys: keys}
	c.log.Info("node labelled", "node", node.Name, "group", want, "keys", keyList)
	c.event(ctx, node, corev1.EventTypeNormal, EventDonorLabelled,
		fmt.Sprintf("donor pod of group %s on node; labelled %s", want, keyList))
	c.noteConflicts(ctx, node, st, donors)
}

// noteConflicts reports donor pods of other groups on an owned node. The first group wins.
func (c *Controller) noteConflicts(ctx context.Context, node *corev1.Node, st *nodeState, donors donorSet) {
	for group := range st.conflicts {
		if len(donors[group]) == 0 {
			delete(st.conflicts, group)
		}
	}
	for _, group := range groupsByAge(donors) {
		if group != st.group {
			c.conflict(ctx, node, st, st.group, group)
		}
	}
}

// conflict logs and records a group conflict once while it lasts. Labels never flip.
func (c *Controller) conflict(ctx context.Context, node *corev1.Node, st *nodeState, have, want string) {
	if st.conflicts[want] {
		return
	}
	if st.conflicts == nil {
		st.conflicts = map[string]bool{}
	}
	st.conflicts[want] = true
	c.log.Warn("group conflict", "node", node.Name, "have", have, "want", want)
	c.event(ctx, node, corev1.EventTypeWarning, EventGroupConflict,
		fmt.Sprintf("node is labelled for group %s; donor pod of group %s ignored (first group wins)", have, want))
}

func (c *Controller) noteOnce(st *nodeState, msg string, args ...any) {
	sig := msg + fmt.Sprint(args...)
	if st.note == sig {
		return
	}
	st.note = sig
	c.log.Info(msg, args...)
}

// groupsByAge returns the groups in donors, the group of the oldest pod first.
func groupsByAge(donors donorSet) []string {
	type entry struct {
		group string
		first *corev1.Pod
	}
	entries := make([]entry, 0, len(donors))
	for group, pods := range donors {
		first := pods[0]
		for _, p := range pods[1:] {
			if olderPod(p, first) {
				first = p
			}
		}
		entries = append(entries, entry{group, first})
	}
	sort.Slice(entries, func(i, j int) bool { return olderPod(entries[i].first, entries[j].first) })
	out := make([]string, len(entries))
	for i := range entries {
		out[i] = entries[i].group
	}
	return out
}

func olderPod(a, b *corev1.Pod) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// patchNode merge-patches labels and annotations; a nil value removes the key.
func (c *Controller) patchNode(ctx context.Context, name string, lbls, anns map[string]*string) error {
	meta := map[string]any{}
	if len(lbls) > 0 {
		meta["labels"] = lbls
	}
	if len(anns) > 0 {
		meta["annotations"] = anns
	}
	body, err := json.Marshal(map[string]any{"metadata": meta})
	if err != nil {
		return err
	}
	_, err = c.cs.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, body,
		metav1.PatchOptions{FieldManager: LabelledByValue})
	return err
}

// event records a Node event. Failures are logged and otherwise ignored.
func (c *Controller) event(ctx context.Context, node *corev1.Node, eventType, reason, msg string) {
	c.eventSeq++
	stamp := metav1.NewTime(c.clock())
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.%x.%d", node.Name, time.Now().UnixNano(), c.eventSeq),
			Namespace: metav1.NamespaceDefault,
		},
		InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID},
		Reason:         reason,
		Message:        msg,
		Type:           eventType,
		Source:         corev1.EventSource{Component: LabelledByValue},
		FirstTimestamp: stamp,
		LastTimestamp:  stamp,
		Count:          1,
	}
	if _, err := c.cs.CoreV1().Events(metav1.NamespaceDefault).Create(ctx, ev, metav1.CreateOptions{}); err != nil {
		c.log.Warn("recording event failed", "node", node.Name, "reason", reason, "err", err)
	}
}
