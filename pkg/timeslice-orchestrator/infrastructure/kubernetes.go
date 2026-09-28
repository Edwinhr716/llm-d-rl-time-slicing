// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infrastructure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	corev1informers "k8s.io/client-go/informers/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	NodeLabelPrefix = "group.timeslice.io/"
	PodLabelKey     = "timeslice.io/group"
	JobLabelKey     = "timeslice.io/job-id"
)

// Pod role label, written by the virtual kubelet on guest mirror pods. Only
// RoleBackground has a meaning; any other value, or no label, is a foreground job.
const (
	RoleLabelKey   = "timeslice.io/role"
	RoleBackground = "background"
)

// PodInfo contains simplified information about a pod.
type PodInfo struct {
	UID   string
	JobID string
	// Background is true for a guest mirror pod (label timeslice.io/role=background).
	Background bool
	// NodeName is the node the pod is bound to, if any.
	NodeName string
}

// KubernetesOrchestrator implements controller.InfrastructureOrchestrator for Kubernetes.
type KubernetesOrchestrator struct {
	nodeInformer       corev1informers.NodeInformer
	podInformers       []corev1informers.PodInformer
	nodeLister         corev1listers.NodeLister
	podListers         []corev1listers.PodLister
	nodeSynced         cache.InformerSynced
	podSynced          []cache.InformerSynced
	nodeScopedPods     bool
	exemptBackground   bool
	groupStore         *store.GroupStore
	jobStore           *store.JobStore
	snapshotAgentStore store.SnapshotAgentStore

	// observedJobsMu guards observedJobs, the last job-ID set logged per
	// group by logGroupJobs.
	observedJobsMu sync.Mutex
	observedJobs   map[string][]string
}

// Option configures a KubernetesOrchestrator.
type Option func(*KubernetesOrchestrator)

// WithPodInformers adds pod informers next to the one passed to
// NewKubernetesOrchestrator. Use it to watch several namespaces, one
// namespace-scoped informer each (see Scope.NewInformerFactories). Pods from
// all informers are treated as one set.
func WithPodInformers(podInformers ...corev1informers.PodInformer) Option {
	return func(k *KubernetesOrchestrator) {
		for _, pi := range podInformers {
			k.addPodInformer(pi)
		}
	}
}

// WithNodeScopedPods ignores pods bound to a node that the node informer does
// not see. Set it when the node informer is limited by --node-selector, so a
// pod on a node outside the selector joins no group. Pods not yet bound to a
// node are kept.
func WithNodeScopedPods() Option {
	return func(k *KubernetesOrchestrator) {
		k.nodeScopedPods = true
	}
}

// NewKubernetesOrchestrator creates a new KubernetesOrchestrator.
func NewKubernetesOrchestrator(
	nodeInformer corev1informers.NodeInformer,
	podInformer corev1informers.PodInformer,
	groupStore *store.GroupStore,
	jobStore *store.JobStore,
	snapshotAgentStore store.SnapshotAgentStore,
	opts ...Option,
) *KubernetesOrchestrator {
	k := &KubernetesOrchestrator{
		nodeInformer:       nodeInformer,
		nodeLister:         nodeInformer.Lister(),
		nodeSynced:         nodeInformer.Informer().HasSynced,
		groupStore:         groupStore,
		jobStore:           jobStore,
		snapshotAgentStore: snapshotAgentStore,
	}
	k.addPodInformer(podInformer)
	for _, opt := range opts {
		opt(k)
	}
	return k
}

func (k *KubernetesOrchestrator) addPodInformer(pi corev1informers.PodInformer) {
	k.podInformers = append(k.podInformers, pi)
	k.podListers = append(k.podListers, pi.Lister())
	k.podSynced = append(k.podSynced, pi.Informer().HasSynced)
}

// podOnWatchedNode reports whether the pod may join a group: always, unless
// WithNodeScopedPods is set and the pod is bound to a node outside the node
// informer's scope. With WithNodeSelectorExemptBackground, a background pod
// bound to such a node is kept too (see keepExemptBackgroundPod).
func (k *KubernetesOrchestrator) podOnWatchedNode(ctx context.Context, pod *corev1.Pod) bool {
	if !k.nodeScopedPods || pod.Spec.NodeName == "" {
		return true
	}
	if _, err := k.nodeLister.Get(pod.Spec.NodeName); err == nil {
		return true
	}
	return k.keepExemptBackgroundPod(ctx, pod)
}

// Init initializes the KubernetesOrchestrator by waiting for informer caches to sync.
func (k *KubernetesOrchestrator) Init(ctx context.Context) error {
	synced := append([]cache.InformerSynced{k.nodeSynced}, k.podSynced...)
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return fmt.Errorf("failed to wait for informer caches to sync")
	}
	return nil
}

// getNodesForGroup returns the names of the nodes that belong to the given group.
func (k *KubernetesOrchestrator) getNodesForGroup(groupID string) ([]string, error) {
	selector := labels.SelectorFromSet(labels.Set{NodeLabelPrefix + groupID: "true"})
	nodes, err := k.nodeLister.List(selector)
	if err != nil {
		return nil, err
	}
	var groupNodes []string
	for _, node := range nodes {
		groupNodes = append(groupNodes, node.Name)
	}
	return groupNodes, nil
}

// getPodsForGroup returns the pods that are tied to the given group.
func (k *KubernetesOrchestrator) getPodsForGroup(ctx context.Context, groupID string) ([]PodInfo, error) {
	selector := labels.SelectorFromSet(labels.Set{PodLabelKey: groupID})
	pods := make([]*corev1.Pod, 0)
	for _, lister := range k.podListers {
		listed, err := lister.List(selector)
		if err != nil {
			return nil, err
		}
		pods = append(pods, listed...)
	}
	var podInfos []PodInfo
	for _, pod := range pods {
		jobID := pod.Labels[JobLabelKey]
		if jobID == "" || !k.podOnWatchedNode(ctx, pod) {
			continue
		}
		background := pod.Labels[RoleLabelKey] == RoleBackground
		// A mirror pod in a terminal phase counts as gone: its guest holds
		// nothing (vacated). A pod that is only being deleted still counts.
		if background && (pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed) {
			continue
		}
		podInfos = append(podInfos, PodInfo{
			UID:        string(pod.UID),
			JobID:      jobID,
			Background: background,
			NodeName:   pod.Spec.NodeName,
		})
	}
	return podInfos, nil
}

// ObserveGroupState observes the current state of the infrastructure for the given group
// and updates the groupStore and jobStore accordingly.
func (k *KubernetesOrchestrator) ObserveGroupState(ctx context.Context, groupID string) error {
	ctx = logging.WithGroupID(ctx, groupID)
	// 1. Find nodes belonging to the group
	groupNodes, err := k.getNodesForGroup(groupID)
	if err != nil {
		return fmt.Errorf("failed to get nodes for group %s: %w", groupID, err)
	}

	// 2. Find pods tied to the group
	pods, err := k.getPodsForGroup(ctx, groupID)
	if err != nil {
		return fmt.Errorf("failed to get pods for group %s: %w", groupID, err)
	}

	// If no nodes and no pods, we clean up the group and its jobs and return early.
	if len(groupNodes) == 0 && len(pods) == 0 {
		if err := k.cleanupGroup(ctx, groupID); err != nil {
			return fmt.Errorf("failed to cleanup group %s: %w", groupID, err)
		}
		k.logGroupJobs(ctx, groupID, nil)
		return nil
	}

	// 3. Update group nodes in store
	if err := k.updateGroupNodes(ctx, groupID, groupNodes); err != nil {
		return err
	}

	// 4. Update jobs and their pods in store
	if err := k.updateJobsAndPods(ctx, groupID, pods); err != nil {
		return err
	}
	k.logGroupJobs(ctx, groupID, pods)

	return nil
}

// logGroupJobs logs "Group jobs observed" with the sorted job IDs of the group
// whenever that set differs from the one logged last for the group.
func (k *KubernetesOrchestrator) logGroupJobs(ctx context.Context, groupID string, pods []PodInfo) {
	jobs := make([]string, 0, len(pods))
	for _, pod := range pods {
		jobs = append(jobs, pod.JobID)
	}
	slices.Sort(jobs)
	jobs = slices.Compact(jobs)

	k.observedJobsMu.Lock()
	defer k.observedJobsMu.Unlock()
	if last, seen := k.observedJobs[groupID]; seen && slices.Equal(last, jobs) {
		return
	}
	if k.observedJobs == nil {
		k.observedJobs = map[string][]string{}
	}
	k.observedJobs[groupID] = jobs
	slog.InfoContext(ctx, "Group jobs observed", "group", groupID, "jobs", jobs)
}

func (k *KubernetesOrchestrator) updateGroupNodes(ctx context.Context, groupID string, groupNodes []string) error {
	g, _, err := k.groupStore.GetOrCreate(ctx, groupID)
	if err != nil {
		return fmt.Errorf("failed to get or create group %s in store: %w", groupID, err)
	}

	// Clean up clients for nodes that were removed from the group
	oldNodes := g.Status().Nodes()
	removedNodes := findRemovedNodes(oldNodes, groupNodes)
	for _, nodeName := range removedNodes {
		if err := k.snapshotAgentStore.CloseClient(nodeName); err != nil {
			slog.ErrorContext(ctx, "Failed to close snapshot agent client for removed node", "error", err, "node", nodeName)
		}
	}

	g.Status().SetNodes(groupNodes)
	slog.InfoContext(ctx, "Updated nodes for group", "nodes", groupNodes)
	return nil
}

func (k *KubernetesOrchestrator) updateJobsAndPods(ctx context.Context, groupID string, pods []PodInfo) error {
	jobPods := make(map[string][]string)
	// A job is background only if every one of its pods says so. A foreground
	// job mistaken for a guest would have its faults ignored, so any doubt
	// resolves to foreground.
	jobBackground := make(map[string]bool)
	// jobNodes holds the nodes of each job's non-terminal mirror pods.
	jobNodes := make(map[string][]string)
	for _, pod := range pods {
		jobPods[pod.JobID] = append(jobPods[pod.JobID], pod.UID)
		background, seen := jobBackground[pod.JobID]
		jobBackground[pod.JobID] = pod.Background && (background || !seen)
		if pod.Background && pod.NodeName != "" && !slices.Contains(jobNodes[pod.JobID], pod.NodeName) {
			jobNodes[pod.JobID] = append(jobNodes[pod.JobID], pod.NodeName)
		}
	}

	// Update or create jobs
	for jobID, uids := range jobPods {
		job, err := k.jobStore.Get(ctx, groupID, jobID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				job = store.NewJob(groupID, jobID)
			} else {
				return err
			}
		}
		job.SetPods(uids)
		role := store.RoleForeground
		if jobBackground[jobID] {
			role = store.RoleBackground
		}
		job.SetRole(role)
		job.SetPodNodes(jobNodes[jobID])
		if err := k.jobStore.Put(ctx, job); err != nil {
			return err
		}
	}

	// Delete jobs that no longer have pods
	existingJobs, err := k.jobStore.ListByGroup(ctx, groupID)
	if err != nil {
		return err
	}
	for _, ej := range existingJobs {
		if _, ok := jobPods[ej.JobID()]; !ok {
			if err := k.jobStore.Delete(ctx, groupID, ej.JobID()); err != nil {
				return err
			}
			slog.InfoContext(ctx, "Deleted job from store because it has no pods", "job", ej.JobID())
		}
	}
	return nil
}

func (k *KubernetesOrchestrator) cleanupGroup(ctx context.Context, groupID string) error {
	existingJobs, err := k.jobStore.ListByGroup(ctx, groupID)
	if err != nil {
		return err
	}
	for _, ej := range existingJobs {
		if err := k.jobStore.Delete(ctx, groupID, ej.JobID()); err != nil {
			return err
		}
		slog.InfoContext(ctx, "Deleted job from store because group has no nodes and no pods", "job", ej.JobID())
	}

	// Close clients for all nodes that were in the group
	oldGroup, err := k.groupStore.Get(ctx, groupID)
	if err == nil && oldGroup != nil {
		for _, nodeName := range oldGroup.Status().Nodes() {
			if err := k.snapshotAgentStore.CloseClient(nodeName); err != nil {
				slog.ErrorContext(ctx, "Failed to close snapshot agent client on group deletion", "error", err, "node", nodeName)
			}
		}
	}

	if err := k.groupStore.Delete(ctx, groupID); err != nil {
		return err
	}
	slog.InfoContext(ctx, "Deleted group from store because it has no nodes and no pods")
	return nil
}

// Start registers event handlers on the informers.
func (k *KubernetesOrchestrator) Start(ctx context.Context, queue controller.WorkQueue) error {
	if err := k.setupNodeInformer(ctx, queue); err != nil {
		return fmt.Errorf("failed to setup node informer: %w", err)
	}
	if err := k.setupPodInformer(ctx, queue); err != nil {
		return fmt.Errorf("failed to setup pod informer: %w", err)
	}
	return nil
}

func (k *KubernetesOrchestrator) setupNodeInformer(ctx context.Context, queue controller.WorkQueue) error {
	_, err := k.nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			k.enqueueNode(ctx, obj, queue)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			k.enqueueNode(ctx, newObj, queue)
			k.enqueueNode(ctx, oldObj, queue)
		},
		DeleteFunc: func(obj interface{}) {
			k.enqueueNode(ctx, obj, queue)
		},
	})
	return err
}

func (k *KubernetesOrchestrator) enqueueNode(ctx context.Context, obj interface{}, queue controller.WorkQueue) {
	var node *corev1.Node
	var ok bool
	if node, ok = obj.(*corev1.Node); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		node, ok = tombstone.Obj.(*corev1.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
	}

	slog.InfoContext(ctx, "Enqueue Node", "node", node.Name)

	groups := k.getGroupsFromNode(node)
	for _, group := range groups {
		queue.Add(group)
	}
}

func (k *KubernetesOrchestrator) getGroupsFromNode(node *corev1.Node) []string {
	var groups []string
	for k := range node.Labels {
		if strings.HasPrefix(k, NodeLabelPrefix) {
			group := strings.TrimPrefix(k, NodeLabelPrefix)
			if group != "" {
				groups = append(groups, group)
			}
		}
	}
	return groups
}

func (k *KubernetesOrchestrator) setupPodInformer(ctx context.Context, queue controller.WorkQueue) error {
	for _, pi := range k.podInformers {
		_, err := pi.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				k.enqueuePod(ctx, obj, queue)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				k.enqueuePod(ctx, newObj, queue)
				k.enqueuePod(ctx, oldObj, queue)
			},
			DeleteFunc: func(obj interface{}) {
				k.enqueuePod(ctx, obj, queue)
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KubernetesOrchestrator) enqueuePod(ctx context.Context, obj interface{}, queue controller.WorkQueue) {
	var pod *corev1.Pod
	var ok bool
	if pod, ok = obj.(*corev1.Pod); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
	}

	// Before the node cache has synced every node looks unknown; the node's
	// own Add event enqueues its groups once it arrives.
	if k.nodeSynced() && !k.podOnWatchedNode(ctx, pod) {
		slog.InfoContext(ctx, "Ignoring pod bound to a node outside --node-selector",
			"pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name), "node", pod.Spec.NodeName)
		return
	}

	slog.InfoContext(ctx, "Enqueue Pod", "pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))

	group := k.getGroupFromPod(pod)
	if group != "" {
		queue.Add(group)
	}
}

func (k *KubernetesOrchestrator) getGroupFromPod(pod *corev1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	return pod.Labels[PodLabelKey]
}

func findRemovedNodes(oldNodes, newNodes []string) []string {
	newSet := make(map[string]bool)
	for _, n := range newNodes {
		newSet[n] = true
	}
	var removed []string
	for _, o := range oldNodes {
		if !newSet[o] {
			removed = append(removed, o)
		}
	}
	return removed
}

// NodeAddress returns the InternalIP of a node from the node cache, or the
// node name when the node has none (the name is then resolved by DNS).
func (k *KubernetesOrchestrator) NodeAddress(nodeName string) (string, error) {
	node, err := k.nodeLister.Get(nodeName)
	if err != nil {
		return "", fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
			return addr.Address, nil
		}
	}
	return nodeName, nil
}
