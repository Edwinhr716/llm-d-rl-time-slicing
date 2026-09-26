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
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// Scope limits what the orchestrator watches. The zero value watches every
// namespace and every node, which is the behaviour before scoping existed.
type Scope struct {
	// Namespaces is the set of namespaces whose pods are watched. Empty means
	// all namespaces.
	Namespaces []string
	// NodeSelector is a label selector (kubectl syntax) that limits the nodes
	// the orchestrator sees. Empty means all nodes.
	NodeSelector string
}

// ParseScope validates the --watch-namespaces (comma-separated) and
// --node-selector flag values. Whitespace around entries is ignored and
// duplicate namespaces are dropped.
func ParseScope(watchNamespaces, nodeSelector string) (Scope, error) {
	var scope Scope
	seen := map[string]bool{}
	for _, ns := range strings.Split(watchNamespaces, ",") {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			continue
		}
		if errs := validation.IsDNS1123Label(ns); len(errs) > 0 {
			return Scope{}, fmt.Errorf("invalid namespace %q in --watch-namespaces: %s", ns, strings.Join(errs, "; "))
		}
		if !seen[ns] {
			seen[ns] = true
			scope.Namespaces = append(scope.Namespaces, ns)
		}
	}
	sort.Strings(scope.Namespaces)

	nodeSelector = strings.TrimSpace(nodeSelector)
	if nodeSelector != "" {
		sel, err := labels.Parse(nodeSelector)
		if err != nil {
			return Scope{}, fmt.Errorf("invalid --node-selector %q: %w", nodeSelector, err)
		}
		if sel.Empty() {
			nodeSelector = ""
		} else {
			nodeSelector = sel.String()
		}
	}
	scope.NodeSelector = nodeSelector
	return scope, nil
}

// AllNamespaces reports whether pods are watched in every namespace.
func (s Scope) AllNamespaces() bool { return len(s.Namespaces) == 0 }

// AllNodes reports whether every node is watched.
func (s Scope) AllNodes() bool { return s.NodeSelector == "" }

// InformerFactories are the shared informer factories built for a Scope. The
// caller starts them.
type InformerFactories struct {
	// Nodes lists only nodes matching the Scope's NodeSelector.
	Nodes informers.SharedInformerFactory
	// Pods holds one factory per watched namespace, or a single cluster-wide
	// factory when every namespace is watched.
	Pods []informers.SharedInformerFactory
}

// NewInformerFactories returns the node informer factory and one pod informer
// factory per watched namespace (a single cluster-wide factory when every
// namespace is watched). The node factory lists only nodes matching
// NodeSelector; the pod factories list only pods carrying PodLabelKey, as
// before scoping existed. The caller starts the factories.
func (s Scope) NewInformerFactories(client kubernetes.Interface, resync time.Duration) InformerFactories {
	nodeOpts := []informers.SharedInformerOption{}
	if !s.AllNodes() {
		sel := s.NodeSelector
		nodeOpts = append(nodeOpts, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = sel
		}))
	}
	nodeFactory := informers.NewSharedInformerFactoryWithOptions(client, resync, nodeOpts...)

	podTweak := informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.LabelSelector = PodLabelKey
	})
	if s.AllNamespaces() {
		return InformerFactories{
			Nodes: nodeFactory,
			Pods:  []informers.SharedInformerFactory{informers.NewSharedInformerFactoryWithOptions(client, resync, podTweak)},
		}
	}
	podFactories := make([]informers.SharedInformerFactory, 0, len(s.Namespaces))
	for _, ns := range s.Namespaces {
		podFactories = append(podFactories,
			informers.NewSharedInformerFactoryWithOptions(client, resync, podTweak, informers.WithNamespace(ns)))
	}
	return InformerFactories{Nodes: nodeFactory, Pods: podFactories}
}
