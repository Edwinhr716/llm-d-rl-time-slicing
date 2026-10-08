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

package infrastructure //nolint:testpackage // O3 eval hook: callers need the unexported lookup methods.

import (
	"context"
	"os"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// evalOrchestrator returns an orchestrator over cs whose node and pod informers
// have synced, for tests that call getNodesForGroup, getGroupsFromNode and
// getPodsForGroup directly. The node group label mode comes from ZZ_MODE
// (prefix when unset). The informers stop when ctx is done.
func evalOrchestrator(ctx context.Context, cs kubernetes.Interface) (*KubernetesOrchestrator, error) {
	mode := os.Getenv("ZZ_MODE")
	if mode == "" {
		mode = NodeGroupLabelsPrefix
	}
	return newTestOrchestrator(ctx, cs, mode)
}

// newTestOrchestrator is evalOrchestrator with an explicit mode; "" leaves it unset.
func newTestOrchestrator(ctx context.Context, cs kubernetes.Interface, mode string) (*KubernetesOrchestrator, error) {
	factory := informers.NewSharedInformerFactory(cs, 0)
	k := NewKubernetesOrchestrator(
		factory.Core().V1().Nodes(),
		factory.Core().V1().Pods(),
		store.NewGroupStore(store.NewMemLockStore()),
		store.NewJobStore(),
		store.NewGRPCSnapshotAgentStore(0, 0),
	)
	if mode != "" { // "" leaves the mode unset, which is prefix
		if err := k.SetNodeGroupLabels(mode); err != nil {
			return nil, err
		}
	}
	factory.Start(ctx.Done())
	if err := k.Init(ctx); err != nil {
		return nil, err
	}
	return k, nil
}
