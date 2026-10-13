package provider

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// NodeProvider is the node half of the provider and the one path by which the VK changes its
// Node after registration. It holds the Node template. The virtual-kubelet NodeController keeps
// its own copy of the Node and, on every periodic update, writes that copy's status, labels and
// annotations; a change pushed through NotifyNodeStatus replaces the copy. So every change goes
// into the template first (Update) and is then pushed whole: a later periodic update writes the
// same values and never resets them.
//
// Users of this path: the guest budget refresh (--guest-budget=computed, SetCPUMemory). A
// cordon (D-NS-8) plugs in the same way: it patches spec.unschedulable on the API server and
// records the held value with Update, so the template, which is also what a re-registration
// creates (Node), always matches.
type NodeProvider struct {
	mu       sync.Mutex
	node     *corev1.Node
	notify   func(*corev1.Node)
	ctx      context.Context
	starters []func(context.Context)

	pushMu sync.Mutex // keeps pushes in template order
}

// NewNodeProvider returns a NodeProvider whose template is spec.
func NewNodeProvider(spec *corev1.Node) *NodeProvider {
	return &NodeProvider{node: spec.DeepCopy()}
}

// Node returns a copy of the current template: what to register, or re-register after a delete.
func (p *NodeProvider) Node() *corev1.Node {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.node.DeepCopy()
}

// OnStart registers f to run in its own goroutine once the NodeController has started (when it
// calls NotifyNodeStatus), with the controller's context. Call it before the controller runs.
func (p *NodeProvider) OnStart(f func(context.Context)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.starters = append(p.starters, f)
}

// Ping reports the node as healthy while the process is alive. The library calls it every 10 s
// and only writes node status if it succeeds.
func (p *NodeProvider) Ping(ctx context.Context) error { return ctx.Err() }

// NotifyNodeStatus is called once by the NodeController when it starts. It must not block.
func (p *NodeProvider) NotifyNodeStatus(ctx context.Context, cb func(*corev1.Node)) {
	p.mu.Lock()
	p.notify, p.ctx = cb, ctx
	starters := p.starters
	p.mu.Unlock()
	for _, f := range starters {
		go f(ctx)
	}
}

// Update applies mutate to the template. If mutate reports a change and the NodeController is
// running, the whole template is pushed to it; it then patches the Node's status (a three-way
// patch, so only the changed fields move). Update blocks until the controller takes the push or
// its context ends.
func (p *NodeProvider) Update(mutate func(*corev1.Node) bool) {
	p.pushMu.Lock()
	defer p.pushMu.Unlock()
	p.mu.Lock()
	if !mutate(p.node) {
		p.mu.Unlock()
		return
	}
	cp, cb, ctx := p.node.DeepCopy(), p.notify, p.ctx
	p.mu.Unlock()
	if cb == nil {
		return // not started: the controller copies the template when it starts
	}
	done := make(chan struct{})
	go func() { cb(cp); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// SetCPUMemory sets the Node's cpu and memory capacity and allocatable to the same values, and
// reports whether they changed. Nothing else in the Node is touched.
func (p *NodeProvider) SetCPUMemory(cpu, memory resource.Quantity) bool {
	changed := false
	p.Update(func(n *corev1.Node) bool {
		for _, list := range []corev1.ResourceList{n.Status.Capacity, n.Status.Allocatable} {
			for name, want := range map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: cpu, corev1.ResourceMemory: memory} {
				if cur, ok := list[name]; !ok || cur.Cmp(want) != 0 {
					list[name] = want.DeepCopy()
					changed = true
				}
			}
		}
		return changed
	})
	return changed
}

// SetGPUs sets the Node's nvidia.com/gpu capacity and allocatable to n and reports whether
// they changed (pooled mode: the virtual node advertises what the host's pooled shadow
// resource has allocatable).
func (p *NodeProvider) SetGPUs(n int64) bool {
	changed := false
	want := *resource.NewQuantity(n, resource.DecimalSI)
	p.Update(func(node *corev1.Node) bool {
		for _, list := range []corev1.ResourceList{node.Status.Capacity, node.Status.Allocatable} {
			if cur, ok := list[GPUResource]; !ok || cur.Cmp(want) != 0 {
				list[GPUResource] = want.DeepCopy()
				changed = true
			}
		}
		return changed
	})
	return changed
}
