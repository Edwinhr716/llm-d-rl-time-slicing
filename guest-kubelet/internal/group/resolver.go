package group

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Result is one resolution of the host node's labels.
type Result struct {
	Groups []string // zero or one element
	Reason string
}

// Group returns the resolved group, or false when there is none.
func (r Result) Group() (string, bool) {
	if len(r.Groups) != 1 {
		return "", false
	}
	return r.Groups[0], true
}

func (r Result) equal(o Result) bool {
	return r.Reason == o.Reason && strings.Join(r.Groups, ",") == strings.Join(o.Groups, ",")
}

// Resolver watches the host Node and keeps its current group. At the first sight of the node
// and on every label change it writes one line:
//
//	msg="group resolved" host=<node> groups=<comma list> reason=<reason>
//
// (logfmt, not the process's JSON log, so the line reads the same in every workstream).
type Resolver struct {
	host string
	out  io.Writer
	now  func() time.Time
	// OnChange is called after the first resolution and whenever the result changes.
	OnChange func(Result)

	mu     sync.RWMutex
	seen   bool
	labels map[string]string
	cur    Result
}

// NewResolver returns a resolver for host. Lines go to out.
func NewResolver(host string, out io.Writer) *Resolver {
	return &Resolver{
		host: host, out: out, now: time.Now,
		cur: Result{Reason: "not-yet-observed"},
	}
}

// Current returns the latest result. Before the host node is seen it has no group.
func (r *Resolver) Current() Result {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return Result{Groups: append([]string(nil), r.cur.Groups...), Reason: r.cur.Reason}
}

// Observe resolves the host node's labels. The informer calls it; tests call it directly.
func (r *Resolver) Observe(labels map[string]string) {
	groups, reason := FromNodeLabels(labels)
	r.set(labels, Result{Groups: groups, Reason: reason}, false)
}

// Gone records that the host node was deleted: no group until it comes back.
func (r *Resolver) Gone() { r.set(nil, Result{Reason: ReasonHostNodeGone}, true) }

func (r *Resolver) set(labels map[string]string, res Result, force bool) {
	r.mu.Lock()
	first := !r.seen
	labelsChanged := first || force || !maps.Equal(labels, r.labels)
	changed := first || !res.equal(r.cur)
	r.seen, r.labels, r.cur = true, maps.Clone(labels), res
	cb := r.OnChange
	r.mu.Unlock()
	if labelsChanged {
		_, _ = fmt.Fprintf(r.out, "time=%s level=INFO msg=\"group resolved\" host=%s groups=%s reason=%s\n",
			r.now().UTC().Format(time.RFC3339Nano), r.host, strings.Join(res.Groups, ","), res.Reason)
	}
	if changed && cb != nil {
		cb(res)
	}
}

// Start watches the host Node (a watch on this one object; events arrive in under a second,
// with a 30 s resync as backstop) and blocks until the first list is in.
func (r *Resolver) Start(ctx context.Context, client kubernetes.Interface) error {
	factory := informers.NewSharedInformerFactoryWithOptions(client, 30*time.Second,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.FieldSelector = fields.OneTermEqualSelector("metadata.name", r.host).String()
		}))
	inf := factory.Core().V1().Nodes().Informer()
	// The field selector already limits the watch to the host; the name checks keep a
	// client that ignores field selectors (the fake clientset) correct too.
	observe := func(obj any) {
		if n, ok := obj.(*corev1.Node); ok && n.Name == r.host {
			r.Observe(n.Labels)
		}
	}
	_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    observe,
		UpdateFunc: func(_, obj any) { observe(obj) },
		DeleteFunc: func(obj any) {
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if n, ok := obj.(*corev1.Node); ok && n.Name == r.host {
				r.Gone()
			}
		},
	})
	if err != nil {
		return err
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		return fmt.Errorf("host node informer did not sync")
	}
	r.mu.RLock()
	seen := r.seen
	r.mu.RUnlock()
	if !seen {
		r.Gone() // the host node does not exist
	}
	return nil
}
