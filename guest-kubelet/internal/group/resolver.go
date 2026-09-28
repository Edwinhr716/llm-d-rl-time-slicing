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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

// EventGroupUnresolved is recorded on the virtual Node whenever the host resolves to no group.
const EventGroupUnresolved = "GroupUnresolved"

// Resolver watches the host Node and keeps its resolved group current.
type Resolver struct {
	Host        string // real node name
	VirtualNode string // where GroupUnresolved events go
	Recorder    record.EventRecorder
	Out         io.Writer // receives the "group resolved" lines (stderr in main)
	Resync      time.Duration

	mu     sync.RWMutex
	seen   bool
	labels map[string]string
	groups []string
	reason Reason
}

// Start watches the host Node (informer with a field selector on its name) and blocks until the
// first list is in. It logs the starting resolution, so a host that is missing or unlabelled at
// start is reported at once.
func (r *Resolver) Start(ctx context.Context, client kubernetes.Interface) error {
	resync := r.Resync
	if resync == 0 {
		resync = 30 * time.Second
	}
	f := informers.NewSharedInformerFactoryWithOptions(client, resync,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.FieldSelector = fields.OneTermEqualSelector("metadata.name", r.Host).String()
		}))
	inf := f.Core().V1().Nodes().Informer()
	if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.observeNode(obj) },
		UpdateFunc: func(_, obj any) { r.observeNode(obj) },
		DeleteFunc: func(any) { r.Observe(nil, ReasonHostNotFound) },
	}); err != nil {
		return fmt.Errorf("watch host node: %w", err)
	}
	f.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		return fmt.Errorf("host node informer did not sync")
	}
	r.mu.RLock()
	seen := r.seen
	r.mu.RUnlock()
	if !seen {
		r.Observe(nil, ReasonHostNotFound)
	}
	return nil
}

func (r *Resolver) observeNode(obj any) {
	n, ok := obj.(*corev1.Node)
	if !ok || n.Name != r.Host {
		return
	}
	r.Observe(n.Labels, "")
}

// Observe records the host's labels. On the first call and on every label change it writes the
// "group resolved" line; when nothing resolves it also records GroupUnresolved on the virtual
// Node. reasonOverride replaces FromNodeLabels' reason (used when the host is gone).
func (r *Resolver) Observe(labels map[string]string, reasonOverride Reason) {
	groups, reason := FromNodeLabels(Source, labels)
	if reasonOverride != "" {
		reason = reasonOverride
	}
	r.mu.Lock()
	if r.seen && maps.Equal(r.labels, labels) && r.reason == reason {
		r.mu.Unlock()
		return // a resync or a status-only update
	}
	r.seen, r.labels, r.groups, r.reason = true, maps.Clone(labels), groups, reason
	r.mu.Unlock()

	if r.Out != nil {
		_, _ = fmt.Fprintln(r.Out, Line(time.Now(), r.Host, groups, reason))
	}
	if len(groups) == 0 && r.Recorder != nil {
		ref := &corev1.ObjectReference{Kind: "Node", Name: r.VirtualNode, UID: types.UID(r.VirtualNode)}
		r.Recorder.Eventf(ref, corev1.EventTypeWarning, EventGroupUnresolved,
			"host node %s resolves to no single group (%s, source %s); no mirror will be created", r.Host, reason, Source)
	}
}

// Group returns the resolved group and the reason; the group is "" when there is none. Callers check it
// just before each mirror create.
func (r *Resolver) Group() (string, Reason) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.seen {
		return "", ReasonNotStarted
	}
	return One(r.groups), r.reason
}

// Line is the "group resolved" log line, logfmt, so it can be grepped as msg="group resolved"
// and groups=<list> (empty when unresolved).
func Line(t time.Time, host string, groups []string, reason Reason) string {
	return fmt.Sprintf(`time=%s level=INFO msg="group resolved" host=%s groups=%s source=%s reason=%s`,
		t.UTC().Format(time.RFC3339Nano), host, strings.Join(groups, ","), Source, reason)
}
