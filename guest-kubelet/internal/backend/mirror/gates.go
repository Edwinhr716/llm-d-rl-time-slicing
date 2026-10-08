package mirror

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
)

// Readiness gates for lead decision D-VK-5 option c, the only option that admits guests with
// readinessGates. The kubelet leaves readiness-gate conditions to whoever owns them and folds
// them into Ready. The guest kubelet writes the guest's whole status through the
// virtual-kubelet library, from a translation that may predate the gate owner's last write, so
// two things are needed:
//   - GateKeepingClient: every guest status write takes the gate conditions from the API's
//     current copy (and its resourceVersion) and recomputes Ready from them, so a write never
//     reverts a gate and Ready is never true while a gate is not;
//   - WatchReadinessGates: a gate change re-translates the guest at once, so Ready follows a
//     gate without waiting for a mirror event or a resync.

// ReasonReadinessGatesNotReady is the Ready condition's reason while a gate is not true.
const ReasonReadinessGatesNotReady = "ReadinessGatesNotReady"

// GateKeepingClient wraps a client so that pod status updates of pods with readinessGates keep
// the API's current gate conditions (see KeepReadinessGates). Give it to the virtual-kubelet
// library, which writes the guests' status; everything else passes through.
func GateKeepingClient(client kubernetes.Interface) kubernetes.Interface {
	return gateClient{Interface: client}
}

type gateClient struct{ kubernetes.Interface }

func (g gateClient) CoreV1() corev1client.CoreV1Interface {
	return gateCore{CoreV1Interface: g.Interface.CoreV1()}
}

type gateCore struct{ corev1client.CoreV1Interface }

func (g gateCore) Pods(namespace string) corev1client.PodInterface {
	return gatePods{PodInterface: g.CoreV1Interface.Pods(namespace)}
}

type gatePods struct{ corev1client.PodInterface }

// UpdateStatus reads the pod, keeps its gate conditions and writes at its resourceVersion: a
// gate written before the read is kept, one written after it makes the write conflict, and the
// library retries.
//
//nolint:gocritic // hugeParam: PodInterface takes UpdateOptions by value
func (g gatePods) UpdateStatus(ctx context.Context, pod *corev1.Pod, opts metav1.UpdateOptions) (*corev1.Pod, error) {
	if len(pod.Spec.ReadinessGates) == 0 {
		return g.PodInterface.UpdateStatus(ctx, pod, opts)
	}
	cur, err := g.Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read guest before status write: %w", err)
	}
	if cur.UID != pod.UID {
		return g.PodInterface.UpdateStatus(ctx, pod, opts) // a different pod now: let the server refuse it
	}
	out := pod.DeepCopy()
	out.ResourceVersion = cur.ResourceVersion
	KeepReadinessGates(&out.Status, cur.Status.Conditions, out.Spec.ReadinessGates)
	return g.PodInterface.UpdateStatus(ctx, out, opts)
}

// KeepReadinessGates replaces the gate conditions in st with those in current (dropping a gate
// current does not have) and recomputes Ready as the kubelet does: true only when
// ContainersReady is true, the pod runs and every gate condition is true. A Ready condition
// keeps its transition time while its status does not change.
func KeepReadinessGates(st *corev1.PodStatus, current []corev1.PodCondition, gates []corev1.PodReadinessGate) {
	isGate := map[corev1.PodConditionType]bool{}
	for _, gate := range gates {
		isGate[gate.ConditionType] = true
	}
	conds := make([]corev1.PodCondition, 0, len(st.Conditions)+len(gates))
	for i := range st.Conditions {
		if !isGate[st.Conditions[i].Type] {
			conds = append(conds, st.Conditions[i])
		}
	}
	allGates := true
	for _, gate := range gates {
		cond := findCondition(current, gate.ConditionType)
		if cond == nil {
			allGates = false
			continue
		}
		conds = append(conds, *cond)
		if cond.Status != corev1.ConditionTrue {
			allGates = false
		}
	}
	st.Conditions = conds
	ready := findCondition(st.Conditions, corev1.PodReady)
	if ready == nil {
		return
	}
	cr := findCondition(st.Conditions, corev1.ContainersReady)
	want := corev1.ConditionFalse
	if cr != nil && cr.Status == corev1.ConditionTrue && allGates && st.Phase == corev1.PodRunning {
		want = corev1.ConditionTrue
	}
	if ready.Status == want {
		return
	}
	ready.Status, ready.Reason, ready.Message = want, "", ""
	if want == corev1.ConditionFalse {
		ready.Reason = ReasonReadinessGatesNotReady
		if allGates && cr != nil {
			ready.Reason, ready.Message = cr.Reason, cr.Message
		}
	}
	ready.LastTransitionTime = metav1.Now()
	if prev := findCondition(current, corev1.PodReady); prev != nil && prev.Status == want {
		ready.LastTransitionTime = prev.LastTransitionTime
	}
}

// WatchReadinessGates re-translates a guest bound to the virtual node whenever one of its
// readiness-gate conditions changes. It returns once its informer has synced; the watch runs
// until ctx ends.
func (b *Backend) WatchReadinessGates(ctx context.Context) error {
	f := informers.NewSharedInformerFactoryWithOptions(b.client, b.opts.Resync,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", b.opts.VirtualNode).String()
		}))
	inf := f.Core().V1().Pods().Informer()
	if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj any) {
			oldPod, ok1 := oldObj.(*corev1.Pod)
			newPod, ok2 := newObj.(*corev1.Pod)
			if ok1 && ok2 && GatesChanged(oldPod, newPod) {
				b.refreshGuest(newPod)
			}
		},
	}); err != nil {
		return fmt.Errorf("readiness gate watch: %w", err)
	}
	f.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		return fmt.Errorf("readiness gate informer did not sync")
	}
	return nil
}

// GatesChanged reports whether any readiness-gate condition of the pod differs between two
// versions of it (status or presence).
func GatesChanged(oldPod, newPod *corev1.Pod) bool {
	for _, gate := range newPod.Spec.ReadinessGates {
		before := findCondition(oldPod.Status.Conditions, gate.ConditionType)
		after := findCondition(newPod.Status.Conditions, gate.ConditionType)
		if (before == nil) != (after == nil) || (before != nil && before.Status != after.Status) {
			return true
		}
	}
	return false
}

// refreshGuest is Refresh from a guest object in hand, which may be newer than the library's
// lister.
func (b *Backend) refreshGuest(guest *corev1.Pod) {
	if m, ok := b.mirrorOf(guest); ok {
		b.emit(b.translate(guest, m))
	}
}
