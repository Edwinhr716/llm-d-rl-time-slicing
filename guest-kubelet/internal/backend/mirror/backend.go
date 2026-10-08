package mirror

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
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

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
	"github.com/edwinhr716/guest-kubelet/internal/group"
)

// Options configures the backend beyond the pure builder config.
type Options struct {
	Config
	// OrphanGrace is how long a mirror whose guest is gone is kept for a guest re-created with
	// the same name (StatefulSet, LWS, a re-applied bare pod) to re-adopt. Only matters without
	// OwnerRef; with it, the garbage collector deletes orphans first.
	OrphanGrace time.Duration
	// Resync is the mirror informer's resync period.
	Resync time.Duration
	// Group returns the host node's current group (internal/group). When set, a mirror is
	// created only while it resolves to exactly one group, and carries that group; otherwise
	// Create fails (the library retries) and OnUnresolved is called. Nil: no group gating.
	Group func() group.Result
	// OnUnresolved is told about each create refused for lack of a group.
	OnUnresolved func(guest *corev1.Pod, reason string)
	// Gated turns on host-command mode (D-NS-4 ns-push-vk): mirrors are created only by the
	// host command server (internal/hostcmd), carry the background contract labels, and a guest
	// is held NotReady until the server releases it (after a Resume and the engine check) and
	// again before each Suspend. Off, the guest's Ready follows its mirror as in M1.
	Gated bool
	// Prober runs the guests' readinessProbes and supplies the ready flags (M2). Nil keeps the
	// M1 behaviour: the mirror's ready flags, which mean only "running", are copied.
	Prober Prober
	// Suspend configures suspend and resume (M3). Its zero value disables them.
	Suspend SuspendOptions
	// GPUDonorSelector selects donor pods on the host (label selector, --gpu-donor-selector).
	// Empty means DefaultGPUDonorSelector.
	GPUDonorSelector string
	// Recorder records GPUUnavailable and DonorGone on guests. Optional.
	Recorder record.EventRecorder
	// FenceInterval is how often the fence is re-checked besides donor and mirror events.
	FenceInterval time.Duration
	// OnDonorChange is told about every add, update or delete of a donor pod on the host, so
	// the no-donor cordon reacts at once. It must not block. Optional.
	OnDonorChange func()
}

// Prober is the readiness prober the backend drives (internal/probe implements it).
type Prober interface {
	Readiness
	// Sync starts or stops the guest's probe workers for the mirror's current state.
	Sync(guest, mirror *corev1.Pod)
	// Forget drops everything held for a guest whose mirror is gone.
	Forget(uid types.UID, namespace, name string)
}

// ErrGroupUnresolved is returned by Create while the host node resolves to no group.
var ErrGroupUnresolved = errors.New("no mirror: the host node's group is unresolved")

// Backend creates, watches and deletes mirror pods. It keeps no state of its own that matters
// across a restart: the mirrors in the API are the state, found again through the informer.
type Backend struct {
	client kubernetes.Interface
	opts   Options
	guests corev1listers.PodLister // pods bound to the virtual node (the library's lister)

	factory informers.SharedInformerFactory
	mirrors corev1listers.PodLister
	synced  cache.InformerSynced

	// Donor pods on the host, and the fence (fence.go).
	donorFactory informers.SharedInformerFactory
	donors       corev1listers.PodLister
	donorsSynced cache.InformerSynced
	fenceKick    chan struct{}
	// attachMu serializes the capacity check and mirror create, so two guests never take the
	// same GPUs. pooledAssigned covers the gap until the mirror informer has a just-created
	// mirror, per guest.
	attachMu       sync.Mutex
	pooledAssigned map[types.UID]pooledAssignment

	mu          sync.Mutex
	onStatus    func(*corev1.Pod) // the library's notify callback, wrapped by the provider
	orphanSince map[types.UID]time.Time
	gate        gateState  // host-command mode only; guarded by mu
	locks       guestLocks // one suspend, resume or agent call per guest at a time
	// resumeMu resumes one guest at a time (Q6); hostMu runs one host-level call at a time.
	resumeMu, hostMu sync.Mutex
	// Guarded by mu: kill messages of guests whose mirror the kill sequence is removing, the
	// mirrors created per guest (the job id attempt, M4 mode), the last host epoch and the agent Status.
	killed    map[types.UID]string
	killing   map[types.UID]string // causes of kill sequences that have not recorded the kill yet
	attempts  map[types.UID]int
	hostEpoch int64
	agent     agentView

	fenceKnown bool // fenceState is what the node's fence taint was last seen or written as
	fenceState bool
}

// New builds the backend. guests must list the pods bound to the virtual node.
func New(client kubernetes.Interface, guests corev1listers.PodLister, options *Options) *Backend {
	opts := *options
	if opts.Resync == 0 {
		opts.Resync = 30 * time.Second
	}
	if opts.GPUDonorSelector == "" {
		opts.GPUDonorSelector = DefaultGPUDonorSelector
	}
	if opts.FenceInterval == 0 {
		opts.FenceInterval = 2 * time.Second
	}
	// Like an LWS controller's Owns() watch, but filtered by label, not ownerRef: mirrors
	// must still be found when they have no owner (OwnerRef=false, or the guest is gone).
	f := informers.NewSharedInformerFactoryWithOptions(client, opts.Resync,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.LabelSelector = labels.Set{LabelMirrorNode: opts.VirtualNode}.String()
		}))
	inf := f.Core().V1().Pods()
	b := &Backend{
		client: client, opts: opts, guests: guests, factory: f,
		mirrors: inf.Lister(), synced: inf.Informer().HasSynced,
		orphanSince: map[types.UID]time.Time{},
		gate:        newGateState(),
		killed:      map[types.UID]string{},
		killing:     map[types.UID]string{},
		attempts:    map[types.UID]int{},

		pooledAssigned: map[types.UID]pooledAssignment{},
	}
	_, _ = inf.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { b.mirrorChanged(obj) },
		UpdateFunc: func(_, obj any) { b.mirrorChanged(obj) },
		DeleteFunc: b.mirrorDeleted,
	})
	b.watchDonors(inf.Informer())
	return b
}

// watchDonors sets up the donor informer (pods on the host that match the
// selector, in every namespace) and kicks the fence on every donor or mirror change.
func (b *Backend) watchDonors(mirrors cache.SharedIndexInformer) {
	opts := &b.opts
	df := informers.NewSharedInformerFactoryWithOptions(b.client, opts.Resync,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.LabelSelector = opts.GPUDonorSelector
			lo.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", opts.HostNode).String()
		}))
	dinf := df.Core().V1().Pods()
	b.donorFactory, b.donors, b.donorsSynced = df, dinf.Lister(), dinf.Informer().HasSynced
	b.fenceKick = make(chan struct{}, 1)
	kick := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { b.pokeFence() },
		UpdateFunc: func(any, any) { b.pokeFence() },
		DeleteFunc: func(any) { b.pokeFence() },
	}
	for _, informer := range []cache.SharedIndexInformer{dinf.Informer(), mirrors} {
		if _, err := informer.AddEventHandler(kick); err != nil {
			log.L.WithError(err).Warn("fence: could not watch pods; the fence runs on its interval only")
		}
	}
	if f := opts.OnDonorChange; f != nil {
		donorPoke := cache.ResourceEventHandlerFuncs{
			AddFunc:    func(any) { f() },
			UpdateFunc: func(any, any) { f() },
			DeleteFunc: func(any) { f() },
		}
		if _, err := dinf.Informer().AddEventHandler(donorPoke); err != nil {
			log.L.WithError(err).Warn("could not watch donor pods; the no-donor cordon runs on its poll only")
		}
	}
}

// Start runs the informer and blocks until it has synced. Call it before the pod controller
// starts, so the first GetPod after a restart already sees every existing mirror: that is
// what stops the library from calling CreatePod again for a guest that already runs.
func (b *Backend) Start(ctx context.Context) error {
	b.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), b.synced) {
		return fmt.Errorf("mirror informer did not sync")
	}
	if b.donorFactory != nil {
		// The fence must not run before the donor cache is full: an empty cache would make
		// every donor look gone and stop every mirror.
		b.donorFactory.Start(ctx.Done())
		if !cache.WaitForCacheSync(ctx.Done(), b.donorsSynced) {
			return fmt.Errorf("donor informer did not sync")
		}
		go b.fenceLoop(ctx)
	}
	go b.orphanLoop(ctx)
	if b.opts.Suspend.Agent != nil && b.opts.Suspend.AgentStatusPoll > 0 {
		go b.agentStatusLoop(ctx)
	}
	return nil
}

// SetStatusCallback installs the function that receives translated guest pods.
func (b *Backend) SetStatusCallback(cb func(*corev1.Pod)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onStatus = cb
}

func (b *Backend) emit(p *corev1.Pod) {
	b.mu.Lock()
	cb := b.onStatus
	b.mu.Unlock()
	if cb != nil {
		cb(p)
	}
}

// guestFor returns the live guest a mirror belongs to, or nil if it is orphaned.
func (b *Backend) guestFor(m *corev1.Pod) *corev1.Pod {
	g, err := b.guests.Pods(m.Namespace).Get(m.Annotations[AnnotationGuestName])
	if err != nil || string(g.UID) != m.Labels[LabelMirrorOf] {
		return nil
	}
	return g
}

func (b *Backend) mirrorChanged(obj any) {
	m, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	g := b.guestFor(m)
	if g == nil {
		return
	}
	if b.opts.Prober != nil {
		b.opts.Prober.Sync(g, m)
	}
	b.emit(b.translate(g, m))
}

// translateProbed is TranslateStatusWith the configured prober, if any. A guest whose mirror the
// kill sequence removes is Failed from the kill on (M4), in either mode.
func (b *Backend) translateProbed(guest, m *corev1.Pod) *corev1.Pod {
	if st := b.killedStatus(guest, m); st != nil {
		return st // Failed from the kill on, while the mirror still terminates
	}
	if b.opts.Prober == nil {
		return TranslateStatus(guest, m)
	}
	return TranslateStatusWith(guest, m, b.opts.Prober)
}

// Refresh re-translates a guest's status and hands it to the pod controller. The prober calls
// it when a readiness verdict changes, which no mirror event would report.
func (b *Backend) Refresh(namespace, name string) {
	g, err := b.guests.Pods(namespace).Get(name)
	if err != nil {
		return
	}
	if m, ok := b.mirrorOf(g); ok {
		b.emit(b.translate(g, m))
	}
}

func (b *Backend) mirrorDeleted(obj any) {
	mirrorPod, ok := obj.(*corev1.Pod)
	if !ok {
		tomb, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		if mirrorPod, ok = tomb.Obj.(*corev1.Pod); !ok {
			return
		}
	}
	b.forgetProbes(mirrorPod)
	g := b.guestFor(mirrorPod)
	if g == nil {
		b.forgetKilled(types.UID(mirrorPod.Labels[LabelMirrorOf]))
		return
	}
	if st := b.killedStatus(g, mirrorPod); st != nil && g.DeletionTimestamp == nil {
		b.emit(st)
		return // the kill record stays: the guest must keep showing Failed
	}
	// The annotation covers a restart between the delete and this event (M5).
	if vacated := b.takeVacated(g.UID); vacated || mirrorPod.Annotations[AnnotationVacated] == "true" {
		b.emit(VacatedStatus(g))
		return
	}
	if g.DeletionTimestamp == nil {
		b.emit(TerminalStatus(g, mirrorPod, ReasonMirrorDeleted))
		return
	}
	b.forgetKilled(g.UID)
	b.emit(TerminalStatus(g, mirrorPod, ReasonGuestDeleted))
	go b.finishGuestDeletion(context.Background(), g)
}

// forgetProbes drops the prober's state for the guest of a deleted mirror, found from the mirror
// alone: the guest may be gone too.
func (b *Backend) forgetProbes(mirrorPod *corev1.Pod) {
	if b.opts.Prober != nil {
		uid := types.UID(mirrorPod.Labels[LabelMirrorOf])
		b.opts.Prober.Forget(uid, mirrorPod.Namespace, mirrorPod.Annotations[AnnotationGuestName])
	}
}

// finishGuestDeletion removes a guest whose containers have stopped, as the real kubelet's
// status manager does (delete with grace 0 once the pod is terminal). The library would do it
// only after the full grace period: it re-syncs a pod on spec or metadata changes, not on the
// status change that says the containers are gone.
func (b *Backend) finishGuestDeletion(ctx context.Context, g *corev1.Pod) {
	// Give the library's status write a moment, so the guest's last status is the terminal one.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cur, err := b.guests.Pods(g.Namespace).Get(g.Name)
		if err != nil || cur.UID != g.UID {
			return // already gone
		}
		if cur.Status.Phase == corev1.PodSucceeded || cur.Status.Phase == corev1.PodFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	uid := g.UID
	err := b.client.CoreV1().Pods(g.Namespace).Delete(ctx, g.Name, metav1.DeleteOptions{
		GracePeriodSeconds: new(int64),
		Preconditions:      &metav1.Preconditions{UID: &uid},
	})
	if err == nil {
		log.G(ctx).WithField("guest", g.Namespace+"/"+g.Name).Info("guest removed after its mirror stopped")
	} else if !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		log.G(ctx).WithError(err).WithField("guest", g.Namespace+"/"+g.Name).Warn("could not remove guest; the library will after the grace period")
	}
}

// mirrorOf returns the mirror currently serving this guest (same name, same guest UID).
func (b *Backend) mirrorOf(guest *corev1.Pod) (*corev1.Pod, bool) {
	m, err := b.mirrors.Pods(guest.Namespace).Get(Name(guest.Name))
	if err != nil || m.Labels[LabelMirrorOf] != string(guest.UID) {
		return nil, false
	}
	return m, true
}

// Get returns the guest with its translated status, or errdefs.NotFound when no mirror serves
// it. The library calls this before every create/update; after a restart it is the re-adoption
// point: an existing mirror means "already running", so there is no second CreatePod.
func (b *Backend) Get(namespace, name string) (*corev1.Pod, error) {
	g, err := b.guests.Pods(namespace).Get(name)
	if err != nil {
		return nil, errdefs.NotFoundf("guest %s/%s not in the lister", namespace, name)
	}
	m, ok := b.mirrorOf(g)
	if !ok {
		return nil, errdefs.NotFoundf("no mirror for guest %s/%s", namespace, name)
	}
	return b.translate(g, m), nil
}

// List returns every guest that has a mirror. At startup the library deletes any of these
// whose guest the API no longer has; orphans are left to orphanLoop instead.
func (b *Backend) List() ([]*corev1.Pod, error) {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	var out []*corev1.Pod
	for _, m := range ms {
		if g := b.guestFor(m); g != nil {
			out = append(out, b.translate(g, m))
		}
	}
	return out, nil
}

// Create builds and creates the mirror. It is idempotent: an existing mirror for this guest is
// fine; an orphaned mirror with the same name and the same containers is adopted.
func (b *Backend) Create(ctx context.Context, guest *corev1.Pod) error {
	cfg := b.buildConfig(guest) // host-command mode: background contract and attempt
	if b.opts.Group != nil {
		res := b.opts.Group()
		g, ok := res.Group()
		if !ok {
			// Fail closed: no mirror runs for a group the node does not clearly yield to.
			if b.opts.OnUnresolved != nil {
				b.opts.OnUnresolved(guest, res.Reason)
			}
			// The error becomes the guest's status.message: keep the reason code, drop the
			// group names some reasons carry (they embed the owner namespace).
			code, _, _ := strings.Cut(res.Reason, ":")
			return fmt.Errorf("%w: host %s: %s", ErrGroupUnresolved, cfg.HostNode, code)
		}
		cfg.Group = g
	}
	if b.opts.Suspend.Agent != nil {
		// Q6: admit a guest only while one restore plus the sum of checkpoints fits N - K.
		if _, exists := b.mirrorOf(guest); !exists {
			if err := b.admit(guest); err != nil {
				return err
			}
		}
		if !b.opts.Gated {
			// M4 without host commands: the job id attempt is counted here (host-command
			// mode counts it in buildConfig).
			cfg.Background = true
			b.mu.Lock()
			cfg.Attempt = b.attempts[guest.UID]
			b.mu.Unlock()
		}
	}
	var att *GPUAttachment
	if RequestsGPU(guest) {
		// D-NS-10: the donor's own GPUs through the pooled shadow resource.
		b.attachMu.Lock()
		defer b.attachMu.Unlock()
		if m, ok := b.mirrorOf(guest); ok {
			b.emit(b.translate(guest, m)) // already attached (a retry, or a restart)
			return nil
		}
		var err error
		if att, err = b.attachPooled(ctx, guest); err != nil {
			return err // fail closed: no mirror; the library retries the create
		}
	}
	want, err := BuildWithGPU(guest, &cfg, att)
	if err != nil {
		return errdefs.AsInvalidInput(err)
	}
	logger := log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).WithField("mirror", want.Name)

	mirrorPod, err := b.client.CoreV1().Pods(guest.Namespace).Create(ctx, want, metav1.CreateOptions{})
	switch {
	case err == nil:
		b.created(logger, guest, mirrorPod, cfg.Background)
	case apierrors.IsAlreadyExists(err):
		mirrorPod, err = b.adoptOrReplace(ctx, guest, want)
		if apierrors.IsNotFound(err) {
			// The clashing mirror was gone by the time it was read (a delete that just
			// finished, for example a vacate's): create again, once.
			mirrorPod, err = b.createAgain(ctx, logger, guest, want, cfg.Background)
		}
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("create mirror: %w", err)
	}
	if att != nil {
		b.attached(ctx, guest, want, att)
	}
	b.emit(b.translate(guest, mirrorPod))
	return nil
}

// created counts a new mirror's attempt (a mirror created again for this guest gets a new job id)
// and logs it.
func (b *Backend) created(logger log.Logger, guest, mirrorPod *corev1.Pod, background bool) {
	if background && !b.opts.Gated {
		b.mu.Lock()
		b.attempts[guest.UID]++
		b.mu.Unlock()
	}
	logger.WithField("mirrorUID", mirrorPod.UID).WithField("job", mirrorPod.Labels[LabelJobID]).Info("mirror created")
	logResources(logger, ResourceSummary(guest, mirrorPod))
}

// createAgain is the one retry of a create whose name clash was gone by the time it was read.
func (b *Backend) createAgain(
	ctx context.Context, logger log.Logger, guest, want *corev1.Pod, background bool,
) (*corev1.Pod, error) {
	mirrorPod, err := b.client.CoreV1().Pods(guest.Namespace).Create(ctx, want, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create mirror again: %w", err)
	}
	b.created(logger, guest, mirrorPod, background)
	return mirrorPod, nil
}

// logResources is the "mirror resources" line: what the mirror requests and whether the
// static-mode cap applied.
func logResources(logger log.Logger, res *Resources) {
	logger.WithField("req_cpu", res.ReqCPU.String()).WithField("req_memory", res.ReqMemory.String()).
		WithField("lim_memory", res.LimMemory.String()).WithField("capped", res.Capped).
		Info("mirror resources")
}

// attached logs a GPU attach and remembers it until the mirror informer has the mirror.
// The caller holds attachMu.
func (b *Backend) attached(ctx context.Context, guest, mirror *corev1.Pod, att *GPUAttachment) {
	name := mirror.Namespace + "/" + mirror.Name
	log.G(ctx).WithField("mirror", name).WithField("mode", string(GPUModePooled)).
		WithField("attach", AttachShadowDevicePlugin).WithField("resource", string(api.PooledResource)).
		WithField("qty", att.Qty).WithField("donor", att.Donor).Info("mirror gpu attached")
	b.pooledAssigned[guest.UID] = pooledAssignment{mirror: name, qty: att.Qty, at: time.Now()}
}

// adoptOrReplace handles a name clash with an existing mirror.
func (b *Backend) adoptOrReplace(ctx context.Context, guest, want *corev1.Pod) (*corev1.Pod, error) {
	logger := log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name)
	cur, err := b.client.CoreV1().Pods(guest.Namespace).Get(ctx, want.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get existing mirror: %w", err)
	}
	if cur.Labels[LabelMirrorOf] == string(guest.UID) {
		return cur, nil // ours already (a retry, or a restart that raced the informer)
	}
	oldOwnerGone := cur.Labels[LabelMirrorOf] != "" && cur.Labels[LabelMirrorNode] == b.opts.VirtualNode &&
		b.guestFor(cur) == nil
	if oldOwnerGone && cur.DeletionTimestamp == nil && cur.Annotations[AnnotationGuestSpecHash] == SpecHash(guest) &&
		cur.Status.Phase != corev1.PodFailed && cur.Status.Phase != corev1.PodSucceeded {
		upd := cur.DeepCopy()
		upd.Labels[LabelMirrorOf] = string(guest.UID)
		if g, ok := want.Labels[LabelGroup]; ok {
			upd.Labels[LabelGroup] = g // the group now, not the one at the orphan's creation
		}
		upd.OwnerReferences = nil
		if b.opts.OwnerRef {
			upd.OwnerReferences = []metav1.OwnerReference{OwnerRef(guest)}
		}
		adopted, err := b.client.CoreV1().Pods(guest.Namespace).Update(ctx, upd, metav1.UpdateOptions{})
		if err != nil {
			return nil, fmt.Errorf("adopt mirror: %w", err)
		}
		b.mu.Lock()
		delete(b.orphanSince, cur.UID)
		b.mu.Unlock()
		logger.WithField("mirrorUID", adopted.UID).WithField("podIP", adopted.Status.PodIP).Warn("re-adopted orphaned mirror")
		return adopted, nil
	}
	// Not adoptable (different containers, not ours, or finished): remove it and let the
	// library retry the create.
	uid := cur.UID
	if err := b.client.CoreV1().Pods(guest.Namespace).Delete(ctx, cur.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	}); err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("delete stale mirror: %w", err)
	}
	return nil, fmt.Errorf("stale mirror %s/%s is being replaced; retrying", cur.Namespace, cur.Name)
}

// Delete deletes the guest's mirror with the guest's grace period, so the real kubelet sends
// SIGTERM and waits as it would for the guest. It returns errdefs.NotFound if there is none.
func (b *Backend) Delete(ctx context.Context, guest *corev1.Pod) error {
	mirrorPod, ok := b.mirrorOf(guest)
	if !ok {
		// Nothing runs, so report the guest terminated now; the library then removes it
		// without waiting out the grace period.
		b.emit(TerminalStatus(guest, nil, ReasonGuestDeleted))
		go b.finishGuestDeletion(context.Background(), guest)
		return errdefs.NotFoundf("no mirror for guest %s/%s", guest.Namespace, guest.Name)
	}
	b.killBeforeDelete(ctx, mirrorPod)
	uid := mirrorPod.UID
	opts := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
	if guest.DeletionGracePeriodSeconds != nil {
		opts.GracePeriodSeconds = guest.DeletionGracePeriodSeconds
	}
	err := b.client.CoreV1().Pods(mirrorPod.Namespace).Delete(ctx, mirrorPod.Name, opts)
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return errdefs.NotFoundf("mirror %s/%s already gone", mirrorPod.Namespace, mirrorPod.Name)
	}
	if err != nil {
		return fmt.Errorf("delete mirror: %w", err)
	}
	log.G(ctx).WithField("mirror", mirrorPod.Namespace+"/"+mirrorPod.Name).Info("mirror deleted")
	return nil
}

// orphanLoop deletes mirrors whose guest has been gone for longer than OrphanGrace. It is the
// safety net for OwnerRef=false; with OwnerRef the garbage collector gets there first.
func (b *Backend) orphanLoop(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.collectOrphans(ctx, time.Now())
		}
	}
}

func (b *Backend) collectOrphans(ctx context.Context, now time.Time) {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return
	}
	seen := map[types.UID]bool{}
	for _, m := range ms {
		if m.DeletionTimestamp != nil || b.guestFor(m) != nil {
			continue
		}
		seen[m.UID] = true
		b.mu.Lock()
		since, ok := b.orphanSince[m.UID]
		if !ok {
			b.orphanSince[m.UID] = now
			since = now
			log.G(ctx).WithField("mirror", m.Namespace+"/"+m.Name).WithField("podIP", m.Status.PodIP).
				Warn("mirror has no guest; keeping it for re-adoption until the grace period ends")
		}
		b.mu.Unlock()
		if now.Sub(since) < b.opts.OrphanGrace {
			continue
		}
		uid := m.UID
		err := b.client.CoreV1().Pods(m.Namespace).Delete(ctx, m.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		})
		if err == nil || apierrors.IsNotFound(err) {
			log.G(ctx).WithField("mirror", m.Namespace+"/"+m.Name).Warn("deleted orphaned mirror")
		}
	}
	b.mu.Lock()
	for uid := range b.orphanSince {
		if !seen[uid] {
			delete(b.orphanSince, uid)
		}
	}
	b.mu.Unlock()
}
