package mirror

import (
	"context"
	"fmt"
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
)

// Options configures the backend beyond the pure builder config.
type Options struct {
	Config
	// ReserveClaim adds each GPU mirror to its claim's status.reservedFor. The scheduler does
	// this for pods it binds; a mirror skips the scheduler, and the kubelet refuses to prepare a
	// claim for a pod that is not in reservedFor.
	ReserveClaim bool
	// OrphanGrace is how long a mirror whose guest is gone is kept for a guest re-created with
	// the same name (StatefulSet, LWS, a re-applied bare pod) to re-adopt. Only matters without
	// OwnerRef; with it, the garbage collector deletes orphans first.
	OrphanGrace time.Duration
	// Resync is the mirror informer's resync period.
	Resync time.Duration

	// GPUMode is how guests' nvidia.com/gpu is attached (--gpu-mode). Empty means claim.
	// The fields below are for GPUModeDevicePlugin only.
	GPUMode GPUMode
	// DonorSelector selects donor pods on the host (label selector).
	DonorSelector string
	// Holders reads which pod holds which GPU on the host (the shadow plugin).
	Holders HoldersSource
	// Recorder records GPUUnavailable and DonorGone on guests. Optional.
	Recorder record.EventRecorder
	// FenceInterval is how often the fence is re-checked besides donor and mirror events.
	FenceInterval time.Duration
}

// Backend creates, watches and deletes mirror pods. It keeps no state of its own that matters
// across a restart: the mirrors in the API are the state, found again through the informer.
type Backend struct {
	client kubernetes.Interface
	opts   Options
	guests corev1listers.PodLister // pods bound to the virtual node (the library's lister)

	factory informers.SharedInformerFactory
	mirrors corev1listers.PodLister
	synced  cache.InformerSynced

	// deviceplugin mode only: donor pods on the host, and the fence (fence.go).
	donorFactory informers.SharedInformerFactory
	donors       corev1listers.PodLister
	donorsSynced cache.InformerSynced
	fenceKick    chan struct{}
	// attachMu serializes GPU choice and mirror create, so two guests never pick one GPU.
	// assigned covers the gap until the mirror informer has a just-created mirror.
	attachMu sync.Mutex
	assigned map[corev1.ResourceName]assignment

	mu          sync.Mutex
	onStatus    func(*corev1.Pod) // the library's notify callback, wrapped by the provider
	orphanSince map[types.UID]time.Time
	fenceKnown  bool // fenceState is what the node's fence taint was last seen or written as
	fenceState  bool
}

// New builds the backend. guests must list the pods bound to the virtual node.
func New(client kubernetes.Interface, guests corev1listers.PodLister, opts Options) *Backend {
	if opts.Resync == 0 {
		opts.Resync = 30 * time.Second
	}
	if opts.GPUMode == "" {
		opts.GPUMode = GPUModeClaim
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
		orphanSince: map[types.UID]time.Time{}, assigned: map[corev1.ResourceName]assignment{},
	}
	_, _ = inf.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { b.mirrorChanged(obj) },
		UpdateFunc: func(_, obj any) { b.mirrorChanged(obj) },
		DeleteFunc: b.mirrorDeleted,
	})
	if opts.GPUMode == GPUModeDevicePlugin {
		b.watchDonors(inf.Informer())
	}
	return b
}

// watchDonors sets up the deviceplugin-mode donor informer (pods on the host that match the
// selector, in every namespace) and kicks the fence on every donor or mirror change.
func (b *Backend) watchDonors(mirrors cache.SharedIndexInformer) {
	opts := &b.opts
	df := informers.NewSharedInformerFactoryWithOptions(b.client, opts.Resync,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.LabelSelector = opts.DonorSelector
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
	if g := b.guestFor(m); g != nil {
		b.emit(TranslateStatus(g, m))
	}
}

func (b *Backend) mirrorDeleted(obj any) {
	m, ok := obj.(*corev1.Pod)
	if !ok {
		tomb, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		if m, ok = tomb.Obj.(*corev1.Pod); !ok {
			return
		}
	}
	g := b.guestFor(m)
	if g == nil {
		return
	}
	if g.DeletionTimestamp == nil {
		b.emit(TerminalStatus(g, m, ReasonMirrorDeleted))
		return
	}
	b.emit(TerminalStatus(g, m, ReasonGuestDeleted))
	go b.finishGuestDeletion(context.Background(), g)
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
	return TranslateStatus(g, m), nil
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
			out = append(out, TranslateStatus(g, m))
		}
	}
	return out, nil
}

// Create builds and creates the mirror. It is idempotent: an existing mirror for this guest is
// fine; an orphaned mirror with the same name and the same containers is adopted.
func (b *Backend) Create(ctx context.Context, guest *corev1.Pod) error {
	var att *GPUAttachment
	if b.opts.GPUMode == GPUModeDevicePlugin && RequestsGPU(guest) {
		b.attachMu.Lock()
		defer b.attachMu.Unlock()
		if m, ok := b.mirrorOf(guest); ok {
			b.emit(TranslateStatus(guest, m)) // already attached (a retry, or a restart)
			return nil
		}
		var err error
		if att, err = b.attachGPU(ctx, guest); err != nil {
			return err // fail closed: no mirror; the library retries the create
		}
	}
	want, err := BuildWithGPU(guest, &b.opts.Config, att)
	if err != nil {
		return errdefs.AsInvalidInput(err)
	}
	logger := log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).WithField("mirror", want.Name)

	pod, err := b.client.CoreV1().Pods(guest.Namespace).Create(ctx, want, metav1.CreateOptions{})
	switch {
	case err == nil:
		logger.WithField("mirrorUID", pod.UID).Info("mirror created")
	case apierrors.IsAlreadyExists(err):
		if pod, err = b.adoptOrReplace(ctx, guest, want); err != nil {
			return err
		}
	default:
		return fmt.Errorf("create mirror: %w", err)
	}
	if att != nil {
		b.attached(ctx, guest, want, att)
	}

	if RequestsGPU(guest) && b.opts.ReserveClaim {
		if err := b.reserveClaim(ctx, guest.Namespace, b.opts.GPUClaim, pod); err != nil {
			return err
		}
	}
	b.emit(TranslateStatus(guest, pod))
	return nil
}

// attached logs a GPU attach and remembers it until the mirror informer has the mirror.
// The caller holds attachMu.
func (b *Backend) attached(ctx context.Context, guest, mirror *corev1.Pod, att *GPUAttachment) {
	name := mirror.Namespace + "/" + mirror.Name
	log.G(ctx).WithField("mirror", name).WithField("mode", string(GPUModeDevicePlugin)).
		WithField("uuid", att.UUID).WithField("attach", AttachShadowDevicePlugin).
		WithField("resource", string(att.Resource)).WithField("donor", att.Donor).Info("mirror gpu attached")
	b.assigned[att.Resource] = assignment{guest: guest.UID, mirror: name, at: time.Now()}
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
	m, ok := b.mirrorOf(guest)
	if !ok {
		// Nothing runs, so report the guest terminated now; the library then removes it
		// without waiting out the grace period.
		b.emit(TerminalStatus(guest, nil, ReasonGuestDeleted))
		go b.finishGuestDeletion(context.Background(), guest)
		return errdefs.NotFoundf("no mirror for guest %s/%s", guest.Namespace, guest.Name)
	}
	uid := m.UID
	opts := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
	if guest.DeletionGracePeriodSeconds != nil {
		opts.GracePeriodSeconds = guest.DeletionGracePeriodSeconds
	}
	err := b.client.CoreV1().Pods(m.Namespace).Delete(ctx, m.Name, opts)
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return errdefs.NotFoundf("mirror %s/%s already gone", m.Namespace, m.Name)
	}
	if err != nil {
		return fmt.Errorf("delete mirror: %w", err)
	}
	log.G(ctx).WithField("mirror", m.Namespace+"/"+m.Name).Info("mirror deleted")
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
