package mirror

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestHasDonorFollowsTheDonorPod(t *testing.T) {
	var pokes atomic.Int32
	opts := pooledOptions(nil)
	opts.OnDonorChange = func() { pokes.Add(1) }
	hn := newHarness(t, &opts, pooledHost(2), donorPod())
	if ok, why := hn.b.HasDonor(); !ok {
		t.Fatalf("donor running: HasDonor = false (%s)", why)
	}
	before := pokes.Load()
	if before == 0 {
		t.Error("the initial donor add must poke OnDonorChange")
	}

	ctx := context.Background()
	if err := hn.client.CoreV1().Pods("ns").Delete(ctx, "donor", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok, _ := hn.b.HasDonor(); !ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ok, why := hn.b.HasDonor()
	if ok || why != "no donor pod on the host" {
		t.Fatalf("donor deleted: HasDonor = %v (%s)", ok, why)
	}
	if pokes.Load() <= before {
		t.Error("the donor delete must poke OnDonorChange")
	}

	// A donor that comes back counts again.
	if _, err := hn.client.CoreV1().Pods("ns").Create(ctx, donorPod(), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(deadline) {
		if ok, _ := hn.b.HasDonor(); ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("donor re-created: HasDonor stays false")
}

func TestHasDonorTerminatingAndTerminalDonorsDoNotCount(t *testing.T) {
	terminating := donorPod()
	now := metav1.Now()
	terminating.DeletionTimestamp, terminating.Finalizers = &now, []string{"test/hold"}
	done := donorPod()
	done.Name, done.UID = "donor-done", "donor-done-uid"
	done.Status.Phase = corev1.PodSucceeded
	opts := pooledOptions(nil)
	hn := newHarness(t, &opts, pooledHost(2), terminating, done)
	if ok, why := hn.b.HasDonor(); ok {
		t.Fatalf("only a terminating and a finished donor: HasDonor = true (%s)", why)
	}
}

func TestHasDonorBeforeSyncAnswersTrue(t *testing.T) {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	opts := pooledOptions(nil)
	b := New(fake.NewClientset(), corev1listers.NewPodLister(idx), &opts) // never started
	if ok, why := b.HasDonor(); !ok || why != "donor cache not synced" {
		t.Fatalf("unsynced cache: HasDonor = %v (%s); an empty cache must not look like a departed donor", ok, why)
	}
}

func TestBoundPodsListsTheLibraryLister(t *testing.T) {
	opts := pooledOptions(nil)
	hn := newHarness(t, &opts, pooledHost(2), donorPod())
	hn.addGuest(testGuest())
	pods, err := hn.b.BoundPods()
	if err != nil || len(pods) != 1 || pods[0].Name != testGuest().Name {
		t.Fatalf("BoundPods = %v, %v", pods, err)
	}
}
