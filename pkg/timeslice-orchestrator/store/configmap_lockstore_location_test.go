package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestConfigMapLockStore_DefaultLocation(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	s := store.NewConfigMapLockStore(client)

	if got, want := s.ConfigMapRef(), store.Namespace+"/"+store.ConfigMapName; got != want {
		t.Fatalf("ConfigMapRef() = %s, want %s", got, want)
	}
	if err := s.Lock(ctx, "group-1", "job-a"); err != nil {
		t.Fatalf("Lock() error = %v", err)
	}
	cm, err := client.CoreV1().ConfigMaps(store.Namespace).Get(ctx, store.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("default lock ConfigMap not created: %v", err)
	}
	if got := cm.Data["group-1"]; got != "job-a" {
		t.Errorf("lock value = %q, want %q (plain holder string)", got, "job-a")
	}
}

func TestConfigMapLockStore_WithConfigMapEmptyKeepsDefaults(t *testing.T) {
	s := store.NewConfigMapLockStore(fake.NewClientset(), store.WithConfigMap("", ""))
	if got, want := s.ConfigMapRef(), store.Namespace+"/"+store.ConfigMapName; got != want {
		t.Fatalf("ConfigMapRef() = %s, want defaults %s", got, want)
	}
}

// Two installs in one cluster, each with its own lock ConfigMap, must not see
// each other's holders.
func TestConfigMapLockStore_SeparateLocationsDoNotInterfere(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	defaultStore := store.NewConfigMapLockStore(client)
	demoStore := store.NewConfigMapLockStore(client, store.WithConfigMap("demo-orch", "demo-locks"))

	if got := demoStore.ConfigMapRef(); got != "demo-orch/demo-locks" {
		t.Fatalf("ConfigMapRef() = %s, want demo-orch/demo-locks", got)
	}

	if err := defaultStore.Lock(ctx, "group-1", "job-a"); err != nil {
		t.Fatalf("defaultStore.Lock() error = %v", err)
	}
	// Same group name, different install: demoStore must be free to lock it.
	if err := demoStore.Lock(ctx, "group-1", "job-b"); err != nil {
		t.Fatalf("demoStore.Lock() error = %v, want nil (separate lock table)", err)
	}
	if got := holder(t, ctx, defaultStore); got != "job-a" {
		t.Errorf("defaultStore.GetLock() = %q, want job-a", got)
	}
	if got := holder(t, ctx, demoStore); got != "job-b" {
		t.Errorf("demoStore.GetLock() = %q, want job-b", got)
	}
	if err := demoStore.Unlock(ctx, "group-1", "job-a"); !errors.Is(err, store.ErrNotLockHolder) {
		t.Errorf("demoStore.Unlock(job-a) error = %v, want ErrNotLockHolder", err)
	}
	if err := demoStore.Unlock(ctx, "group-1", "job-b"); err != nil {
		t.Fatalf("demoStore.Unlock() error = %v", err)
	}
	if got := holder(t, ctx, defaultStore); got != "job-a" {
		t.Errorf("defaultStore.GetLock() after demoStore.Unlock = %q, want job-a", got)
	}

	cm, err := client.CoreV1().ConfigMaps("demo-orch").Get(ctx, "demo-locks", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("custom lock ConfigMap not created: %v", err)
	}
	if _, ok := cm.Data["group-1"]; ok {
		t.Errorf("custom ConfigMap still holds group-1 after unlock: %v", cm.Data)
	}
	_, err = client.CoreV1().ConfigMaps("demo-orch").Get(ctx, store.ConfigMapName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("default-named ConfigMap created in custom namespace: err = %v", err)
	}
}

func holder(t *testing.T, ctx context.Context, s *store.ConfigMapLockStore) string {
	t.Helper()
	got, err := s.GetLock(ctx, "group-1")
	if err != nil {
		t.Fatalf("GetLock() error = %v", err)
	}
	return got
}
