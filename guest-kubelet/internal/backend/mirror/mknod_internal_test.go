package mirror

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// Every mirror container, init containers included, drops MKNOD,
// merged with what the guest set.
func TestBuildWithGPU_DropsMknodMergedWithGuest(t *testing.T) {
	guest := testGuest()
	guest.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		Capabilities: &corev1.Capabilities{
			Add:  []corev1.Capability{"NET_ADMIN", "CAP_MKNOD"},
			Drop: []corev1.Capability{"NET_RAW"},
		},
	}
	guest.Spec.InitContainers = []corev1.Container{{Name: "init", Image: "busybox"}}
	cfg := testConfig()
	mir, err := BuildWithGPU(guest, &cfg, testAttachment())
	if err != nil {
		t.Fatal(err)
	}
	caps := mir.Spec.Containers[0].SecurityContext.Capabilities
	if !reflect.DeepEqual(caps.Add, []corev1.Capability{"NET_ADMIN"}) {
		t.Errorf("add = %v, want [NET_ADMIN]", caps.Add)
	}
	if !reflect.DeepEqual(caps.Drop, []corev1.Capability{"NET_RAW", CapMknod}) {
		t.Errorf("drop = %v, want [NET_RAW MKNOD]", caps.Drop)
	}
	ic := mir.Spec.InitContainers[0].SecurityContext
	if ic == nil || ic.Capabilities == nil || !reflect.DeepEqual(ic.Capabilities.Drop, []corev1.Capability{CapMknod}) {
		t.Errorf("init container: %+v", ic)
	}
	if g := guest.Spec.Containers[0].SecurityContext.Capabilities; len(g.Add) != 2 || len(g.Drop) != 1 {
		t.Errorf("builder modified the guest: %+v", g)
	}
	// Claim mode too: the rule is per mirror, not per GPU mode.
	claimMir, err := Build(testGuest(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range claimMir.Spec.Containers {
		if c.SecurityContext == nil || !reflect.DeepEqual(c.SecurityContext.Capabilities.Drop, []corev1.Capability{CapMknod}) {
			t.Errorf("claim-mode container %s: %+v", c.Name, c.SecurityContext)
		}
	}
}

func TestDropMknod_DropAllUntouchedAddAllKept(t *testing.T) {
	c := &corev1.Container{SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{
		Drop: []corev1.Capability{"ALL"},
	}}}
	dropMknod(c)
	if !reflect.DeepEqual(c.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("drop ALL changed: %v", c.SecurityContext.Capabilities.Drop)
	}
	c = &corev1.Container{SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{
		Add: []corev1.Capability{"ALL"},
	}}}
	dropMknod(c)
	caps := c.SecurityContext.Capabilities
	if !reflect.DeepEqual(caps.Add, []corev1.Capability{"ALL"}) || !reflect.DeepEqual(caps.Drop, []corev1.Capability{CapMknod}) {
		t.Errorf("add ALL: %+v", caps)
	}
	// Idempotent: a mirror re-built or re-adopted does not grow the list.
	dropMknod(c)
	if len(caps.Drop) != 1 {
		t.Errorf("not idempotent: %v", caps.Drop)
	}
}

func TestCheckDevicePluginGuest_RefusesPrivileged(t *testing.T) {
	guest := testGuest()
	yes := true
	guest.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &yes}
	if err := CheckDevicePluginGuest(guest); err == nil {
		t.Fatal("privileged guest accepted on the shared-GPU path")
	}
}
