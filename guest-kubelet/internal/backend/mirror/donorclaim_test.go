package mirror_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// Tests for --gpu-claim-mode (decision D-NS-18, hook M3). Static is today's behaviour; donor
// takes the claim of the trainer pod on the VK's own host.

const (
	testNS   = "ns"
	testHost = "real-node"
)

func allocatedClaim(name string) *resourcev1.ResourceClaim {
	return &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
		Status:     resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{}},
	}
}

// donorPod is a running trainer pod on node that uses the named claim.
func donorPod(name, node, claim string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS, Name: name,
			Labels: map[string]string{"timeslice.io/job-id": "rl-trainer", "timeslice.io/group": "trainers"},
		},
		Spec: corev1.PodSpec{
			NodeName:       node,
			ResourceClaims: []corev1.PodResourceClaim{{Name: "accelerator", ResourceClaimName: &claim}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// guestPod is a guest bound to the virtual node; gpu says whether it requests nvidia.com/gpu.
func guestPod(gpu bool) *corev1.Pod {
	req := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	if gpu {
		req[mirror.GPUResource] = resource.MustParse("1")
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "vllm", UID: "guest-uid"},
		Spec: corev1.PodSpec{
			NodeName:   "vk-x",
			Containers: []corev1.Container{{Name: "vllm", Image: "vllm", Resources: corev1.ResourceRequirements{Requests: req}}},
		},
	}
}

func staticOptions() *mirror.Options {
	return &mirror.Options{
		Config: mirror.Config{
			HostNode: testHost, VirtualNode: "vk-x", GPUClaim: "shared-gpu", GuestTaintKey: "timeslice.io/guest",
		},
		ReserveClaim: true, OrphanGrace: time.Minute,
	}
}

func donorOptions() *mirror.Options {
	opts := staticOptions()
	opts.GPUClaim = "" // donor mode does not need --gpu-claim
	opts.ClaimMode = mirror.ClaimModeDonor
	return opts
}

// createGuest starts a backend on a fake clientset holding objs and creates the guest's mirror.
func createGuest(t *testing.T, opts *mirror.Options, guest *corev1.Pod, objs ...runtime.Object) (*fake.Clientset, error) {
	t.Helper()
	client := fake.NewClientset(objs...)
	client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if p, ok := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod); ok && p.UID == "" {
			p.UID = uuid.NewUUID() // the API server sets UIDs; the fake does not
		}
		return false, nil, nil
	})
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if err := idx.Add(guest); err != nil {
		t.Fatal(err)
	}
	b := mirror.New(client, corev1listers.NewPodLister(idx), *opts)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return client, b.Create(ctx, guest)
}

func getMirror(client *fake.Clientset) *corev1.Pod {
	m, err := client.CoreV1().Pods(testNS).Get(context.Background(), "vllm"+mirror.Suffix, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return m
}

func mirrorClaim(t *testing.T, client *fake.Clientset) string {
	t.Helper()
	m := getMirror(client)
	if m == nil {
		t.Fatal("mirror not created")
	}
	if len(m.Spec.ResourceClaims) != 1 || m.Spec.ResourceClaims[0].ResourceClaimName == nil {
		t.Fatalf("mirror resourceClaims: %+v", m.Spec.ResourceClaims)
	}
	return *m.Spec.ResourceClaims[0].ResourceClaimName
}

func TestGPUClaimMode_Static_UsesFlagClaimAndIgnoresDonors(t *testing.T) {
	client, err := createGuest(t, staticOptions(), guestPod(true),
		allocatedClaim("shared-gpu"), allocatedClaim("claim-a"), donorPod("trainer-a", testHost, "claim-a"))
	if err != nil {
		t.Fatal(err)
	}
	if got := mirrorClaim(t, client); got != "shared-gpu" {
		t.Errorf("static mode: mirror claim %q, want shared-gpu", got)
	}
}

func TestGPUClaimMode_Donor_UsesClaimOfDonorOnOwnHost(t *testing.T) {
	oldMirror := donorPod("old-m", testHost, "claim-x")
	oldMirror.Labels[mirror.LabelMirrorOf] = "some-guest"
	background := donorPod("bg", testHost, "claim-y")
	background.Labels["timeslice.io/role"] = "background"
	deleting := donorPod("trainer-old", testHost, "claim-z")
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	deleting.Finalizers = []string{"test/keep"}
	client, err := createGuest(t, donorOptions(), guestPod(true),
		allocatedClaim("claim-a"),
		donorPod("trainer-a", testHost, "claim-a"),
		donorPod("trainer-b", "other-node", "claim-b"), // the other host of the group
		oldMirror, background, deleting)
	if err != nil {
		t.Fatal(err)
	}
	if got := mirrorClaim(t, client); got != "claim-a" {
		t.Errorf("mirror claim %q, want claim-a (the donor on %s)", got, testHost)
	}
	c, err := client.ResourceV1().ResourceClaims(testNS).Get(context.Background(), "claim-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Status.ReservedFor) != 1 || c.Status.ReservedFor[0].Name != "vllm"+mirror.Suffix {
		t.Errorf("reservedFor on the donor's claim: %+v", c.Status.ReservedFor)
	}
}

func TestGPUClaimMode_Donor_TwoDonorsSameClaim(t *testing.T) {
	client, err := createGuest(t, donorOptions(), guestPod(true), allocatedClaim("claim-a"),
		donorPod("trainer-a", testHost, "claim-a"), donorPod("standin-a", testHost, "claim-a"))
	if err != nil {
		t.Fatal(err)
	}
	if got := mirrorClaim(t, client); got != "claim-a" {
		t.Errorf("mirror claim %q, want claim-a", got)
	}
}

func TestGPUClaimMode_Donor_GeneratedClaimFromTemplate(t *testing.T) {
	p := donorPod("trainers-worker-0", testHost, "")
	tmpl, generated := "trainer-gpu", "trainers-worker-0-accelerator-x7k2p"
	p.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "accelerator", ResourceClaimTemplateName: &tmpl}}
	p.Status.ResourceClaimStatuses = []corev1.PodResourceClaimStatus{{Name: "accelerator", ResourceClaimName: &generated}}
	client, err := createGuest(t, donorOptions(), guestPod(true), allocatedClaim(generated), p)
	if err != nil {
		t.Fatal(err)
	}
	if got := mirrorClaim(t, client); got != generated {
		t.Errorf("mirror claim %q, want the generated %q", got, generated)
	}
}

func TestGPUClaimMode_Donor_TemplateNotGeneratedYetFails(t *testing.T) {
	p := donorPod("trainers-worker-0", testHost, "")
	tmpl := "trainer-gpu"
	p.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "accelerator", ResourceClaimTemplateName: &tmpl}}
	client, err := createGuest(t, donorOptions(), guestPod(true), p)
	if err == nil || !strings.Contains(err.Error(), "not generated") {
		t.Fatalf("want a not-generated error, got %v", err)
	}
	if getMirror(client) != nil {
		t.Error("no mirror may be created without a claim")
	}
}

func TestGPUClaimMode_Donor_NoDonorFails(t *testing.T) {
	finished := donorPod("trainer-done", testHost, "claim-old")
	finished.Status.Phase = corev1.PodSucceeded
	client, err := createGuest(t, donorOptions(), guestPod(true), donorPod("trainer-b", "other-node", "claim-b"), finished)
	if err == nil || !strings.Contains(err.Error(), "no donor pod") {
		t.Fatalf("want a no-donor error, got %v", err)
	}
	if getMirror(client) != nil {
		t.Error("no mirror may be created without a claim")
	}
}

func TestGPUClaimMode_Donor_TwoClaimsOnHostFails(t *testing.T) {
	client, err := createGuest(t, donorOptions(), guestPod(true),
		donorPod("trainer-c", testHost, "claim-c"), donorPod("trainer-a", testHost, "claim-a"))
	if err == nil || !strings.Contains(err.Error(), "claim-a, claim-c") {
		t.Fatalf("want an ambiguous-claim error naming both, got %v", err)
	}
	if getMirror(client) != nil {
		t.Error("no mirror may be created with an ambiguous claim")
	}
}

func TestGPUClaimMode_Donor_CPUGuestNeedsNoDonor(t *testing.T) {
	client, err := createGuest(t, donorOptions(), guestPod(false))
	if err != nil {
		t.Fatal(err)
	}
	if m := getMirror(client); m == nil || len(m.Spec.ResourceClaims) != 0 {
		t.Errorf("CPU guest: want a mirror without claims, got %+v", m)
	}
}

func TestGPUClaimMode_ParseClaimMode(t *testing.T) {
	for in, want := range map[string]mirror.ClaimMode{
		"": mirror.ClaimModeStatic, "static": mirror.ClaimModeStatic, "donor": mirror.ClaimModeDonor,
	} {
		if got, err := mirror.ParseClaimMode(in); err != nil || got != want {
			t.Errorf("ParseClaimMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := mirror.ParseClaimMode("per-host"); err == nil {
		t.Error("want an error for an unknown mode")
	}
}
