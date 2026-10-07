package prepull

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func guest() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "vllm-0", Namespace: "batch", UID: types.UID("u-1"),
			Labels: map[string]string{"timeslice.io/guest": "true"}},
		Spec: corev1.PodSpec{
			ServiceAccountName: "batch-sa",
			ImagePullSecrets:   []corev1.LocalObjectReference{{Name: "regcred"}},
			InitContainers:     []corev1.Container{{Name: "init", Image: "busybox"}},
			Containers: []corev1.Container{
				{Name: "vllm", Image: "vllm/vllm-openai"},
				{Name: "side", Image: "busybox"},
			},
		},
	}
}

func TestPod(t *testing.T) {
	p := Pod(guest(), "host-1")
	if p.Name != "vllm-0"+Suffix || p.Namespace != "batch" || p.Spec.NodeName != "host-1" {
		t.Errorf("identity = %s/%s on %s", p.Namespace, p.Name, p.Spec.NodeName)
	}
	if len(p.Spec.Containers) != 2 || p.Spec.Containers[0].Image != "busybox" || p.Spec.Containers[1].Image != "vllm/vllm-openai" {
		t.Errorf("containers = %+v, want the two distinct images", p.Spec.Containers)
	}
	for _, c := range p.Spec.Containers {
		if _, gpu := c.Resources.Limits["nvidia.com/gpu"]; gpu {
			t.Error("prepull container holds a GPU")
		}
		if c.SecurityContext == nil || *c.SecurityContext.AllowPrivilegeEscalation {
			t.Error("prepull container may escalate")
		}
	}
	for k := range p.Labels {
		if k == "timeslice.io/guest" || k == "timeslice.io/role" {
			t.Errorf("prepull pod carries %s", k)
		}
	}
	if p.Labels[LabelFor] != "u-1" || p.Spec.ServiceAccountName != "batch-sa" || len(p.Spec.ImagePullSecrets) != 1 ||
		*p.Spec.AutomountServiceAccountToken || p.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("spec = %+v", p.Spec)
	}
}

func status(phase corev1.PodPhase, states ...corev1.ContainerState) *corev1.Pod {
	p := Pod(guest(), "h")
	p.Status.Phase = phase
	for _, s := range states {
		p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{State: s})
	}
	return p
}

func waiting(r string) corev1.ContainerState {
	return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: r}}
}

func TestPulled(t *testing.T) {
	done := corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}
	cases := map[string]struct {
		pod  *corev1.Pod
		want bool
	}{
		"no status":         {status(corev1.PodPending), false},
		"one still pulling": {status(corev1.PodPending, done, waiting("ContainerCreating")), false},
		"backoff":           {status(corev1.PodPending, done, waiting("ImagePullBackOff")), false},
		"start error":       {status(corev1.PodPending, done, waiting("CreateContainerConfigError")), true},
		"all exited":        {status(corev1.PodPending, done, done), true},
		"succeeded":         {status(corev1.PodSucceeded), true},
	}
	for name, tc := range cases {
		if got := Pulled(tc.pod); got != tc.want {
			t.Errorf("%s: Pulled = %v, want %v", name, got, tc.want)
		}
	}
}

func TestPrepull_CreatesWaitsDeletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client := fake.NewClientset()
	p := New(ctx, client, "host-1")
	p.Poll = 10 * time.Millisecond
	g := guest()
	p.Prepull(ctx, g)
	p.Prepull(ctx, g) // in flight: skipped
	name := g.Name + Suffix
	var pod *corev1.Pod
	for {
		got, err := client.CoreV1().Pods("batch").Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			pod = got
			break
		}
		if ctx.Err() != nil {
			t.Fatal("prepull pod never created")
		}
		time.Sleep(5 * time.Millisecond)
	}
	pod.Status.Phase = corev1.PodSucceeded
	if _, err := client.CoreV1().Pods("batch").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := client.CoreV1().Pods("batch").Get(ctx, name, metav1.GetOptions{}); err != nil {
			break // deleted
		}
		if ctx.Err() != nil {
			t.Fatal("prepull pod never deleted")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestForget_DeletesPod(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client := fake.NewClientset()
	p := New(ctx, client, "host-1")
	p.Poll = 10 * time.Millisecond
	g := guest()
	p.Prepull(ctx, g)
	name := g.Name + Suffix
	for {
		if _, err := client.CoreV1().Pods("batch").Get(ctx, name, metav1.GetOptions{}); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.Forget(g)
	for {
		if _, err := client.CoreV1().Pods("batch").Get(ctx, name, metav1.GetOptions{}); err != nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("prepull pod not deleted after Forget")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
