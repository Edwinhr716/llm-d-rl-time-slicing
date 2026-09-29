package donor_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/edwinhr716/guest-kubelet/internal/donor"
)

const (
	hostName = "host-a"
	vkName   = "vk-a"
)

func node(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), Labels: labels}}
}

func vkNode() *corev1.Node {
	return node(vkName, map[string]string{donor.VirtualNodeLabel: "true"})
}

func newStandin(client *fake.Clientset) *donor.Standin {
	return &donor.Standin{
		Client: client, VKNode: vkName, HostNode: hostName, Confirm: 3,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func step(t *testing.T, standin *donor.Standin) (bool, error) {
	t.Helper()
	return standin.Step(context.Background())
}

func deletes(client *fake.Clientset) []k8stesting.DeleteAction {
	var out []k8stesting.DeleteAction
	for _, action := range client.Actions() {
		if del, ok := action.(k8stesting.DeleteAction); ok {
			out = append(out, del)
		}
	}
	return out
}

func vkExists(t *testing.T, client *fake.Clientset) bool {
	t.Helper()
	_, err := client.CoreV1().Nodes().Get(context.Background(), vkName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get %s: %v", vkName, err)
	}
	return err == nil
}

func TestStandin_HostPresent_NeverDeletes(t *testing.T) {
	client := fake.NewClientset(node(hostName, nil), vkNode())
	standin := newStandin(client)
	for range 10 {
		if done, err := step(t, standin); done || err != nil {
			t.Fatalf("step = %v, %v; want false, nil", done, err)
		}
	}
	if len(deletes(client)) != 0 || !vkExists(t, client) {
		t.Fatalf("virtual node deleted while the host exists")
	}
}

func TestStandin_HostGone_DeletesAfterConfirm(t *testing.T) {
	client := fake.NewClientset(vkNode())
	standin := newStandin(client)
	for i := 1; i < standin.Confirm; i++ {
		if done, err := step(t, standin); done || err != nil {
			t.Fatalf("miss %d: step = %v, %v; want false, nil", i, done, err)
		}
		if len(deletes(client)) != 0 {
			t.Fatalf("miss %d: deleted before %d misses in a row", i, standin.Confirm)
		}
	}
	done, err := step(t, standin)
	if !done || err != nil {
		t.Fatalf("step = %v, %v; want true, nil", done, err)
	}
	if vkExists(t, client) {
		t.Fatalf("virtual node still exists")
	}
	dels := deletes(client)
	if len(dels) != 1 || dels[0].GetName() != vkName {
		t.Fatalf("deletes = %v; want one of %s", dels, vkName)
	}
	pre := dels[0].GetDeleteOptions().Preconditions
	if pre == nil || pre.UID == nil || *pre.UID != "uid-"+vkName {
		t.Fatalf("delete preconditions = %+v; want the virtual node's uid", pre)
	}
}

func TestStandin_HostBack_ResetsMisses(t *testing.T) {
	client := fake.NewClientset(vkNode())
	standin := newStandin(client)
	for range standin.Confirm - 1 {
		if _, err := step(t, standin); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.CoreV1().Nodes().Create(context.Background(), node(hostName, nil), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if done, err := step(t, standin); done || err != nil {
		t.Fatalf("host back: step = %v, %v", done, err)
	}
	if err := client.CoreV1().Nodes().Delete(context.Background(), hostName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < standin.Confirm; i++ {
		if done, err := step(t, standin); done || err != nil {
			t.Fatalf("miss %d: step = %v, %v; the count should restart when the host came back", i, done, err)
		}
	}
	if done, err := step(t, standin); !done || err != nil {
		t.Fatalf("step = %v, %v; want true, nil", done, err)
	}
}

func TestStandin_HostGetError_FailsClosed(t *testing.T) {
	client := fake.NewClientset(vkNode())
	client.PrependReactor("get", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if get, ok := action.(k8stesting.GetAction); ok && get.GetName() == hostName {
			return true, nil, apierrors.NewServiceUnavailable("apiserver down")
		}
		return false, nil, nil
	})
	standin := newStandin(client)
	for range 10 {
		if done, err := step(t, standin); done || err == nil {
			t.Fatalf("step = %v, %v; want false and an error", done, err)
		}
	}
	if len(deletes(client)) != 0 {
		t.Fatalf("deleted on a get error; only NotFound counts as a miss")
	}
}

func TestStandin_RefusesNodeWithoutVirtualLabel(t *testing.T) {
	client := fake.NewClientset(node(vkName, nil))
	standin := newStandin(client)
	standin.Confirm = 1
	if done, err := step(t, standin); done || err == nil {
		t.Fatalf("step = %v, %v; want false and an error", done, err)
	}
	if len(deletes(client)) != 0 || !vkExists(t, client) {
		t.Fatalf("deleted a node without %s=true", donor.VirtualNodeLabel)
	}
}

func TestStandin_DeleteDenied_Retries(t *testing.T) {
	client := fake.NewClientset(vkNode())
	denied := true
	client.PrependReactor("delete", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if denied {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, vkName, errors.New("policy"))
		}
		return false, nil, nil
	})
	standin := newStandin(client)
	standin.Confirm = 1
	if done, err := step(t, standin); done || !apierrors.IsForbidden(err) {
		t.Fatalf("step = %v, %v; want false and the Forbidden error", done, err)
	}
	denied = false
	if done, err := step(t, standin); !done || err != nil {
		t.Fatalf("retry: step = %v, %v; want true, nil", done, err)
	}
	if vkExists(t, client) {
		t.Fatalf("virtual node still exists after the retry")
	}
}

func TestStandin_VirtualNodeAlreadyGone(t *testing.T) {
	client := fake.NewClientset()
	standin := newStandin(client)
	standin.Confirm = 1
	if done, err := step(t, standin); !done || err != nil {
		t.Fatalf("step = %v, %v; want true, nil", done, err)
	}
	if len(deletes(client)) != 0 {
		t.Fatalf("delete sent for a node that does not exist")
	}
}
