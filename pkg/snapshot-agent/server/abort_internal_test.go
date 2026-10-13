// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// recordAborts makes the fixture's abort record each call, with the freeze state at the time,
// in the backend's call list (so that its order against the checkpoint shows), and fail with err.
func recordAborts(f *guestFixture, err error) {
	f.g.procRoot = "/host/proc"
	f.g.abortConns = func(nsPath string, ports []int, deadline time.Time) (int, error) {
		frozen, ferr := f.g.cgroups.Frozen(f.podDir)
		if ferr != nil {
			f.t.Error(ferr)
		}
		if left := time.Until(deadline); left <= 0 || left > abortTimeout {
			f.t.Errorf("abort deadline %v away, want within %v", left, abortTimeout)
		}
		f.backend.mu.Lock()
		f.backend.calls = append(f.backend.calls, fmt.Sprintf("abort %s %v frozen=%v", nsPath, ports, frozen))
		f.backend.mu.Unlock()
		return 2, err
	}
}

func TestGuest_SuspendAbortsInFlightConnections(t *testing.T) {
	f := newGuestFixture(t)
	f.mirror.Spec.Containers[0].Ports = []corev1.ContainerPort{
		{Name: "http", ContainerPort: 8000},
		{Name: "metrics", ContainerPort: 8000},
		{Name: "dns", ContainerPort: 53, Protocol: corev1.ProtocolUDP},
		{Name: "grpc", ContainerPort: 9000, Protocol: corev1.ProtocolTCP},
	}
	recordAborts(f, nil)
	res, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
	if err != nil || res.Outcome != pb.Outcome_OUTCOME_SUSPENDED {
		t.Fatalf("got %v, %v", res, err)
	}
	want := []string{
		"abort /host/proc/100/ns/net [8000 9000] frozen=false",
		"checkpoint [100] locked=0",
		"abort /host/proc/100/ns/net [8000 9000] frozen=true",
	}
	if got := f.backend.getCalls(); !reflect.DeepEqual(got, want) {
		t.Errorf("calls %q, want %q", got, want)
	}
}

func TestGuest_SuspendAbortFailureDoesNotBlock(t *testing.T) {
	f := newGuestFixture(t)
	f.mirror.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8000}}
	recordAborts(f, errors.New("sock_destroy: operation not supported"))
	res, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
	if err != nil || res.Outcome != pb.Outcome_OUTCOME_SUSPENDED {
		t.Fatalf("a failed abort must not fail the Suspend: %v, %v", res, err)
	}
	if !f.frozen(t) {
		t.Error("not frozen after a failed abort")
	}
}

func TestGuest_SuspendAbortPortFromReadinessProbe(t *testing.T) {
	for name, tc := range map[string]struct {
		probe *corev1.Probe
		want  string
	}{
		"http named port": {
			probe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("http")}}},
			want:  "[8080]",
		},
		"tcp number": {
			probe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(7000)}}},
			want:  "[7000]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newGuestFixture(t)
			// The mirror declares no ports: the guest pod's readiness probe names the port.
			f.guest.Spec.Containers = []corev1.Container{{
				Name: "vllm", ReadinessProbe: tc.probe,
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}}
			recordAborts(f, nil)
			if _, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if got := f.backend.getCalls(); len(got) != 3 || got[0] != "abort /host/proc/100/ns/net "+tc.want+" frozen=false" {
				t.Errorf("calls %q, want an abort of %s first", got, tc.want)
			}
		})
	}
}

func TestGuest_SuspendAbortSkipped(t *testing.T) {
	t.Run("no serving port", func(t *testing.T) {
		f := newGuestFixture(t)
		recordAborts(f, nil)
		if _, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		if got := f.backend.getCalls(); !reflect.DeepEqual(got, []string{"checkpoint [100] locked=0"}) {
			t.Errorf("calls %q", got)
		}
	})
	t.Run("host network", func(t *testing.T) {
		f := newGuestFixture(t)
		f.mirror.Spec.HostNetwork = true
		f.mirror.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8000}}
		recordAborts(f, nil)
		if _, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		if got := f.backend.getCalls(); !reflect.DeepEqual(got, []string{"checkpoint [100] locked=0"}) {
			t.Errorf("the node's own connections must never be reset: calls %q", got)
		}
	})
	t.Run("re-issued on a frozen guest", func(t *testing.T) {
		f := newGuestFixture(t)
		f.mirror.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8000}}
		recordAborts(f, nil)
		ctx := context.Background()
		for range 2 {
			if _, err := f.g.suspend(ctx, guestJob, time.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
		want := []string{
			"abort /host/proc/100/ns/net [8000] frozen=false",
			"checkpoint [100] locked=0",
			"abort /host/proc/100/ns/net [8000] frozen=true",
			"abort /host/proc/100/ns/net [8000] frozen=true", // the re-issue only sweeps
		}
		if got := f.backend.getCalls(); !reflect.DeepEqual(got, want) {
			t.Errorf("calls %q, want %q", got, want)
		}
	})
}
