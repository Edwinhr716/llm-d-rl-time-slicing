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
	"log/slog"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// abortTimeout bounds one abort of a guest's in-flight connections. An abort that fails or runs
// out of time is logged and the Suspend goes on: it must never hold up the reclaim.
const abortTimeout = time.Second

// abortFunc resets the TCP connections into ports in the network namespace at nsPath and
// returns how many it reset (connabort.Abort).
type abortFunc func(nsPath string, ports []int, deadline time.Time) (int, error)

// abortInFlight resets the connections into the guest's serving ports, so that the requests in
// flight fail at once, with a reset the caller can retry elsewhere, instead of hanging until the
// guest is resumed. It runs once the guest is NotReady, before the checkpoint (vLLM sees the
// disconnects and drops the work), and again after the freeze, for a request that reached the
// guest before every router dropped its endpoint.
func (g *guestPipeline) abortInFlight(ctx context.Context, jobID, phase string, t *guestTarget, pid int) {
	if g.abortConns == nil || t.pod.Spec.HostNetwork {
		return
	}
	ports := g.servingPorts(ctx, t.pod)
	if len(ports) == 0 {
		return
	}
	procRoot := g.procRoot
	if procRoot == "" {
		procRoot = "/proc"
	}
	start := g.now()
	n, err := g.abortConns(filepath.Join(procRoot, strconv.Itoa(pid), "ns", "net"), ports, start.Add(abortTimeout))
	attrs := []any{"jobID", jobID, "phase", phase, "ports", ports, "connections", n, "ms", g.now().Sub(start).Milliseconds()}
	if err != nil {
		slog.WarnContext(ctx, "Suspend: aborting in-flight connections failed; suspending anyway",
			append(attrs, "error", err)...)
		return
	}
	slog.InfoContext(ctx, "Suspend: aborted in-flight connections", attrs...)
}

// servingPorts returns the TCP ports the guest serves on: the mirror's container ports, else the
// port of the owner guest pod's readiness probe (the mirror carries no probes).
func (g *guestPipeline) servingPorts(ctx context.Context, mirror *corev1.Pod) []int {
	var ports []int
	add := func(p int) {
		if p > 0 && p <= 65535 && !slices.Contains(ports, p) {
			ports = append(ports, p)
		}
	}
	for i := range mirror.Spec.Containers {
		for _, cp := range mirror.Spec.Containers[i].Ports {
			if cp.Protocol == "" || cp.Protocol == corev1.ProtocolTCP {
				add(int(cp.ContainerPort))
			}
		}
	}
	if len(ports) > 0 {
		return ports
	}
	owner := ownerPod(mirror)
	if owner == "" || g.getPod == nil {
		return nil
	}
	pod, err := g.getPod(ctx, mirror.Namespace, owner)
	if err != nil {
		return nil // the readiness precondition already read it; a guest pod gone receives no traffic
	}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if p := c.ReadinessProbe; p != nil {
			switch {
			case p.HTTPGet != nil:
				add(probePort(p.HTTPGet.Port, c))
			case p.TCPSocket != nil:
				add(probePort(p.TCPSocket.Port, c))
			}
		}
	}
	return ports
}

// probePort resolves a probe port, a number or the name of one of c's ports; 0 if it cannot.
func probePort(port intstr.IntOrString, c *corev1.Container) int {
	if port.Type == intstr.Int {
		return int(port.IntVal)
	}
	for _, cp := range c.Ports {
		if cp.Name == port.StrVal {
			return int(cp.ContainerPort)
		}
	}
	n, err := strconv.Atoi(port.StrVal)
	if err != nil {
		return 0
	}
	return n
}
