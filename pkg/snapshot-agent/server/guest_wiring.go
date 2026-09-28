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
	"fmt"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
)

// timestampTime converts a request deadline; a missing one is the zero
// time, which StartGuestOp refuses.
func timestampTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// newGuestPipeline wires the pipelines to the watcher's pod cache, the API
// server, the host cgroup hierarchy, cuda-checkpoint and NVML.
func newGuestPipeline(cfg GuestConfig, w *Watcher, client kubernetes.Interface, backend guestBackend) *guestPipeline {
	return &guestPipeline{
		mirror: func(jobID string) (*corev1.Pod, bool) {
			return mirrorPod(w.getLocalPodsForJob(jobID))
		},
		getPod: func(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
			return client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		},
		cgroups:   cgroup.New(cfg.CgroupRoot),
		backend:   backend,
		gpu:       nvmlInspector{},
		qualified: cfg.VRAMZeroingQualified,
		now:       time.Now,
		records:   map[string]*guestRecord{},
	}
}

// mirrorPod picks the job's mirror pod from the pods carrying its job-id
// label: the one that is not terminal, else the first.
func mirrorPod(pods []*corev1.Pod) (*corev1.Pod, bool) {
	if len(pods) == 0 {
		return nil, false
	}
	for _, p := range pods {
		if p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
			return p, true
		}
	}
	return pods[0], true
}

// nvmlInspector queries NVML directly. It opens and closes NVML per call,
// like the rest of the agent.
type nvmlInspector struct{}

func withNVML[T any](f func() (T, error)) (T, error) {
	var zero T
	if ret := nvml.Init(); ret != nvml.SUCCESS {
		return zero, fmt.Errorf("initialize NVML: %v", nvml.ErrorString(ret))
	}
	defer nvml.Shutdown() //nolint:errcheck // best-effort cleanup
	return f()
}

func (nvmlInspector) Devices() ([]gpuDevice, error) {
	return withNVML(func() ([]gpuDevice, error) {
		driver, ret := nvml.SystemGetDriverVersion()
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("driver version: %v", nvml.ErrorString(ret))
		}
		count, ret := nvml.DeviceGetCount()
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("device count: %v", nvml.ErrorString(ret))
		}
		out := make([]gpuDevice, 0, count)
		for i := 0; i < count; i++ {
			dev, ret := nvml.DeviceGetHandleByIndex(i)
			if ret != nvml.SUCCESS {
				return nil, fmt.Errorf("device %d: %v", i, nvml.ErrorString(ret))
			}
			name, ret := dev.GetName()
			if ret != nvml.SUCCESS {
				return nil, fmt.Errorf("device %d name: %v", i, nvml.ErrorString(ret))
			}
			out = append(out, gpuDevice{Name: name, DriverVersion: driver})
		}
		return out, nil
	})
}

func (nvmlInspector) Processes() ([]gpuProcess, error) {
	return withNVML(func() ([]gpuProcess, error) {
		count, ret := nvml.DeviceGetCount()
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("device count: %v", nvml.ErrorString(ret))
		}
		var out []gpuProcess
		for i := 0; i < count; i++ {
			dev, ret := nvml.DeviceGetHandleByIndex(i)
			if ret != nvml.SUCCESS {
				return nil, fmt.Errorf("device %d: %v", i, nvml.ErrorString(ret))
			}
			for _, list := range []func() ([]nvml.ProcessInfo, nvml.Return){
				dev.GetComputeRunningProcesses, dev.GetGraphicsRunningProcesses,
			} {
				procs, ret := list()
				if ret == nvml.ERROR_NOT_SUPPORTED {
					continue
				}
				if ret != nvml.SUCCESS {
					return nil, fmt.Errorf("device %d processes: %v", i, nvml.ErrorString(ret))
				}
				for _, p := range procs {
					out = append(out, gpuProcess{PID: int(p.Pid), UsedBytes: p.UsedGpuMemory})
				}
			}
		}
		return out, nil
	})
}
