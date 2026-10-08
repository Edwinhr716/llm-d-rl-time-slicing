// Copyright 2025 The llm-d Authors.
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

package scrub

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
)

// Identity is what the allowlist is matched against.
type Identity struct {
	GPUName       string
	DriverVersion string
	UUID          string
}

// IdentifyFunc returns the identity of the GPU with the given UUID, or of the
// only visible GPU when uuid is "".
type IdentifyFunc func(uuid string) (Identity, error)

// NVMLIdentities lists the identity of every GPU NVML can see.
func NVMLIdentities() ([]Identity, error) {
	if ret := nvml.Init(); ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to initialize NVML: %v", nvml.ErrorString(ret))
	}
	defer nvmlShutdown()

	driver, ret := nvml.SystemGetDriverVersion()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to get driver version: %v", nvml.ErrorString(ret))
	}
	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to get device count: %v", nvml.ErrorString(ret))
	}
	ids := make([]Identity, 0, count)
	for idx := range count {
		device, ret := nvml.DeviceGetHandleByIndex(idx)
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("failed to get device %d: %v", idx, nvml.ErrorString(ret))
		}
		name, ret := device.GetName()
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("failed to get name of device %d: %v", idx, nvml.ErrorString(ret))
		}
		uuid, ret := device.GetUUID()
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("failed to get UUID of device %d: %v", idx, nvml.ErrorString(ret))
		}
		ids = append(ids, Identity{GPUName: name, DriverVersion: driver, UUID: uuid})
	}
	return ids, nil
}

// NVMLIdentify is the default IdentifyFunc. With uuid "" exactly one GPU must
// be visible, so the scrub can never land on a GPU nobody named.
func NVMLIdentify(uuid string) (Identity, error) {
	ids, err := NVMLIdentities()
	if err != nil {
		return Identity{}, err
	}
	return pickIdentity(ids, uuid)
}

func pickIdentity(ids []Identity, uuid string) (Identity, error) {
	if uuid == "" {
		switch len(ids) {
		case 0:
			return Identity{}, errors.New("no GPU visible")
		case 1:
			return ids[0], nil
		default:
			return Identity{}, fmt.Errorf("%d GPUs visible: pass the GPU UUID", len(ids))
		}
	}
	for _, id := range ids {
		if id.UUID == uuid {
			return id, nil
		}
	}
	return Identity{}, fmt.Errorf("GPU %s not visible", uuid)
}

// AllQualified reports whether every identity is on the allowlist. An empty
// list is not qualified.
func AllQualified(list Allowlist, ids []Identity) bool {
	if len(ids) == 0 {
		return false
	}
	for _, id := range ids {
		if !list.Qualified(id.GPUName, id.DriverVersion) {
			return false
		}
	}
	return true
}

func nvmlShutdown() {
	if ret := nvml.Shutdown(); ret != nvml.SUCCESS {
		slog.Warn("Failed to shutdown NVML", "error", nvml.ErrorString(ret))
	}
}
