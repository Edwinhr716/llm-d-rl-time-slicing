// Package api is what the guest kubelet and the GPU shadow device plugin share: the shadow
// resource names and the JSON the plugin serves about which pod holds which GPU. It has no
// gRPC or kubelet dependency, so the guest kubelet can import it without the plugin code.
//
// D-NS-10 option ns-deviceplugin (--gpu-mode=deviceplugin). The donor books plain
// nvidia.com/gpu through the cluster's normal device plugin. The shadow plugin advertises each
// physical GPU a second time, under its own resource name (one name per GPU, one device each),
// and its Allocate hands out the same device nodes and driver mount the normal plugin does.
// The guest kubelet asks for the shadow resource of the GPU the donor holds, so the real
// kubelet attaches exactly that GPU and sets the device cgroup: the mirror stays unprivileged.
package api

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	// ShadowResourcePrefix starts every shadow resource name. A ValidatingAdmissionPolicy in
	// deploy/opt-deviceplugin refuses pods asking for it unless the guest kubelet creates them.
	ShadowResourcePrefix = "timeslice.io/gpu-shadow-"

	// DefaultHoldersAddr is where the plugin serves Holders (host network, loopback only).
	DefaultHoldersAddr = "127.0.0.1:10262"
	// HoldersPath is the HTTP path of the Holders document.
	HoldersPath = "/holders"
)

// ShadowResource is the resource name that stands for the GPU with this device minor
// (/dev/nvidia<minor>).
func ShadowResource(minor int) corev1.ResourceName {
	return corev1.ResourceName(ShadowResourcePrefix + strconv.Itoa(minor))
}

// IsShadowResource reports whether name is a shadow resource.
func IsShadowResource(name corev1.ResourceName) bool {
	return strings.HasPrefix(string(name), ShadowResourcePrefix)
}

// GPU is one physical GPU on the host.
type GPU struct {
	Minor    int                 `json:"minor"`    // N in /dev/nvidiaN
	UUID     string              `json:"uuid"`     // GPU-..., empty if the driver did not report it
	Device   string              `json:"device"`   // "nvidiaN"
	Resource corev1.ResourceName `json:"resource"` // shadow resource name for this GPU
}

// Holder is one container that holds devices of the normal GPU resource, as the real kubelet's
// pod-resources API reports it.
type Holder struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Container string   `json:"container"`
	Resource  string   `json:"resource"`
	DeviceIDs []string `json:"deviceIDs"`
	// Minors are the GPUs behind DeviceIDs. A device ID the plugin cannot map is left out.
	Minors []int `json:"minors"`
}

// Holders is the document the plugin serves at HoldersPath.
type Holders struct {
	GPUs    []GPU    `json:"gpus"`
	Holders []Holder `json:"holders"`
}

// GPUByMinor returns the GPU with this minor.
func (h *Holders) GPUByMinor(minor int) (GPU, bool) {
	for _, g := range h.GPUs {
		if g.Minor == minor {
			return g, true
		}
	}
	return GPU{}, false
}

// HeldBy returns the GPUs a pod holds through resource, in minor order as reported.
func (h *Holders) HeldBy(namespace, name, resource string) []GPU {
	out := make([]GPU, 0, len(h.GPUs))
	seen := map[int]bool{}
	for _, hd := range h.Holders {
		if hd.Namespace != namespace || hd.Name != name || hd.Resource != resource {
			continue
		}
		for _, m := range hd.Minors {
			if g, ok := h.GPUByMinor(m); ok && !seen[m] {
				seen[m] = true
				out = append(out, g)
			}
		}
	}
	return out
}

// MinorFromDeviceID maps a device-plugin device ID to a GPU minor. The GKE plugin and the
// NVIDIA plugin in "index" mode use "nvidiaN"; the NVIDIA plugin's default is the GPU UUID.
func MinorFromDeviceID(id string, gpus []GPU) (int, error) {
	if rest, ok := strings.CutPrefix(id, "nvidia"); ok {
		if n, err := strconv.Atoi(rest); err == nil && n >= 0 {
			return n, nil
		}
	}
	for _, g := range gpus {
		if g.UUID != "" && g.UUID == id {
			return g.Minor, nil
		}
	}
	return 0, fmt.Errorf("device id %q is neither nvidiaN nor a known GPU UUID", id)
}
