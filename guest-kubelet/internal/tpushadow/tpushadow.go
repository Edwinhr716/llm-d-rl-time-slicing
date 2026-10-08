// Package tpushadow is the TPU counterpart of gpushadow's pooled mode. It advertises one
// resource, PooledResource, whose devices are exactly the TPU chips (VFIO groups) that the
// node's one google.com/tpu holder (the donor) holds, read from the kubelet's pod-resources
// API. A guest that requests PooledResource gets the same chips, device nodes, mounts and
// TPU environment the stock TPU device plugin gives the donor, but no google.com/tpu
// request, so the kubelet does not count it against the donor's chips. Only one of them
// may have the chips open at a time; the snapshot agent's park/restore enforces that.
//
// Scope: single-host TPU slices (one VM, all chips on the node). No holder, or more than
// one holder pod, advertises zero devices (fail closed).
package tpushadow

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
)

// PooledResource is the shadow resource a guest requests.
const PooledResource corev1.ResourceName = "timeslice.io/tpu-shadow"

// PooledChipsEnv lists the VFIO groups a guest was given.
const PooledChipsEnv = "TIMESLICE_TPU_CHIPS"

// Chip is one TPU chip: a VFIO group /dev/vfio/<Group>. The stock TPU device plugin uses the
// group number as the device ID, so it is also the pod-resources device ID.
type Chip struct {
	Group string `json:"group"`
}

// Discover lists the TPU chips under devRoot (the host's /dev): every numeric entry of
// devRoot/vfio. The VFIO container device devRoot/vfio/vfio must exist.
func Discover(devRoot string) ([]Chip, error) {
	dir := filepath.Join(devRoot, "vfio")
	if _, err := os.Stat(filepath.Join(dir, "vfio")); err != nil {
		return nil, fmt.Errorf("no VFIO container device: %w", err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Chip
	for _, e := range ents {
		if _, err := strconv.Atoi(e.Name()); err == nil {
			out = append(out, Chip{Group: e.Name()})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := strconv.Atoi(out[i].Group)
		b, _ := strconv.Atoi(out[j].Group)
		return a < b
	})
	if len(out) == 0 {
		return nil, fmt.Errorf("no VFIO groups under %s", dir)
	}
	return out, nil
}

// PooledChips returns the chips the pooled resource advertises for this pod-resources
// listing, and why (for the log). tpuResource is the donor's resource (google.com/tpu).
func PooledChips(resp *podresourcesapi.ListPodResourcesResponse, chips []Chip, tpuResource string) ([]Chip, string) {
	known := map[string]bool{}
	for _, c := range chips {
		known[c.Group] = true
	}
	pods := map[string]bool{}
	held := map[string]bool{}
	var unmapped []string
	for _, p := range resp.GetPodResources() {
		for _, c := range p.GetContainers() {
			for _, d := range c.GetDevices() {
				if d.GetResourceName() != tpuResource || len(d.GetDeviceIds()) == 0 {
					continue
				}
				pods[p.GetNamespace()+"/"+p.GetName()] = true
				for _, id := range d.GetDeviceIds() {
					if known[id] {
						held[id] = true
					} else {
						unmapped = append(unmapped, id)
					}
				}
			}
		}
	}
	names := make([]string, 0, len(pods))
	for p := range pods {
		names = append(names, p)
	}
	sort.Strings(names)
	switch {
	case len(pods) == 0:
		return nil, "no pod holds " + tpuResource
	case len(pods) > 1:
		return nil, fmt.Sprintf("fail closed: %d pods hold %s (%s); one donor per node is required",
			len(pods), tpuResource, strings.Join(names, ", "))
	case len(unmapped) > 0:
		return nil, fmt.Sprintf("fail closed: %s device ids %v are not VFIO groups on this node", tpuResource, unmapped)
	}
	out := make([]Chip, 0, len(held))
	for _, c := range chips {
		if held[c.Group] {
			out = append(out, c)
		}
	}
	return out, fmt.Sprintf("donor %s holds %d chip(s)", names[0], len(out))
}

// chipsPerHostBounds maps a chip count to the libtpu host bounds and topology the stock
// plugin uses for a single-host slice of that size.
var chipsPerHostBounds = map[int]struct{ bounds, topology string }{
	1: {"1,1,1", "1x1"},
	2: {"1,2,1", "1x2"},
	4: {"2,2,1", "2x2"},
	8: {"2,4,1", "2x4"},
}

// Env returns the TPU environment for a guest given n chips. generation is the accelerator
// prefix (for example "v6e"); metricsBase is the first runtime-metrics port.
func Env(generation string, n, metricsBase int, chips []Chip) (map[string]string, error) {
	b, ok := chipsPerHostBounds[n]
	if !ok {
		return nil, fmt.Errorf("unsupported single-host chip count %d", n)
	}
	ports := make([]string, n)
	groups := make([]string, n)
	for i := range n {
		ports[i] = strconv.Itoa(metricsBase + i)
		groups[i] = chips[i].Group
	}
	return map[string]string{
		"TPU_ACCELERATOR_TYPE":      fmt.Sprintf("%s-%d", generation, n),
		"TPU_CHIPS_PER_HOST_BOUNDS": b.bounds,
		"CHIPS_PER_HOST_BOUNDS":     b.bounds,
		"TPU_HOST_BOUNDS":           "1,1,1",
		"HOST_BOUNDS":               "1,1,1",
		"TPU_TOPOLOGY":              b.topology,
		"TPU_TOPOLOGY_WRAP":         "false,false,false",
		"WRAP":                      "false,false,false",
		"TPU_TOPOLOGY_ALT":          "false",
		"ALT":                       "false",
		"TPU_WORKER_ID":             "0",
		"TPU_WORKER_HOSTNAMES":      "localhost",
		"TPU_SKIP_MDS_QUERY":        "true",
		"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
		"TPU_RUNTIME_METRICS_PORTS": strings.Join(ports, ","),
		PooledChipsEnv:              strings.Join(groups, ","),
	}, nil
}
