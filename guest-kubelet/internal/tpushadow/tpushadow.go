// Package tpushadow is the TPU counterpart of gpushadow's pooled mode. It advertises one
// resource, PooledResource, whose devices are exactly the TPU chips (VFIO groups) that the
// node's one google.com/tpu holder (the donor) holds, read from the kubelet's pod-resources
// API. A guest that requests PooledResource gets the same chips, device nodes, mounts and
// TPU environment the stock TPU device plugin gives the donor, but no google.com/tpu
// request, so the kubelet does not count it against the donor's chips. Only one of them
// may have the chips open at a time; the snapshot agent's park/restore enforces that.
//
// Scope: one plugin per TPU VM. A multi-host donor holds google.com/tpu on every host of its
// slice, so each host's plugin advertises that host's chips; a guest gets chips of one host
// only (it cannot span hosts). A guest takes one chip or all of the host's chips (see
// ValidSubset); a one-chip guest gets the libtpu subset environment for exactly that chip. No
// holder, or more than one holder pod, advertises zero devices (fail closed). The plugin
// advertises the chips while the donor holds them, parked or not: guests must be created
// only after the donor's whole slice is parked (a guest that opens a held chip gets EBUSY).
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

// ValidSubset reports whether chip indices idx (0-based positions among the host's
// hostChips chips, sorted ascending) form a guest libtpu can start on a host of a slice:
// one chip or every chip. Multi-chip subsets (for example a row pair with
// TPU_VISIBLE_CHIPS) pass device checks but fail libtpu's ICI session start on v6e, so
// they are rejected here instead of failing a minute into JAX init.
func ValidSubset(idx []int, hostChips int) error {
	n := len(idx)
	if n == 0 || n > hostChips {
		return fmt.Errorf("%d of %d chips", n, hostChips)
	}
	for i, x := range idx {
		if x < 0 || x >= hostChips || (i > 0 && x <= idx[i-1]) {
			return fmt.Errorf("chip indices %v are not ascending positions below %d", idx, hostChips)
		}
	}
	if n == 1 || n == hostChips {
		return nil
	}
	return fmt.Errorf("unsupported chip count %d (want 1 or all %d)", n, hostChips)
}

// Indices returns the positions of chips among the host's chips (sorted ascending) and
// whether every chip was found.
func Indices(chips, host []Chip) ([]int, bool) {
	pos := map[string]int{}
	for i, c := range host {
		pos[c.Group] = i
	}
	out := make([]int, 0, len(chips))
	for _, c := range chips {
		i, ok := pos[c.Group]
		if !ok {
			return nil, false
		}
		out = append(out, i)
	}
	sort.Ints(out)
	return out, true
}

// Env returns the TPU environment for a guest given chips (sorted by host position) out of
// the host's chips. generation is the accelerator prefix (for example "v6e"); metricsBase is
// the first runtime-metrics port. A guest with fewer chips than the host also gets
// TPU_VISIBLE_CHIPS and single-process bounds. The guest's container holds only its own
// VFIO groups and libtpu numbers the chips it finds from 0, so TPU_VISIBLE_CHIPS is the
// container-local index (0..n-1), not the host position: a host position above 0 fails
// with "Failed to get global TPU topology".
func Env(generation string, metricsBase int, chips, host []Chip) (map[string]string, error) {
	count := len(chips)
	idx, ok := Indices(chips, host)
	if !ok {
		return nil, fmt.Errorf("chips %v are not all on this host", chips)
	}
	if err := ValidSubset(idx, len(host)); err != nil {
		return nil, err
	}
	bounds, ok := chipsPerHostBounds[count]
	if !ok {
		return nil, fmt.Errorf("unsupported single-host chip count %d", count)
	}
	ports := make([]string, count)
	groups := make([]string, count)
	visible := make([]string, count)
	for i := range count {
		ports[i] = strconv.Itoa(metricsBase + i)
		groups[i] = host[idx[i]].Group
		visible[i] = strconv.Itoa(i)
	}
	env := map[string]string{
		"TPU_ACCELERATOR_TYPE":      fmt.Sprintf("%s-%d", generation, count),
		"TPU_CHIPS_PER_HOST_BOUNDS": bounds.bounds,
		"CHIPS_PER_HOST_BOUNDS":     bounds.bounds,
		"TPU_HOST_BOUNDS":           "1,1,1",
		"HOST_BOUNDS":               "1,1,1",
		"TPU_TOPOLOGY":              bounds.topology,
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
	}
	if count < len(host) {
		env["TPU_VISIBLE_CHIPS"] = strings.Join(visible, ",")
		env["TPU_PROCESS_BOUNDS"] = "1,1,1"
		env["TPU_CHIPS_PER_PROCESS_BOUNDS"] = bounds.bounds
	}
	return env, nil
}

// Preferred picks size chips from available (device IDs = VFIO groups) that include
// mustInclude and form a valid subset (ValidSubset), lowest host positions first. It
// returns nil when no valid subset fits.
func Preferred(host []Chip, available, mustInclude []string, size int) []string {
	avail := map[string]bool{}
	for _, a := range available {
		avail[a] = true
	}
	must := map[string]bool{}
	for _, m := range mustInclude {
		must[m] = true
	}
	n := len(host)
	if size <= 0 || size > n {
		return nil
	}
	// Every valid subset (one chip or all) is a contiguous run of host positions; try each start.
	for start := 0; start+size <= n; start++ {
		idx := make([]int, size)
		for i := range idx {
			idx[i] = start + i
		}
		if ValidSubset(idx, n) != nil {
			continue
		}
		ids := make([]string, 0, size)
		inc, fits := 0, true
		for _, i := range idx {
			g := host[i].Group
			if !avail[g] {
				fits = false
				break
			}
			if must[g] {
				inc++
			}
			ids = append(ids, g)
		}
		if fits && inc == len(must) {
			return ids
		}
	}
	return nil
}
