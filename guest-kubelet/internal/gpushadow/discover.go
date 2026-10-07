// Package gpushadow is the GPU shadow device plugin (D-NS-10, the guest kubelet's
// --gpu-mode=pooled). It runs as a DaemonSet on the donor host, next to the cluster's normal GPU
// device plugin, and:
//   - advertises one resource, timeslice.io/gpu-shadow, whose devices are the GPUs the node's
//     single nvidia.com/gpu holder (the donor) holds, read from the kubelet's pod-resources API;
//   - answers Allocate with those GPUs' device nodes, the NVIDIA control devices and the driver
//     mount, as the normal plugin would, so the kubelet sets the device cgroup and the mirror
//     needs no privilege and no hostPath.
//
// The plugin never talks to the API server.
package gpushadow

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

var gpuDeviceRE = regexp.MustCompile(`^nvidia(\d+)$`)

// Discover lists the GPUs on the host. devRoot is the host's /dev as the plugin sees it;
// procDriverRoot is the host's /proc/driver/nvidia (its gpus/*/information files give each
// GPU's device minor and UUID). A GPU whose information file is missing keeps an empty UUID.
func Discover(devRoot, procDriverRoot string) ([]api.GPU, error) {
	entries, err := os.ReadDir(devRoot)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", devRoot, err)
	}
	uuids, pcis, err := readUUIDs(filepath.Join(procDriverRoot, "gpus"))
	if err != nil {
		return nil, err
	}
	gpus := make([]api.GPU, 0, len(entries))
	for _, e := range entries {
		m := gpuDeviceRE.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		minor, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		gpus = append(gpus, api.GPU{
			Minor: minor, UUID: uuids[minor], Device: e.Name(), Resource: api.ShadowResource(minor),
			PCI: pcis[minor], NUMA: -1,
		})
	}
	if len(gpus) == 0 {
		return nil, fmt.Errorf("no /dev/nvidiaN devices under %s", devRoot)
	}
	sort.Slice(gpus, func(i, j int) bool { return gpus[i].Minor < gpus[j].Minor })
	return gpus, nil
}

// readUUIDs maps device minor to GPU UUID and to PCI address from
// <dir>/<pci address>/information.
func readUUIDs(dir string) (map[int]string, map[int]string, error) {
	out, pcis := map[int]string{}, map[int]string{}
	infos, err := filepath.Glob(filepath.Join(dir, "*", "information"))
	if err != nil {
		return nil, nil, err
	}
	for _, p := range infos {
		gpu, err := parseInformation(p)
		if err != nil {
			return nil, nil, err
		}
		if gpu.Minor >= 0 {
			out[gpu.Minor] = gpu.UUID
			pcis[gpu.Minor] = strings.ToLower(filepath.Base(filepath.Dir(p)))
		}
	}
	return out, pcis, nil
}

// AddNUMA sets each GPU's NUMA node from <sysRoot>/bus/pci/devices/<pci>/numa_node. A GPU
// without a PCI address or a readable value keeps -1 (no topology reported).
func AddNUMA(gpus []api.GPU, sysRoot string) {
	for i := range gpus {
		gpus[i].NUMA = -1
		if gpus[i].PCI == "" || sysRoot == "" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(sysRoot, "bus", "pci", "devices", gpus[i].PCI, "numa_node"))
		if err != nil {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && n >= 0 {
			gpus[i].NUMA = n
		}
	}
}

// parseInformation reads "Device Minor:" and "GPU UUID:" from one information file (only Minor
// and UUID are set). Minor is -1 if the file has no minor.
func parseInformation(path string) (api.GPU, error) {
	info := api.GPU{Minor: -1}
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return info, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		switch strings.TrimSpace(k) {
		case "Device Minor":
			if n, err := strconv.Atoi(v); err == nil {
				info.Minor = n
			}
		case "GPU UUID":
			info.UUID = v
		}
	}
	return info, sc.Err()
}
