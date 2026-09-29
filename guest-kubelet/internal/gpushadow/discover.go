// Package gpushadow is the GPU shadow device plugin (D-NS-10 option ns-deviceplugin). It runs as
// a DaemonSet on the donor host, next to the cluster's normal GPU device plugin, and:
//   - advertises every physical GPU a second time, one resource per GPU
//     (timeslice.io/gpu-shadow-<minor>, one device each), so the guest kubelet can ask the real
//     kubelet for one specific GPU;
//   - answers Allocate with that GPU's device node, the NVIDIA control devices and the driver
//     mount, as the normal plugin would, so the kubelet sets the device cgroup and the mirror
//     needs no privilege and no hostPath;
//   - serves, on the host loopback, which pod holds which GPU through the normal resource,
//     read from the kubelet's pod-resources API (the guest kubelet uses it to find the donor's
//     GPU).
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
	uuids, err := readUUIDs(filepath.Join(procDriverRoot, "gpus"))
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
		})
	}
	if len(gpus) == 0 {
		return nil, fmt.Errorf("no /dev/nvidiaN devices under %s", devRoot)
	}
	sort.Slice(gpus, func(i, j int) bool { return gpus[i].Minor < gpus[j].Minor })
	return gpus, nil
}

// readUUIDs maps device minor to GPU UUID from <dir>/<pci address>/information.
func readUUIDs(dir string) (map[int]string, error) {
	out := map[int]string{}
	infos, err := filepath.Glob(filepath.Join(dir, "*", "information"))
	if err != nil {
		return nil, err
	}
	for _, p := range infos {
		gpu, err := parseInformation(p)
		if err != nil {
			return nil, err
		}
		if gpu.Minor >= 0 {
			out[gpu.Minor] = gpu.UUID
		}
	}
	return out, nil
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
