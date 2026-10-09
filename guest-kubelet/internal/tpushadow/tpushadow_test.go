package tpushadow_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/edwinhr716/guest-kubelet/internal/tpushadow"
)

// fakeDev builds a host /dev with VFIO groups 0..3 (created out of order) and the
// container device; regular files stand in for character devices.
func fakeDev(t *testing.T) string {
	t.Helper()
	dev := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dev, "vfio"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"3", "0", "10", "1", "2", "vfio", "noise"} {
		if err := os.WriteFile(filepath.Join(dev, "vfio", n), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dev
}

func groups(cs []tpushadow.Chip) string {
	s := make([]string, 0, len(cs))
	for _, c := range cs {
		s = append(s, c.Group)
	}
	return strings.Join(s, ",")
}

func TestDiscover(t *testing.T) {
	chips, err := tpushadow.Discover(fakeDev(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := groups(chips); got != "0,1,2,3,10" {
		t.Errorf("Discover = %s", got)
	}
	if _, err := tpushadow.Discover(t.TempDir()); err == nil {
		t.Error("Discover without /dev/vfio/vfio: want an error")
	}
}

func holding(pods map[string][]string) *podresourcesapi.ListPodResourcesResponse {
	resp := &podresourcesapi.ListPodResourcesResponse{}
	for name, ids := range pods {
		resp.PodResources = append(resp.PodResources, &podresourcesapi.PodResources{
			Namespace: "ns", Name: name, Containers: []*podresourcesapi.ContainerResources{{
				Name: "c", Devices: []*podresourcesapi.ContainerDevices{
					{ResourceName: "google.com/tpu", DeviceIds: ids},
					{ResourceName: "example.com/other", DeviceIds: []string{"9"}},
				},
			}},
		})
	}
	return resp
}

func TestPooledChips(t *testing.T) {
	chips := []tpushadow.Chip{{Group: "0"}, {Group: "1"}, {Group: "2"}, {Group: "3"}}
	cases := map[string]struct {
		resp      *podresourcesapi.ListPodResourcesResponse
		want, why string
	}{
		"no donor":    {holding(nil), "", "no pod holds"},
		"donor all":   {holding(map[string][]string{"donor": {"3", "0", "1", "2"}}), "0,1,2,3", "donor ns/donor holds 4"},
		"two holders": {holding(map[string][]string{"donor": {"0", "1"}, "intruder": {"2", "3"}}), "", "fail closed: 2 pods"},
		"unmapped":    {holding(map[string][]string{"donor": {"0", "7"}}), "", "fail closed"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, why := tpushadow.PooledChips(tc.resp, chips, "google.com/tpu")
			if groups(got) != tc.want || !strings.Contains(why, tc.why) {
				t.Errorf("PooledChips = %q (%s); want %q (%s)", groups(got), why, tc.want, tc.why)
			}
		})
	}
}

func TestContainerResponse(t *testing.T) {
	cfg := &tpushadow.Config{
		DevRoot: fakeDev(t), HostDevRoot: "/dev", Generation: "v6e", MetricsBase: 8431,
		Mounts: []string{"/var/run/tpu-plugin", "/tmp/tpu_logs"},
	}
	host := []tpushadow.Chip{{Group: "0"}, {Group: "1"}, {Group: "2"}, {Group: "3"}}
	chips := []tpushadow.Chip{{Group: "3"}, {Group: "0"}, {Group: "1"}, {Group: "2"}}
	resp, err := tpushadow.ContainerResponse(cfg, chips, host)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetDevices()) != 5 || resp.GetDevices()[4].GetHostPath() != "/dev/vfio/vfio" ||
		resp.GetDevices()[0].GetContainerPath() != "/dev/vfio/0" {
		t.Errorf("devices = %v", resp.GetDevices())
	}
	env := resp.GetEnvs()
	for k, v := range map[string]string{
		"TPU_ACCELERATOR_TYPE": "v6e-4", "TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1", "TPU_TOPOLOGY": "2x2",
		"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434", tpushadow.PooledChipsEnv: "0,1,2,3",
		"VBAR_CONTROL_SERVICE_URL": "unix:///var/run/tpu-plugin/vbar.sock",
	} {
		if env[k] != v {
			t.Errorf("env %s = %q, want %q", k, env[k], v)
		}
	}
	if _, ok := env["TPU_VISIBLE_CHIPS"]; ok {
		t.Errorf("whole host: TPU_VISIBLE_CHIPS = %q, want unset", env["TPU_VISIBLE_CHIPS"])
	}
	if len(resp.GetMounts()) != 2 || resp.GetMounts()[0].GetContainerPath() != "/var/run/tpu-plugin" {
		t.Errorf("mounts = %v", resp.GetMounts())
	}
	if _, err := tpushadow.ContainerResponse(cfg, []tpushadow.Chip{{Group: "5"}}, host); err == nil {
		t.Error("chip not on the host: want an error")
	}
	if _, err := tpushadow.ContainerResponse(cfg, chips[:3], host); err == nil {
		t.Error("3 chips: want an unsupported-count error")
	}
}

// A guest given part of a host gets exactly its chips' devices and the libtpu subset env.
func TestContainerResponseSubset(t *testing.T) {
	cfg := &tpushadow.Config{DevRoot: fakeDev(t), HostDevRoot: "/dev", Generation: "v6e", MetricsBase: 8431}
	host := []tpushadow.Chip{{Group: "0"}, {Group: "1"}, {Group: "2"}, {Group: "3"}}
	cases := map[string]struct {
		chips                                   []tpushadow.Chip
		devs, visible, bounds, accel, pooledEnv string
	}{
		// Container-local index: the container holds only /dev/vfio/2, which libtpu calls chip 0.
		"one chip": {[]tpushadow.Chip{{Group: "2"}}, "/dev/vfio/2,/dev/vfio/vfio", "0", "1,1,1", "v6e-1", "2"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resp, err := tpushadow.ContainerResponse(cfg, tc.chips, host)
			if err != nil {
				t.Fatal(err)
			}
			devs := make([]string, 0, len(resp.GetDevices()))
			for _, d := range resp.GetDevices() {
				devs = append(devs, d.GetContainerPath())
			}
			env := resp.GetEnvs()
			if strings.Join(devs, ",") != tc.devs || env["TPU_VISIBLE_CHIPS"] != tc.visible ||
				env["TPU_CHIPS_PER_PROCESS_BOUNDS"] != tc.bounds || env["TPU_CHIPS_PER_HOST_BOUNDS"] != tc.bounds ||
				env["TPU_PROCESS_BOUNDS"] != "1,1,1" || env["TPU_ACCELERATOR_TYPE"] != tc.accel ||
				env[tpushadow.PooledChipsEnv] != tc.pooledEnv {
				t.Errorf("devs %v env %v", devs, env)
			}
		})
	}
	// Multi-chip subsets fail libtpu's ICI session start on a host of a slice: rejected.
	for _, pair := range [][]tpushadow.Chip{{{Group: "3"}, {Group: "2"}}, {{Group: "1"}, {Group: "2"}}} {
		if _, err := tpushadow.ContainerResponse(cfg, pair, host); err == nil {
			t.Errorf("pair %v: want an error", pair)
		}
	}
}

func TestValidSubset(t *testing.T) {
	for _, tc := range []struct {
		idx  []int
		host int
		ok   bool
	}{
		{[]int{3}, 4, true},
		{[]int{0, 1, 2, 3}, 4, true},
		{[]int{0, 1}, 4, false},
		{[]int{2, 3}, 4, false},
		{[]int{1, 2}, 4, false},
		{[]int{0, 1, 2}, 4, false},
		{nil, 4, false},
		{[]int{1, 0}, 4, false},
		{[]int{4}, 4, false},
		{[]int{4, 5, 6, 7}, 8, false},
		{[]int{7}, 8, true},
		{[]int{0, 1, 2, 3, 4, 5, 6, 7}, 8, true},
		{[]int{0}, 1, true},
	} {
		if err := tpushadow.ValidSubset(tc.idx, tc.host); (err == nil) != tc.ok {
			t.Errorf("ValidSubset(%v, %d) = %v, want ok=%v", tc.idx, tc.host, err, tc.ok)
		}
	}
}

func TestPreferred(t *testing.T) {
	host := []tpushadow.Chip{{Group: "0"}, {Group: "1"}, {Group: "2"}, {Group: "3"}}
	for _, tc := range []struct {
		avail, must []string
		size        int
		want        string
	}{
		{[]string{"0", "1", "2", "3"}, nil, 1, "0"},
		{[]string{"3", "1"}, nil, 1, "1"},
		{[]string{"0", "1", "2", "3"}, []string{"3"}, 1, "3"},
		{[]string{"0", "1", "2", "3"}, nil, 2, ""}, // pairs are not offered
		{[]string{"0", "1", "3"}, nil, 4, ""},      // a whole-host guest needs every chip
		{[]string{"0", "1", "2", "3"}, nil, 4, "0,1,2,3"},
		{[]string{"0", "1", "2", "3"}, nil, 3, ""},
	} {
		if got := strings.Join(tpushadow.Preferred(host, tc.avail, tc.must, tc.size), ","); got != tc.want {
			t.Errorf("Preferred(%v, must %v, %d) = %q, want %q", tc.avail, tc.must, tc.size, got, tc.want)
		}
	}
}
