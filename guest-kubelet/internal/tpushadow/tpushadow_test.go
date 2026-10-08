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
	chips := []tpushadow.Chip{{Group: "3"}, {Group: "0"}, {Group: "1"}, {Group: "2"}}
	resp, err := tpushadow.ContainerResponse(cfg, chips)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetDevices()) != 5 || resp.GetDevices()[4].GetHostPath() != "/dev/vfio/vfio" ||
		resp.GetDevices()[0].GetContainerPath() != "/dev/vfio/3" {
		t.Errorf("devices = %v", resp.GetDevices())
	}
	env := resp.GetEnvs()
	for k, v := range map[string]string{
		"TPU_ACCELERATOR_TYPE": "v6e-4", "TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1", "TPU_TOPOLOGY": "2x2",
		"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434", tpushadow.PooledChipsEnv: "3,0,1,2",
		"VBAR_CONTROL_SERVICE_URL": "unix:///var/run/tpu-plugin/vbar.sock",
	} {
		if env[k] != v {
			t.Errorf("env %s = %q, want %q", k, env[k], v)
		}
	}
	if len(resp.GetMounts()) != 2 || resp.GetMounts()[0].GetContainerPath() != "/var/run/tpu-plugin" {
		t.Errorf("mounts = %v", resp.GetMounts())
	}
	if _, err := tpushadow.ContainerResponse(cfg, []tpushadow.Chip{{Group: "5"}}); err == nil {
		t.Error("missing group: want an error")
	}
	if _, err := tpushadow.ContainerResponse(cfg, chips[:3]); err == nil {
		t.Error("3 chips: want an unsupported-count error")
	}
}
