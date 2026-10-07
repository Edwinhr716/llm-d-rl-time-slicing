package gpushadow_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow"
	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// clientOf adapts the fake pod-resources server to the client interface RunPooled takes.
type clientOf struct{ f *fakePodResources }

func (c clientOf) List(
	ctx context.Context, in *podresourcesapi.ListPodResourcesRequest, _ ...grpc.CallOption,
) (*podresourcesapi.ListPodResourcesResponse, error) {
	return c.f.List(ctx, in)
}

func holding(pods map[string][]string) *podresourcesapi.ListPodResourcesResponse {
	resp := &podresourcesapi.ListPodResourcesResponse{}
	for name, ids := range pods {
		resp.PodResources = append(resp.PodResources, &podresourcesapi.PodResources{
			Namespace: "ns", Name: name, Containers: []*podresourcesapi.ContainerResources{{
				Name: "c", Devices: []*podresourcesapi.ContainerDevices{{ResourceName: "nvidia.com/gpu", DeviceIds: ids}},
			}},
		})
	}
	return resp
}

func TestPooledDevices(t *testing.T) {
	host := fakeHost(t)
	gpus, err := gpushadow.Discover(host.dev, host.proc)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		resp *podresourcesapi.ListPodResourcesResponse
		want []string
		why  string
	}{
		"no donor":         {holding(nil), nil, "no pod holds"},
		"donor holds both": {holding(map[string][]string{"donor": {"nvidia0", "GPU-bbb"}}), []string{"nvidia0", "nvidia1"}, "donor ns/donor"},
		"donor holds one":  {holding(map[string][]string{"donor": {"nvidia1"}}), []string{"nvidia1"}, "1 GPU"},
		"two holders":      {holding(map[string][]string{"donor": {"nvidia0"}, "intruder": {"nvidia1"}}), nil, "fail closed: 2 pods"},
		"unmapped id":      {holding(map[string][]string{"donor": {"weird"}}), nil, "fail closed"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, why := gpushadow.PooledDevices(tc.resp, gpus, "nvidia.com/gpu")
			ids := []string{}
			for _, g := range got {
				ids = append(ids, g.Device)
			}
			if strings.Join(ids, ",") != strings.Join(tc.want, ",") || !strings.Contains(why, tc.why) {
				t.Errorf("PooledDevices = %v (%s), want %v (%s)", ids, why, tc.want, tc.why)
			}
		})
	}
}

func TestAddNUMA(t *testing.T) {
	host := fakeHost(t)
	gpus, err := gpushadow.Discover(host.dev, host.proc)
	if err != nil {
		t.Fatal(err)
	}
	sys := shortTempDir(t)
	writeFile(t, filepath.Join(sys, "bus", "pci", "devices", "0000:00:03.0", "numa_node"), "1\n")
	writeFile(t, filepath.Join(sys, "bus", "pci", "devices", "0000:00:04.0", "numa_node"), "-1\n")
	gpushadow.AddNUMA(gpus, sys)
	if gpus[0].NUMA != 1 || gpus[1].NUMA != -1 {
		t.Errorf("NUMA = %d, %d; want 1, -1", gpus[0].NUMA, gpus[1].NUMA)
	}
}

func recvDevices(t *testing.T, stream grpc.ServerStreamingClient[pluginapi.ListAndWatchResponse]) []*pluginapi.Device {
	t.Helper()
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	return resp.GetDevices()
}

// The pooled plugin registers one resource, follows the donor (0 -> 2 -> 0 devices, and 0
// when a second pod holds a GPU), reports NUMA, and allocates several GPUs in one call with
// the UUID env.
func TestRunPooledFollowsDonorAndAllocates(t *testing.T) {
	host := fakeHost(t)
	gpus, err := gpushadow.Discover(host.dev, host.proc)
	if err != nil {
		t.Fatal(err)
	}
	sys := shortTempDir(t)
	writeFile(t, filepath.Join(sys, "bus", "pci", "devices", "0000:00:03.0", "numa_node"), "0\n")
	gpushadow.AddNUMA(gpus, sys)
	pluginDir := shortTempDir(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kubelet := &fakeKubelet{registered: make(chan *pluginapi.RegisterRequest, 16)}
	serveUnix(ctx, t, filepath.Join(pluginDir, "kubelet.sock"), func(s *grpc.Server) {
		pluginapi.RegisterRegistrationServer(s, kubelet)
	})
	pr := &fakePodResources{resp: holding(nil)}
	cfg := &gpushadow.Config{
		PluginDir: pluginDir, DevRoot: host.dev, HostDevRoot: "/dev",
		HostDriverRoot: "/home/kubernetes/bin/nvidia", ContainerDriverRoot: "/usr/local/nvidia",
		LibraryPath: "/usr/local/nvidia/lib64", Tools: []string{"nvidia-smi"},
		HealthInterval: time.Hour, SocketCheckInterval: 20 * time.Millisecond,
	}
	go func() { _ = gpushadow.RunPooled(ctx, gpus, clientOf{pr}, "nvidia.com/gpu", 20*time.Millisecond, cfg) }()

	endpoints := registrations(t, kubelet, 1)
	ep, ok := endpoints[string(api.PooledResource)]
	if !ok || len(endpoints) != 1 {
		t.Fatalf("registered %v, want only %s", endpoints, api.PooledResource)
	}
	client := pluginapi.NewDevicePluginClient(dial(t, filepath.Join(pluginDir, ep)))
	opts, err := client.GetDevicePluginOptions(ctx, &pluginapi.Empty{})
	if err != nil || opts.GetGetPreferredAllocationAvailable() || opts.GetPreStartRequired() {
		t.Fatalf("options %v, %v", opts, err)
	}
	stream, err := client.ListAndWatch(ctx, &pluginapi.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if d := recvDevices(t, stream); len(d) != 0 {
		t.Fatalf("no donor: advertised %v", d)
	}
	pr.mu.Lock()
	pr.resp = holding(map[string][]string{"donor": {"nvidia0", "nvidia1"}})
	pr.mu.Unlock()
	d := recvDevices(t, stream)
	if len(d) != 2 || d[0].GetID() != "nvidia0" || d[1].GetID() != "nvidia1" {
		t.Fatalf("donor arrived: advertised %v", d)
	}
	if d[0].GetTopology() == nil || d[0].GetTopology().GetNodes()[0].GetID() != 0 || d[1].GetTopology() != nil {
		t.Errorf("topology: %v / %v", d[0].GetTopology(), d[1].GetTopology())
	}
	resp, err := client.Allocate(ctx, &pluginapi.AllocateRequest{ContainerRequests: []*pluginapi.ContainerAllocateRequest{
		{DevicesIds: []string{"nvidia0", "nvidia1"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	cr := resp.GetContainerResponses()[0]
	if len(cr.GetDevices()) != 4 || cr.GetEnvs()[api.PooledUUIDsEnv] != "GPU-aaa,GPU-bbb" {
		t.Errorf("allocate: devices %v envs %v", cr.GetDevices(), cr.GetEnvs())
	}
	if _, err := client.Allocate(ctx, &pluginapi.AllocateRequest{ContainerRequests: []*pluginapi.ContainerAllocateRequest{
		{DevicesIds: []string{"nvidia7"}},
	}}); err == nil {
		t.Error("allocated an unknown device")
	}
	pr.mu.Lock()
	pr.resp = holding(map[string][]string{"donor": {"nvidia0"}, "intruder": {"nvidia1"}})
	pr.mu.Unlock()
	if d := recvDevices(t, stream); len(d) != 0 {
		t.Fatalf("two holders: advertised %v (must fail closed)", d)
	}
	pr.mu.Lock()
	pr.resp = holding(map[string][]string{"donor": {"nvidia0", "nvidia1"}})
	pr.mu.Unlock()
	if d := recvDevices(t, stream); len(d) != 2 {
		t.Fatalf("back to one donor: %v", d)
	}
	pr.fail(context.DeadlineExceeded)
	time.Sleep(100 * time.Millisecond) // a List error keeps the last set: nothing is sent
	pr.fail(nil)
	pr.mu.Lock()
	pr.resp = holding(nil)
	pr.mu.Unlock()
	if d := recvDevices(t, stream); len(d) != 0 {
		t.Fatalf("donor left: advertised %v", d)
	}
}
