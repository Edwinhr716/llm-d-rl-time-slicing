package gpushadow_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow"
	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// shortTempDir is a temp dir with a short path: unix socket paths are limited to about 100 bytes.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Log(err)
		}
	})
	return dir
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// hostDirs are a fake host's /dev and /proc/driver/nvidia.
type hostDirs struct{ dev, proc string }

// fakeHost makes a /dev with two GPUs and the control devices, and a /proc/driver/nvidia whose
// information files give their UUIDs (as the NVIDIA driver writes them).
func fakeHost(t *testing.T) hostDirs {
	t.Helper()
	dev, proc := shortTempDir(t), shortTempDir(t)
	for _, name := range []string{"nvidia0", "nvidia1", "nvidiactl", "nvidia-uvm", "nvidia-caps", "null"} {
		writeFile(t, filepath.Join(dev, name), "")
	}
	writeFile(t, filepath.Join(proc, "gpus", "0000:00:03.0", "information"),
		"Model: \t\t NVIDIA L4\nIRQ:   \t\t 35\nGPU UUID: \t GPU-aaa\nDevice Minor: \t 0\n")
	writeFile(t, filepath.Join(proc, "gpus", "0000:00:04.0", "information"),
		"Model: \t\t NVIDIA L4\nGPU UUID: \t GPU-bbb\nDevice Minor: \t 1\n")
	return hostDirs{dev: dev, proc: proc}
}

func TestDiscover(t *testing.T) {
	host := fakeHost(t)
	dev, proc := host.dev, host.proc
	gpus, err := gpushadow.Discover(dev, proc)
	if err != nil {
		t.Fatal(err)
	}
	want := []api.GPU{
		{Minor: 0, UUID: "GPU-aaa", Device: "nvidia0", Resource: "timeslice.io/gpu-shadow-0", PCI: "0000:00:03.0", NUMA: -1},
		{Minor: 1, UUID: "GPU-bbb", Device: "nvidia1", Resource: "timeslice.io/gpu-shadow-1", PCI: "0000:00:04.0", NUMA: -1},
	}
	if len(gpus) != len(want) {
		t.Fatalf("Discover = %+v, want %+v", gpus, want)
	}
	for i := range want {
		if gpus[i] != want[i] {
			t.Errorf("gpu %d = %+v, want %+v", i, gpus[i], want[i])
		}
	}
	if _, err := gpushadow.Discover(shortTempDir(t), proc); err == nil {
		t.Error("no /dev/nvidiaN: want an error")
	}
}

// fakeKubelet is the kubelet's device-plugin registration service.
type fakeKubelet struct {
	pluginapi.UnimplementedRegistrationServer

	registered chan *pluginapi.RegisterRequest
}

func (k *fakeKubelet) Register(_ context.Context, req *pluginapi.RegisterRequest) (*pluginapi.Empty, error) {
	k.registered <- req
	return &pluginapi.Empty{}, nil
}

func serveUnix(ctx context.Context, t *testing.T, socket string, register func(*grpc.Server)) {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Log(err)
		}
	}()
	t.Cleanup(srv.Stop)
}

func dial(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Log(err)
		}
	})
	return conn
}

// registrations waits for n Register calls and returns resource name -> endpoint.
func registrations(t *testing.T, kubelet *fakeKubelet, count int) map[string]string {
	t.Helper()
	out := map[string]string{}
	timeout := time.After(10 * time.Second)
	for len(out) < count {
		select {
		case req := <-kubelet.registered:
			if req.GetVersion() != pluginapi.Version {
				t.Errorf("registered with version %q", req.GetVersion())
			}
			out[req.GetResourceName()] = req.GetEndpoint()
		case <-timeout:
			t.Fatalf("only %d of %d plugins registered: %v", len(out), count, out)
		}
	}
	return out
}

// fakePodResources is the kubelet's pod-resources service.
type fakePodResources struct {
	podresourcesapi.UnimplementedPodResourcesListerServer

	mu   sync.Mutex
	resp *podresourcesapi.ListPodResourcesResponse
	err  error
}

func (f *fakePodResources) List(
	context.Context, *podresourcesapi.ListPodResourcesRequest,
) (*podresourcesapi.ListPodResourcesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resp, f.err
}

func (f *fakePodResources) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func podResources() *podresourcesapi.ListPodResourcesResponse {
	gpu := func(ids ...string) []*podresourcesapi.ContainerDevices {
		return []*podresourcesapi.ContainerDevices{{ResourceName: "nvidia.com/gpu", DeviceIds: ids}}
	}
	return &podresourcesapi.ListPodResourcesResponse{PodResources: []*podresourcesapi.PodResources{
		{Namespace: "ns", Name: "neighbor", Containers: []*podresourcesapi.ContainerResources{
			{Name: "hold", Devices: gpu("nvidia0")},
		}},
		{Namespace: "ns", Name: "donor", Containers: []*podresourcesapi.ContainerResources{
			{Name: "sidecar"},
			{Name: "load", Devices: gpu("GPU-bbb")},
		}},
		{Namespace: "ns", Name: "other", Containers: []*podresourcesapi.ContainerResources{
			{Name: "c", Devices: []*podresourcesapi.ContainerDevices{
				{ResourceName: "example.com/fpga", DeviceIds: []string{"nvidia1"}},
			}},
			{Name: "d", Devices: gpu("weird-id")},
		}},
	}}
}

func TestBuildHolders(t *testing.T) {
	host := fakeHost(t)
	dev, proc := host.dev, host.proc
	gpus, err := gpushadow.Discover(dev, proc)
	if err != nil {
		t.Fatal(err)
	}
	holders := gpushadow.BuildHolders(podResources(), gpus, "nvidia.com/gpu")
	if len(holders.GPUs) != 2 || len(holders.Holders) != 3 {
		t.Fatalf("holders %+v", holders)
	}
	if got := holders.HeldBy("ns", "donor", "nvidia.com/gpu"); len(got) != 1 || got[0].UUID != "GPU-bbb" {
		t.Errorf("donor holds %+v, want GPU-bbb (device ID given as a UUID)", got)
	}
	if got := holders.HeldBy("ns", "neighbor", "nvidia.com/gpu"); len(got) != 1 || got[0].UUID != "GPU-aaa" {
		t.Errorf("neighbor holds %+v, want GPU-aaa", got)
	}
	if got := holders.HeldBy("ns", "other", "nvidia.com/gpu"); len(got) != 0 {
		t.Errorf("an unknown device ID and another resource must map to no GPU: %+v", got)
	}
}
