package gpushadow_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
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

func recvHealth(t *testing.T, stream grpc.ServerStreamingClient[pluginapi.ListAndWatchResponse]) *pluginapi.Device {
	t.Helper()
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetDevices()) != 1 {
		t.Fatalf("want one device per shadow resource, got %v", resp.GetDevices())
	}
	return resp.GetDevices()[0]
}

func checkAllocate(t *testing.T, resp *pluginapi.AllocateResponse) {
	t.Helper()
	if len(resp.GetContainerResponses()) != 1 {
		t.Fatalf("container responses: %v", resp.GetContainerResponses())
	}
	cresp := resp.GetContainerResponses()[0]
	want := map[string]string{
		"/dev/nvidia1": "/dev/nvidia1", "/dev/nvidiactl": "/dev/nvidiactl", "/dev/nvidia-uvm": "/dev/nvidia-uvm",
	}
	if len(cresp.GetDevices()) != len(want) {
		t.Errorf("devices: %v (the absent optional devices must be left out)", cresp.GetDevices())
	}
	for _, spec := range cresp.GetDevices() {
		if want[spec.GetHostPath()] != spec.GetContainerPath() || spec.GetPermissions() != "mrw" {
			t.Errorf("unexpected device %v", spec)
		}
	}
	mounts := cresp.GetMounts()
	if len(mounts) != 2 || mounts[0].GetHostPath() != "/home/kubernetes/bin/nvidia" ||
		mounts[0].GetContainerPath() != "/usr/local/nvidia" || !mounts[0].GetReadOnly() {
		t.Errorf("driver mount: %v", mounts)
	}
	if len(mounts) == 2 && (mounts[1].GetHostPath() != "/home/kubernetes/bin/nvidia/bin/nvidia-smi" ||
		mounts[1].GetContainerPath() != "/usr/bin/nvidia-smi" || !mounts[1].GetReadOnly()) {
		t.Errorf("tool mount: %v", mounts[1])
	}
	envs := cresp.GetEnvs()
	if len(envs) != 1 || envs["LD_LIBRARY_PATH"] != "/usr/local/nvidia/lib64" || len(cresp.GetAnnotations()) != 0 {
		t.Errorf("env must be only LD_LIBRARY_PATH, no annotations: %v %v", envs, cresp.GetAnnotations())
	}
}

// The plugin registers one resource per GPU with the kubelet, advertises one device on it,
// allocates only that GPU, reports it unhealthy when its node goes, and registers again after
// a kubelet restart.
func TestRunRegistersAllocatesAndReregisters(t *testing.T) {
	host := fakeHost(t)
	dev, proc := host.dev, host.proc
	gpus, err := gpushadow.Discover(dev, proc)
	if err != nil {
		t.Fatal(err)
	}
	pluginDir := shortTempDir(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kubelet := &fakeKubelet{registered: make(chan *pluginapi.RegisterRequest, 16)}
	serveUnix(ctx, t, filepath.Join(pluginDir, "kubelet.sock"), func(s *grpc.Server) {
		pluginapi.RegisterRegistrationServer(s, kubelet)
	})

	cfg := &gpushadow.Config{
		PluginDir:           pluginDir,
		DevRoot:             dev,
		HostDevRoot:         "/dev",
		HostDriverRoot:      "/home/kubernetes/bin/nvidia",
		ContainerDriverRoot: "/usr/local/nvidia",
		LibraryPath:         "/usr/local/nvidia/lib64",
		Tools:               []string{"nvidia-smi"},
		HealthInterval:      20 * time.Millisecond,
		SocketCheckInterval: 20 * time.Millisecond,
	}
	done := make(chan error, 1)
	go func() { done <- gpushadow.Run(ctx, gpus, cfg) }()

	endpoints := registrations(t, kubelet, 2)
	endpoint, ok := endpoints["timeslice.io/gpu-shadow-1"]
	if !ok || endpoints["timeslice.io/gpu-shadow-0"] == "" {
		t.Fatalf("registered %v", endpoints)
	}
	client := pluginapi.NewDevicePluginClient(dial(t, filepath.Join(pluginDir, endpoint)))

	stream, err := client.ListAndWatch(ctx, &pluginapi.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if device := recvHealth(t, stream); device.GetID() != "nvidia1" || device.GetHealth() != pluginapi.Healthy {
		t.Errorf("advertised %v", device)
	}

	resp, err := client.Allocate(ctx, &pluginapi.AllocateRequest{ContainerRequests: []*pluginapi.ContainerAllocateRequest{
		{DevicesIds: []string{"nvidia1"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	checkAllocate(t, resp)
	if _, err := client.Allocate(ctx, &pluginapi.AllocateRequest{ContainerRequests: []*pluginapi.ContainerAllocateRequest{
		{DevicesIds: []string{"nvidia0"}},
	}}); err == nil {
		t.Error("the shadow resource of GPU 1 must not allocate GPU 0")
	}

	if err := os.Remove(filepath.Join(dev, "nvidia1")); err != nil {
		t.Fatal(err)
	}
	if device := recvHealth(t, stream); device.GetHealth() != pluginapi.Unhealthy {
		t.Errorf("after the device node went: %v", device)
	}

	// A kubelet restart removes every socket in the directory.
	for _, ep := range endpoints {
		if err := os.Remove(filepath.Join(pluginDir, ep)); err != nil {
			t.Fatal(err)
		}
	}
	registrations(t, kubelet, 2)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	for _, ep := range endpoints {
		if _, err := os.Stat(filepath.Join(pluginDir, ep)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("socket %s left behind: %v", ep, err)
		}
	}
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

// The holders endpoint, reading the kubelet's pod-resources socket through DialPodResources.
func TestHoldersEndpoint(t *testing.T) {
	host := fakeHost(t)
	dev, proc := host.dev, host.proc
	gpus, err := gpushadow.Discover(dev, proc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socket := filepath.Join(shortTempDir(t), "kubelet.sock")
	fake := &fakePodResources{resp: podResources()}
	serveUnix(ctx, t, socket, func(s *grpc.Server) { podresourcesapi.RegisterPodResourcesListerServer(s, fake) })
	lister, closeLister, err := gpushadow.DialPodResources(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeLister(); err != nil {
			t.Log(err)
		}
	})
	handler := &gpushadow.HoldersHandler{Lister: lister, GPUs: gpus, Resource: "nvidia.com/gpu"}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, api.HoldersPath, http.NoBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var holders api.Holders
	if err := json.NewDecoder(rec.Body).Decode(&holders); err != nil {
		t.Fatal(err)
	}
	if got := holders.HeldBy("ns", "donor", "nvidia.com/gpu"); len(got) != 1 || got[0].Resource != api.ShadowResource(1) {
		t.Errorf("donor holds %+v", got)
	}

	fake.fail(errors.New("kubelet says no"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, api.HoldersPath, http.NoBody))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("a pod-resources error must be a 502, got %d", rec.Code)
	}
}
