package gpushadow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// Config says where things are on the host and in the plugin's container.
type Config struct {
	// PluginDir is the kubelet's device-plugin directory as the plugin sees it (a hostPath
	// mount of /var/lib/kubelet/device-plugins). It holds kubelet.sock and our sockets.
	PluginDir string
	// DevRoot is the host's /dev as the plugin sees it, used only to check devices exist.
	DevRoot string
	// HostDevRoot is /dev on the host: the paths handed to the kubelet in Allocate.
	HostDevRoot string
	// HostDriverRoot is mounted read-only at ContainerDriverRoot in the mirror (driver
	// libraries and nvidia-smi). GKE: /home/kubernetes/bin/nvidia -> /usr/local/nvidia.
	HostDriverRoot, ContainerDriverRoot string
	// LibraryPath is set as LD_LIBRARY_PATH in the mirror (empty: not set). The GKE driver
	// libraries are not in an image's ld.so cache, and CUDA images that ship forward-compat
	// libraries then load the wrong libcuda (error 803). A pod-level env still wins.
	LibraryPath string
	// Tools are binaries in HostDriverRoot/bin that are also mounted read-only into the
	// mirror's /usr/bin (nvidia-smi), as the image's PATH does not include the driver root.
	Tools []string
	// HealthInterval is how often device health is re-checked.
	HealthInterval time.Duration
	// SocketCheckInterval is how often the plugin checks that the kubelet still has its
	// sockets (a kubelet restart deletes them; the plugin then serves and registers again).
	SocketCheckInterval time.Duration
	Log                 *slog.Logger
}

func (c *Config) defaults() {
	if c.HealthInterval == 0 {
		c.HealthInterval = 10 * time.Second
	}
	if c.SocketCheckInterval == 0 {
		c.SocketCheckInterval = 2 * time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// Control devices every CUDA process needs. The first two are required.
var (
	requiredControlDevices = []string{"nvidiactl", "nvidia-uvm"}
	optionalControlDevices = []string{"nvidia-uvm-tools", "nvidia-modeset"}
)

// shadow is the device plugin for one physical GPU: resource timeslice.io/gpu-shadow-<minor>
// with one device, whose ID is the GPU's device name (nvidiaN).
type shadow struct {
	pluginapi.UnimplementedDevicePluginServer

	gpu    api.GPU
	cfg    *Config
	socket string

	server *grpc.Server
}

func newShadow(gpu *api.GPU, cfg *Config) *shadow {
	return &shadow{gpu: *gpu, cfg: cfg, socket: filepath.Join(cfg.PluginDir, "timeslice-gpu-shadow-"+gpu.Device+".sock")}
}

func (s *shadow) health() string {
	if _, err := os.Stat(filepath.Join(s.cfg.DevRoot, s.gpu.Device)); err != nil {
		return pluginapi.Unhealthy
	}
	return pluginapi.Healthy
}

// GetDevicePluginOptions says the plugin needs no PreStartContainer and no preferred
// allocation (one device per resource).
func (*shadow) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

// ListAndWatch sends the one device and re-sends it when its health changes.
func (s *shadow) ListAndWatch(_ *pluginapi.Empty, stream grpc.ServerStreamingServer[pluginapi.ListAndWatchResponse]) error {
	last := ""
	ticker := time.NewTicker(s.cfg.HealthInterval)
	defer ticker.Stop()
	for {
		if health := s.health(); health != last {
			last = health
			resp := &pluginapi.ListAndWatchResponse{Devices: []*pluginapi.Device{{ID: s.gpu.Device, Health: health}}}
			if err := stream.Send(resp); err != nil {
				return err
			}
			s.cfg.Log.Info("shadow device advertised",
				"resource", s.gpu.Resource, "device", s.gpu.Device, "uuid", s.gpu.UUID, "health", health)
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Allocate returns the GPU's device node, the control devices and the driver mount. The kubelet
// adds the device nodes to the container's device cgroup; the container needs no privilege.
func (s *shadow) Allocate(_ context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	out := &pluginapi.AllocateResponse{}
	for _, creq := range req.GetContainerRequests() {
		for _, id := range creq.GetDevicesIds() {
			if id != s.gpu.Device {
				return nil, fmt.Errorf("resource %s has only device %s, got %q", s.gpu.Resource, s.gpu.Device, id)
			}
		}
		cresp, err := s.containerResponse()
		if err != nil {
			return nil, err
		}
		out.ContainerResponses = append(out.ContainerResponses, cresp)
		s.cfg.Log.Info("shadow device allocated", "resource", s.gpu.Resource, "device", s.gpu.Device, "uuid", s.gpu.UUID)
	}
	return out, nil
}

func (s *shadow) exists(name string) bool {
	_, err := os.Stat(filepath.Join(s.cfg.DevRoot, name))
	return err == nil
}

func (s *shadow) deviceSpec(name string) *pluginapi.DeviceSpec {
	return &pluginapi.DeviceSpec{
		HostPath: filepath.Join(s.cfg.HostDevRoot, name), ContainerPath: "/dev/" + name, Permissions: "mrw",
	}
}

func (s *shadow) containerResponse() (*pluginapi.ContainerAllocateResponse, error) {
	if !s.exists(s.gpu.Device) {
		return nil, fmt.Errorf("device %s is gone", s.gpu.Device)
	}
	devices := []*pluginapi.DeviceSpec{s.deviceSpec(s.gpu.Device)}
	for _, name := range requiredControlDevices {
		if !s.exists(name) {
			return nil, fmt.Errorf("control device %s is missing (is the NVIDIA driver loaded?)", name)
		}
		devices = append(devices, s.deviceSpec(name))
	}
	for _, name := range optionalControlDevices {
		if s.exists(name) {
			devices = append(devices, s.deviceSpec(name))
		}
	}
	resp := &pluginapi.ContainerAllocateResponse{Devices: devices}
	if s.cfg.HostDriverRoot != "" {
		resp.Mounts = []*pluginapi.Mount{{HostPath: s.cfg.HostDriverRoot, ContainerPath: s.cfg.ContainerDriverRoot, ReadOnly: true}}
		for _, tool := range s.cfg.Tools {
			resp.Mounts = append(resp.Mounts, &pluginapi.Mount{
				HostPath: filepath.Join(s.cfg.HostDriverRoot, "bin", tool), ContainerPath: "/usr/bin/" + tool, ReadOnly: true,
			})
		}
	}
	if s.cfg.LibraryPath != "" {
		resp.Envs = map[string]string{"LD_LIBRARY_PATH": s.cfg.LibraryPath}
	}
	return resp, nil
}

// PreStartContainer is not requested (GetDevicePluginOptions), so the kubelet never calls it.
func (*shadow) PreStartContainer(
	context.Context, *pluginapi.PreStartContainerRequest,
) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

// GetPreferredAllocation is not requested either.
func (*shadow) GetPreferredAllocation(
	context.Context, *pluginapi.PreferredAllocationRequest,
) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (s *shadow) serve(ctx context.Context) error {
	if err := os.Remove(s.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "unix", s.socket)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.socket, err)
	}
	s.server = grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(s.server, s)
	server, log := s.server, s.cfg.Log
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Warn("plugin server stopped", "socket", filepath.Base(s.socket), "err", err)
		}
	}()
	return nil
}

func (s *shadow) stop() {
	if s.server != nil {
		s.server.Stop()
		s.server = nil
	}
	if err := os.Remove(s.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.cfg.Log.Warn("could not remove plugin socket", "socket", s.socket, "err", err)
	}
}

// register tells the kubelet about this plugin's socket and resource.
func (s *shadow) register(ctx context.Context) error {
	conn, err := grpc.NewClient("unix://"+filepath.Join(s.cfg.PluginDir, filepath.Base(pluginapi.KubeletSocket)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer closeLogged(s.cfg.Log, "kubelet registration connection", conn.Close)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = pluginapi.NewRegistrationClient(conn).Register(ctx, &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     filepath.Base(s.socket),
		ResourceName: string(s.gpu.Resource),
		Options:      &pluginapi.DevicePluginOptions{},
	})
	return err
}

func closeLogged(log *slog.Logger, what string, closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Warn("close failed", "what", what, "err", err)
	}
}

// Run serves and registers one shadow plugin per GPU until ctx ends. When the kubelet restarts
// (it deletes every socket in its directory), all plugins are served and registered again.
func Run(ctx context.Context, gpus []api.GPU, cfg *Config) error {
	cfg.defaults()
	shadows := make([]*shadow, len(gpus))
	for i := range gpus {
		shadows[i] = newShadow(&gpus[i], cfg)
	}
	stopAll := func() {
		for _, s := range shadows {
			s.stop()
		}
	}
	defer stopAll()
	for {
		if err := startAll(ctx, shadows); err != nil {
			stopAll()
			if ctx.Err() != nil {
				return nil
			}
			cfg.Log.Warn("shadow plugins not registered; retrying", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
			continue
		}
		if !waitSocketsGone(ctx, shadows, cfg.SocketCheckInterval) {
			return nil
		}
		cfg.Log.Warn("a plugin socket disappeared (kubelet restart?); serving and registering again")
		stopAll()
	}
}

func startAll(ctx context.Context, shadows []*shadow) error {
	for _, s := range shadows {
		if err := s.serve(ctx); err != nil {
			return err
		}
	}
	var (
		mu   sync.Mutex
		errs []string
		wg   sync.WaitGroup
	)
	for _, s := range shadows {
		wg.Go(func() {
			if err := s.register(ctx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", s.gpu.Resource, err))
				mu.Unlock()
				return
			}
			s.cfg.Log.Info("shadow plugin registered", "resource", s.gpu.Resource, "uuid", s.gpu.UUID, "socket", filepath.Base(s.socket))
		})
	}
	wg.Wait()
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// waitSocketsGone blocks until one of the sockets is removed (true) or ctx ends (false).
func waitSocketsGone(ctx context.Context, shadows []*shadow, every time.Duration) bool {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			for _, s := range shadows {
				if _, err := os.Stat(s.socket); errors.Is(err, os.ErrNotExist) {
					return true
				}
			}
		}
	}
}
