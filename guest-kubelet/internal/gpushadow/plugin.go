package gpushadow

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
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

func devExists(cfg *Config, name string) bool {
	_, err := os.Stat(filepath.Join(cfg.DevRoot, name))
	return err == nil
}

func deviceSpec(cfg *Config, name string) *pluginapi.DeviceSpec {
	return &pluginapi.DeviceSpec{
		HostPath: filepath.Join(cfg.HostDevRoot, name), ContainerPath: "/dev/" + name, Permissions: "mrw",
	}
}

// containerResponse is one container's Allocate answer for these GPUs: their device nodes,
// the control devices, the driver mount and LD_LIBRARY_PATH, and with uuidEnv the
// PooledUUIDsEnv variable.
func containerResponse(cfg *Config, gpus []api.GPU, uuidEnv bool) (*pluginapi.ContainerAllocateResponse, error) {
	devices := make([]*pluginapi.DeviceSpec, 0, len(gpus)+4)
	uuids := make([]string, 0, len(gpus))
	for _, g := range gpus {
		if !devExists(cfg, g.Device) {
			return nil, fmt.Errorf("device %s is gone", g.Device)
		}
		devices = append(devices, deviceSpec(cfg, g.Device))
		uuids = append(uuids, g.UUID)
	}
	for _, name := range requiredControlDevices {
		if !devExists(cfg, name) {
			return nil, fmt.Errorf("control device %s is missing (is the NVIDIA driver loaded?)", name)
		}
		devices = append(devices, deviceSpec(cfg, name))
	}
	for _, name := range optionalControlDevices {
		if devExists(cfg, name) {
			devices = append(devices, deviceSpec(cfg, name))
		}
	}
	resp := &pluginapi.ContainerAllocateResponse{Devices: devices, Envs: map[string]string{}}
	if cfg.HostDriverRoot != "" {
		resp.Mounts = []*pluginapi.Mount{{HostPath: cfg.HostDriverRoot, ContainerPath: cfg.ContainerDriverRoot, ReadOnly: true}}
		for _, tool := range cfg.Tools {
			resp.Mounts = append(resp.Mounts, &pluginapi.Mount{
				HostPath: filepath.Join(cfg.HostDriverRoot, "bin", tool), ContainerPath: "/usr/bin/" + tool, ReadOnly: true,
			})
		}
	}
	if cfg.LibraryPath != "" {
		resp.Envs["LD_LIBRARY_PATH"] = cfg.LibraryPath
	}
	if uuidEnv {
		resp.Envs[api.PooledUUIDsEnv] = strings.Join(uuids, ",")
	}
	if len(resp.Envs) == 0 {
		resp.Envs = nil
	}
	return resp, nil
}

// registerResource tells the kubelet about a plugin socket and its resource.
func registerResource(ctx context.Context, cfg *Config, socket string, resource corev1.ResourceName) error {
	conn, err := grpc.NewClient("unix://"+filepath.Join(cfg.PluginDir, filepath.Base(pluginapi.KubeletSocket)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer closeLogged(cfg.Log, "kubelet registration connection", conn.Close)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = pluginapi.NewRegistrationClient(conn).Register(ctx, &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     filepath.Base(socket),
		ResourceName: string(resource),
		Options:      &pluginapi.DevicePluginOptions{},
	})
	return err
}

func closeLogged(log *slog.Logger, what string, closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Warn("close failed", "what", what, "err", err)
	}
}
