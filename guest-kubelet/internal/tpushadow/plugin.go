package tpushadow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow"
)

// DefaultPollInterval is how often the plugin re-reads pod-resources.
const DefaultPollInterval = 3 * time.Second

// Config is the plugin's host layout and the TPU environment it hands out.
type Config struct {
	PluginDir   string // kubelet device-plugin directory
	DevRoot     string // the host's /dev as mounted in this container
	HostDevRoot string // the host's /dev as the kubelet sees it
	// Mounts are host directories bind-mounted at the same path into the guest
	// (the TPU runtime socket directory and the libtpu log directory).
	Mounts         []string
	Generation     string // accelerator prefix, e.g. "v6e"
	MetricsBase    int    // first TPU runtime metrics port
	HealthInterval time.Duration
	SocketCheck    time.Duration
	Log            *slog.Logger
}

func (c *Config) defaults() {
	if c.HealthInterval == 0 {
		c.HealthInterval = 10 * time.Second
	}
	if c.SocketCheck == 0 {
		c.SocketCheck = 2 * time.Second
	}
	if c.MetricsBase == 0 {
		c.MetricsBase = 8431
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

func (c *Config) exists(rel string) bool {
	_, err := os.Stat(filepath.Join(c.DevRoot, rel))
	return err == nil
}

func (c *Config) spec(rel string) *pluginapi.DeviceSpec {
	return &pluginapi.DeviceSpec{HostPath: filepath.Join(c.HostDevRoot, rel), ContainerPath: "/dev/" + rel, Permissions: "mrw"}
}

// ContainerResponse is what a guest container given chips receives: the VFIO group devices,
// the VFIO container device, the runtime mounts and the TPU environment.
func ContainerResponse(cfg *Config, chips []Chip) (*pluginapi.ContainerAllocateResponse, error) {
	devs := make([]*pluginapi.DeviceSpec, 0, len(chips)+1)
	for _, c := range chips {
		if !cfg.exists("vfio/" + c.Group) {
			return nil, fmt.Errorf("VFIO group %s is gone", c.Group)
		}
		devs = append(devs, cfg.spec("vfio/"+c.Group))
	}
	if !cfg.exists("vfio/vfio") {
		return nil, errors.New("VFIO container device is missing")
	}
	devs = append(devs, cfg.spec("vfio/vfio"))
	env, err := Env(cfg.Generation, len(chips), cfg.MetricsBase, chips)
	if err != nil {
		return nil, err
	}
	resp := &pluginapi.ContainerAllocateResponse{Devices: devs, Envs: env}
	for _, m := range cfg.Mounts {
		resp.Mounts = append(resp.Mounts, &pluginapi.Mount{HostPath: m, ContainerPath: m})
	}
	return resp, nil
}

type plugin struct {
	pluginapi.UnimplementedDevicePluginServer

	cfg     *Config
	chips   []Chip
	lister  gpushadow.PodResourcesLister
	tpuRes  string
	poll    time.Duration
	socket  string
	server  *grpc.Server
	mu      sync.Mutex
	current []Chip
	changed chan struct{}
}

func (p *plugin) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := p.lister.List(ctx, &podresourcesapi.ListPodResourcesRequest{})
	if err != nil {
		p.cfg.Log.Warn("pod-resources List failed; keeping the advertised chips", "err", err)
		return
	}
	chips, why := PooledChips(resp, p.chips, p.tpuRes)
	p.mu.Lock()
	defer p.mu.Unlock()
	if slices.Equal(chips, p.current) {
		return
	}
	p.current = chips
	close(p.changed)
	p.changed = make(chan struct{})
	ids := make([]string, 0, len(chips))
	for _, c := range chips {
		ids = append(ids, c.Group)
	}
	p.cfg.Log.Info("pooled chips changed", "resource", PooledResource, "chips", strings.Join(ids, ","), "why", why)
}

func (p *plugin) snapshot() ([]Chip, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.current), p.changed
}

func (*plugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (p *plugin) devices(chips []Chip) []*pluginapi.Device {
	out := make([]*pluginapi.Device, 0, len(chips))
	for _, c := range chips {
		d := &pluginapi.Device{ID: c.Group, Health: pluginapi.Healthy}
		if !p.cfg.exists("vfio/" + c.Group) {
			d.Health = pluginapi.Unhealthy
		}
		out = append(out, d)
	}
	return out
}

// ListAndWatch sends the advertised chips, again on every change and on health changes.
func (p *plugin) ListAndWatch(_ *pluginapi.Empty, stream grpc.ServerStreamingServer[pluginapi.ListAndWatchResponse]) error {
	health := time.NewTicker(p.cfg.HealthInterval)
	defer health.Stop()
	var last []*pluginapi.Device
	sent := false
	for {
		cur, changed := p.snapshot()
		devs := p.devices(cur)
		if !sent || !same(devs, last) {
			if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: devs}); err != nil {
				return err
			}
			sent, last = true, devs
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-changed:
		case <-health.C:
		}
	}
}

func same(a, b []*pluginapi.Device) bool {
	return slices.EqualFunc(a, b, func(x, y *pluginapi.Device) bool {
		return x.GetID() == y.GetID() && x.GetHealth() == y.GetHealth()
	})
}

// Allocate accepts any chip of the host: the kubelet chose from what it was told, and the
// advertised set may have changed since.
func (p *plugin) Allocate(_ context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	out := &pluginapi.AllocateResponse{}
	for _, creq := range req.GetContainerRequests() {
		chosen := make([]Chip, 0, len(creq.GetDevicesIds()))
		for _, id := range creq.GetDevicesIds() {
			i := slices.IndexFunc(p.chips, func(c Chip) bool { return c.Group == id })
			if i < 0 {
				return nil, fmt.Errorf("resource %s has no chip %q", PooledResource, id)
			}
			chosen = append(chosen, p.chips[i])
		}
		cresp, err := ContainerResponse(p.cfg, chosen)
		if err != nil {
			return nil, err
		}
		out.ContainerResponses = append(out.ContainerResponses, cresp)
		p.cfg.Log.Info("pooled chips allocated", "resource", PooledResource, "chips", strings.Join(creq.GetDevicesIds(), ","))
	}
	return out, nil
}

func (*plugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

func (*plugin) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (p *plugin) serve(ctx context.Context) error {
	if err := os.Remove(p.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "unix", p.socket)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.socket, err)
	}
	p.server = grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(p.server, p)
	srv, log := p.server, p.cfg.Log
	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Warn("plugin server stopped", "err", err)
		}
	}()
	return nil
}

func (p *plugin) stop() {
	if p.server != nil {
		p.server.Stop()
		p.server = nil
	}
	if err := os.Remove(p.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		p.cfg.Log.Warn("could not remove plugin socket", "err", err)
	}
}

func (p *plugin) register(ctx context.Context) error {
	conn, err := grpc.NewClient("unix://"+filepath.Join(p.cfg.PluginDir, filepath.Base(pluginapi.KubeletSocket)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = pluginapi.NewRegistrationClient(conn).Register(ctx, &pluginapi.RegisterRequest{
		Version: pluginapi.Version, Endpoint: filepath.Base(p.socket),
		ResourceName: string(PooledResource), Options: &pluginapi.DevicePluginOptions{},
	})
	return err
}

// Run serves and registers the plugin until ctx ends, polling pod-resources every poll, and
// serves and registers again after a kubelet restart.
func Run(ctx context.Context, chips []Chip, lister gpushadow.PodResourcesLister, tpuResource string, poll time.Duration, cfg *Config) error {
	cfg.defaults()
	if poll <= 0 {
		poll = DefaultPollInterval
	}
	p := &plugin{
		cfg: cfg, chips: chips, lister: lister, tpuRes: tpuResource, poll: poll,
		socket: filepath.Join(cfg.PluginDir, "timeslice-tpu-shadow.sock"), changed: make(chan struct{}),
	}
	p.refresh(ctx)
	go func() {
		t := time.NewTicker(poll)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.refresh(ctx)
			}
		}
	}()
	defer p.stop()
	for {
		err := p.serve(ctx)
		if err == nil {
			err = p.register(ctx)
		}
		if err != nil {
			p.stop()
			if ctx.Err() != nil {
				return nil
			}
			cfg.Log.Warn("plugin not registered; retrying", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
			continue
		}
		cfg.Log.Info("plugin registered", "resource", PooledResource)
		if !waitGone(ctx, p.socket, cfg.SocketCheck) {
			return nil
		}
		cfg.Log.Warn("plugin socket disappeared (kubelet restart?); registering again")
		p.stop()
	}
}

func waitGone(ctx context.Context, socket string, every time.Duration) bool {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			if _, err := os.Stat(socket); errors.Is(err, os.ErrNotExist) {
				return true
			}
		}
	}
}
