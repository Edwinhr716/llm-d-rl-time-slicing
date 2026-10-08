package gpushadow

// --mode=pooled: one resource,
// api.PooledResource, whose devices are exactly the GPUs the node's one nvidia.com/gpu holder
// (the donor) holds, read from pod-resources every PollInterval. No holder, or more than one
// holder pod (the one-donor assumption is broken), advertises zero devices. The kubelet picks
// the devices for a mirror; Allocate hands out their device nodes, the control devices, the
// driver mount and api.PooledUUIDsEnv. No GetPreferredAllocation, no API token.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// DefaultPollInterval is how often the pooled plugin re-reads pod-resources.
const DefaultPollInterval = 3 * time.Second

// PooledDevices returns the GPUs the pooled resource advertises for this pod-resources
// listing, and why (for the log). gpuResource is the donor's resource (nvidia.com/gpu).
func PooledDevices(resp *podresourcesapi.ListPodResourcesResponse, gpus []api.GPU, gpuResource string) ([]api.GPU, string) {
	h := BuildHolders(resp, gpus, gpuResource)
	pods := map[string]bool{}
	minors := map[int]bool{}
	unmapped := 0
	for _, hd := range h.Holders {
		pods[hd.Namespace+"/"+hd.Name] = true
		for _, m := range hd.Minors {
			minors[m] = true
		}
		unmapped += len(hd.DeviceIDs) - len(hd.Minors)
	}
	switch {
	case len(pods) == 0:
		return nil, "no pod holds " + gpuResource
	case len(pods) > 1:
		names := make([]string, 0, len(pods))
		for p := range pods {
			names = append(names, p)
		}
		sort.Strings(names)
		return nil, fmt.Sprintf("fail closed: %d pods hold %s (%s); one donor per node is required",
			len(pods), gpuResource, strings.Join(names, ", "))
	case unmapped > 0:
		return nil, fmt.Sprintf("fail closed: %d %s device ids do not map to a GPU", unmapped, gpuResource)
	}
	out := make([]api.GPU, 0, len(minors))
	for _, g := range gpus {
		if minors[g.Minor] {
			out = append(out, g)
		}
	}
	var donor string
	for p := range pods {
		donor = p
	}
	return out, fmt.Sprintf("donor %s holds %d GPU(s)", donor, len(out))
}

// pooled is the --mode=pooled device plugin.
type pooled struct {
	pluginapi.UnimplementedDevicePluginServer

	cfg     *Config
	gpus    []api.GPU // every GPU on the host, for Allocate
	lister  PodResourcesLister
	gpuRes  string
	poll    time.Duration
	socket  string
	server  *grpc.Server
	mu      sync.Mutex
	current []api.GPU
	changed chan struct{} // closed and replaced on every change of current
}

func newPooled(gpus []api.GPU, lister PodResourcesLister, gpuRes string, poll time.Duration, cfg *Config) *pooled {
	if poll <= 0 {
		poll = DefaultPollInterval
	}
	return &pooled{
		cfg: cfg, gpus: gpus, lister: lister, gpuRes: gpuRes, poll: poll,
		socket:  filepath.Join(cfg.PluginDir, "timeslice-gpu-shadow-pooled.sock"),
		changed: make(chan struct{}),
	}
}

// refresh reads pod-resources once and updates the advertised set. A List error keeps the
// last set: the kubelet keeps existing allocations anyway, and a transient error must not
// withdraw the donor's GPUs.
func (p *pooled) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := p.lister.List(ctx, &podresourcesapi.ListPodResourcesRequest{})
	if err != nil {
		p.cfg.Log.Warn("pod-resources List failed; keeping the advertised devices", "err", err)
		return
	}
	devs, why := PooledDevices(resp, p.gpus, p.gpuRes)
	p.mu.Lock()
	defer p.mu.Unlock()
	if slices.Equal(devs, p.current) {
		return
	}
	p.current = devs
	close(p.changed)
	p.changed = make(chan struct{})
	ids := make([]string, 0, len(devs))
	for _, g := range devs {
		ids = append(ids, g.Device)
	}
	p.cfg.Log.Info("pooled devices changed", "resource", api.PooledResource, "devices", strings.Join(ids, ","),
		"count", len(devs), "why", why)
}

func (p *pooled) snapshot() ([]api.GPU, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.current), p.changed
}

// GetDevicePluginOptions: no PreStartContainer, no GetPreferredAllocation.
func (*pooled) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (p *pooled) devices(gpus []api.GPU) []*pluginapi.Device {
	out := make([]*pluginapi.Device, 0, len(gpus))
	for _, g := range gpus {
		d := &pluginapi.Device{ID: g.Device, Health: pluginapi.Healthy}
		if !devExists(p.cfg, g.Device) {
			d.Health = pluginapi.Unhealthy
		}
		if g.NUMA >= 0 {
			d.Topology = &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: int64(g.NUMA)}}}
		}
		out = append(out, d)
	}
	return out
}

// ListAndWatch sends the advertised devices, again on every change and on health changes.
func (p *pooled) ListAndWatch(_ *pluginapi.Empty, stream grpc.ServerStreamingServer[pluginapi.ListAndWatchResponse]) error {
	health := time.NewTicker(p.cfg.HealthInterval)
	defer health.Stop()
	var last []*pluginapi.Device
	sent := false
	for {
		cur, changed := p.snapshot()
		devs := p.devices(cur)
		if !sent || !sameDevices(devs, last) {
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

func sameDevices(a, b []*pluginapi.Device) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetID() != b[i].GetID() || a[i].GetHealth() != b[i].GetHealth() {
			return false
		}
	}
	return true
}

// Allocate answers each container with its chosen GPUs. It accepts any GPU of the host, not
// only the advertised ones: the kubelet chose from what it was told, and the set may have
// changed since.
func (p *pooled) Allocate(_ context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	out := &pluginapi.AllocateResponse{}
	for _, creq := range req.GetContainerRequests() {
		chosen := make([]api.GPU, 0, len(creq.GetDevicesIds()))
		for _, id := range creq.GetDevicesIds() {
			i := slices.IndexFunc(p.gpus, func(g api.GPU) bool { return g.Device == id })
			if i < 0 {
				return nil, fmt.Errorf("resource %s has no device %q", api.PooledResource, id)
			}
			chosen = append(chosen, p.gpus[i])
		}
		cresp, err := containerResponse(p.cfg, chosen, true)
		if err != nil {
			return nil, err
		}
		out.ContainerResponses = append(out.ContainerResponses, cresp)
		p.cfg.Log.Info("pooled devices allocated", "resource", api.PooledResource,
			"devices", strings.Join(creq.GetDevicesIds(), ","), "uuids", cresp.GetEnvs()[api.PooledUUIDsEnv])
	}
	return out, nil
}

// PreStartContainer is not requested.
func (*pooled) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

// GetPreferredAllocation is not requested.
func (*pooled) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (p *pooled) serve(ctx context.Context) error {
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
	server, log := p.server, p.cfg.Log
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Warn("plugin server stopped", "socket", filepath.Base(p.socket), "err", err)
		}
	}()
	return nil
}

func (p *pooled) stop() {
	if p.server != nil {
		p.server.Stop()
		p.server = nil
	}
	if err := os.Remove(p.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		p.cfg.Log.Warn("could not remove plugin socket", "socket", p.socket, "err", err)
	}
}

// RunPooled serves and registers the pooled plugin until ctx ends, polling pod-resources
// every poll, and serves and registers again after a kubelet restart.
func RunPooled(ctx context.Context, gpus []api.GPU, lister PodResourcesLister, gpuResource string, poll time.Duration, cfg *Config) error {
	cfg.defaults()
	p := newPooled(gpus, lister, gpuResource, poll, cfg)
	p.refresh(ctx)
	go func() {
		t := time.NewTicker(p.poll)
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
			err = registerResource(ctx, cfg, p.socket, api.PooledResource)
		}
		if err != nil {
			p.stop()
			if ctx.Err() != nil {
				return nil
			}
			cfg.Log.Warn("pooled plugin not registered; retrying", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
			continue
		}
		cfg.Log.Info("pooled plugin registered", "resource", api.PooledResource, "socket", filepath.Base(p.socket))
		if !waitSocketGone(ctx, p.socket, cfg.SocketCheckInterval) {
			return nil
		}
		cfg.Log.Warn("the pooled plugin socket disappeared (kubelet restart?); serving and registering again")
		p.stop()
	}
}

func waitSocketGone(ctx context.Context, socket string, every time.Duration) bool {
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
