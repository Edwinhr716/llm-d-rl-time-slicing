package gpushadow

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// PodResourcesLister is the part of the kubelet's pod-resources API the plugin uses.
type PodResourcesLister interface {
	List(
		ctx context.Context, in *podresourcesapi.ListPodResourcesRequest, opts ...grpc.CallOption,
	) (*podresourcesapi.ListPodResourcesResponse, error)
}

// DialPodResources connects to the kubelet's pod-resources socket.
func DialPodResources(socket string) (PodResourcesLister, func() error, error) {
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return podresourcesapi.NewPodResourcesListerClient(conn), conn.Close, nil
}

// BuildHolders turns a pod-resources listing into the Holders document: every container that
// holds devices of resource (the normal GPU resource, nvidia.com/gpu), with the GPUs behind them.
func BuildHolders(resp *podresourcesapi.ListPodResourcesResponse, gpus []api.GPU, resource string) api.Holders {
	out := api.Holders{GPUs: append([]api.GPU(nil), gpus...), Holders: []api.Holder{}}
	for _, p := range resp.GetPodResources() {
		for _, c := range p.GetContainers() {
			for _, d := range c.GetDevices() {
				if d.GetResourceName() != resource || len(d.GetDeviceIds()) == 0 {
					continue
				}
				h := api.Holder{
					Namespace: p.GetNamespace(), Name: p.GetName(), Container: c.GetName(),
					Resource: resource, DeviceIDs: append([]string(nil), d.GetDeviceIds()...), Minors: []int{},
				}
				for _, id := range d.GetDeviceIds() {
					if m, err := api.MinorFromDeviceID(id, gpus); err == nil {
						h.Minors = append(h.Minors, m)
					}
				}
				out.Holders = append(out.Holders, h)
			}
		}
	}
	return out
}
