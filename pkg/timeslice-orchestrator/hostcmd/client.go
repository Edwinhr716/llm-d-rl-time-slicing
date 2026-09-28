// Package hostcmd commands the hosts of a group: it tells each host's
// snapshot agent to suspend or resume every guest on the host
// (HostCommandService), waits for every host to ack, and tells the
// controller when all hosts of a group are clear.
package hostcmd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client sends host commands to the host of a node.
type Client interface {
	SuspendAll(ctx context.Context, node string, req *agentpb.SuspendAllRequest) (*agentpb.HostCommandAck, error)
	ResumeAll(ctx context.Context, node string, req *agentpb.ResumeAllRequest) (*agentpb.HostCommandAck, error)
}

// GRPCClient is a Client over gRPC. It reaches the host of a node at
// <resolve(node)>:<port> and keeps one connection per address.
type GRPCClient struct {
	resolve func(node string) string
	port    int

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

var _ Client = (*GRPCClient)(nil)

// NewGRPCClient returns a GRPCClient. resolve maps a node name to the host
// address (usually the node's InternalIP).
func NewGRPCClient(resolve func(node string) string, port int) *GRPCClient {
	return &GRPCClient{resolve: resolve, port: port, conns: make(map[string]*grpc.ClientConn)}
}

func (c *GRPCClient) client(node string) (agentpb.HostCommandServiceClient, error) {
	addr := net.JoinHostPort(c.resolve(node), strconv.Itoa(c.port))
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return agentpb.NewHostCommandServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial host of node %s at %s: %w", node, addr, err)
	}
	c.conns[addr] = conn
	return agentpb.NewHostCommandServiceClient(conn), nil
}

// SuspendAll implements Client.
func (c *GRPCClient) SuspendAll(
	ctx context.Context, node string, req *agentpb.SuspendAllRequest,
) (*agentpb.HostCommandAck, error) {
	cl, err := c.client(node)
	if err != nil {
		return nil, err
	}
	return cl.SuspendAll(ctx, req)
}

// ResumeAll implements Client.
func (c *GRPCClient) ResumeAll(
	ctx context.Context, node string, req *agentpb.ResumeAllRequest,
) (*agentpb.HostCommandAck, error) {
	cl, err := c.client(node)
	if err != nil {
		return nil, err
	}
	return cl.ResumeAll(ctx, req)
}

// Close closes every connection.
func (c *GRPCClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, addr)
	}
}
