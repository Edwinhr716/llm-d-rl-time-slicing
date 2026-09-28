// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package hostcmd is the orchestrator's client for the host command API: it
// pushes vacate and resume commands to the per-host component of each shared
// host and returns the host's ack.
package hostcmd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AddressResolver returns the host part of a node's address, normally its
// InternalIP. It returns the node name itself when it knows nothing better.
type AddressResolver func(node string) string

// Client sends host commands to <resolved node address>:<port>.
type Client struct {
	port    int
	resolve AddressResolver

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewClient returns a client for hosts listening on port. A nil resolve dials
// the node name.
func NewClient(port int, resolve AddressResolver) *Client {
	if resolve == nil {
		resolve = func(node string) string { return node }
	}
	return &Client{port: port, resolve: resolve, conns: make(map[string]*grpc.ClientConn)}
}

// Address returns the address the client dials for node.
func (c *Client) Address(node string) string {
	host := c.resolve(node)
	if host == "" {
		host = node
	}
	return net.JoinHostPort(host, strconv.Itoa(c.port))
}

func (c *Client) client(node string) (hcpb.HostCommandServiceClient, error) {
	addr := c.Address(node)
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return hcpb.NewHostCommandServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial host %s at %s: %w", node, addr, err)
	}
	c.conns[addr] = conn
	return hcpb.NewHostCommandServiceClient(conn), nil
}

// Vacate asks node to suspend every guest before deadline and returns its ack.
func (c *Client) Vacate(ctx context.Context, groupID, node string, epoch int64, deadline time.Time) (*hcpb.HostAck, error) {
	cl, err := c.client(node)
	if err != nil {
		return nil, err
	}
	return cl.Vacate(ctx, &hcpb.VacateRequest{
		GroupId:  groupID,
		NodeName: node,
		Epoch:    epoch,
		Deadline: timestamppb.New(deadline),
	})
}

// Resume asks node to resume its guests before deadline and returns its ack.
func (c *Client) Resume(ctx context.Context, groupID, node string, epoch int64, deadline time.Time) (*hcpb.HostAck, error) {
	cl, err := c.client(node)
	if err != nil {
		return nil, err
	}
	return cl.Resume(ctx, &hcpb.ResumeRequest{
		GroupId:  groupID,
		NodeName: node,
		Epoch:    epoch,
		Deadline: timestamppb.New(deadline),
	})
}

// Close closes every connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for addr, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
		delete(c.conns, addr)
	}
	return first
}

// OutcomeName returns the short lower-case name of an outcome, as used in log
// lines: "vacated", "resumed", "failed", "stale_epoch".
func OutcomeName(outcome hcpb.Outcome) string {
	return strings.ToLower(strings.TrimPrefix(outcome.String(), "OUTCOME_"))
}
