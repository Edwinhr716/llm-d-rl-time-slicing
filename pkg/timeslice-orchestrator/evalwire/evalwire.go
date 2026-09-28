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

//go:build evalwire

// Package evalwire starts an in-process orchestrator for evaluation drivers.
// It is built only with the evalwire tag and never ships in the binary. The
// orchestrator is wired by app.Run, exactly as the binary wires it.
package evalwire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/app"
	"k8s.io/client-go/kubernetes"
)

// Config configures one orchestrator process.
type Config struct {
	// Clientset is the (usually fake) cluster the orchestrator watches.
	Clientset kubernetes.Interface
	// AgentPort becomes --snapshot-agent-port unless Args sets it. 0 keeps
	// the default.
	AgentPort int
	// HostPort becomes --host-command-port unless Args sets it. 0 leaves the
	// host command push off.
	HostPort int
	// Args are flags as the binary accepts them.
	Args []string
}

// Orch is a running orchestrator.
type Orch struct {
	// Addr is the gRPC address, 127.0.0.1:<port>.
	Addr string
	// MetricsAddr is the metrics address, 127.0.0.1:<port>.
	MetricsAddr string
	// Stop stops the orchestrator as a crash would (RPCs in flight are cut)
	// and waits until it is down. A later Start on the same clientset
	// listens on the same ports, so clients can reconnect.
	Stop func()
}

var (
	portsMu sync.Mutex
	// ports remembers the ports picked for a clientset, so a restart on the
	// same clientset comes back at the same address.
	ports = map[kubernetes.Interface][2]int{}
)

// Start starts an orchestrator and returns once its gRPC port accepts
// connections.
func Start(ctx context.Context, cfg Config) (*Orch, error) {
	if cfg.Clientset == nil {
		return nil, errors.New("evalwire: Config.Clientset is required")
	}
	args := append([]string(nil), cfg.Args...)
	port, metricsPort, err := pickPorts(cfg.Clientset, args)
	if err != nil {
		return nil, err
	}
	if _, ok := flagValue(args, "port"); !ok {
		args = append(args, "--port="+strconv.Itoa(port))
	}
	if _, ok := flagValue(args, "metrics-port"); !ok {
		args = append(args, "--metrics-port="+strconv.Itoa(metricsPort))
	}
	if _, ok := flagValue(args, "snapshot-agent-port"); !ok && cfg.AgentPort > 0 {
		args = append(args, "--snapshot-agent-port="+strconv.Itoa(cfg.AgentPort))
	}
	if _, ok := flagValue(args, "host-command-port"); !ok && cfg.HostPort > 0 {
		args = append(args, "--host-command-port="+strconv.Itoa(cfg.HostPort))
	}
	if v, ok := flagValue(args, "port"); ok {
		if port, err = strconv.Atoi(v); err != nil {
			return nil, fmt.Errorf("evalwire: bad --port %q: %w", v, err)
		}
	}
	if v, ok := flagValue(args, "metrics-port"); ok {
		if metricsPort, err = strconv.Atoi(v); err != nil {
			return nil, fmt.Errorf("evalwire: bad --metrics-port %q: %w", v, err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- app.Run(runCtx, args, app.Deps{Clientset: cfg.Clientset, ImmediateStop: true})
	}()

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	if err := waitListening(addr, done, 15*time.Second); err != nil {
		cancel()
		return nil, err
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
			}
		})
	}
	return &Orch{
		Addr:        addr,
		MetricsAddr: net.JoinHostPort("127.0.0.1", strconv.Itoa(metricsPort)),
		Stop:        stop,
	}, nil
}

func pickPorts(cs kubernetes.Interface, args []string) (int, int, error) {
	portsMu.Lock()
	defer portsMu.Unlock()
	if p, ok := ports[cs]; ok {
		return p[0], p[1], nil
	}
	var p [2]int
	for i := range p {
		name := []string{"port", "metrics-port"}[i]
		if v, ok := flagValue(args, name); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				return 0, 0, fmt.Errorf("evalwire: bad --%s %q: %w", name, v, err)
			}
			p[i] = n
			continue
		}
		n, err := freePort()
		if err != nil {
			return 0, 0, err
		}
		p[i] = n
	}
	ports[cs] = p
	return p[0], p[1], nil
}

func freePort() (int, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("evalwire: no free port: %w", err)
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().(*net.TCPAddr).Port, nil
}

// waitListening waits until addr accepts TCP connections, Run fails, or the
// timeout passes.
func waitListening(addr string, done <-chan error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			if err == nil {
				err = errors.New("orchestrator exited")
			}
			return fmt.Errorf("evalwire: orchestrator did not start: %w", err)
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("evalwire: orchestrator not listening on %s after %v", addr, timeout)
}

// flagValue returns the value of flag name in args ("-name=v", "--name=v",
// "-name v" or "--name v"), and whether it is set.
func flagValue(args []string, name string) (string, bool) {
	val, found := "", false
	for i := 0; i < len(args); i++ {
		a := strings.TrimLeft(args[i], "-")
		if a == args[i] {
			continue
		}
		if k, v, ok := strings.Cut(a, "="); ok {
			if k == name {
				val, found = v, true
			}
			continue
		}
		if a == name && i+1 < len(args) {
			val, found = args[i+1], true
			i++
		}
	}
	return val, found
}
