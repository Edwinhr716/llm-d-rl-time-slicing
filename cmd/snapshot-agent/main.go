// Copyright 2025 The llm-d Authors.
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

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/features"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/server"
)

func main() {
	// Initialize slog with ContextHandler
	jsonHandler := slog.NewJSONHandler(os.Stdout, nil)
	ctxHandler := logging.NewContextHandler(jsonHandler)
	slog.SetDefault(slog.New(ctxHandler))

	port := flag.Int("port", 9001, "The port to listen on")
	deploymentMode := flag.String("deployment-mode", "standalone", "Deployment mode ('standalone' or 'k8s')")
	featureGateSpec := flag.String("feature-gates", "",
		"Comma-separated Name=bool pairs enabling experimental features, "+
			"e.g. 'MemoryRegionsBackend=true,DirectMemoryBackend=true'")
	flag.Parse()

	depMode := *deploymentMode
	if envDepMode := os.Getenv("DEPLOYMENT_MODE"); envDepMode != "" {
		depMode = envDepMode
	}

	// AGENT_PORT overrides the flag, mirroring DEPLOYMENT_MODE: the Helm
	// chart configures the agent through env vars, not flags.
	listenPort := *port
	if envPort := os.Getenv("AGENT_PORT"); envPort != "" {
		p, err := strconv.Atoi(envPort)
		if err != nil {
			slog.Error("Invalid AGENT_PORT", "value", envPort, "error", err)
			os.Exit(1)
		}
		listenPort = p
	}

	// FEATURE_GATES overrides the flag, mirroring DEPLOYMENT_MODE.
	gateSpec := *featureGateSpec
	if envGates := os.Getenv("FEATURE_GATES"); envGates != "" {
		gateSpec = envGates
	}
	featureGates, err := features.Parse(gateSpec)
	if err != nil {
		slog.Error("Invalid --feature-gates / FEATURE_GATES", "value", gateSpec, "error", err)
		os.Exit(1)
	}

	if depMode != "standalone" && depMode != "k8s" {
		slog.Error("Invalid deployment mode, must be 'standalone' or 'k8s'", "mode", depMode)
		os.Exit(1)
	}
	ctx := context.Background()

	// The channel registry is shared between the app-channel backend and the
	// server's WorkloadChannel RPC handler.
	channelRegistry := backends.NewChannelRegistry()
	registeredBackends := map[backends.BackendType]backends.Backend{
		backends.BackendCuda:          backends.NewCudaCheckpoint(),
		backends.BackendNoop:          backends.NewNoopBackend(),
		backends.BackendAppEndpoint:   backends.NewAppEndpointBackend(),
		backends.BackendAppChannel:    backends.NewAppChannelBackend(channelRegistry),
		backends.BackendMemoryRegions: backends.NewMemoryRegions(),
	}

	// GPU-CR (memory-regions backend) housekeeping runs only when the shared
	// checkpoint dir is configured (the Helm chart sets EXPORT_FILE_PATH iff
	// memoryRegions.enabled), keeping CUDA/app-only deployments untouched.
	if ctlDir := os.Getenv("EXPORT_FILE_PATH"); ctlDir != "" {
		// The dir must be writable by the (unprivileged) GPU-CR workloads
		// that mmap their dump buffers in it.
		if _, err := os.Stat(ctlDir); err == nil {
			if err := os.Chmod(ctlDir, 0o777); err != nil {
				slog.WarnContext(ctx, "Failed to chmod GPU-CR checkpoint dir to 0777", "dir", ctlDir, "error", err)
			} else {
				slog.InfoContext(ctx, "Set GPU-CR checkpoint dir permissions to 0777", "dir", ctlDir)
			}
		}
		// Sweep stale GPU-CR artifacts: on hugetlbfs each leaked dump pair
		// pins ~27Gi of hugepage reservations, so leaks exhaust the pool in
		// two runs.
		backends.StartGC(ctx, ctlDir, 10*time.Minute)
	}

	slog.InfoContext(ctx, "Starting Snapshot Agent",
		"port", listenPort, "deploymentMode", depMode, "featureGates", featureGates.String())
	if err := server.StartServer(ctx, listenPort, registeredBackends, backends.BackendCuda, depMode, channelRegistry, featureGates); err != nil {
		slog.ErrorContext(ctx, "Failed to start server", "error", err)
		os.Exit(1)
	}
}
