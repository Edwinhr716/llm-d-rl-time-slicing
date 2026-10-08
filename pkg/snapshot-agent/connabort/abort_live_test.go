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

//go:build linux

package connabort_test

import (
	"errors"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/connabort"
)

// TestAbortResetsServingConnections runs against the real kernel. It needs CAP_NET_ADMIN
// (and CAP_SYS_ADMIN for the namespace case); without them, or without
// CONFIG_INET_DIAG_DESTROY, it skips.
func TestAbortResetsServingConnections(t *testing.T) {
	for _, ns := range []string{"", "/proc/self/ns/net"} {
		t.Run("ns="+ns, func(t *testing.T) {
			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			_, portStr, err := net.SplitHostPort(ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				t.Fatal(err)
			}
			accepted := make(chan net.Conn, 1)
			go func() {
				c, err := ln.Accept()
				if err == nil {
					accepted <- c
				}
			}()
			client, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server := <-accepted
			defer server.Close()

			n, err := connabort.Abort(ns, []int{port}, time.Now().Add(2*time.Second))
			if errors.Is(err, syscall.EPERM) || errors.Is(err, connabort.ErrUnsupported) {
				t.Skipf("no privilege or no kernel support: %v", err)
			}
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Fatalf("aborted %d connections, want 1 (the server side only)", n)
			}
			if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := client.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
				t.Fatalf("client read: %v, want ECONNRESET", err)
			}
			// The listener survives: a new client still connects.
			c2, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", ln.Addr().String())
			if err != nil {
				t.Fatalf("listener gone after the abort: %v", err)
			}
			c2.Close()
		})
	}
}
