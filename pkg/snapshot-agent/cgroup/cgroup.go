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

// Package cgroup reads and drives the cgroup v2 hierarchy of a pod on this
// node: it finds the pod and container cgroups, lists their processes,
// freezes, thaws and kills them, and reads their memory files.
//
// Every path is resolved against one root, the host's cgroup v2 mount as the
// agent sees it (DefaultRoot). The agent runs privileged with the host
// /sys/fs/cgroup mounted read-write.
package cgroup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// DefaultRoot is where the host's cgroup v2 hierarchy is mounted.
const DefaultRoot = "/sys/fs/cgroup"

// defaultPoll is how often Freeze and Thaw re-read cgroup.events.
const defaultPoll = 2 * time.Millisecond

// maxSearchDepth bounds the walk that looks for a pod cgroup below the
// kubepods roots. The deepest known layout is kubepods.slice/<qos>/<pod>.
const maxSearchDepth = 3

// ErrNotFound is returned when a pod or container cgroup does not exist.
var ErrNotFound = errors.New("cgroup not found")

// Manager resolves and drives pod cgroups below Root.
type Manager struct {
	// Root is the cgroup v2 mount point.
	Root string
	// Poll is how often Freeze and Thaw re-read cgroup.events.
	Poll time.Duration
	// kill sends a signal to a process; replaced in tests.
	kill func(pid int, sig syscall.Signal) error
}

// New returns a Manager for the hierarchy mounted at root. An empty root
// means DefaultRoot.
func New(root string) *Manager {
	if root == "" {
		root = DefaultRoot
	}
	return &Manager{Root: root, Poll: defaultPoll, kill: syscall.Kill}
}

// IsV2 reports whether Root is a cgroup v2 (unified) hierarchy.
func (m *Manager) IsV2() bool {
	_, err := os.Stat(filepath.Join(m.Root, "cgroup.controllers"))
	return err == nil
}

// PodCgroupPath returns the pod-level cgroup directory of the pod with the
// given UID. It accepts both the systemd layout
// (kubepods.slice/kubepods-<qos>.slice/kubepods-<qos>-pod<uid_>.slice, with
// Guaranteed pods directly under kubepods.slice) and the cgroupfs layout
// (kubepods/<qos>/pod<uid>). The UID may appear with dashes or with
// underscores, as on /proc/<pid>/cgroup.
func (m *Manager) PodCgroupPath(podUID string) (string, error) {
	if podUID == "" {
		return "", fmt.Errorf("empty pod UID: %w", ErrNotFound)
	}
	names := []string{"pod" + podUID, "pod" + strings.ReplaceAll(podUID, "-", "_")}
	for _, top := range []string{"kubepods.slice", "kubepods"} {
		base := filepath.Join(m.Root, top)
		if _, err := os.Stat(base); err != nil {
			continue
		}
		if dir := findDir(base, maxSearchDepth, func(name string) bool {
			return matchesPod(name, names)
		}); dir != "" {
			return dir, nil
		}
	}
	return "", fmt.Errorf("pod %s under %s: %w", podUID, m.Root, ErrNotFound)
}

// matchesPod reports whether a cgroup directory name is the pod cgroup for
// one of names ("pod<uid>" spellings): "pod<uid>" (cgroupfs) or
// "kubepods[-<qos>]-pod<uid>.slice" (systemd).
func matchesPod(name string, names []string) bool {
	for _, n := range names {
		if name == n || (strings.HasSuffix(name, "-"+n+".slice")) {
			return true
		}
	}
	return false
}

// findDir walks base breadth-first, at most depth levels down, and returns
// the first directory whose name matches.
func findDir(base string, depth int, match func(string) bool) string {
	level := []string{base}
	for d := 0; d < depth && len(level) > 0; d++ {
		var next []string
		for _, dir := range level {
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				p := filepath.Join(dir, e.Name())
				if match(e.Name()) {
					return p
				}
				next = append(next, p)
			}
		}
		level = next
	}
	return ""
}

// ContainerCgroupPath returns the cgroup directory of one container of the
// pod whose cgroup is podDir. containerID may carry a runtime prefix
// ("containerd://<id>"). It matches "<id>" (cgroupfs) and
// "<runtime>-<id>.scope" (systemd, for example cri-containerd-<id>.scope).
func (m *Manager) ContainerCgroupPath(podDir, containerID string) (string, error) {
	id := StripRuntimePrefix(containerID)
	if id == "" {
		return "", fmt.Errorf("empty container ID: %w", ErrNotFound)
	}
	entries, err := os.ReadDir(podDir)
	if err != nil {
		return "", fmt.Errorf("read pod cgroup %s: %w", podDir, err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if name == id || strings.HasSuffix(name, "-"+id+".scope") {
			return filepath.Join(podDir, name), nil
		}
	}
	return "", fmt.Errorf("container %s under %s: %w", id, podDir, ErrNotFound)
}

// StripRuntimePrefix removes a "<runtime>://" prefix from a container ID.
func StripRuntimePrefix(containerID string) string {
	if i := strings.Index(containerID, "://"); i >= 0 {
		return containerID[i+3:]
	}
	return containerID
}

// Procs returns the sorted, de-duplicated PIDs in the given cgroups and
// every cgroup below them. The agent passes the pod's container cgroups, not
// the pod cgroup, so the sandbox (pause) process is not counted and an empty
// result means the workload has exited. A missing cgroup counts as empty: a
// container that exited has had its cgroup removed.
func (m *Manager) Procs(dirs ...string) ([]int, error) {
	seen := map[int]bool{}
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if !d.IsDir() {
				return nil
			}
			pids, err := readPIDs(filepath.Join(path, "cgroup.procs"))
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			for _, p := range pids {
				seen[p] = true
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("list processes of %s: %w", dir, err)
		}
	}
	out := make([]int, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Ints(out)
	return out, nil
}

func readPIDs(path string) ([]int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, f := range strings.Fields(string(data)) {
		p, err := strconv.Atoi(f)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		pids = append(pids, p)
	}
	return pids, nil
}

// Events returns the key/value pairs of dir's cgroup.events
// ("populated", "frozen").
func (m *Manager) Events(dir string) (map[string]int64, error) {
	return readKeyed(filepath.Join(dir, "cgroup.events"))
}

// Frozen reports whether dir's cgroup.events says "frozen 1".
func (m *Manager) Frozen(dir string) (bool, error) {
	ev, err := m.Events(dir)
	if err != nil {
		return false, err
	}
	return ev["frozen"] == 1, nil
}

// Freeze writes 1 to dir's cgroup.freeze and waits until cgroup.events
// reports frozen 1 or ctx is done. It does nothing if dir is already frozen.
func (m *Manager) Freeze(ctx context.Context, dir string) error {
	return m.setFrozen(ctx, dir, true)
}

// Thaw writes 0 to dir's cgroup.freeze and waits until cgroup.events
// reports frozen 0 or ctx is done. It does nothing if dir is not frozen.
func (m *Manager) Thaw(ctx context.Context, dir string) error {
	return m.setFrozen(ctx, dir, false)
}

func (m *Manager) setFrozen(ctx context.Context, dir string, frozen bool) error {
	verb, value := "thaw", "0"
	if frozen {
		verb, value = "freeze", "1"
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s %s: %w", verb, dir, err)
	}
	cur, err := m.Frozen(dir)
	if err != nil {
		return fmt.Errorf("%s %s: %w", verb, dir, err)
	}
	if cur == frozen {
		return nil
	}
	if err := writeControl(filepath.Join(dir, "cgroup.freeze"), value); err != nil {
		return fmt.Errorf("%s %s: %w", verb, dir, err)
	}
	poll := m.Poll
	if poll <= 0 {
		poll = defaultPoll
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		cur, err := m.Frozen(dir)
		if err != nil {
			return fmt.Errorf("%s %s: %w", verb, dir, err)
		}
		if cur == frozen {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s %s: waiting for frozen=%s: %w", verb, dir, value, ctx.Err())
		case <-ticker.C:
		}
	}
}

// Kill kills every process in dir and below by writing 1 to cgroup.kill.
// When the kernel has no cgroup.kill it sends SIGKILL to every PID listed
// in dir's cgroup.procs files instead. It does not wait for the processes
// to exit; callers confirm with Procs.
func (m *Manager) Kill(dir string) error {
	err := writeControl(filepath.Join(dir, "cgroup.kill"), "1")
	if err == nil {
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("kill %s: %w", dir, err)
	}
	if _, statErr := os.Stat(dir); statErr != nil {
		return fmt.Errorf("kill %s: %w", dir, statErr)
	}
	pids, err := m.Procs(dir)
	if err != nil {
		return fmt.Errorf("kill %s: %w", dir, err)
	}
	var errs []error
	for _, pid := range pids {
		if err := m.kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			errs = append(errs, fmt.Errorf("SIGKILL %d: %w", pid, err))
		}
	}
	return errors.Join(errs...)
}

// MemoryCurrent returns dir's memory.current in bytes.
func (m *Manager) MemoryCurrent(dir string) (int64, error) {
	data, err := os.ReadFile(filepath.Join(dir, "memory.current"))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// MemoryMax returns dir's memory.max in bytes. unlimited is true when the
// file says "max" (no limit).
//
//nolint:nonamedreturns // Conflict between gocritic's unnamedResult and nonamedreturns
func (m *Manager) MemoryMax(dir string) (limit int64, unlimited bool, err error) {
	data, err := os.ReadFile(filepath.Join(dir, "memory.max"))
	if err != nil {
		return 0, false, err
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0, true, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, false, err
}

// MemoryStat returns the counters in dir's memory.stat, in bytes.
func (m *Manager) MemoryStat(dir string) (map[string]int64, error) {
	return readKeyed(filepath.Join(dir, "memory.stat"))
}

// kernelStatKeys are summed when memory.stat has no "kernel" line (kernels
// before 5.18).
var kernelStatKeys = []string{"kernel_stack", "pagetables", "sec_pagetables", "percpu", "sock", "vmalloc", "slab"}

// HostBytes returns the host memory a cgroup holds, from its memory.stat:
// anon + shmem + kernel. Page cache that the kernel can reclaim is left out,
// so this is the memory a frozen guest really pins.
func HostBytes(stat map[string]int64) int64 {
	total := stat["anon"] + stat["shmem"]
	if k, ok := stat["kernel"]; ok {
		return total + k
	}
	for _, key := range kernelStatKeys {
		total += stat[key]
	}
	return total
}

// readKeyed parses a flat "key value" file.
func readKeyed(path string) (map[string]int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		v, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			continue
		}
		out[f[0]] = v
	}
	return out, nil
}

// writeControl writes value to an existing cgroup control file. It never
// creates the file, so a kernel without the file reports fs.ErrNotExist.
func writeControl(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(value); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
