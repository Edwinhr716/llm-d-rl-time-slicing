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

// Package cgroup reads and acts on the cgroup v2 tree of the pods on this
// node. Every path it takes or returns is relative to one root, the cgroup
// v2 mount (DefaultRoot in production).
package cgroup

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
)

// DefaultRoot is where the host's cgroup v2 hierarchy is mounted.
const DefaultRoot = "/sys/fs/cgroup"

// ErrNotFound means no cgroup exists for the pod.
var ErrNotFound = errors.New("pod cgroup not found")

// maxSearchDepth bounds the search for a pod cgroup below the root. Pod
// cgroups sit at depth 2 (kubepods.slice/kubepods-pod<uid>.slice) or 3
// (kubepods.slice/kubepods-<qos>.slice/kubepods-<qos>-pod<uid>.slice).
const maxSearchDepth = 3

// FS is the cgroup v2 tree under one root.
type FS struct {
	root   string
	fsys   fs.FS
	signal func(pid int) error
}

// New returns the cgroup tree mounted at root.
func New(root string) *FS {
	return &FS{
		root:   root,
		fsys:   os.DirFS(root),
		signal: sigkill,
	}
}

func sigkill(pid int) error {
	return syscall.Kill(pid, syscall.SIGKILL)
}

// Root returns the mount the FS reads.
func (c *FS) Root() string {
	return c.root
}

// PodCgroupPath returns the pod-level cgroup of the pod with podUID,
// relative to the root. It matches the directory name the way
// /proc/<pid>/cgroup is matched elsewhere, including the systemd form with
// underscores. It returns ErrNotFound when the pod has no cgroup, and an
// error when the root is not a cgroup v2 mount.
func (c *FS) PodCgroupPath(podUID string) (string, error) {
	if podUID == "" {
		return "", errors.New("pod UID is empty")
	}
	if _, err := fs.Stat(c.fsys, "cgroup.controllers"); err != nil {
		return "", fmt.Errorf("%s is not a cgroup v2 mount: %w", c.root, err)
	}
	dashed := "pod" + podUID
	underscored := "pod" + strings.ReplaceAll(podUID, "-", "_")

	found := ""
	walkErr := fs.WalkDir(c.fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			if name == "." {
				return err
			}
			return nil // A cgroup that vanished or cannot be read is skipped.
		}
		if !entry.IsDir() || name == "." {
			return nil
		}
		depth := strings.Count(name, "/") + 1
		if depth == 1 && !strings.HasPrefix(entry.Name(), "kubepods") {
			return fs.SkipDir
		}
		if strings.Contains(entry.Name(), dashed) || strings.Contains(entry.Name(), underscored) {
			found = name
			return fs.SkipAll
		}
		if depth >= maxSearchDepth {
			return fs.SkipDir
		}
		return nil
	})
	if walkErr != nil {
		return "", fmt.Errorf("searching %s for pod %s: %w", c.root, podUID, walkErr)
	}
	if found == "" {
		return "", fmt.Errorf("pod %s: %w", podUID, ErrNotFound)
	}
	return found, nil
}

// Procs returns the PIDs in the pod cgroup podPath and its descendants.
// With containerIDs set (for example "containerd://<id>" from the pod's
// container statuses), only the cgroups of those containers are read, so
// the pod sandbox (pause) process is left out and an empty result means
// the containers have exited. A cgroup that no longer exists counts as
// empty.
func (c *FS) Procs(podPath string, containerIDs []string) ([]int, error) {
	ids := bareContainerIDs(containerIDs)
	var pids []int
	seen := make(map[int]bool)
	err := fs.WalkDir(c.fsys, podPath, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		if len(ids) > 0 {
			if name == podPath {
				return nil
			}
			if path.Dir(name) == podPath && !matchesAny(entry.Name(), ids) {
				return fs.SkipDir
			}
		}
		found, readErr := c.readProcs(name)
		if readErr != nil {
			return readErr
		}
		for _, pid := range found {
			if !seen[pid] {
				seen[pid] = true
				pids = append(pids, pid)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading processes of %s: %w", podPath, err)
	}
	return pids, nil
}

func (c *FS) readProcs(dir string) ([]int, error) {
	data, err := fs.ReadFile(c.fsys, path.Join(dir, "cgroup.procs"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(data))
	pids := make([]int, 0, len(fields))
	for _, field := range fields {
		pid, err := strconv.Atoi(field)
		if err != nil {
			return nil, fmt.Errorf("%s/cgroup.procs: %w", dir, err)
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

// Kill kills every process in the pod cgroup podPath and its descendants,
// frozen or not, by writing 1 to its cgroup.kill (Linux 5.14 or later).
// Without cgroup.kill it sends SIGKILL to every PID it finds. A pod cgroup
// that no longer exists has nothing to kill. Kill does not wait: callers
// confirm with Procs.
func (c *FS) Kill(podPath string) error {
	root, err := os.OpenRoot(c.root)
	if err != nil {
		return fmt.Errorf("opening %s: %w", c.root, err)
	}
	defer root.Close()

	file, err := root.OpenFile(path.Join(podPath, "cgroup.kill"), os.O_WRONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		if _, statErr := fs.Stat(c.fsys, podPath); errors.Is(statErr, fs.ErrNotExist) {
			return nil
		}
		return c.signalAll(podPath)
	}
	if err != nil {
		return fmt.Errorf("opening %s/cgroup.kill: %w", podPath, err)
	}
	_, writeErr := file.WriteString("1")
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("writing %s/cgroup.kill: %w", podPath, err)
	}
	return nil
}

// signalAll sends SIGKILL to every process under podPath. A process that
// is already gone is not an error.
func (c *FS) signalAll(podPath string) error {
	pids, err := c.Procs(podPath, nil)
	if err != nil {
		return err
	}
	var errs []error
	for _, pid := range pids {
		if err := c.signal(pid); err != nil && !errors.Is(err, syscall.ESRCH) {
			errs = append(errs, fmt.Errorf("SIGKILL %d: %w", pid, err))
		}
	}
	return errors.Join(errs...)
}

// bareContainerIDs strips the runtime prefix ("containerd://") and drops
// empty IDs.
func bareContainerIDs(containerIDs []string) []string {
	ids := make([]string, 0, len(containerIDs))
	for _, id := range containerIDs {
		if i := strings.Index(id, "://"); i >= 0 {
			id = id[i+len("://"):]
		}
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func matchesAny(name string, ids []string) bool {
	for _, id := range ids {
		if strings.Contains(name, id) {
			return true
		}
	}
	return false
}
