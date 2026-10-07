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

package connabort

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"golang.org/x/sys/unix"
)

// Abort resets every TCP connection into one of ports in the network namespace at nsPath (for
// example /proc/<pid>/ns/net; "" is the caller's own namespace), and returns how many it reset.
// Listening sockets are left alone, so the server keeps accepting after a resume. Each netlink
// round trip is bounded by the time left to deadline. A connection that closes on its own
// meanwhile is not an error.
func Abort(nsPath string, ports []int, deadline time.Time) (int, error) {
	if len(ports) == 0 {
		return 0, nil
	}
	fd, err := diagSocket(nsPath)
	if err != nil {
		return 0, err
	}
	defer unix.Close(fd)
	c := &conn{fd: fd, deadline: deadline}
	aborted := 0
	var errs []error
	for _, family := range []uint8{unix.AF_INET, unix.AF_INET6} {
		socks, err := c.dump(family)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for i := range socks {
			s := &socks[i]
			if !s.abortable(ports) {
				continue
			}
			err := c.destroy(s)
			switch {
			case err == nil:
				aborted++
			case errors.Is(err, unix.ENOENT): // closed meanwhile
			case errors.Is(err, unix.EOPNOTSUPP):
				return aborted, ErrUnsupported
			default:
				errs = append(errs, err)
			}
		}
	}
	return aborted, errors.Join(errs...)
}

// diagSocket opens a NETLINK_SOCK_DIAG socket in the network namespace at nsPath. A socket
// stays in the namespace it was created in, so only its creation runs on a thread moved into
// the pod's namespace. That thread is never unlocked: the Go runtime ends it with the goroutine
// instead of reusing it in the wrong namespace.
func diagSocket(nsPath string) (int, error) {
	if nsPath == "" {
		return newDiagSocket()
	}
	type result struct {
		fd  int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		ns, err := unix.Open(nsPath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			ch <- result{-1, fmt.Errorf("open %s: %w", nsPath, err)}
			return
		}
		err = unix.Setns(ns, unix.CLONE_NEWNET)
		unix.Close(ns)
		if err != nil {
			ch <- result{-1, fmt.Errorf("enter %s: %w", nsPath, err)}
			return
		}
		fd, err := newDiagSocket()
		ch <- result{fd, err}
	}()
	r := <-ch
	return r.fd, r.err
}

func newDiagSocket() (int, error) {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_SOCK_DIAG)
	if err != nil {
		return -1, fmt.Errorf("sock_diag socket: %w", err)
	}
	return fd, nil
}

// conn is one netlink socket with a deadline for every round trip.
type conn struct {
	fd       int
	deadline time.Time
	seq      uint32
	buf      [32 << 10]byte
}

func (c *conn) send(msg []byte) error {
	left := time.Until(c.deadline)
	if left <= 0 {
		return fmt.Errorf("abort deadline passed: %w", unix.ETIMEDOUT)
	}
	tv := unix.NsecToTimeval(left.Nanoseconds())
	if err := unix.SetsockoptTimeval(c.fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
		return fmt.Errorf("set receive timeout: %w", err)
	}
	if err := unix.Sendto(c.fd, msg, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return fmt.Errorf("netlink send: %w", err)
	}
	return nil
}

func (c *conn) recv() ([]message, error) {
	n, _, err := unix.Recvfrom(c.fd, c.buf[:], 0)
	if err != nil {
		return nil, fmt.Errorf("netlink receive: %w", err)
	}
	return parseMessages(c.buf[:n])
}

// dump lists the TCP sockets of one family in the abortable states.
func (c *conn) dump(family uint8) ([]socket, error) {
	c.seq++
	if err := c.send(dumpRequest(c.seq, family)); err != nil {
		return nil, err
	}
	var out []socket
	for {
		msgs, err := c.recv()
		if err != nil {
			return nil, err
		}
		for _, m := range msgs {
			switch m.typ {
			case nlmsgDone:
				return out, nil
			case nlmsgError:
				code, err := errno(m.data)
				if err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("sock_diag dump (family %d): %w", family, code)
			case sockDiagByFamily:
				s, err := parseDiagMsg(m.data)
				if err != nil {
					return nil, err
				}
				out = append(out, s)
			}
		}
	}
}

// destroy aborts one socket and waits for the kernel's acknowledgement.
func (c *conn) destroy(s *socket) error {
	c.seq++
	if err := c.send(destroyRequest(c.seq, s.family, &s.id)); err != nil {
		return err
	}
	for {
		msgs, err := c.recv()
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if m.typ != nlmsgError {
				continue
			}
			code, err := errno(m.data)
			if err != nil {
				return err
			}
			if code != 0 {
				return fmt.Errorf("sock_destroy port %d: %w", s.id.sport, code)
			}
			return nil
		}
	}
}
