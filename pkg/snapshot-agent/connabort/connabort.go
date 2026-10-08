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

// Package connabort resets the TCP connections into a pod's serving ports, so that the requests
// in flight on a guest fail at once instead of hanging while the guest is frozen.
//
// It uses the kernel's sock_diag netlink interface from inside the pod's network namespace: one
// SOCK_DIAG_BY_FAMILY dump per address family lists the connections, and one SOCK_DESTROY per
// connection aborts it. The kernel sends the peer a RST, as `ss -K` does. SOCK_DESTROY needs
// CONFIG_INET_DIAG_DESTROY (set on Container-Optimized OS and Ubuntu node images) and
// CAP_NET_ADMIN; entering the namespace needs CAP_SYS_ADMIN.
package connabort

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"syscall"
)

// ErrUnsupported means the kernel cannot destroy sockets (no CONFIG_INET_DIAG_DESTROY).
var ErrUnsupported = errors.New("the kernel cannot destroy sockets (CONFIG_INET_DIAG_DESTROY)")

// Netlink and sock_diag constants (linux/netlink.h, linux/sock_diag.h, linux/inet_diag.h).
const (
	nlmsgError       = 2
	nlmsgDone        = 3
	sockDiagByFamily = 20
	sockDestroy      = 21

	nlmFRequest = 0x1
	nlmFAck     = 0x4
	nlmFDump    = 0x300

	ipprotoTCP = 6

	nlmsgHdrLen = 16
	// sockIDLen is struct inet_diag_sockid; reqLen is struct inet_diag_req_v2; diagMsgLen is
	// the fixed part of struct inet_diag_msg.
	sockIDLen  = 48
	reqLen     = 8 + sockIDLen
	diagMsgLen = 4 + sockIDLen + 20

	// TCP states (include/net/tcp_states.h). Only connections that can still carry a
	// response are aborted: ESTABLISHED, and CLOSE_WAIT (the client half-closed).
	tcpEstablished = 1
	tcpCloseWait   = 8
	abortStates    = 1<<tcpEstablished | 1<<tcpCloseWait
)

// sockID is struct inet_diag_sockid: ports in network byte order, addresses as on the wire,
// interface and cookie in host byte order. A SOCK_DESTROY names a socket by the sockID its
// dump reported, cookie included.
type sockID struct {
	sport, dport uint16
	src, dst     [16]byte
	ifindex      uint32
	cookie       [2]uint32
}

func (id *sockID) put(b []byte) {
	binary.BigEndian.PutUint16(b[0:], id.sport)
	binary.BigEndian.PutUint16(b[2:], id.dport)
	copy(b[4:20], id.src[:])
	copy(b[20:36], id.dst[:])
	binary.NativeEndian.PutUint32(b[36:], id.ifindex)
	binary.NativeEndian.PutUint32(b[40:], id.cookie[0])
	binary.NativeEndian.PutUint32(b[44:], id.cookie[1])
}

func parseSockID(b *[sockIDLen]byte) sockID {
	var id sockID
	id.sport = binary.BigEndian.Uint16(b[0:])
	id.dport = binary.BigEndian.Uint16(b[2:])
	copy(id.src[:], b[4:20])
	copy(id.dst[:], b[20:36])
	id.ifindex = binary.NativeEndian.Uint32(b[36:])
	id.cookie[0] = binary.NativeEndian.Uint32(b[40:])
	id.cookie[1] = binary.NativeEndian.Uint32(b[44:])
	return id
}

// request builds one netlink message carrying a struct inet_diag_req_v2 for TCP.
func request(msgType, flags uint16, seq uint32, family uint8, states uint32, id *sockID) []byte {
	b := make([]byte, nlmsgHdrLen+reqLen)
	binary.NativeEndian.PutUint32(b[0:], nlmsgHdrLen+reqLen)
	binary.NativeEndian.PutUint16(b[4:], msgType)
	binary.NativeEndian.PutUint16(b[6:], flags|nlmFRequest)
	binary.NativeEndian.PutUint32(b[8:], seq)
	r := b[nlmsgHdrLen:]
	r[0], r[1] = family, ipprotoTCP
	binary.NativeEndian.PutUint32(r[4:], states)
	if id != nil {
		id.put(r[8:])
	}
	return b
}

// dumpRequest lists the TCP sockets of one family in the abortable states.
func dumpRequest(seq uint32, family uint8) []byte {
	return request(sockDiagByFamily, nlmFDump, seq, family, abortStates, nil)
}

// destroyRequest aborts one socket; the kernel acknowledges it with an NLMSG_ERROR.
func destroyRequest(seq uint32, family uint8, id *sockID) []byte {
	return request(sockDestroy, nlmFAck, seq, family, ^uint32(0), id)
}

// message is one netlink message: its type and payload.
type message struct {
	typ  uint16
	data []byte
}

// parseMessages splits a netlink receive buffer into messages.
func parseMessages(b []byte) ([]message, error) {
	var out []message
	for len(b) >= nlmsgHdrLen {
		n := int(binary.NativeEndian.Uint32(b[0:]))
		if n < nlmsgHdrLen || n > len(b) {
			return nil, fmt.Errorf("netlink message length %d out of range (%d bytes left)", n, len(b))
		}
		out = append(out, message{typ: binary.NativeEndian.Uint16(b[4:]), data: b[nlmsgHdrLen:n]})
		b = b[min((n+3)&^3, len(b)):] // NLMSG_ALIGN
	}
	return out, nil
}

// errno returns the error of an NLMSG_ERROR payload: 0 for an acknowledgement.
func errno(data []byte) (syscall.Errno, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("short NLMSG_ERROR payload (%d bytes)", len(data))
	}
	// The kernel writes -errno as an int32: negate it in two's complement.
	return syscall.Errno(^binary.NativeEndian.Uint32(data) + 1), nil
}

// socket is one TCP socket a dump reported.
type socket struct {
	family uint8
	state  uint8
	id     sockID
}

func parseDiagMsg(data []byte) (socket, error) {
	if len(data) < diagMsgLen {
		return socket{}, fmt.Errorf("short inet_diag_msg (%d bytes)", len(data))
	}
	return socket{family: data[0], state: data[1], id: parseSockID((*[sockIDLen]byte)(data[4 : 4+sockIDLen]))}, nil
}

// abortable reports whether s is a connection into one of ports, in a state that can still carry
// a response. Listening sockets are never aborted.
func (s *socket) abortable(ports []int) bool {
	return (s.state == tcpEstablished || s.state == tcpCloseWait) && slices.Contains(ports, int(s.id.sport))
}
