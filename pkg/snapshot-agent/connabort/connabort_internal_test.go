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

package connabort

import (
	"encoding/binary"
	"slices"
	"syscall"
	"testing"
)

func testID() sockID {
	id := sockID{sport: 8000, dport: 54568, ifindex: 3, cookie: [2]uint32{7, 9}}
	copy(id.src[:], []byte{10, 0, 0, 5})
	copy(id.dst[:], []byte{10, 0, 0, 9})
	return id
}

func TestDumpRequestLayout(t *testing.T) {
	b := dumpRequest(5, 2)
	if len(b) != 72 {
		t.Fatalf("length %d, want 72", len(b))
	}
	if n := binary.NativeEndian.Uint32(b[0:]); n != 72 {
		t.Errorf("nlmsg_len %d", n)
	}
	if typ := binary.NativeEndian.Uint16(b[4:]); typ != sockDiagByFamily {
		t.Errorf("type %d", typ)
	}
	if fl := binary.NativeEndian.Uint16(b[6:]); fl != nlmFRequest|nlmFDump {
		t.Errorf("flags %#x", fl)
	}
	if seq := binary.NativeEndian.Uint32(b[8:]); seq != 5 {
		t.Errorf("seq %d", seq)
	}
	r := b[nlmsgHdrLen:]
	if r[0] != 2 || r[1] != ipprotoTCP {
		t.Errorf("family %d protocol %d", r[0], r[1])
	}
	if st := binary.NativeEndian.Uint32(r[4:]); st != 1<<1|1<<8 {
		t.Errorf("states %#x: want ESTABLISHED and CLOSE_WAIT only (never LISTEN)", st)
	}
}

func TestDestroyRequestCarriesTheSockID(t *testing.T) {
	id := testID()
	b := destroyRequest(9, 10, &id)
	if typ := binary.NativeEndian.Uint16(b[4:]); typ != sockDestroy {
		t.Errorf("type %d", typ)
	}
	if fl := binary.NativeEndian.Uint16(b[6:]); fl != nlmFRequest|nlmFAck {
		t.Errorf("flags %#x", fl)
	}
	r := b[nlmsgHdrLen:]
	if r[0] != 10 {
		t.Errorf("family %d", r[0])
	}
	if p := binary.BigEndian.Uint16(r[8:]); p != 8000 {
		t.Errorf("sport %d: ports are in network byte order", p)
	}
	if got := parseSockID((*[sockIDLen]byte)(r[8 : 8+sockIDLen])); got != id {
		t.Errorf("sockid round trip %+v, want %+v", got, id)
	}
}

// diagReply builds one SOCK_DIAG_BY_FAMILY reply message for id in state.
func diagReply(state uint8, id *sockID) []byte {
	const n = nlmsgHdrLen + diagMsgLen + 8 // 8 bytes of attributes, ignored
	b := make([]byte, n)
	binary.NativeEndian.PutUint32(b[0:], n)
	binary.NativeEndian.PutUint16(b[4:], sockDiagByFamily)
	b[nlmsgHdrLen], b[nlmsgHdrLen+1] = 2, state
	id.put(b[nlmsgHdrLen+4:])
	return b
}

// doneMsg is an NLMSG_DONE with its 4-byte payload.
func doneMsg() []byte {
	const n = nlmsgHdrLen + 4
	b := make([]byte, n)
	binary.NativeEndian.PutUint32(b[0:], n)
	binary.NativeEndian.PutUint16(b[4:], nlmsgDone)
	return b
}

func TestParseDumpReplies(t *testing.T) {
	id := testID()
	other := testID()
	other.sport = 9090
	buf := slices.Concat(diagReply(tcpEstablished, &id), diagReply(tcpCloseWait, &other), doneMsg())
	msgs, err := parseMessages(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 || msgs[2].typ != nlmsgDone {
		t.Fatalf("messages %+v", msgs)
	}
	s, err := parseDiagMsg(msgs[0].data)
	if err != nil {
		t.Fatal(err)
	}
	if s.family != 2 || s.state != tcpEstablished || s.id != id {
		t.Errorf("socket %+v", s)
	}
	if _, err := parseMessages(buf[:20]); err == nil {
		t.Error("a truncated message must be refused")
	}
	if _, err := parseDiagMsg(make([]byte, 10)); err == nil {
		t.Error("a short inet_diag_msg must be refused")
	}
}

func TestErrno(t *testing.T) {
	// neg is the payload the kernel writes for errno e: -e as an int32, then the request.
	neg := func(e uint32) []byte {
		b := make([]byte, 4+nlmsgHdrLen)
		binary.NativeEndian.PutUint32(b, ^e+1)
		return b
	}
	for _, tc := range []struct {
		in   uint32
		want syscall.Errno
	}{{0, 0}, {2, syscall.ENOENT}, {95, syscall.Errno(95)}} {
		got, err := errno(neg(tc.in))
		if err != nil || got != tc.want {
			t.Errorf("errno(%d) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
	if code, err := errno([]byte{1}); err == nil {
		t.Errorf("a short NLMSG_ERROR must be refused, got %v", code)
	}
}

func TestAbortable(t *testing.T) {
	ports := []int{8000, 8001}
	for _, tc := range []struct {
		state uint8
		sport uint16
		want  bool
	}{
		{tcpEstablished, 8000, true},
		{tcpCloseWait, 8001, true},
		{tcpEstablished, 9090, false}, // not a serving port
		{10, 8000, false},             // LISTEN: the server must keep accepting after the resume
		{6, 8000, false},              // TIME_WAIT
	} {
		s := socket{state: tc.state, id: sockID{sport: tc.sport}}
		if got := s.abortable(ports); got != tc.want {
			t.Errorf("state %d port %d: abortable %v, want %v", tc.state, tc.sport, got, tc.want)
		}
	}
}
