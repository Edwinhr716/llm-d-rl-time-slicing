package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
)

// fakeHosts is a scripted server.HostCommander.
type fakeHosts struct {
	mu       sync.Mutex
	allClear bool
	lent     bool
	noticeAt []time.Time
}

func (f *fakeHosts) AllClear(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.allClear
}

func (f *fakeHosts) Lent(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lent
}

func (f *fakeHosts) StartVacate(_ string, noticeAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.noticeAt = append(f.noticeAt, noticeAt)
}

func (f *fakeHosts) setClear(allClear bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allClear = allClear
}

func (f *fakeHosts) vacates() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.noticeAt...)
}

func TestNS4_Push_AcquireWaitsForEveryHost(t *testing.T) {
	gs, group := backgroundGroup(t, "trainer", true)
	hosts := &fakeHosts{}
	client := backgroundClient(t, gs, server.WithHostCommander(hosts),
		server.WithNoticeTiming(30*time.Second, 3*time.Second))

	type result struct {
		resp *pb.AcquireResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := client.Acquire(ctx, &pb.AcquireRequest{GroupId: bgGroup, JobId: "trainer"})
		done <- result{resp, err}
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case res := <-done:
		t.Fatalf("Acquire returned while hosts are not clear: %v, %v", res.resp, res.err)
	default:
	}
	noticeAt := group.Spec().NoticeAt()
	if noticeAt.IsZero() {
		t.Fatal("no notice recorded while hosts are not clear")
	}
	vacates := hosts.vacates()
	if len(vacates) == 0 {
		t.Fatal("no vacate started")
	}
	for _, v := range vacates {
		if !v.Equal(noticeAt) {
			t.Errorf("vacate for notice %v, want the one notice %v", v, noticeAt)
		}
	}
	if got := groupStatus(t, client, "").GetGroupState(); got != pb.GroupStatus_STATE_VACATING {
		t.Errorf("state = %v, want VACATING during the notice", got)
	}

	hosts.setClear(true)
	res := <-done
	if res.err != nil || !res.resp.GetSuccess() {
		t.Fatalf("Acquire = %v, %v, want success once every host is clear", res.resp, res.err)
	}
	if !group.Spec().NoticeAt().IsZero() {
		t.Error("notice still set after the grant")
	}
}

func TestNS4_Push_AcquireImmediateWhenHostsClear(t *testing.T) {
	gs, _ := backgroundGroup(t, "trainer", true)
	hosts := &fakeHosts{allClear: true}
	client := backgroundClient(t, gs, server.WithHostCommander(hosts))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Acquire(ctx, &pb.AcquireRequest{GroupId: bgGroup, JobId: "trainer"})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Acquire = %v, %v, want success", resp, err)
	}
	if len(hosts.vacates()) != 0 {
		t.Error("vacate started while every host is clear")
	}
}

func TestNS4_Push_StatusBackgroundWhileLent(t *testing.T) {
	gs, _ := backgroundGroup(t, "", false)
	hosts := &fakeHosts{lent: true}
	client := backgroundClient(t, gs, server.WithHostCommander(hosts))
	if got := groupStatus(t, client, "").GetGroupState(); got != pb.GroupStatus_STATE_BACKGROUND {
		t.Errorf("state = %v, want BACKGROUND while the hosts are lent", got)
	}
}
