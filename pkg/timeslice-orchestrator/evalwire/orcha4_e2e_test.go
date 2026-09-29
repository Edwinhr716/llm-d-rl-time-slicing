//go:build evalwire

package evalwire_test

// ORCH-A4 fake-agent tests: the in-process orchestrator (evalwire) with the
// reference host command endpoint (evalwire/host) on each node and a fake
// snapshot agent gRPC server on each node. Guests are mirror pods labelled as
// the VK writes them. With the plan's flags, N = 4s and K = 1s, so
// T = notice + 3s.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire/host"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	a4Notice = 4 * time.Second
	a4T      = 3 * time.Second // notice + N - K

	// a4AcquirePoll: the server checks a waiting Acquire every second, so a
	// grant is returned up to 1s after the last host is clear.
	a4AcquirePoll = time.Second

	// The hung-host test runs with K = 2s, so T = notice + 2s. With the e2e K
	// of 1s, the Kill and the 1s Acquire poll together do not fit before N.
	a4HungKillBudget = "--kill-budget=2s"
	a4HungT          = 2 * time.Second
)

// Kill modes of the fake agent.
const (
	killConfirm     = "confirm"
	killUnconfirmed = "unconfirmed"
)

// agentKill is one Kill the fake agent received.
type agentKill struct {
	job, reason string
	deadline    time.Time
	at          time.Time
}

// fakeAgent is a snapshot agent for one node that runs the node's guest.
type fakeAgent struct {
	agentpb.UnimplementedSnapshotAgentServiceServer

	mu     sync.Mutex
	mode   string
	guests map[string]*agentpb.JobStatus
	ops    map[string]*agentpb.GetOperationResponse
	kills  []agentKill
}

func newFakeAgent(mode string, guests ...string) *fakeAgent {
	a := &fakeAgent{mode: mode, guests: map[string]*agentpb.JobStatus{}, ops: map[string]*agentpb.GetOperationResponse{}}
	for _, g := range guests {
		a.guests[g] = &agentpb.JobStatus{JobId: g, State: agentpb.JobState_JOB_STATE_RUNNING, DeviceBytes: 1 << 30}
	}
	return a
}

func (a *fakeAgent) Status(context.Context, *agentpb.StatusRequest) (*agentpb.StatusResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	resp := &agentpb.StatusResponse{}
	for _, js := range a.guests {
		resp.JobStatuses = append(resp.JobStatuses, &agentpb.JobStatus{
			JobId: js.GetJobId(), State: js.GetState(), LastOutcome: js.GetLastOutcome(), DeviceBytes: js.GetDeviceBytes(),
		})
	}
	return resp, nil
}

func (a *fakeAgent) Kill(_ context.Context, req *agentpb.KillRequest) (*agentpb.KillResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kills = append(a.kills, agentKill{
		job: req.GetJobId(), reason: req.GetReason(), deadline: req.GetDeadline().AsTime(), at: time.Now(),
	})
	id := fmt.Sprintf("kill-%d", len(a.kills))
	switch a.mode {
	case killUnconfirmed:
		msg := "device memory still mapped"
		a.ops[id] = &agentpb.GetOperationResponse{
			Status: agentpb.OperationStatus_OPERATION_STATUS_FAILED, Error: &msg,
			ErrorReason: agentpb.ErrorReason_KILL_UNCONFIRMED,
		}
	default:
		if js, ok := a.guests[req.GetJobId()]; ok {
			js.State = agentpb.JobState_JOB_STATE_IDLE
			js.LastOutcome = agentpb.Outcome_OUTCOME_KILLED
			js.DeviceBytes = 0
		}
		a.ops[id] = &agentpb.GetOperationResponse{
			Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: agentpb.Outcome_OUTCOME_KILLED,
			ElapsedMs: 50,
		}
	}
	return &agentpb.KillResponse{OperationId: id}, nil
}

func (a *fakeAgent) GetOperation(
	_ context.Context, req *agentpb.GetOperationRequest,
) (*agentpb.GetOperationResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	op, ok := a.ops[req.GetOperationId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "operation %s not found", req.GetOperationId())
	}
	return op, nil
}

func (a *fakeAgent) killList() []agentKill {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]agentKill(nil), a.kills...)
}

// a4Cluster is a cluster with a guest mirror pod per node and the agents
// that are up.
type a4Cluster struct {
	*cluster
	agentPort int
	agents    map[string]*fakeAgent
}

// newA4Cluster builds the cluster with one guest per node. agents maps a
// node to its agent's kill mode; a node missing from it has no agent
// (unreachable).
func newA4Cluster(t *testing.T, agents map[string]string) *a4Cluster {
	t.Helper()
	clu := &a4Cluster{cluster: newCluster(t), agentPort: freePort(t, e2eNodes[0]), agents: map[string]*fakeAgent{}}
	for _, node := range e2eNodes {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mirror-" + node, Namespace: "guests",
				Labels: map[string]string{
					infrastructure.PodLabelKey:  e2eGroup,
					infrastructure.JobLabelKey:  guestOf(node),
					infrastructure.RoleLabelKey: infrastructure.RoleBackground,
				},
			},
			Spec:   corev1.PodSpec{NodeName: node, RestartPolicy: corev1.RestartPolicyNever},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		if _, err := clu.cs.CoreV1().Pods("guests").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		mode, ok := agents[node]
		if !ok {
			continue
		}
		agent := newFakeAgent(mode, guestOf(node))
		clu.agents[node] = agent
		serveAgent(t, node, clu.agentPort, agent)
	}
	return clu
}

// serveAgent serves agent as the snapshot agent of node on port until the
// test ends.
func serveAgent(t *testing.T, node string, port int, agent *fakeAgent) {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(node, strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	agentpb.RegisterSnapshotAgentServiceServer(srv, agent)
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("fake agent %s: %v", node, err)
		}
	}()
	t.Cleanup(srv.Stop)
}

// startHostsOn starts the reference host on the given nodes only; the other
// nodes' command endpoints are unreachable.
func (c *a4Cluster) startHostsOn(t *testing.T, nodes ...string) {
	t.Helper()
	for _, node := range nodes {
		hst, err := host.Start(context.Background(), host.Config{
			Node:       node,
			ListenAddr: net.JoinHostPort(node, strconv.Itoa(c.hostPort)),
			Exec:       c.execs[node],
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(hst.Stop)
	}
}

func (c *a4Cluster) startOrch(t *testing.T, extra ...string) *evalwire.Orch {
	t.Helper()
	orch, err := evalwire.Start(context.Background(), evalwire.Config{
		Clientset: c.cs,
		AgentPort: c.agentPort,
		HostPort:  c.hostPort,
		Args:      append(pushArgs(), extra...),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Stop)
	return orch
}

// waitGuestsKnown waits until the orchestrator sees both guests, so a notice
// never races the pod watch.
func waitGuestsKnown(t *testing.T, sink *logSink) {
	t.Helper()
	waitFor(t, "host registry", func() bool {
		return sink.has(t, "Host registry updated", map[string]any{"group": e2eGroup})
	})
	time.Sleep(300 * time.Millisecond)
}

// timedAcquire calls Acquire and returns the response, the error and how long
// after the call the trainer was granted.
func timedAcquire(api pb.TimeSliceOrchestratorServiceClient, bound time.Duration) (*pb.AcquireResponse, time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	defer cancel()
	start := time.Now()
	resp, err := acquire(ctx, api)
	return resp, time.Since(start), err
}

func mustGrantBy(t *testing.T, api pb.TimeSliceOrchestratorServiceClient, by time.Duration) (*pb.AcquireResponse, time.Duration) {
	t.Helper()
	resp, took, err := timedAcquire(api, 20*time.Second)
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Acquire = %v, %v", resp, err)
	}
	if took > by {
		t.Fatalf("trainer granted after %v, want by %v", took, by)
	}
	t.Logf("trainer granted after %v", took)
	return resp, took
}

// TestORCHA4_GuestOK: every host vacates at once; the trainer is granted
// well before T and no Kill is sent.
func TestORCHA4_GuestOK(t *testing.T) {
	sink := captureLogs(t)
	clu := newA4Cluster(t, map[string]string{e2eNodes[0]: killConfirm, e2eNodes[1]: killConfirm})
	clu.startHostsOn(t, e2eNodes...)
	api := client(t, clu.startOrch(t))
	waitGuestsKnown(t, sink)

	resp, _ := mustGrantBy(t, api, a4T)
	if resp.GetVramUnconfirmed() {
		t.Error("vram_unconfirmed on a clean handoff")
	}
	for node, agent := range clu.agents {
		if k := agent.killList(); len(k) != 0 {
			t.Errorf("Kill sent to %s: %+v", node, k)
		}
	}
	if !sink.has(t, "Foreground granted", map[string]any{"group": e2eGroup, "vram_unconfirmed": false}) {
		t.Error("no Foreground granted line with vram_unconfirmed=false")
	}
}

// TestORCHA4_GuestSlow: one host takes 1.5s to vacate, still before T. The
// trainer is granted after it and before T, with no Kill.
func TestORCHA4_GuestSlow(t *testing.T) {
	sink := captureLogs(t)
	clu := newA4Cluster(t, map[string]string{e2eNodes[0]: killConfirm, e2eNodes[1]: killConfirm})
	slow := clu.execs[e2eNodes[1]]
	slow.hang.Store(true)
	clu.startHostsOn(t, e2eNodes...)
	api := client(t, clu.startOrch(t))
	waitGuestsKnown(t, sink)

	released := make(chan struct{})
	go func() {
		<-slow.blocked
		time.Sleep(1500 * time.Millisecond)
		close(released)
		close(slow.release)
	}()
	_, took := mustGrantBy(t, api, a4T)
	select {
	case <-released:
	default:
		t.Error("granted before the slow host was released")
	}
	if took < 1400*time.Millisecond {
		t.Errorf("granted after %v, before the slow host acked", took)
	}
	for node, agent := range clu.agents {
		if k := agent.killList(); len(k) != 0 {
			t.Errorf("Kill sent to %s: %+v", node, k)
		}
	}
}

// TestORCHA4_GuestHung: one host never acks. Its guest is killed at T
// through the agent, the host is cleared and the trainer is granted by N.
// Runs with K = 2s (see a4HungKillBudget).
func TestORCHA4_GuestHung(t *testing.T) {
	sink := captureLogs(t)
	clu := newA4Cluster(t, map[string]string{e2eNodes[0]: killConfirm, e2eNodes[1]: killConfirm})
	hung := clu.execs[e2eNodes[1]]
	hung.hang.Store(true)
	t.Cleanup(func() { close(hung.release) })
	clu.startHostsOn(t, e2eNodes...)
	api := client(t, clu.startOrch(t, a4HungKillBudget))
	waitGuestsKnown(t, sink)

	resp, took := mustGrantBy(t, api, a4Notice)
	if took < a4HungT-200*time.Millisecond {
		t.Errorf("granted after %v, before T with a hung host", took)
	}
	if resp.GetVramUnconfirmed() {
		t.Error("vram_unconfirmed after a confirmed kill")
	}
	kills := clu.agents[e2eNodes[1]].killList()
	if len(kills) != 1 || kills[0].job != guestOf(e2eNodes[1]) || kills[0].reason != "deadline" {
		t.Fatalf("kills on the hung host = %+v, want one deadline kill of its guest", kills)
	}
	if d := kills[0].deadline.Sub(kills[0].at); d < 1900*time.Millisecond || d > 2100*time.Millisecond {
		t.Errorf("Kill deadline = sent + %v, want sent + K (2s)", d)
	}
	if k := clu.agents[e2eNodes[0]].killList(); len(k) != 0 {
		t.Errorf("Kill sent to the healthy host: %+v", k)
	}
	for _, check := range []struct {
		msg   string
		attrs map[string]any
	}{
		{"Host not clear at deadline", map[string]any{"group": e2eGroup, "node": e2eNodes[1]}},
		{"Kill sent", map[string]any{"group": e2eGroup, "node": e2eNodes[1], "reason": "deadline"}},
		{"Kill confirmed", map[string]any{"group": e2eGroup, "node": e2eNodes[1]}},
		{"Host clear", map[string]any{"group": e2eGroup, "node": e2eNodes[1], "how": "kill"}},
		{"Foreground granted", map[string]any{"group": e2eGroup, "vram_unconfirmed": false}},
	} {
		if !sink.has(t, check.msg, check.attrs) {
			t.Errorf("no %q log line with %v", check.msg, check.attrs)
		}
	}
}

// TestORCHA4_AgentUnreachable_HostsOK: no snapshot agent answers, but every
// host vacates: the trainer is granted before T and no Kill is needed.
func TestORCHA4_AgentUnreachable_HostsOK(t *testing.T) {
	sink := captureLogs(t)
	clu := newA4Cluster(t, map[string]string{})
	clu.startHostsOn(t, e2eNodes...)
	api := client(t, clu.startOrch(t))
	waitGuestsKnown(t, sink)

	mustGrantBy(t, api, a4T)
	if sink.has(t, "Kill sent", nil) {
		t.Error("Kill sent although every host vacated")
	}
}

// TestORCHA4_HostUnreachable_KilledAfterL: the host command endpoint of one
// node never answers (the VK is unseen). After L its guest is killed through
// its agent and the trainer is granted before T.
func TestORCHA4_HostUnreachable_KilledAfterL(t *testing.T) {
	sink := captureLogs(t)
	clu := newA4Cluster(t, map[string]string{e2eNodes[0]: killConfirm, e2eNodes[1]: killConfirm})
	clu.startHostsOn(t, e2eNodes[0])
	api := client(t, clu.startOrch(t, "--background-liveness=1s"))
	waitGuestsKnown(t, sink)

	_, took := mustGrantBy(t, api, a4T)
	if took < 900*time.Millisecond {
		t.Errorf("granted after %v, before L", took)
	}
	kills := clu.agents[e2eNodes[1]].killList()
	if len(kills) != 1 || kills[0].reason != "vk-unseen" {
		t.Fatalf("kills on the unseen host = %+v, want one vk-unseen kill", kills)
	}
	if !sink.has(t, "Host clear", map[string]any{"node": e2eNodes[1], "how": "kill"}) {
		t.Error("no Host clear how=kill line for the unseen host")
	}
}

// TestORCHA4_AgentUnreachable_HungHostHeld: a host that never acks and whose
// agent cannot be reached is never cleared (fail closed): the grant is held
// past N and the Kill is retried.
func TestORCHA4_AgentUnreachable_HungHostHeld(t *testing.T) {
	sink := captureLogs(t)
	clu := newA4Cluster(t, map[string]string{e2eNodes[0]: killConfirm})
	hung := clu.execs[e2eNodes[1]]
	hung.hang.Store(true)
	t.Cleanup(func() { close(hung.release) })
	clu.startHostsOn(t, e2eNodes...)
	api := client(t, clu.startOrch(t))
	waitGuestsKnown(t, sink)

	_, _, err := timedAcquire(api, 6*time.Second)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Acquire = %v, want DeadlineExceeded while a hung host's agent is unreachable", err)
	}
	if !sink.has(t, "Kill not delivered: agent unreachable, will retry", map[string]any{"node": e2eNodes[1]}) {
		t.Error("no Kill not delivered line for the hung host")
	}
	if sink.has(t, "Foreground granted", nil) {
		t.Error("granted over a hung host whose guest could not be killed")
	}
}

// TestORCHA4_UnconfirmedKill_GrantedAtN: the agent cannot confirm the Kill.
// D-NS-6 today: the trainer is granted at N with vram_unconfirmed (returned on
// the next 1s Acquire poll), and timeslice_kill_unconfirmed_total counts it.
func TestORCHA4_UnconfirmedKill_GrantedAtN(t *testing.T) {
	sink := captureLogs(t)
	clu := newA4Cluster(t, map[string]string{e2eNodes[0]: killConfirm, e2eNodes[1]: killUnconfirmed})
	hung := clu.execs[e2eNodes[1]]
	hung.hang.Store(true)
	t.Cleanup(func() { close(hung.release) })
	clu.startHostsOn(t, e2eNodes...)
	orch := clu.startOrch(t)
	api := client(t, orch)
	waitGuestsKnown(t, sink)

	resp, took := mustGrantBy(t, api, a4Notice+a4AcquirePoll+300*time.Millisecond)
	if took < a4Notice-200*time.Millisecond {
		t.Errorf("granted after %v, before N with an unconfirmed kill", took)
	}
	if !resp.GetVramUnconfirmed() {
		t.Error("AcquireResponse.vram_unconfirmed = false after an unconfirmed kill")
	}
	for _, check := range []struct {
		msg   string
		attrs map[string]any
	}{
		{"Kill sent", map[string]any{"node": e2eNodes[1], "reason": "deadline"}},
		{"Kill attempt not confirmed", map[string]any{"node": e2eNodes[1], "signal": "agent-kill-unconfirmed"}},
		{"Kill unconfirmed", map[string]any{"group": e2eGroup, "node": e2eNodes[1], "action": "grant"}},
		{"Host clear", map[string]any{"node": e2eNodes[1], "how": "unconfirmed-kill"}},
		{"Foreground granted", map[string]any{"group": e2eGroup, "vram_unconfirmed": true}},
	} {
		if !sink.has(t, check.msg, check.attrs) {
			t.Errorf("no %q log line with %v", check.msg, check.attrs)
		}
	}
	if v := scrapeCounter(t, orch.MetricsAddr, "timeslice_kill_unconfirmed_total"); v < 1 {
		t.Errorf("timeslice_kill_unconfirmed_total = %v, want >= 1", v)
	}
}

// scrapeCounter reads one unlabelled metric from the orchestrator's /metrics.
func scrapeCounter(t *testing.T, addr, name string) float64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 2 && fields[0] == name {
			v, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				t.Fatalf("parse %q: %v", sc.Text(), err)
			}
			return v
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
