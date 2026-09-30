package server_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

// Tests for --reject-unwatched-jobs (decision D-ORCH-5, option "reject").
// Namespace "watched" is in --watch-namespaces, "unwatched" is not.

const unwatchedGroup = "group-1"

func unwatchedPod(ns, name, job, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			Labels:    map[string]string{infrastructure.PodLabelKey: unwatchedGroup, infrastructure.JobLabelKey: job},
		},
		Spec: corev1.PodSpec{NodeName: node},
	}
}

// watchedJobs returns an infrastructure.WatchedJobs over the pod informers
// that main.go builds for --watch-namespaces=watchNamespaces, on a fake
// cluster with:
//   - job-watched: a pod in "watched", bound to node-a;
//   - job-pending: a pod in "watched", not bound to any node;
//   - job-unwatched: a pod in "unwatched" only;
//   - job-none: no pod anywhere.
func watchedJobs(t *testing.T, watchNamespaces string) (*infrastructure.WatchedJobs, infrastructure.Scope) {
	t.Helper()
	cs := fake.NewClientset(
		unwatchedPod("watched", "trainer", "job-watched", "node-a"),
		unwatchedPod("watched", "pending", "job-pending", ""),
		unwatchedPod("unwatched", "stranger", "job-unwatched", "node-a"),
	)
	scope, err := infrastructure.ParseScope(watchNamespaces, "")
	if err != nil {
		t.Fatalf("ParseScope: %v", err)
	}
	factories := scope.NewInformerFactories(cs, 0)
	informers := make([]corev1informers.PodInformer, 0, len(factories.Pods))
	synced := make([]cache.InformerSynced, 0, len(factories.Pods))
	for _, f := range factories.Pods {
		pi := f.Core().V1().Pods()
		informers = append(informers, pi)
		synced = append(synced, pi.Informer().HasSynced)
	}
	w := infrastructure.NewWatchedJobs(informers...)
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	for _, f := range factories.Pods {
		f.Start(stop)
	}
	if !cache.WaitForCacheSync(stop, synced...) {
		t.Fatal("pod informers did not sync")
	}
	return w, scope
}

// acquireAsHolder calls Acquire for jobID on a group whose lock jobID already
// holds with its context loaded, so an admitted call returns success at once
// and only the unwatched-job check can make it fail.
func acquireAsHolder(t *testing.T, jobID string, opts ...server.Option) (*pb.AcquireResponse, error) {
	t.Helper()
	ctx := context.Background()
	client := statusClient(t, servingGroup(t, ctx, jobID, true), opts...)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return client.Acquire(ctx, &pb.AcquireRequest{GroupId: unwatchedGroup, JobId: jobID})
}

func wantAccepted(t *testing.T, jobID string, opts ...server.Option) {
	t.Helper()
	resp, err := acquireAsHolder(t, jobID, opts...)
	if err != nil || !resp.GetSuccess() {
		t.Errorf("Acquire(%s) = %v, %v; want success", jobID, resp, err)
	}
}

func wantRejected(t *testing.T, jobID string, opts ...server.Option) {
	t.Helper()
	_, err := acquireAsHolder(t, jobID, opts...)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Acquire(%s) = %v, want PermissionDenied", jobID, err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "not observed in the watched namespaces") {
		t.Errorf("Acquire(%s) message = %q, want it to say the job was not observed in the watched namespaces", jobID, msg)
	}
}

// lockedBuffer is a bytes.Buffer safe for concurrent writers.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureSlog(t *testing.T) *lockedBuffer {
	t.Helper()
	logs := &lockedBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logs
}

// TestRejectUnwatched_Off: with the flag false (the default) no option is
// added and every job is accepted, whatever namespace its pod is in.
func TestRejectUnwatched_Off(t *testing.T) {
	w, scope := watchedJobs(t, "watched")
	opts := server.RejectUnwatchedJobsOption(false, scope.Namespaces, w)
	if len(opts) != 0 {
		t.Fatalf("RejectUnwatchedJobsOption(false) = %d options, want 0", len(opts))
	}
	logs := captureSlog(t)
	for _, job := range []string{"job-watched", "job-unwatched", "job-none"} {
		wantAccepted(t, job, opts...)
	}
	if strings.Contains(logs.String(), "Rejected Acquire") {
		t.Errorf("rejection logged with the flag off:\n%s", logs)
	}
}

// TestRejectUnwatched_On: with the flag true and a watch list, Acquire is
// rejected for a job whose only pod is unwatched, or that has no pod at all.
// A job with a pod in a watched namespace is accepted whether or not the pod
// is bound (the check ignores bound state and --node-selector). Yield and
// GetGroupStatus are not covered.
func TestRejectUnwatched_On(t *testing.T) {
	jobs, scope := watchedJobs(t, "watched")
	opts := server.RejectUnwatchedJobsOption(true, scope.Namespaces, jobs)
	if len(opts) != 1 {
		t.Fatalf("RejectUnwatchedJobsOption(true, [watched]) = %d options, want 1", len(opts))
	}
	logs := captureSlog(t)

	wantAccepted(t, "job-watched", opts...)
	wantAccepted(t, "job-pending", opts...)
	wantRejected(t, "job-unwatched", opts...)
	wantRejected(t, "job-none", opts...)

	out := logs.String()
	for _, want := range []string{
		`msg="Rejected Acquire for unwatched job" job=job-unwatched group=group-1`,
		`msg="Rejected Acquire for unwatched job" job=job-none group=group-1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
	for _, job := range []string{"job-watched", "job-pending"} {
		if strings.Contains(out, `Rejected Acquire for unwatched job" job=`+job+" ") {
			t.Errorf("%s logged as rejected:\n%s", job, out)
		}
	}

	// The pod must carry both labels: the job ID under another group is not
	// observed.
	if jobs.Observed("other-group", "job-watched") {
		t.Error("Observed(other-group, job-watched) = true, want false")
	}

	// Yield and GetGroupStatus are unchanged: with the unwatched job holding
	// the lock, both still succeed.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := statusClient(t, servingGroup(t, ctx, "job-unwatched", true), opts...)
	if _, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: unwatchedGroup}); err != nil {
		t.Errorf("GetGroupStatus = %v, want nil", err)
	}
	resp, err := client.Yield(ctx, &pb.YieldRequest{GroupId: unwatchedGroup, JobId: "job-unwatched"})
	if err != nil || !resp.GetSuccess() {
		t.Errorf("Yield(job-unwatched) = %v, %v; want success", resp, err)
	}
}

// TestRejectUnwatched_NoWatchNamespacesNoop: with the flag true but no
// --watch-namespaces every namespace is watched, so the option is a no-op
// (a warning is logged once) and even a job with no pod is accepted, as in
// the default install.
func TestRejectUnwatched_NoWatchNamespacesNoop(t *testing.T) {
	logs := captureSlog(t)
	w, scope := watchedJobs(t, "")
	opts := server.RejectUnwatchedJobsOption(true, scope.Namespaces, w)
	if len(opts) != 0 {
		t.Fatalf("RejectUnwatchedJobsOption(true, []) = %d options, want 0", len(opts))
	}
	if got := strings.Count(logs.String(), "--reject-unwatched-jobs has no effect without --watch-namespaces"); got != 1 {
		t.Errorf("no-op warning logged %d times, want 1:\n%s", got, logs)
	}
	for _, job := range []string{"job-watched", "job-unwatched", "job-none"} {
		wantAccepted(t, job, opts...)
	}
	if strings.Contains(logs.String(), "Rejected Acquire") {
		t.Errorf("rejection logged without a watch list:\n%s", logs)
	}
}

// TestRejectUnwatched_SpoofAccepted documents the limit of the option: it
// checks the job, not the caller. Any client, from any namespace, that sends
// the group and job ID of a watched job is accepted exactly like the job's own
// pod, because the request carries nothing else to check.
func TestRejectUnwatched_SpoofAccepted(t *testing.T) {
	jobs, scope := watchedJobs(t, "watched")
	opts := server.RejectUnwatchedJobsOption(true, scope.Namespaces, jobs)
	// The caller here is the test process, not the pod of job-watched.
	wantAccepted(t, "job-watched", opts...)
}
