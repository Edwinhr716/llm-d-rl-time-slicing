package server

import (
	"context"
	"log/slog"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This file holds the --reject-unwatched-jobs option (decision D-ORCH-5,
// option "reject"). Everything it adds is reached only through
// WithRejectUnwatchedJobs, so the option can be deleted with this file, its
// tests and the flag.

// WatchedJobs reports whether a job has a pod in the namespaces this
// orchestrator watches. infrastructure.WatchedJobs implements it.
type WatchedJobs interface {
	Observed(groupID, jobID string) bool
}

// RejectUnwatchedJobsOption returns the server option for
// --reject-unwatched-jobs. When enabled is false it returns nil options. When
// enabled is true but watchNamespaces is empty, every namespace is watched, so
// there is nothing to reject: it logs a warning once and returns nil options.
func RejectUnwatchedJobsOption(enabled bool, watchNamespaces []string, jobs WatchedJobs) []Option {
	if !enabled {
		return nil
	}
	if len(watchNamespaces) == 0 {
		slog.Warn("--reject-unwatched-jobs has no effect without --watch-namespaces")
		return nil
	}
	return []Option{WithRejectUnwatchedJobs(jobs)}
}

// WithRejectUnwatchedJobs makes Acquire fail with PermissionDenied for a job
// that jobs has not observed. It checks the job, not the caller: any client
// that sends the job_id of a watched job is accepted. Yield and
// GetGroupStatus are unchanged. Nil (the default) accepts every job.
func WithRejectUnwatchedJobs(jobs WatchedJobs) Option {
	return func(s *Server) {
		s.watchedJobs = jobs
	}
}

// rejectUnwatched returns a PermissionDenied error when the unwatched-job
// check is on and the job has no pod in the watched namespaces, and nil
// otherwise.
func (s *Server) rejectUnwatched(ctx context.Context, req *pb.AcquireRequest) error {
	if s.watchedJobs == nil {
		return nil
	}
	if s.watchedJobs.Observed(req.GetGroupId(), req.GetJobId()) {
		return nil
	}
	slog.InfoContext(ctx, "Rejected Acquire for unwatched job",
		"job", req.GetJobId(), "group", req.GetGroupId())
	return status.Errorf(codes.PermissionDenied,
		"job %s of group %s was not observed in the watched namespaces", req.GetJobId(), req.GetGroupId())
}
