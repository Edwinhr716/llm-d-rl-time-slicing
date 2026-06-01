package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	podutils "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the SnapshotAgentService gRPC server.
type Server struct {
	pb.UnimplementedSnapshotAgentServiceServer
	state *sm.StateManager
}

// NewServer creates a new Server instance.
func NewServer() *Server {
	return &Server{
		state: sm.NewStateManager(),
	}
}

// Snapshot triggers an asynchronous snapshot of the accelerator context for a job.
func (s *Server) Snapshot(ctx context.Context, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	log.Printf("Snapshot called: JobID=%s, Group=%s", req.GetJobId(), req.GetGroup())

	opID, err := s.state.StartSnapshot(req.GetJobId(), req.GetGroup(), func() (int64, int64, error) {
		// TODO: Implement actual backend snapshot logic (e.g. CRIU, GPU context)
		// For now, we simulate work and use existing pod/pid discovery
		log.Printf("Background: Starting snapshot for %s", req.GetJobId())
		pods, err := podutils.GetLocalPods(context.Background(), req.GetJobId())
		if err != nil {
			return 0, 0, fmt.Errorf("failed to get local pods: %w", err)
		}
		if len(pods) == 0 {
			return 0, 0, fmt.Errorf("no pods found for job %s", req.GetJobId())
		}
		var results []string
		for _, pod := range pods {
			log.Printf("Processing pod: %s/%s", pod.Namespace, pod.Name)
			pids, err := podutils.GetPodPIDs(context.Background(), pod.Name, pod.Namespace)
			if err != nil {
				log.Printf("Error getting PIDs for pod %s: %v", pod.Name, err)
				continue
			}	
			podInfo := fmt.Sprintf("%s", pod.Name)
			for _, pid := range pids {
				podInfo += fmt.Sprintf(":%d", pid)
			}
			results = append(results, podInfo)
		}
		log.Printf("Snapshot results: %v", results)

		// Simulate some processing time
		time.Sleep(2 * time.Second)

		// Dummy values for now
		return 1024 * 1024, 2048 * 1024, nil
	})

	if err != nil {
		return nil, err
	}

	return &pb.SnapshotResponse{OperationId: opID}, nil
}

// Restore triggers an asynchronous restoration of the accelerator context for a job.
func (s *Server) Restore(ctx context.Context, req *pb.RestoreRequest) (*pb.RestoreResponse, error) {
	log.Printf("Restore called: JobID=%s, Group=%s", req.GetJobId(), req.GetGroup())

	opID, err := s.state.StartRestore(req.GetJobId(), req.GetGroup(), func() error {
		// TODO: Implement actual backend restore logic
		log.Printf("Background: Starting restore for %s", req.GetJobId())

		// Simulate some processing time
		time.Sleep(2 * time.Second)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &pb.RestoreResponse{OperationId: opID}, nil
}

// GetOperation polls the status of a long-running snapshot or restore operation.
func (s *Server) GetOperation(ctx context.Context, req *pb.GetOperationRequest) (*pb.GetOperationResponse, error) {
	log.Printf("GetOperation called: OperationID=%s", req.GetOperationId())

	op, ok := s.state.GetOperation(req.GetOperationId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "operation %s not found", req.GetOperationId())
	}

	elapsed := time.Since(op.StartedAt).Milliseconds()
	if !op.FinishedAt.IsZero() {
		elapsed = op.FinishedAt.Sub(op.StartedAt).Milliseconds()
	}

	resp := &pb.GetOperationResponse{
		Status:    op.Status,
		ElapsedMs: elapsed,
	}

	if op.Status == pb.OperationStatus_OPERATION_STATUS_COMPLETE {
		storageBytes := op.StorageBytes
		deviceBytes := op.SnapshotDeviceBytes
		resp.StorageBytes = &storageBytes
		resp.SnapshotDeviceBytes = &deviceBytes
	}

	if op.Status == pb.OperationStatus_OPERATION_STATUS_FAILED {
		errStr := op.Error
		resp.Error = &errStr
	}

	return resp, nil
}

// Status returns the current state of jobs and accelerators on the node.
func (s *Server) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	log.Printf("Status called")
	return &pb.StatusResponse{
		JobStatuses: s.state.GetJobStatus(),
		// TODO: Implement accelerator status discovery
		AcceleratorStatuses: nil,
	}, nil
}

// Health returns the health status of the agent.
func (s *Server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	log.Printf("Health called")
	return &pb.HealthResponse{Healthy: true}, nil
}

// StartServer starts the gRPC server on the specified port.
func StartServer(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterSnapshotAgentServiceServer(s, NewServer())

	log.Printf("Starting gRPC server on port %d...", port)
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
