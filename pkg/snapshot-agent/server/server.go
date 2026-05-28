package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	podutils "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
)

// Server implements the SnapshotAgentService gRPC server.
type Server struct {
	pb.UnimplementedSnapshotAgentServiceServer
}

// NewServer creates a new Server instance.
func NewServer() *Server {
	return &Server{}
}

// Snapshot implements SnapshotAgentService.Snapshot.
func (s *Server) Snapshot(ctx context.Context, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	log.Printf("Snapshot called: JobID=%s, Group=%s", req.GetJobId(), req.GetGroup())
	pods, err := podutils.GetLocalPods(ctx)
	if err != nil {
		log.Printf("Error getting local pods: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to get local pods: %v", err)
	}

	if len(pods) == 0 {
		log.Printf("No pods found with label %s=%s on node %s", podutils.SnapshotAgentLabel, podutils.SnapshotAgentValue, os.Getenv("NODE_NAME"))
		return &pb.SnapshotResponse{OperationId: "no-pods-found"}, nil
	}

	var results []string
	for _, pod := range pods {
		log.Printf("Processing pod: %s/%s", pod.Namespace, pod.Name)
		pids, err := podutils.GetPodPIDs(ctx, pod.Name, pod.Namespace)
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

	return &pb.SnapshotResponse{OperationId: strings.Join(results, " ")}, nil
}

// Restore implements SnapshotAgentService.Restore.
func (s *Server) Restore(ctx context.Context, req *pb.RestoreRequest) (*pb.RestoreResponse, error) {
	log.Printf("Restore called: JobID=%s, Group=%s", req.GetJobId(), req.GetGroup())
	return nil, status.Errorf(codes.Unimplemented, "method Restore not implemented")
}

// GetOperation implements SnapshotAgentService.GetOperation.
func (s *Server) GetOperation(ctx context.Context, req *pb.GetOperationRequest) (*pb.GetOperationResponse, error) {
	log.Printf("GetOperation called: OperationID=%s", req.GetOperationId())
	return nil, status.Errorf(codes.Unimplemented, "method GetOperation not implemented")
}

// Status implements SnapshotAgentService.Status.
func (s *Server) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	log.Printf("Status called")
	return nil, status.Errorf(codes.Unimplemented, "method Status not implemented")
}

// Health implements SnapshotAgentService.Health.
func (s *Server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	log.Printf("Health called")
	return nil, status.Errorf(codes.Unimplemented, "method Health not implemented")
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
