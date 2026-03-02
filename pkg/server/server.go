package server

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "object-queue/api/proto"
	"object-queue/pkg/broker"
	"object-queue/pkg/queue"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// QueueServer implements the gRPC QueueService
type QueueServer struct {
	pb.UnimplementedQueueServiceServer
	broker     *broker.Broker
	grpcServer *grpc.Server
}

// NewQueueServer creates a new gRPC server wrapping a broker
func NewQueueServer(brk *broker.Broker) *QueueServer {
	return &QueueServer{
		broker: brk,
	}
}

// Push implements QueueService.Push
func (s *QueueServer) Push(ctx context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	// Convert proto map to Go map
	data := make(map[string]interface{})
	for k, v := range req.Data {
		data[k] = v
	}

	// Create response channel
	responseCh := make(chan queue.Response, 1)

	// Send request to broker
	queueReq := queue.Request{
		Type:       queue.RequestTypePush,
		JobData:    data,
		ResponseCh: responseCh,
	}

	s.broker.HandleRequest(queueReq)

	resp, err := awaitResponse(ctx, responseCh)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("push failed: %v", resp.Error)
	}
	return &pb.PushResponse{Job: jobToProto(resp.Job)}, nil
}

// Claim implements QueueService.Claim
func (s *QueueServer) Claim(ctx context.Context, req *pb.ClaimRequest) (*pb.ClaimResponse, error) {
	responseCh := make(chan queue.Response, 1)

	queueReq := queue.Request{
		Type:       queue.RequestTypeClaim,
		WorkerID:   req.WorkerId,
		ResponseCh: responseCh,
	}

	s.broker.HandleRequest(queueReq)

	resp, err := awaitResponse(ctx, responseCh)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("claim failed: %v", resp.Error)
	}
	return &pb.ClaimResponse{Job: jobToProto(resp.Job)}, nil
}

// Heartbeat implements QueueService.Heartbeat
func (s *QueueServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	responseCh := make(chan queue.Response, 1)

	queueReq := queue.Request{
		Type:       queue.RequestTypeHeartbeat,
		JobID:      req.JobId,
		WorkerID:   req.WorkerId,
		ResponseCh: responseCh,
	}

	s.broker.HandleRequest(queueReq)

	resp, err := awaitResponse(ctx, responseCh)
	if err != nil {
		return nil, err
	}
	return &pb.HeartbeatResponse{Success: resp.Success}, nil
}

// Complete implements QueueService.Complete
func (s *QueueServer) Complete(ctx context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	responseCh := make(chan queue.Response, 1)

	queueReq := queue.Request{
		Type:       queue.RequestTypeComplete,
		JobID:      req.JobId,
		WorkerID:   req.WorkerId,
		ResponseCh: responseCh,
	}

	s.broker.HandleRequest(queueReq)

	resp, err := awaitResponse(ctx, responseCh)
	if err != nil {
		return nil, err
	}
	return &pb.CompleteResponse{Success: resp.Success}, nil
}

// Serve starts the gRPC server on the given address
func (s *QueueServer) Serve(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterQueueServiceServer(s.grpcServer, s)

	log.Printf("gRPC server listening on %s", address)
	return s.grpcServer.Serve(lis)
}

// Stop gracefully stops the gRPC server, draining in-flight RPCs
func (s *QueueServer) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

func awaitResponse(ctx context.Context, ch <-chan queue.Response) (queue.Response, error) {
	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return queue.Response{}, ctx.Err()
	}
}

// jobToProto converts an internal Job to a protobuf Job
func jobToProto(job *queue.Job) *pb.Job {
	if job == nil {
		return nil
	}

	// Convert data map
	data := make(map[string]string)
	for k, v := range job.Data {
		data[k] = fmt.Sprint(v)
	}

	pbJob := &pb.Job{
		Id:        job.ID,
		Status:    string(job.Status),
		Data:      data,
		CreatedAt: timestamppb.New(job.CreatedAt),
		WorkerId:  job.WorkerID,
	}

	if job.ClaimedAt != nil {
		pbJob.ClaimedAt = timestamppb.New(*job.ClaimedAt)
	}
	if job.LastHeartbeat != nil {
		pbJob.LastHeartbeat = timestamppb.New(*job.LastHeartbeat)
	}

	return pbJob
}
