package grpcapi

import (
	"context"

	"github.com/ChesleyC/pay-ledger/internal/cluster"
	payledgerv1 "github.com/ChesleyC/pay-ledger/proto"
)

type statusSource interface {
	Status() cluster.Status
}

type Server struct {
	payledgerv1.UnimplementedClusterServiceServer
	node statusSource
}

func New(node statusSource) *Server { return &Server{node: node} }

func (s *Server) GetStatus(context.Context, *payledgerv1.StatusRequest) (*payledgerv1.StatusResponse, error) {
	status := s.node.Status()
	return &payledgerv1.StatusResponse{
		NodeId: status.NodeID, State: status.State, IsLeader: status.IsLeader,
		LeaderId: status.LeaderID, LeaderRaftAddress: status.LeaderRaft,
		LeaderHttpAddress: status.LeaderHTTP, LastLogIndex: status.LastLogIndex,
		AppliedIndex: status.AppliedIndex,
	}, nil
}
