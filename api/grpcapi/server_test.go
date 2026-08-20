package grpcapi

import (
	"context"
	"testing"

	"github.com/ChesleyC/pay-ledger/internal/cluster"
	payledgerv1 "github.com/ChesleyC/pay-ledger/proto"
)

type fakeStatusSource struct{ status cluster.Status }

func (f fakeStatusSource) Status() cluster.Status { return f.status }

func TestGetStatus(t *testing.T) {
	server := New(fakeStatusSource{status: cluster.Status{
		NodeID: "node2", State: "Follower", LeaderID: "node1",
		LeaderHTTP: "http://localhost:8081", LastLogIndex: 9, AppliedIndex: 8,
	}})
	got, err := server.GetStatus(context.Background(), &payledgerv1.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeId != "node2" || got.LeaderId != "node1" || got.AppliedIndex != 8 {
		t.Fatalf("unexpected status: %+v", got)
	}
}
