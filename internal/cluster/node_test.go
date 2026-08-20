package cluster

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ChesleyC/pay-ledger/internal/payment"
)

func availableAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	return address
}

func TestSingleNodeRaftIdempotencyAndRestart(t *testing.T) {
	address := availableAddress(t)
	dir := filepath.Join(t.TempDir(), "node")
	config := Config{
		ID: "node1", RaftBindAddress: address, RaftAddress: address, DataDir: dir,
		Peers: []Peer{{ID: "node1", RaftAddress: address}}, Bootstrap: true,
	}
	node, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	request := payment.CreateRequest{IdempotencyKey: "checkout-123", AccountID: "acct-42", AmountCents: 2500}
	first, created, err := node.CreatePayment(request)
	if err != nil || !created {
		t.Fatalf("first create = (%+v, %v, %v)", first, created, err)
	}
	second, created, err := node.CreatePayment(request)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("duplicate create = (%+v, %v, %v), want ID %s", second, created, err, first.ID)
	}
	if err := node.Shutdown(); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Shutdown()
	if err := restarted.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.GetPayment(first.ID)
	if err != nil || got.ID != first.ID {
		t.Fatalf("payment after restart = (%+v, %v)", got, err)
	}
}
