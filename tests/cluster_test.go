package tests

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ChesleyC/pay-ledger/internal/cluster"
	"github.com/ChesleyC/pay-ledger/internal/payment"
)

func reserveAddresses(t *testing.T, count int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}
	for _, listener := range listeners {
		listener.Close()
	}
	return addresses
}

func waitForLeader(t *testing.T, nodes []*cluster.Node, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, node := range nodes {
			if node != nil && node.Status().IsLeader {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
	return -1
}

func TestThreeNodeFailoverAndConcurrentIdempotency(t *testing.T) {
	addresses := reserveAddresses(t, 3)
	peers := make([]cluster.Peer, 3)
	for i := range peers {
		peers[i] = cluster.Peer{ID: fmt.Sprintf("node%d", i+1), RaftAddress: addresses[i], HTTPAddress: fmt.Sprintf("http://node%d", i+1)}
	}
	nodes := make([]*cluster.Node, 3)
	for i := range nodes {
		node, err := cluster.New(cluster.Config{
			ID: peers[i].ID, RaftBindAddress: addresses[i], RaftAddress: addresses[i],
			DataDir: filepath.Join(t.TempDir(), peers[i].ID), Peers: peers, Bootstrap: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Shutdown()
			}
		}
	}()

	leaderIndex := waitForLeader(t, nodes, 8*time.Second)
	request := payment.CreateRequest{IdempotencyKey: "shared-key", AccountID: "acct", AmountCents: 500}
	const requests = 32
	ids := make(chan string, requests)
	errs := make(chan error, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _, err := nodes[leaderIndex].CreatePayment(request)
			if err != nil {
				errs <- err
				return
			}
			ids <- p.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var firstID string
	for id := range ids {
		if firstID == "" {
			firstID = id
		}
		if id != firstID {
			t.Fatalf("duplicate request returned IDs %q and %q", firstID, id)
		}
	}

	oldLeader := nodes[leaderIndex]
	nodes[leaderIndex] = nil
	if err := oldLeader.Shutdown(); err != nil {
		t.Fatal(err)
	}
	newLeaderIndex := waitForLeader(t, nodes, 8*time.Second)
	second, _, err := nodes[newLeaderIndex].CreatePayment(payment.CreateRequest{IdempotencyKey: "after-failover", AccountID: "acct", AmountCents: 700})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for _, node := range nodes {
		if node == nil {
			continue
		}
		for {
			first, firstErr := node.GetPayment(firstID)
			gotSecond, secondErr := node.GetPayment(second.ID)
			if firstErr == nil && secondErr == nil && first.ID == firstID && gotSecond.ID == second.ID {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("replicated payments not visible: first=%v second=%v", firstErr, secondErr)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}
