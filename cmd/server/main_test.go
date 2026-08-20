package main

import "testing"

func TestParsePeers(t *testing.T) {
	peers, err := parsePeers("node1@node1:7000@http://localhost:8081@node1:9090,node2@node2:7000@http://localhost:8082@node2:9090")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 2 || peers[1].ID != "node2" || peers[1].HTTPAddress != "http://localhost:8082" {
		t.Fatalf("unexpected peers: %+v", peers)
	}
	if _, err := parsePeers("broken"); err == nil {
		t.Fatal("expected parse error")
	}
}
