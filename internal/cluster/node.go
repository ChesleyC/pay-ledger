package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/ChesleyC/pay-ledger/internal/payment"
	"github.com/ChesleyC/pay-ledger/internal/storage"
	"github.com/hashicorp/raft"
)

var ErrNotLeader = errors.New("node is not the raft leader")

type Peer struct {
	ID          string `json:"id"`
	RaftAddress string `json:"raft_address"`
	HTTPAddress string `json:"http_address"`
	GRPCAddress string `json:"grpc_address"`
}

type Config struct {
	ID              string
	RaftBindAddress string
	RaftAddress     string
	DataDir         string
	Peers           []Peer
	Bootstrap       bool
	ApplyTimeout    time.Duration
	Logger          *slog.Logger
}

type Status struct {
	NodeID       string `json:"node_id"`
	State        string `json:"state"`
	IsLeader     bool   `json:"is_leader"`
	LeaderID     string `json:"leader_id,omitempty"`
	LeaderRaft   string `json:"leader_raft_address,omitempty"`
	LeaderHTTP   string `json:"leader_http_address,omitempty"`
	LastLogIndex uint64 `json:"last_log_index"`
	AppliedIndex uint64 `json:"applied_index"`
}

type Node struct {
	id           string
	raft         *raft.Raft
	store        *storage.Store
	transport    *raft.NetworkTransport
	peers        map[raft.ServerAddress]Peer
	applyTimeout time.Duration
}

func New(config Config) (*Node, error) {
	if config.ID == "" || config.RaftBindAddress == "" || config.RaftAddress == "" || config.DataDir == "" {
		return nil, errors.New("id, raft bind/address, and data directory are required")
	}
	if config.ApplyTimeout == 0 {
		config.ApplyTimeout = 5 * time.Second
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if err := os.MkdirAll(config.DataDir, 0o755); err != nil {
		return nil, err
	}

	store, err := storage.Open(filepath.Join(config.DataDir, "badger"))
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	cleanupStore := true
	defer func() {
		if cleanupStore {
			_ = store.Close()
		}
	}()

	advertise, err := net.ResolveTCPAddr("tcp", config.RaftAddress)
	if err != nil {
		return nil, fmt.Errorf("resolve advertised raft address: %w", err)
	}
	transport, err := raft.NewTCPTransport(config.RaftBindAddress, advertise, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create raft transport: %w", err)
	}
	cleanupTransport := true
	defer func() {
		if cleanupTransport {
			_ = transport.Close()
		}
	}()

	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(config.DataDir, "snapshots"), 2, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.ID)
	raftConfig.SnapshotThreshold = 128
	raftConfig.SnapshotInterval = 20 * time.Second
	raftConfig.Logger = nil
	raftConfig.LogOutput = io.Discard

	r, err := raft.NewRaft(raftConfig, &FSM{store: store}, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("start raft: %w", err)
	}
	if config.Bootstrap {
		servers := make([]raft.Server, 0, len(config.Peers))
		for _, peer := range config.Peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(peer.ID), Address: raft.ServerAddress(peer.RaftAddress)})
		}
		if len(servers) == 0 {
			servers = append(servers, raft.Server{ID: raft.ServerID(config.ID), Address: raft.ServerAddress(config.RaftAddress)})
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			_ = r.Shutdown().Error()
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}

	peerMap := make(map[raft.ServerAddress]Peer, len(config.Peers))
	for _, peer := range config.Peers {
		peerMap[raft.ServerAddress(peer.RaftAddress)] = peer
	}
	cleanupStore, cleanupTransport = false, false
	return &Node{id: config.ID, raft: r, store: store, transport: transport, peers: peerMap, applyTimeout: config.ApplyTimeout}, nil
}

func (n *Node) CreatePayment(request payment.CreateRequest) (payment.Payment, bool, error) {
	if n.raft.State() != raft.Leader {
		return payment.Payment{}, false, ErrNotLeader
	}
	candidate, err := payment.New(request, time.Now())
	if err != nil {
		return payment.Payment{}, false, err
	}
	body, err := json.Marshal(command{Type: commandCreatePayment, Payment: candidate})
	if err != nil {
		return payment.Payment{}, false, err
	}
	future := n.raft.Apply(body, n.applyTimeout)
	if err := future.Error(); err != nil {
		return payment.Payment{}, false, err
	}
	result, ok := future.Response().(ApplyResult)
	if !ok {
		return payment.Payment{}, false, fmt.Errorf("unexpected raft response %T", future.Response())
	}
	return result.Payment, result.Created, result.Err
}

func (n *Node) GetPayment(id string) (payment.Payment, error) {
	return n.store.GetPayment(id)
}

func (n *Node) GetByIdempotencyKey(key string) (payment.Payment, error) {
	return n.store.GetByIdempotencyKey(key)
}

func (n *Node) Status() Status {
	leaderAddress, leaderID := n.raft.LeaderWithID()
	status := Status{
		NodeID: n.id, State: n.raft.State().String(), IsLeader: n.raft.State() == raft.Leader,
		LeaderID: string(leaderID), LeaderRaft: string(leaderAddress),
		LastLogIndex: n.raft.LastIndex(), AppliedIndex: n.raft.AppliedIndex(),
	}
	if peer, ok := n.peers[leaderAddress]; ok {
		status.LeaderHTTP = peer.HTTPAddress
	}
	return status
}

func (n *Node) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if address, _ := n.raft.LeaderWithID(); address != "" {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("timed out waiting for raft leader")
}

func (n *Node) Shutdown() error {
	var errs []error
	if err := n.raft.Shutdown().Error(); err != nil {
		errs = append(errs, err)
	}
	if err := n.transport.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := n.store.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
