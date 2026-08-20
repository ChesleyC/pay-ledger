package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ChesleyC/pay-ledger/api/grpcapi"
	"github.com/ChesleyC/pay-ledger/api/httpapi"
	"github.com/ChesleyC/pay-ledger/internal/cluster"
	payledgerv1 "github.com/ChesleyC/pay-ledger/proto"
	"google.golang.org/grpc"
)

func main() {
	var (
		id            = flag.String("id", "node1", "unique node ID")
		httpAddress   = flag.String("http-addr", ":8080", "REST listen address")
		httpAdvertise = flag.String("http-advertise", "http://localhost:8080", "REST URL returned to clients")
		grpcAddress   = flag.String("grpc-addr", ":9090", "gRPC listen address")
		grpcAdvertise = flag.String("grpc-advertise", "localhost:9090", "gRPC address advertised to peers")
		raftBind      = flag.String("raft-bind", "127.0.0.1:7000", "Raft TCP listen address")
		raftAddress   = flag.String("raft-addr", "127.0.0.1:7000", "Raft address advertised to peers")
		dataDir       = flag.String("data-dir", "data/node1", "persistent node data directory")
		peersValue    = flag.String("peers", "", "comma-separated id@raft@http@grpc peers")
		bootstrap     = flag.Bool("bootstrap", true, "bootstrap a new static cluster")
	)
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	peers, err := parsePeers(*peersValue)
	if err != nil {
		logger.Error("invalid peers", "error", err)
		os.Exit(2)
	}
	if len(peers) == 0 {
		peers = []cluster.Peer{{ID: *id, RaftAddress: *raftAddress, HTTPAddress: *httpAdvertise, GRPCAddress: *grpcAdvertise}}
	}
	node, err := cluster.New(cluster.Config{
		ID: *id, RaftBindAddress: *raftBind, RaftAddress: *raftAddress,
		DataDir: *dataDir, Peers: peers, Bootstrap: *bootstrap, Logger: logger,
	})
	if err != nil {
		logger.Error("start node", "error", err)
		os.Exit(1)
	}

	grpcListener, err := net.Listen("tcp", *grpcAddress)
	if err != nil {
		_ = node.Shutdown()
		logger.Error("listen gRPC", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	payledgerv1.RegisterClusterServiceServer(grpcServer, grpcapi.New(node))
	go func() {
		logger.Info("gRPC server listening", "node", *id, "address", *grpcAddress)
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server stopped", "error", err)
		}
	}()

	httpServer := &http.Server{Addr: *httpAddress, Handler: httpapi.New(node), ReadHeaderTimeout: 5 * time.Second}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "node", *id, "address", *httpAddress)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		logger.Info("shutting down", "signal", sig)
	case err := <-serverErrors:
		logger.Error("HTTP server stopped", "error", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("shutdown HTTP", "error", err)
	}
	grpcServer.GracefulStop()
	if err := node.Shutdown(); err != nil {
		logger.Error("shutdown node", "error", err)
	}
}

func parsePeers(value string) ([]cluster.Peer, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	var peers []cluster.Peer
	for _, encoded := range strings.Split(value, ",") {
		parts := strings.Split(strings.TrimSpace(encoded), "@")
		if len(parts) != 4 || parts[0] == "" || parts[1] == "" || parts[2] == "" || parts[3] == "" {
			return nil, fmt.Errorf("peer %q must be id@raft@http@grpc", encoded)
		}
		peers = append(peers, cluster.Peer{ID: parts[0], RaftAddress: parts[1], HTTPAddress: parts[2], GRPCAddress: parts[3]})
	}
	return peers, nil
}
