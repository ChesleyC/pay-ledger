package cluster

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/ChesleyC/pay-ledger/internal/payment"
	"github.com/ChesleyC/pay-ledger/internal/storage"
	"github.com/hashicorp/raft"
)

const commandCreatePayment = "create_payment"

type command struct {
	Type    string          `json:"type"`
	Payment payment.Payment `json:"payment"`
}

// ApplyResult travels from the Raft FSM back to the HTTP request that proposed
// the command. Returning the stored record is important: duplicate commands
// can carry different candidate IDs, but every caller receives the first ID.
type ApplyResult struct {
	Payment payment.Payment
	Created bool
	Err     error
}

type FSM struct {
	store *storage.Store
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return ApplyResult{Err: fmt.Errorf("decode raft command: %w", err)}
	}
	if cmd.Type != commandCreatePayment {
		return ApplyResult{Err: fmt.Errorf("unknown raft command %q", cmd.Type)}
	}
	p, created, err := f.store.CreatePayment(cmd.Payment)
	return ApplyResult{Payment: p, Created: created, Err: err}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	payments, err := f.store.AllPayments()
	if err != nil {
		return nil, err
	}
	return &snapshot{payments: payments}, nil
}

func (f *FSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	var payments []payment.Payment
	if err := json.NewDecoder(reader).Decode(&payments); err != nil {
		return err
	}
	return f.store.ReplacePayments(payments)
}

type snapshot struct {
	payments []payment.Payment
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.payments); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
