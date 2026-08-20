package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ChesleyC/pay-ledger/internal/payment"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

var ErrNotFound = errors.New("not found")
var errStableNotFound = errors.New("not found")

var (
	paymentPrefix = []byte("payment/")
	keyPrefix     = []byte("idempotency/")
	logPrefix     = []byte("raft/log/")
	stablePrefix  = []byte("raft/stable/")
)

// Store keeps both application state and Raft metadata in one transactional
// Badger database. Distinct key prefixes keep the concerns easy to inspect.
type Store struct {
	db        *badger.DB
	paymentMu sync.Mutex
}

func Open(path string) (*Store, error) {
	opts := badger.DefaultOptions(path).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func OpenInMemory() (*Store, error) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func paymentKey(id string) []byte { return append(append([]byte{}, paymentPrefix...), id...) }
func idemKey(key string) []byte   { return append(append([]byte{}, keyPrefix...), key...) }

// CreatePayment is atomic: the idempotency mapping and payment record are
// committed in the same Badger transaction. Raft serializes calls in normal
// operation, while this transaction also makes direct concurrent use safe.
func (s *Store) CreatePayment(candidate payment.Payment) (payment.Payment, bool, error) {
	s.paymentMu.Lock()
	defer s.paymentMu.Unlock()
	var result payment.Payment
	created := false
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(idemKey(candidate.IdempotencyKey))
		if err == nil {
			id, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			existing, err := getPaymentTxn(txn, string(id))
			if err != nil {
				return err
			}
			result = existing
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		body, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		if err := txn.Set(paymentKey(candidate.ID), body); err != nil {
			return err
		}
		if err := txn.Set(idemKey(candidate.IdempotencyKey), []byte(candidate.ID)); err != nil {
			return err
		}
		result, created = candidate, true
		return nil
	})
	return result, created, err
}

func getPaymentTxn(txn *badger.Txn, id string) (payment.Payment, error) {
	item, err := txn.Get(paymentKey(id))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return payment.Payment{}, ErrNotFound
	}
	if err != nil {
		return payment.Payment{}, err
	}
	body, err := item.ValueCopy(nil)
	if err != nil {
		return payment.Payment{}, err
	}
	var p payment.Payment
	if err := json.Unmarshal(body, &p); err != nil {
		return payment.Payment{}, err
	}
	return p, nil
}

func (s *Store) GetPayment(id string) (payment.Payment, error) {
	var p payment.Payment
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		p, err = getPaymentTxn(txn, id)
		return err
	})
	return p, err
}

func (s *Store) GetByIdempotencyKey(key string) (payment.Payment, error) {
	var p payment.Payment
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(idemKey(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		id, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		p, err = getPaymentTxn(txn, string(id))
		return err
	})
	return p, err
}

func (s *Store) AllPayments() ([]payment.Payment, error) {
	var payments []payment.Payment
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(paymentPrefix); it.ValidForPrefix(paymentPrefix); it.Next() {
			body, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			var p payment.Payment
			if err := json.Unmarshal(body, &p); err != nil {
				return err
			}
			payments = append(payments, p)
		}
		return nil
	})
	return payments, err
}

func (s *Store) ReplacePayments(payments []payment.Payment) error {
	s.paymentMu.Lock()
	defer s.paymentMu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		for _, prefix := range [][]byte{paymentPrefix, keyPrefix} {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().KeyCopy(nil)
				if err := txn.Delete(key); err != nil {
					it.Close()
					return err
				}
			}
			it.Close()
		}
		for _, p := range payments {
			body, err := json.Marshal(p)
			if err != nil {
				return err
			}
			if err := txn.Set(paymentKey(p.ID), body); err != nil {
				return err
			}
			if err := txn.Set(idemKey(p.IdempotencyKey), []byte(p.ID)); err != nil {
				return err
			}
		}
		return nil
	})
}

func logKey(index uint64) []byte {
	key := make([]byte, len(logPrefix)+8)
	copy(key, logPrefix)
	binary.BigEndian.PutUint64(key[len(logPrefix):], index)
	return key
}

func (s *Store) FirstIndex() (uint64, error) { return s.edgeIndex(false) }
func (s *Store) LastIndex() (uint64, error)  { return s.edgeIndex(true) }

func (s *Store) edgeIndex(last bool) (uint64, error) {
	var index uint64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = last
		it := txn.NewIterator(opts)
		defer it.Close()
		if last {
			seek := append(append([]byte{}, logPrefix...), 0xff)
			it.Seek(seek)
		} else {
			it.Seek(logPrefix)
		}
		if it.Valid() && len(it.Item().Key()) == len(logPrefix)+8 && string(it.Item().Key()[:len(logPrefix)]) == string(logPrefix) {
			index = binary.BigEndian.Uint64(it.Item().Key()[len(logPrefix):])
		}
		return nil
	})
	return index, err
}

func (s *Store) GetLog(index uint64, out *raft.Log) error {
	return s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(logKey(index))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return raft.ErrLogNotFound
		}
		if err != nil {
			return err
		}
		body, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return json.Unmarshal(body, out)
	})
}

func (s *Store) StoreLog(log *raft.Log) error { return s.StoreLogs([]*raft.Log{log}) }

func (s *Store) StoreLogs(logs []*raft.Log) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			body, err := json.Marshal(log)
			if err != nil {
				return err
			}
			if err := txn.Set(logKey(log.Index), body); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) DeleteRange(min, max uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for index := min; index <= max; index++ {
			if err := txn.Delete(logKey(index)); err != nil {
				return err
			}
			if index == ^uint64(0) {
				break
			}
		}
		return nil
	})
}

func (s *Store) Set(key, value []byte) error {
	stableKey := append(append([]byte{}, stablePrefix...), key...)
	return s.db.Update(func(txn *badger.Txn) error { return txn.Set(stableKey, value) })
}

func (s *Store) Get(key []byte) ([]byte, error) {
	var value []byte
	stableKey := append(append([]byte{}, stablePrefix...), key...)
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(stableKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errStableNotFound
		}
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

func (s *Store) SetUint64(key []byte, value uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return s.Set(key, b)
}

func (s *Store) GetUint64(key []byte) (uint64, error) {
	b, err := s.Get(key)
	if errors.Is(err, errStableNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid uint64 for stable key %q", key)
	}
	return binary.BigEndian.Uint64(b), nil
}

var _ raft.LogStore = (*Store)(nil)
var _ raft.StableStore = (*Store)(nil)
