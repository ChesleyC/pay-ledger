package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ChesleyC/pay-ledger/internal/payment"
)

func candidate(id string) payment.Payment {
	return payment.Payment{
		ID: id, IdempotencyKey: "checkout-123", AccountID: "acct-42",
		AmountCents: 2500, Status: payment.StatusCreated, CreatedAt: time.Unix(1, 0).UTC(),
	}
}

func TestConcurrentIdempotency(t *testing.T) {
	store, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const workers = 64
	ids := make(chan string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, _, err := store.CreatePayment(candidate(fmt.Sprintf("payment-%d", i)))
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			ids <- p.ID
		}(i)
	}
	wg.Wait()
	close(ids)

	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("different IDs returned: %q and %q", first, id)
		}
	}
	payments, err := store.AllPayments()
	if err != nil {
		t.Fatal(err)
	}
	if len(payments) != 1 {
		t.Fatalf("got %d logical payments, want 1", len(payments))
	}
}

func TestPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badger")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created, _, err := store.CreatePayment(candidate("payment-one"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetByIdempotencyKey("checkout-123")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != created.ID {
		t.Fatalf("got ID %q, want %q", got.ID, created.ID)
	}
}
