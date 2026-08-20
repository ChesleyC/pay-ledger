package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChesleyC/pay-ledger/internal/payment"
)

func TestFollowsLeaderAndRetries(t *testing.T) {
	expected := payment.Payment{ID: "payment-one", IdempotencyKey: "checkout-123"}
	var leaderCalls atomic.Int32
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaderCalls.Add(1)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(expected)
	}))
	defer leader.Close()
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", leader.URL+"/v1/payments")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c := New(follower.URL)
	c.InitialBackoff = time.Nanosecond
	c.sleep = func(context.Context, time.Duration) error { return nil }
	got, err := c.CreatePayment(context.Background(), payment.CreateRequest{
		IdempotencyKey: "checkout-123", AccountID: "acct", AmountCents: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != expected.ID || leaderCalls.Load() != 1 {
		t.Fatalf("got %+v, leader calls %d", got, leaderCalls.Load())
	}
}

func TestRetriesAreBounded(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	c := New(server.URL)
	c.MaxAttempts = 3
	c.sleep = func(context.Context, time.Duration) error { return nil }
	_, err := c.CreatePayment(context.Background(), payment.CreateRequest{IdempotencyKey: "key", AccountID: "acct", AmountCents: 1})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 3 {
		t.Fatalf("got %d attempts, want 3", calls.Load())
	}
}
