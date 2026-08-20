package payment

import (
	"testing"
	"time"
)

func TestNewPayment(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	p, err := New(CreateRequest{IdempotencyKey: "checkout-123", AccountID: "acct-42", AmountCents: 2500}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.ID) != 32 || p.Status != StatusCreated || !p.CreatedAt.Equal(now) {
		t.Fatalf("unexpected payment: %+v", p)
	}
}

func TestCreateRequestValidation(t *testing.T) {
	tests := []CreateRequest{
		{},
		{IdempotencyKey: "key"},
		{IdempotencyKey: "key", AccountID: "acct"},
	}
	for _, request := range tests {
		if request.Validate() == nil {
			t.Fatalf("expected validation error for %+v", request)
		}
	}
}
