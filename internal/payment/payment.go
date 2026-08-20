package payment

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

const StatusCreated = "created"

// Payment is the deliberately small replicated record stored by PayLedger.
type Payment struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	AccountID      string    `json:"account_id"`
	AmountCents    int64     `json:"amount_cents"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}

type CreateRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	AmountCents    int64  `json:"amount_cents"`
}

func (r CreateRequest) Validate() error {
	if strings.TrimSpace(r.IdempotencyKey) == "" {
		return errors.New("idempotency_key is required")
	}
	if strings.TrimSpace(r.AccountID) == "" {
		return errors.New("account_id is required")
	}
	if r.AmountCents <= 0 {
		return errors.New("amount_cents must be positive")
	}
	return nil
}

func New(req CreateRequest, now time.Time) (Payment, error) {
	if err := req.Validate(); err != nil {
		return Payment{}, err
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return Payment{}, err
	}
	return Payment{
		ID:             hex.EncodeToString(b),
		IdempotencyKey: req.IdempotencyKey,
		AccountID:      req.AccountID,
		AmountCents:    req.AmountCents,
		Status:         StatusCreated,
		CreatedAt:      now.UTC(),
	}, nil
}
