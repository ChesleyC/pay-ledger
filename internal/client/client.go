package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ChesleyC/pay-ledger/internal/payment"
)

type Client struct {
	BaseURLs       []string
	HTTPClient     *http.Client
	MaxAttempts    int
	InitialBackoff time.Duration
	sleep          func(context.Context, time.Duration) error
}

func New(baseURLs ...string) *Client {
	return &Client{
		BaseURLs: baseURLs, HTTPClient: &http.Client{
			Timeout:       2 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		MaxAttempts: 5, InitialBackoff: 50 * time.Millisecond, sleep: sleepContext,
	}
}

// CreatePayment retries timeouts, server failures, and follower redirects.
// Attempts are bounded; the idempotency key makes an ambiguous timeout safe.
func (c *Client) CreatePayment(ctx context.Context, request payment.CreateRequest) (payment.Payment, error) {
	if err := request.Validate(); err != nil {
		return payment.Payment{}, err
	}
	if len(c.BaseURLs) == 0 {
		return payment.Payment{}, errors.New("at least one server URL is required")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return payment.Payment{}, err
	}
	attempts := c.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	backoff := c.InitialBackoff
	endpoint := strings.TrimRight(c.BaseURLs[0], "/") + "/v1/payments"
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			if err := c.sleep(ctx, backoff); err != nil {
				return payment.Payment{}, err
			}
			backoff *= 2
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return payment.Payment{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := c.HTTPClient.Do(req)
		if err != nil {
			lastErr = err
			endpoint = strings.TrimRight(c.BaseURLs[(attempt+1)%len(c.BaseURLs)], "/") + "/v1/payments"
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}
		if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated {
			var p payment.Payment
			if err := json.Unmarshal(responseBody, &p); err != nil {
				return payment.Payment{}, err
			}
			return p, nil
		}
		if response.StatusCode == http.StatusTemporaryRedirect {
			if leader := response.Header.Get("Location"); leader != "" {
				endpoint = leader
			} else {
				endpoint = strings.TrimRight(c.BaseURLs[(attempt+1)%len(c.BaseURLs)], "/") + "/v1/payments"
			}
			lastErr = fmt.Errorf("server is a follower")
			continue
		}
		lastErr = fmt.Errorf("server returned %s: %s", response.Status, strings.TrimSpace(string(responseBody)))
		if response.StatusCode < 500 {
			return payment.Payment{}, lastErr
		}
		endpoint = strings.TrimRight(c.BaseURLs[(attempt+1)%len(c.BaseURLs)], "/") + "/v1/payments"
	}
	return payment.Payment{}, fmt.Errorf("create payment failed after %d attempts: %w", attempts, lastErr)
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
