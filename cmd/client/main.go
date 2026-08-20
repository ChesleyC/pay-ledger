package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ChesleyC/pay-ledger/internal/client"
	"github.com/ChesleyC/pay-ledger/internal/payment"
)

func main() {
	servers := flag.String("servers", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma-separated node URLs")
	key := flag.String("key", "checkout-123", "idempotency key")
	account := flag.String("account", "acct-42", "account ID")
	amount := flag.Int64("amount", 2500, "amount in cents")
	timeout := flag.Duration("timeout", 10*time.Second, "overall request timeout")
	flag.Parse()

	c := client.New(splitNonempty(*servers)...)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	p, err := c.CreatePayment(ctx, payment.CreateRequest{IdempotencyKey: *key, AccountID: *account, AmountCents: *amount})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create payment:", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(p); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func splitNonempty(value string) []string {
	var values []string
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			values = append(values, part)
		}
	}
	return values
}
