package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChesleyC/pay-ledger/internal/client"
	"github.com/ChesleyC/pay-ledger/internal/payment"
)

func main() {
	servers := flag.String("servers", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma-separated node URLs")
	requests := flag.Int("requests", 1000, "total requests")
	concurrency := flag.Int("concurrency", 32, "concurrent workers")
	sameKey := flag.Bool("same-key", false, "send one idempotency key for every request")
	flag.Parse()
	if *requests < 1 || *concurrency < 1 {
		fmt.Fprintln(os.Stderr, "requests and concurrency must be positive")
		os.Exit(2)
	}

	c := client.New(strings.Split(*servers, ",")...)
	jobs := make(chan int)
	latencies := make([]time.Duration, 0, *requests)
	var mu sync.Mutex
	var succeeded atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				key := fmt.Sprintf("load-%d-%d", start.UnixNano(), i)
				if *sameKey {
					key = fmt.Sprintf("load-shared-%d", start.UnixNano())
				}
				began := time.Now()
				_, err := c.CreatePayment(context.Background(), payment.CreateRequest{IdempotencyKey: key, AccountID: "load-test", AmountCents: 100})
				elapsed := time.Since(began)
				if err == nil {
					succeeded.Add(1)
					mu.Lock()
					latencies = append(latencies, elapsed)
					mu.Unlock()
				}
			}
		}()
	}
	for i := 0; i < *requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) == 0 {
		fmt.Printf("0/%d requests succeeded in %s\n", *requests, elapsed)
		os.Exit(1)
	}
	p50 := latencies[(len(latencies)-1)*50/100]
	p95 := latencies[(len(latencies)-1)*95/100]
	fmt.Printf("succeeded=%d total=%d elapsed=%s rps=%.1f p50=%s p95=%s\n", succeeded.Load(), *requests, elapsed.Round(time.Millisecond), float64(succeeded.Load())/elapsed.Seconds(), p50.Round(time.Microsecond), p95.Round(time.Microsecond))
}
