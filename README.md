# PayLedger

PayLedger is a small, fault-tolerant payment idempotency service written in Go. A client can safely retry an ambiguous request: a three-node HashiCorp Raft cluster commits the write once, and a Badger transaction atomically stores the payment with its idempotency key.

The project is intentionally educational. It demonstrates one hard backend guarantee without growing into a banking platform or a general-purpose database.

## How it works

```text
                         internal gRPC status API
                +--------------------------------------+
                |                                      |
client --REST--> node 1 <--HashiCorp Raft/TCP--> node 2 <--Raft/TCP--> node 3
                   |                              |                    |
              BadgerDB                       BadgerDB             BadgerDB
                   \________ replicated payment state machine ________/
```

Only the elected leader accepts writes. A follower returns HTTP `307` with `Location`, `X-PayLedger-Leader`, and a JSON body describing the leader. The included client follows that information and also retries timeouts and transient server failures with bounded exponential backoff.

For every committed create command, the Raft FSM runs one Badger transaction:

1. Look up `idempotency/<key>`.
2. If it exists, return the original payment.
3. Otherwise, write both `payment/<id>` and `idempotency/<key>` atomically.

Raft serializes committed commands, while the Badger transaction is a second line of defense against local concurrency. Even if a leader commits a payment and its HTTP response is lost, a retry returns the committed payment ID.

HashiCorp Raft uses its native streaming TCP transport for consensus. A deliberately small Protocol Buffers/gRPC `ClusterService` exposes node and leader status on each node; REST remains the client-facing API.

## Repository tour

```text
api/httpapi/       REST routes and follower responses
api/grpcapi/       protobuf-backed cluster status service
cmd/server/        node process
cmd/client/        retrying example client
cmd/load/          small throughput/latency measurement tool
internal/cluster/  Raft node, FSM, and snapshots
internal/client/   bounded retry and leader discovery
internal/payment/  payment model and validation
internal/storage/  Badger payment, Raft log, and stable stores
proto/             source .proto and generated Go code
tests/             three-node failover test and Docker scenario
```

## Run a three-node cluster

Requirements: Docker with Compose.

```bash
docker compose up --build -d
curl -s http://localhost:8081/v1/status
curl -s http://localhost:8082/v1/status
curl -s http://localhost:8083/v1/status
```

The three REST ports are `8081`–`8083`; gRPC ports are `9091`–`9093`. Raft traffic stays on the Compose network. Named volumes preserve each node's Badger database and snapshots.

Create a payment through the retrying client:

```bash
go run ./cmd/client \
  -servers http://localhost:8081,http://localhost:8082,http://localhost:8083 \
  -key checkout-123 -account acct-42 -amount 2500
```

Run it again with the same key. The returned `id` and `created_at` remain unchanged. Direct REST also works when sent to the leader:

```bash
curl -i -X POST http://localhost:8081/v1/payments \
  -H 'content-type: application/json' \
  -d '{"idempotency_key":"checkout-123","account_id":"acct-42","amount_cents":2500}'
```

A new payment returns `201 Created`; an idempotent replay returns `200 OK`. A follower returns `307 Temporary Redirect`.

To reset the cluster completely, explicitly remove its volumes:

```bash
docker compose down -v
```

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/payments` | Create or replay an idempotent payment |
| `GET` | `/v1/payments/{id}` | Fetch a payment by ID |
| `GET` | `/v1/idempotency/{key}` | Fetch a payment by idempotency key |
| `GET` | `/v1/status` | Show local Raft and leader status |
| `GET` | `/health` | Process liveness |

Reads are served from local replicated state and can briefly lag on a follower. Read from the leader when a current, linearizable view is required. Writes are acknowledged only after Raft commits them on a majority.

The internal gRPC service can be queried with `grpcurl`:

```bash
grpcurl -plaintext -import-path . -proto proto/cluster.proto \
  localhost:9091 payledger.v1.ClusterService/GetStatus
```

## Tests

```bash
go test ./...
go test -race ./...
```

The suite covers:

- repeated and concurrent idempotency requests returning one payment ID;
- atomic Badger behavior under goroutine concurrency;
- payment persistence across a full node restart;
- REST response semantics and follower leader hints;
- client redirect, retry, exponential backoff, and attempt bounds;
- a real three-node Raft election, leader shutdown, re-election, and writes after failover.

The same failover flow can be observed with containers:

```bash
./tests/docker_failover.sh
```

## Measurements

Run a simple local workload:

```bash
go run ./cmd/load -requests 1000 -concurrency 32
```

The command prints successful requests, elapsed time, requests per second, p50 latency, and p95 latency. To stress concurrent duplicate handling instead of unique creates:

```bash
go run ./cmd/load -requests 1000 -concurrency 64 -same-key
```

Numbers depend on the machine and whether Docker is involved, so the repository does not claim a fixed benchmark result. The tool makes the measurement reproducible.

## Scope and tradeoffs

- Membership is a static three-node configuration to keep startup and failure behavior understandable.
- There is no authentication, TLS management, currency conversion, ledger accounting, or payment processor integration.
- The same idempotency key always returns the first record, even if a later request changes account or amount.
- This project uses one Raft group and is not designed for sharding or unbounded scale.
- Health is liveness only. `/v1/status` is the endpoint for leadership and replication details.

These constraints keep the important path—retry, consensus, atomic application, persistence, and failover—small enough to read and discuss end to end.
