#!/bin/sh
set -eu

compose="docker compose"
$compose up --build -d

cleanup() { $compose down; }
trap cleanup EXIT

echo "waiting for a leader"
leader_port=""
for _ in $(seq 1 60); do
  for port in 8081 8082 8083; do
    status=$(curl -fsS "http://localhost:$port/v1/status" 2>/dev/null || true)
    if echo "$status" | grep -q '"is_leader":true'; then leader_port=$port; break; fi
  done
  [ -n "$leader_port" ] && break
  sleep 1
done
[ -n "$leader_port" ] || { echo "no leader elected" >&2; exit 1; }

first=$(curl -fsS -X POST "http://localhost:$leader_port/v1/payments" -H 'content-type: application/json' -d '{"idempotency_key":"before-failover","account_id":"acct-42","amount_cents":2500}')
echo "created before failure: $first"

case "$leader_port" in
  8081) leader_service=node1 ;;
  8082) leader_service=node2 ;;
  8083) leader_service=node3 ;;
esac
$compose stop "$leader_service"

echo "waiting for a replacement leader"
new_port=""
for _ in $(seq 1 60); do
  for port in 8081 8082 8083; do
    [ "$port" = "$leader_port" ] && continue
    status=$(curl -fsS "http://localhost:$port/v1/status" 2>/dev/null || true)
    if echo "$status" | grep -q '"is_leader":true'; then new_port=$port; break; fi
  done
  [ -n "$new_port" ] && break
  sleep 1
done
[ -n "$new_port" ] || { echo "no replacement leader elected" >&2; exit 1; }

second=$(curl -fsS -X POST "http://localhost:$new_port/v1/payments" -H 'content-type: application/json' -d '{"idempotency_key":"after-failover","account_id":"acct-42","amount_cents":3500}')
echo "created after failure: $second"
echo "leader failover test passed"
