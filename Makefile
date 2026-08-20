.PHONY: build test race proto docker-up docker-down load

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative proto/cluster.proto

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

load:
	go run ./cmd/load -requests 1000 -concurrency 32
